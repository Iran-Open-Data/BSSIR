from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from functools import cached_property
from typing import Generic, TypeVar, Annotated, Any, Literal, TYPE_CHECKING
import urllib.parse

import requests
from pydantic import BaseModel, Discriminator, PrivateAttr, Tag, model_validator

from bssir.utils.download import download
from .credential import Credential, S3Credential
from .directory import DirectoriesNames, RemoteDirectories

if TYPE_CHECKING:
    from mypy_boto3_s3.service_resource import Bucket


TCredential = TypeVar("TCredential", bound=Credential)


class BaseMirror(ABC, BaseModel, Generic[TCredential]):
    name: str
    type: str
    directory_names: DirectoriesNames

    _credentials: TCredential | None = PrivateAttr(default=None)

    @abstractmethod
    def exists(self, source: str) -> bool: ...

    @abstractmethod
    def download(self, source: str, destination: Path) -> Path:
        """Download a resource."""

    @abstractmethod
    def upload(self, source: Path, destination: str) -> str:
        """Upload a resource."""

    @cached_property
    def dirs(self) -> RemoteDirectories:
        return self._create_remote_dirs()
    
    @abstractmethod
    def _create_remote_dirs(self) -> RemoteDirectories: ...

    def get_content_length(self, key: str) -> int:
        """Return remote object size in bytes."""
        raise NotImplementedError


class BaseS3Mirror(BaseMirror[S3Credential]):
    type: Literal["s3"] = "s3"
    bucket_name: str
    endpoint: str | None = None
    region_name: str | None = None
    url_format: str | None = None

    @model_validator(mode="after")
    def validate_address_configuration(self) -> "BaseS3Mirror":
        if self.endpoint is None:
            missing = []

            if self.region_name is None:
                missing.append("region_name")

            if self.url_format is None:
                missing.append("url_format")

            if missing:
                raise ValueError(
                    "Mirror configuration is invalid. "
                    f"Missing {', '.join(missing)} when endpoint is not provided."
                )

        return self

    @property
    def address(self) -> str:
        if self.endpoint:
            return urllib.parse.urljoin(f"{self.endpoint}/", self.bucket_name)

        assert self.url_format is not None
        return self.url_format.format(**self.model_dump())

    def _create_remote_dirs(self) -> RemoteDirectories:
        return RemoteDirectories(
            **{
                k: f"{self.address}/{v}"
                for k, v in self.directory_names.model_dump().items()
            }
        )

    @cached_property
    def bucket(self) -> Bucket:
        import boto3

        if self._credentials is None:
            raise

        s3_resource = boto3.resource(
            "s3",
            region_name=self.region_name,
            endpoint_url=self.endpoint,
            aws_access_key_id=self._credentials.access_key,
            aws_secret_access_key=self._credentials.secret_key,
        )
        bucket = s3_resource.Bucket(self.bucket_name)
        bucket.meta.client.head_bucket(Bucket=self.bucket_name)
        return bucket


class PublicS3Mirror(BaseS3Mirror):
    access: Literal["public"] = "public"

    def exists(self, source: str) -> bool:
        """Check if a file exists on the remote server."""
        url = urllib.parse.urljoin(f"{self.address}/", source)
        try:
            response = requests.head(url)
            return response.status_code == 200
        except requests.RequestException:
            return False

    def get_content_length(self, source: str) -> int:
        """Return the size of a public resource in bytes."""

        url = urllib.parse.urljoin(f"{self.address}/", source)

        response = requests.head(url)
        response.raise_for_status()

        return int(response.headers.get("Content-Length", 0))

    def download(self, source: str, destination: Path) -> Path:
        """Download a resource from the public storage."""

        destination.parent.mkdir(parents=True, exist_ok=True)

        url = urllib.parse.urljoin(f"{self.address}/", source)

        download(url, destination)

        return destination

    def upload(self, source: Path, destination: str) -> str:
        """Upload a resource to the public storage.

        Parameters
        ----------
        source : Path
            Local file to upload.
        destination : str
            Destination key inside the bucket.

        Returns
        -------
        str
            The uploaded object key.
        """

        self.bucket.upload_file(
            Filename=str(source),
            Key=destination,
            ExtraArgs={"ACL": "public-read"},
        )

        return destination


class PrivateS3Mirror(BaseS3Mirror):
    access: Literal["private"] = "private"

    def exists(self, source: str) -> bool:
        """Check if an object exists in the bucket."""
        from botocore.exceptions import ClientError

        try:
            self.bucket.meta.client.head_object(
                Bucket=self.bucket.name,
                Key=source,
            )
            return True
        except ClientError as exc:
            error_code = exc.response["Error"]["Code"] # type: ignore
            if error_code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise

    def get_content_length(self, key: str) -> int:
        """Return the size of an S3 object in bytes."""

        return self.bucket.Object(key).content_length

    def download(self, source: str, destination: Path) -> Path:
        """Download a resource from the private storage."""

        destination.parent.mkdir(parents=True, exist_ok=True)

        self.bucket.download_file(
            Key=source,
            Filename=str(destination),
        )

        return destination

    def upload(self, source: Path, destination: str) -> str:
        """Upload a resource to the private storage.

        Parameters
        ----------
        source : Path
            Local file to upload.
        destination : str
            Destination key inside the bucket.

        Returns
        -------
        str
            The uploaded object key.
        """

        self.bucket.upload_file(
            Filename=str(source),
            Key=destination,
        )

        return destination


class LocalMirror(BaseMirror):
    type: Literal["local"]

    path: str


def determine_mirror_type(v: Any) -> str:
    if isinstance(v, dict):
        mirror_type = v.get("type", "s3")
        if mirror_type == "s3":
            access_type = v.get("access", "public")
            return f"s3_{access_type}"
        return str(mirror_type)
    
    mirror_type = getattr(v, "type", None)
    if mirror_type == "s3":
        return f"s3_{getattr(v, 'access', 'public')}"
    return str(mirror_type)


Mirror = Annotated[
    Annotated[PublicS3Mirror, Tag("s3_public")] |
    Annotated[PrivateS3Mirror, Tag("s3_private")] |
    Annotated[LocalMirror, Tag("local")],
    Discriminator(determine_mirror_type)
]
