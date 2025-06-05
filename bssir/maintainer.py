from pathlib import Path
from typing import Optional
import tomllib

import requests
import boto3

from .metadata_reader import Defaults, Metadata, Mirror


def _get_bucket(mirror: Mirror, bucket_name: str):
    with open("tokens.toml", "rb") as file:
        token = tomllib.load(file)[mirror.name]
    s3_resource = boto3.resource(
        "s3",
        endpoint_url=mirror.endpoint,
        aws_access_key_id=token["access_key"],
        aws_secret_access_key=token["secret_key"],
    )
    bucket = s3_resource.Bucket(bucket_name)  # type: ignore
    return bucket


class Maintainer:
    def __init__(
        self,
        lib_defaults: Defaults,
        lib_metadata: Metadata,
        mirror_name: Optional[str] = None,
    ) -> None:
        index = 0 if mirror_name is None else lib_defaults.get_mirror_index(mirror_name)
        self.bucket_name = lib_defaults.mirrors[index].bucket_name
        mirror = lib_defaults.mirrors[index]
        self.bucket = _get_bucket(mirror=mirror, bucket_name=self.bucket_name)
        self.online_dir = lib_defaults.online_dirs[index]
        self.lib_defaults = lib_defaults
        self.lib_metadata = lib_metadata

    def upload_raw_files(
            self,
            years: Optional[list[int]] = None,
        ) -> None:
        for year in self.lib_metadata.raw_files.keys():
            if years:
                if year not in years:
                    continue
            files = self.lib_metadata.raw_files[year]
            for file in files.get("compressed_files", []):
                file_str_path = f"{year}/{file['name']}"
                url = f"{self.online_dir.compressed}/{file_str_path}"
                file_path = self.lib_defaults.dir.compressed.joinpath(file_str_path)
                self._upload_file_to_online_directory(file_path, url)
            for file in files.get("unpacked_files", []):
                file_str_path = f"{year}/{file['name']}"
                url = f"{self.online_dir.unpacked}/{file_str_path}"
                file_path = self.lib_defaults.dir.unpacked.joinpath(file_str_path)
                self._upload_file_to_online_directory(file_path, url)

    def upload_cleaned_files(
            self,
            years: Optional[list[int]] = None,
            table_names: Optional[list[str]] = None,
        ) -> None:
        for file_path in self.lib_defaults.dir.cleaned.iterdir():
            year_str, table_name = file_path.stem.split("_", 1)
            year = int(year_str)
            if years:
                if year not in years:
                    continue
            if table_names:
                if table_name not in table_names:
                    continue
            url = f"{self.online_dir.cleaned}/{file_path.name}"
            self._upload_file_to_online_directory(file_path, url)

    def is_up_to_date(self, file_path: Path, url: str) -> bool:
        response = requests.head(url, timeout=10)
        try:
            online_file_size = int(response.headers["Content-Length"])
        except KeyError:
            online_file_size = 0
        local_file_size = file_path.stat().st_size
        return online_file_size == local_file_size

    def _upload_file_to_online_directory(self, file_path: Path, url: str) -> None:
        if self.is_up_to_date(file_path, url):
            return
        print(f"Upload file to {url}")
        url_parts = url.split("/")
        bucket_index = url_parts.index(self.bucket_name)
        key = "/".join(url_parts[bucket_index + 1 :])
        with open(file_path, "rb") as file:
            self.bucket.put_object(ACL="public-read", Body=file, Key=key)
