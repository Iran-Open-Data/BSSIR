from pathlib import Path
import tomllib

import requests
import boto3

from .metadata_reader import Defaults, Metadata


def _get_bucket(bucket_name: str):
    with open("tokens.toml", "rb") as file:
        token = tomllib.load(file)["arvan"]
    s3_resource = boto3.resource(
        "s3",
        endpoint_url="https://s3.ir-tbz-sh1.arvanstorage.ir",
        aws_access_key_id=token["access_key"],
        aws_secret_access_key=token["secret_key"],
    )
    bucket = s3_resource.Bucket(bucket_name)  # type: ignore
    return bucket


class Maintainer:
    def __init__(self, lib_defaults: Defaults, lib_metadata: Metadata) -> None:
        self.bucket_name = lib_defaults.bucket_address.split("/")[-1]
        self.bucket = _get_bucket(self.bucket_name)
        self.lib_defaults = lib_defaults
        self.lib_metadata = lib_metadata

    def upload_raw_files(self) -> None:
        for year in self.lib_metadata.raw_files.keys():
            files = self.lib_metadata.raw_files[year]
            for file in files.get("compressed_files", []):
                file_str_path = f"{year}/{file['name']}"
                url = f"{self.lib_defaults.online_dirs.compressed}/{file_str_path}"
                file_path = self.lib_defaults.dirs.compressed.joinpath(file_str_path)
                self._upload_file_to_online_directory(file_path, url)
            for file in files.get("unpacked_files", []):
                file_str_path = f"{year}/{file['name']}"
                url = f"{self.lib_defaults.online_dirs.unpacked}/{file_str_path}"
                file_path = self.lib_defaults.dirs.unpacked.joinpath(file_str_path)
                self._upload_file_to_online_directory(file_path, url)

    def upload_cleaned_files(self) -> None:
        for file_path in self.lib_defaults.dirs.cleaned.iterdir():
            url = f"{self.lib_defaults.online_dirs.cleaned}/{file_path.name}"
            self._upload_file_to_online_directory(file_path, url)

    def ckeck_if_up_to_date(self, file_path: Path, url: str) -> bool:
        response = requests.head(url, timeout=10)
        try:
            online_file_size = int(response.headers["Content-Length"])
        except KeyError:
            online_file_size = 0
        local_file_size = file_path.stat().st_size
        return online_file_size == local_file_size

    def _upload_file_to_online_directory(self, file_path: Path, url: str) -> None:
        if self.ckeck_if_up_to_date(file_path, url):
            return
        url_parts = url.split("/")
        bucket_index = url_parts.index(self.bucket_name)
        key = "/".join(url_parts[bucket_index + 1 :])
        with open(file_path, "rb") as file:
            self.bucket.put_object(ACL="public-read", Body=file, Key=key)
