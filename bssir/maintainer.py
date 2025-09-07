import logging
from pathlib import Path
from typing import Optional, Iterable

import requests
from botocore.exceptions import ClientError, NoCredentialsError

from .metadata_reader import Defaults, Metadata
from .utils.s3 import get_bucket


class Maintainer:
    """Manages uploading and syncing data files to an S3-compatible mirror.

    Parameters
    ----------
    lib_defaults : Defaults
        The default configuration object.
    lib_metadata : Metadata
        The metadata object.
    mirror_name : str, optional
        The name of the mirror to use. If None, the default mirror is used.

    Attributes
    ----------
    lib_defaults : Defaults
        An instance of the Defaults class containing configuration.
    lib_metadata : Metadata
        An instance of the Metadata class.
    bucket : boto3.resources.factory.s3.Bucket
        The Boto3 Bucket object for the configured mirror.
    online_dir : DefaultOnlineDirectory
        An object containing the online directory URLs for the mirror.

    Raises
    ------
    FileNotFoundError
        If 'tokens.toml' cannot be found in the root directory.
    KeyError
        If a token for the specified mirror is not found in 'tokens.toml'.
    NoCredentialsError
        If Boto3 cannot find the credentials in the token file.
    ClientError
        If Boto3 fails to connect to the S3 bucket.

    """
    def __init__(
        self,
        lib_defaults: Defaults,
        lib_metadata: Metadata,
        mirror_name: Optional[str] = None,
    ) -> None:
        self.lib_defaults = lib_defaults
        self.lib_metadata = lib_metadata

        index = lib_defaults.get_mirror_index(mirror_name)
        self.online_dir = lib_defaults.online_dirs[index]
        mirror = lib_defaults.mirrors[index]
        self.bucket = get_bucket(mirror)

    def upload_raw_files(self, years: Optional[list[int]] = None) -> None:
        """Uploads raw data files for specified years.

        Parameters
        ----------
        years : list[int], optional
            A list of years to upload raw files for. If None, files for all
            available years will be uploaded.

        """
        logging.info("Starting upload of raw files...")
        file_paths = self._get_raw_file_paths(years)
        self._upload_files(
            files_to_upload=file_paths,
            local_base_path=self.lib_defaults.dir.original,
            online_base_url=self.online_dir.original,
        )
        logging.info("Finished uploading raw files.")

    def _get_raw_file_paths(self, years: Optional[list[int]]) -> Iterable[Path]:
        """Creates a generator for paths to raw files.

        Parameters
        ----------
        years : list[int], optional
            A list of years to include. If None, all years in the metadata
            are used.

        Yields
        ------
        Path
            The path to a raw file that matches the filter criteria.

        """
        years_to_process = years or self.lib_metadata.raw_files.keys()
        for year in years_to_process:
            if year not in self.lib_metadata.raw_files:
                continue
            files_info = self.lib_metadata.raw_files[year].get("files", [])
            for file_info in files_info:
                yield self.lib_defaults.dir.original.joinpath(str(year), file_info["name"])

    def upload_cleaned_files(
        self,
        years: Optional[list[int]] = None,
        table_names: Optional[list[str]] = None,
    ) -> None:
        """Uploads cleaned data files.

        This function can be filtered by year and table name.

        Parameters
        ----------
        years : list[int], optional
            A list of years to upload. If None, files for all years are
            considered.
        table_names : list[str], optional
            A list of table names to upload. If None, all tables are
            considered.

        """
        logging.info("Starting upload of cleaned files...")
        file_paths = self._get_cleaned_file_paths(years, table_names)
        self._upload_files(
            files_to_upload=file_paths,
            local_base_path=self.lib_defaults.dir.cleaned,
            online_base_url=self.online_dir.cleaned,
        )
        logging.info("Finished uploading cleaned files.")

    def _get_cleaned_file_paths(
        self, years: Optional[list[int]], table_names: Optional[list[str]]
    ) -> Iterable[Path]:
        """Creates a generator for paths to cleaned files.

        Parameters
        ----------
        years : list[int], optional
            A list of years to include. If None, all years are considered.
        table_names : list[str], optional
            A list of table names to include. If None, all tables are
            considered.

        Yields
        ------
        Path
            The path to a cleaned file that matches the filter criteria.

        """
        for file_path in self.lib_defaults.dir.cleaned.iterdir():
            year_str, table_name = file_path.stem.split("_", 1)
            
            if years and int(year_str) not in years:
                continue
            if table_names and table_name not in table_names:
                continue
            yield file_path

    def upload_external_files(self) -> None:
        """Uploads all external files."""
        logging.info("Starting upload of external files...")
        file_paths = self.lib_defaults.dir.external.iterdir()
        self._upload_files(
            files_to_upload=file_paths,
            local_base_path=self.lib_defaults.dir.external,
            online_base_url=self.online_dir.external,
        )
        logging.info("Finished uploading external files.")

    def _upload_files(
        self,
        files_to_upload: Iterable[Path],
        local_base_path: Path,
        online_base_url: str,
    ) -> None:
        """Generic helper to upload a collection of files.

        Parameters
        ----------
        files_to_upload : Iterable[Path]
            An iterable of Path objects for the files to upload.
        local_base_path : Path
            The local base directory, used to determine the relative path for
            the S3 key.
        online_base_url : str
            The base URL for constructing the public file URL.

        """
        for file_path in files_to_upload:
            if not file_path.is_file():
                continue

            url = f"{online_base_url}/{file_path.relative_to(local_base_path).as_posix()}"

            if self._is_up_to_date(file_path, url):
                logging.debug(f"File is up-to-date, skipping: {file_path.name}")
                continue

            self._upload_file(file_path, url)

    def _is_up_to_date(self, file_path: Path, url: str) -> bool:
        """Checks if a local file is the same size as the online version.

        Parameters
        ----------
        file_path : Path
            Path to the local file.
        url : str
            The public URL of the file to check against.

        Returns
        -------
        bool
            True if the local and online file sizes match, False otherwise.
            Also returns False if the online check fails for any reason.

        """
        try:
            response = requests.head(url, timeout=10)
            response.raise_for_status()  # Raise exception for 4xx or 5xx status
            online_file_size = int(response.headers.get("Content-Length", 0))
        except requests.exceptions.RequestException as e:
            # If the file doesn't exist online (404) or another error occurs,
            # assume it's not up-to-date.
            logging.info(
                f"Could not check online status for {url}. Proceeding with upload."
            )
            return False
        
        local_file_size = file_path.stat().st_size
        return online_file_size == local_file_size

    def _upload_file(self, file_path: Path, url: str) -> None:
        """Uploads a single file to the S3 bucket.

        Parameters
        ----------
        file_path : Path
            The local path to the file.
        key : str
            The destination key (path) within the S3 bucket.

        """
        s3_key = url.split("/", 4)[-1]
        logging.info(f"Uploading {file_path.name} to {s3_key}")
        extra_args = {}
        if not self.lib_defaults.private_data:
            extra_args["ACL"] = "public-read"
        try:
            self.bucket.upload_file(
                Filename=str(file_path),
                Key=s3_key,
                ExtraArgs=extra_args,
            )
        except ClientError as e:
            logging.error(f"Failed to upload {file_path.name}: {e}")
        except Exception as e:
            logging.error(
                f"An unexpected error occurred during upload of {file_path.name}: {e}"
            )
