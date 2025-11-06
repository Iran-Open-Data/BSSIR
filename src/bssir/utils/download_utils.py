import logging
from pathlib import Path
import platform
from zipfile import ZipFile

import requests
from tqdm.auto import tqdm

from ..metadata_reader import defaults


def download(url: str, path: Path) -> None:
    """Downloads a file from a URL with a progress bar.

    This function downloads a file in chunks while displaying a progress
    bar. It checks if the file already exists and has the same size as the
    remote file, in which case the download is skipped.

    Parameters
    ----------
    url : str
        The URL of the file to download.
    path : Path
        The local path where the downloaded file should be saved.

    Raises
    ------
    requests.exceptions.HTTPError
        If the URL returns an error status code (e.g., 404 Not Found).
    IOError
        If the server does not provide the file size in the headers.
    """
    logging.info(f"Downloading {url} to {path}.")
    part_path = path.with_suffix(path.suffix + ".part")

    try:
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()

        total_size = response.headers.get("content-length")
        if total_size is None:
            total_size = 0
            logging.warning(f"Server did not provide content-length for URL: {url}")
        total_size = int(total_size)

        # Check if the final file already exists and is complete.
        if path.exists() and path.stat().st_size == total_size:
            logging.info(f"File {path.name} already exists. Skipping.")
            # If a partial file is lingering, clean it up.
            if part_path.exists():
                part_path.unlink()
            return

        path.parent.mkdir(parents=True, exist_ok=True)
        chunk_size = 8192

        with open(part_path, "wb") as file, tqdm(
            desc=f"Downloading {path.name}",
            bar_format=defaults.bar_format,
            total=total_size,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            leave=False,
            disable=True,
        ) as progress_bar:
            for chunk in response.iter_content(chunk_size=chunk_size):
                file.write(chunk)
                progress_bar.update(len(chunk))

        path.unlink(missing_ok=True)
        part_path.rename(path)

    except (requests.exceptions.RequestException, IOError) as e:
        logging.error(f"Download failed for {url}. Error: {e}")
        if part_path.exists():
            logging.info(f"Deleting incomplete file: {part_path.name}")
            part_path.unlink()
        raise e


def download_7zip():
    """
    Download the appropriate version of 7-Zip for the current operating system
    and architecture, and extract it to the root directory.

    """
    print(
        f"Downloading 7-Zip for {platform.system()} with "
        f"{platform.architecture()[0]} architecture"
    )
    file_name = f"{platform.system()}-{platform.architecture()[0]}.zip"
    file_path = defaults.root_dir.joinpath(file_name)

    url = f"{defaults.mirrors[0].bucket_address}/7-Zip/{file_name}"
    download(url, file_path)

    with ZipFile(file_path) as zip_file:
        zip_file.extractall(defaults.root_dir)
    file_path.unlink()

    with open(defaults.root_dir.joinpath("7-Zip/.gitignore"), mode="w") as file:
        file.write("# This file created automatically by BSSIR\n*\n")

    if platform.system() == "Linux":
        defaults.root_dir.joinpath("7-Zip", "7zz").chmod(0o771)


def download_map(
    map_name: str, source: str, *, map_metadata: dict, maps_directory: Path
) -> None:
    url = map_metadata[map_name][f"{source}_link"]
    file_path = maps_directory.joinpath("map.zip")
    download(url, file_path)
    path = maps_directory.joinpath(map_name)
    path.mkdir(exist_ok=True, parents=True)
    with ZipFile(file_path) as zip_file:
        zip_file.extractall(path)
    file_path.unlink()
