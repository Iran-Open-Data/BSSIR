from pathlib import Path
import logging
import warnings

import requests
import urllib3
from tqdm import tqdm

_CHUNK_SIZE = 8192
_TIMEOUT = 60


def _get_response(url: str) -> requests.Response:
    """Return a streaming response, retrying without SSL verification."""
    try:
        response = requests.get(url, stream=True, timeout=_TIMEOUT)
    except requests.exceptions.SSLError:
        with warnings.catch_warnings():
            warnings.simplefilter(
                "ignore",
                urllib3.exceptions.InsecureRequestWarning,
            )
            response = requests.get(
                url,
                stream=True,
                timeout=_TIMEOUT,
                verify=False,
            )

    response.raise_for_status()
    return response


def _get_content_length(response: requests.Response, url: str) -> int:
    """Return the reported content length or 0 if unavailable."""
    length = response.headers.get("content-length")
    if length is None:
        logging.warning("Server did not provide content-length for %s.", url)
        return 0

    return int(length)


def _download_file(
    response: requests.Response,
    path: Path,
    progress_bar_options: dict,
) -> None:
    """Download a response into a temporary file."""
    part_path = path.with_suffix(path.suffix + ".part")
    path.parent.mkdir(parents=True, exist_ok=True)

    with (
        part_path.open("wb") as file,
        tqdm(
            desc=f"Downloading {path.name}",
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            leave=False,
            **progress_bar_options
        ) as progress,
    ):
        for chunk in response.iter_content(chunk_size=_CHUNK_SIZE):
            if not chunk:
                continue

            file.write(chunk)
            progress.update(len(chunk))

    path.unlink(missing_ok=True)
    part_path.rename(path)


def download(
    url: str,
    path: Path,
    progress_bar_options: dict | None = None,
) -> Path:
    """Download a file from a URL."""
    logging.info("Downloading %s to %s.", url, path)
    if not progress_bar_options:
        progress_bar_options = {}

    part_path = path.with_suffix(path.suffix + ".part")

    try:
        response = _get_response(url)
        total_size = _get_content_length(response, url)

        if path.exists() and path.stat().st_size == total_size:
            logging.info("File %s already exists. Skipping.", path.name)
            part_path.unlink(missing_ok=True)
            return path

        progress_bar_options["total"] = total_size
        _download_file(response, path, progress_bar_options)

    except (requests.RequestException, OSError):
        logging.exception("Download failed for %s.", url)
        part_path.unlink(missing_ok=True)
        raise

    return path


# def download(url: str, path: Path, progress_bar_options: dict | None) -> Path:
#     """Downloads a file from a URL with a progress bar.

#     This function downloads a file in chunks while displaying a progress
#     bar. It checks if the file already exists and has the same size as the
#     remote file, in which case the download is skipped.

#     Parameters
#     ----------
#     url : str
#         The URL of the file to download.
#     path : Path
#         The local path where the downloaded file should be saved.

#     Raises
#     ------
#     requests.exceptions.HTTPError
#         If the URL returns an error status code (e.g., 404 Not Found).
#     IOError
#         If the server does not provide the file size in the headers.
#     """
#     logging.info(f"Downloading {url} to {path}.")
#     part_path = path.with_suffix(path.suffix + ".part")

#     try:
#         try:
#             response = requests.get(url, stream=True, timeout=60)
#         except requests.exceptions.SSLError:
#             with warnings.catch_warnings():
#                 warnings.simplefilter("ignore", urllib3.exceptions.InsecureRequestWarning)
#                 response = requests.get(
#                     url,
#                     stream=True,
#                     timeout=60,
#                     verify=False,
#                 )

#         response.raise_for_status()

#         total_size = response.headers.get("content-length")
#         if total_size is None:
#             total_size = 0
#             logging.warning(f"Server did not provide content-length for URL: {url}")
#         total_size = int(total_size)

#         # Check if the final file already exists and is complete.
#         if path.exists() and path.stat().st_size == total_size:
#             logging.info(f"File {path.name} already exists. Skipping.")
#             # If a partial file is lingering, clean it up.
#             if part_path.exists():
#                 part_path.unlink()
#             return path

#         path.parent.mkdir(parents=True, exist_ok=True)
#         chunk_size = 8192

#         with open(part_path, "wb") as file, tqdm(
#             desc=f"Downloading {path.name}",
#             # bar_format=defaults.bar_format,
#             total=total_size,
#             unit="B",
#             unit_scale=True,
#             unit_divisor=1024,
#             leave=False,
#             disable=True,
#         ) as progress_bar:
#             for chunk in response.iter_content(chunk_size=chunk_size):
#                 file.write(chunk)
#                 progress_bar.update(len(chunk))

#         path.unlink(missing_ok=True)
#         part_path.rename(path)

#     except (requests.exceptions.RequestException, IOError) as e:
#         logging.error(f"Download failed for {url}. Error: {e}")
#         if part_path.exists():
#             logging.info(f"Deleting incomplete file: {part_path.name}")
#             part_path.unlink()
#         raise e

#     return path
