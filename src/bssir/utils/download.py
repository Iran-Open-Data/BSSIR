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
