"""
Archive extraction utilities for ZIP and RAR files.

This module provides functions to extract compressed files (ZIP and RAR formats)
to a specified output directory. It uses Python's built-in `zipfile` module for ZIP files,
and either 7-Zip (on Windows) or the `unrar` command (on other platforms) for RAR files.

Functions
---------
extract(compressed_file, output_directory, *, seven_zip_directory=BASE_PACKAGE_DIRECTORY)
    Extracts a compressed file (ZIP or RAR) to the specified output directory.

unzip(compressed_file, output_directory)
    Extracts a ZIP file to the specified output directory.

unrar(compressed_file, output_directory, *, seven_zip_directory=BASE_PACKAGE_DIRECTORY)
    Extracts a RAR file to the specified output directory using 7-Zip (Windows) or unrar (other platforms).

Notes
-----
- For RAR extraction on Windows, 7-Zip must be available in the specified directory.
- For RAR extraction on other platforms, the `unrar` command-line tool must be installed.
"""
import subprocess
from pathlib import Path
import platform
import zipfile

from .download_utils import download_7zip
from ..metadata_reader import BASE_PACKAGE_DIRECTORY


def extract(
    compressed_file: Path,
    output_directory: Path,
    *,
    seven_zip_directory: Path = BASE_PACKAGE_DIRECTORY,
) -> None:
    """
    Extract a compressed file (ZIP or RAR) to the specified output directory.

    Parameters
    ----------
    compressed_file : Path
        Path to the compressed file (.zip or .rar).
    output_directory : Path
        Directory where the contents will be extracted.
    seven_zip_directory : Path, optional
        Directory containing 7-Zip executable (used on Windows for RAR files).
    """
    suffix = compressed_file.suffix.lower()
    if suffix == ".zip":
        unzip(compressed_file=compressed_file, output_directory=output_directory)
    elif suffix == ".rar":
        unrar(
            compressed_file=compressed_file,
            output_directory=output_directory,
            seven_zip_directory=seven_zip_directory,
        )


def unzip(compressed_file: Path, output_directory: Path) -> None:
    """
    Extract a ZIP file to the specified output directory.

    Parameters
    ----------
    compressed_file : Path
        Path to the ZIP file.
    output_directory : Path
        Directory where the contents will be extracted.
    """
    with zipfile.ZipFile(compressed_file) as file:
        file.extractall(output_directory)


def unrar(
    compressed_file: Path,
    output_directory: Path,
    *,
    seven_zip_directory: Path = BASE_PACKAGE_DIRECTORY,
) -> None:
    """
    Extract a RAR file to the specified output directory.

    On Windows, uses 7-Zip. On other platforms, uses the `unrar` command-line tool.

    Parameters
    ----------
    compressed_file : Path
        Path to the RAR file.
    output_directory : Path
        Directory where the contents will be extracted.
    seven_zip_directory : Path, optional
        Directory containing 7-Zip executable (used on Windows).
    """
    if platform.system() == "Windows":
        seven_zip_file_path = seven_zip_directory.joinpath("7-Zip", "7z.exe")
        if not seven_zip_file_path.exists():
            download_7zip()
        subprocess.run(
            [
                seven_zip_file_path,
                "e",
                compressed_file,
                f"-o{output_directory}",
                "-y",
            ],
            check=False,
            shell=True,
        )
    else:
        subprocess.run(
            ["unrar", "e", compressed_file, output_directory, "-inul", "-y"],
            check=False,
        )
