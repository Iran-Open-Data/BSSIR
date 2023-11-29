import subprocess
from pathlib import Path
import platform

from .download_utils import download_7zip
from ..metadata_reader import BASE_PACKAGE_DIRECTORY


def extract(
    compressed_file: Path,
    output_directory: Path,
    *,
    seven_zip_directory: Path = BASE_PACKAGE_DIRECTORY,
) -> None:
    """
    Extracts the contents of a compressed file using the 7-Zip tool.

    :param compressed_file_path: The path to the compressed file to be extracted.
    :type compressed_file_path: str
    :param output_directory: The path to the directory where the extracted files will be stored.
    :type output_directory: str
    :return: None, as this function does not return anything.

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
    elif platform.system() == "Linux":
        seven_zip_file_path = seven_zip_directory.joinpath("7-Zip", "7zz")
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
        )
    else:
        print("Your OS is not supported. try buy a new one.")
