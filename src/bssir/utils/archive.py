"""
Utilities for extracting ZIP and RAR archives.

ZIP archives are extracted using Python's standard library. RAR archives are
extracted using `rarfile`, which requires a supported external backend such as
`unrar` or `7z`.
"""

from collections.abc import Mapping
from os import PathLike
from pathlib import Path
import zipfile


RARFILE_TOOLS = {
    "unrar": "UNRAR_TOOL",
    "sevenzip": "SEVENZIP_TOOL",
}


def extract(
    source: Path,
    destination: Path,
    *,
    tools: Mapping[str, PathLike] | None = None,
) -> None:
    """
    Extract an archive to a destination directory.

    Supported archive formats are ZIP and RAR.

    Parameters
    ----------
    source : Path
        Path to the archive file.
    destination : Path
        Directory where the archive contents will be extracted.
    tools : Mapping[str, PathLike], optional
        Mapping of external tool names to executable paths or commands. Used
        to configure RAR extraction backends (e.g. ``"unrar"`` or
        ``"sevenzip"``).
    """
    suffix = source.suffix.lower()

    match suffix:
        case ".zip":
            _extract_zip(source, destination)
        case ".rar":
            _extract_rar(source, destination, tools=tools)
        case _:
            raise ValueError(f"Unsupported archive type: {suffix}")


def _extract_zip(source: Path, destination: Path) -> None:
    """
    Extract a ZIP archive.

    Parameters
    ----------
    source : Path
        Path to the ZIP archive.
    destination : Path
        Directory where the archive contents will be extracted.
    """
    with zipfile.ZipFile(source) as file:
        file.extractall(destination)


def _extract_rar(
    source: Path,
    destination: Path,
    *,
    tools: Mapping[str, PathLike] | None = None,
) -> None:
    """
    Extract a RAR archive.

    Extraction is performed using the :mod:`rarfile` package, which requires
    a supported external backend such as ``unrar`` or ``7z``.

    Parameters
    ----------
    source : Path
        Path to the RAR archive.
    destination : Path
        Directory where the archive contents will be extracted.
    tools : Mapping[str, PathLike], optional
        Mapping of external tool names to executable paths or commands. The
        mapping is used to configure :mod:`rarfile` before extraction.
    """
    import rarfile

    tools = {} if not tools else tools

    for key, attr in RARFILE_TOOLS.items():
        if value := tools.get(key):
            setattr(rarfile, attr, str(value))

    rarfile.tool_setup()

    with rarfile.RarFile(source) as archive:
        archive.extractall(destination)