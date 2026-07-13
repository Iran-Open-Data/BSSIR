"""
Utilities for discovering and classifying files.

This module provides a small abstraction over filename extensions so the rest
of the codebase can work with logical file types rather than raw suffixes.
"""

from collections import defaultdict
from typing import Literal
from pathlib import Path


FileType = Literal[
    "archive",
    "access",
    "dbf",
    "stata",
    "csv",
]


FILE_TYPE_EXTENSIONS: dict[FileType, frozenset[str]] = {
    "archive": frozenset({".zip", ".rar"}),
    "access": frozenset({".mdb", ".accdb"}),
    "dbf": frozenset({".dbf"}),
    "stata": frozenset({".dta"}),
    "csv": frozenset({".csv"}),
}


def get_file_type(path: Path) -> FileType | None:
    """Return the logical type of a file.

    Parameters
    ----------
    path
        File whose type should be determined.

    Returns
    -------
    FileType | None
        The detected file type, or ``None`` if the extension is not
        recognized.
    """
    suffix = path.suffix.lower()

    for file_type, extensions in FILE_TYPE_EXTENSIONS.items():
        if suffix in extensions:
            return file_type

    return None


def is_file_type(path: Path, file_type: FileType) -> bool:
    """Return whether a file belongs to a given logical file type."""
    return get_file_type(path) == file_type


def find_files(
    directory: Path,
    file_type: FileType,
) -> list[Path]:
    """Find files of a given type in a directory.

    The search is non-recursive.

    Parameters
    ----------
    directory
        Directory to search.
    file_type
        Type of files to return.

    Returns
    -------
    list[Path]
        Matching files. Returns an empty list if the directory does not
        exist.
    """
    if not directory.is_dir():
        return []

    return [
        path
        for path in directory.iterdir()
        if path.is_file() and is_file_type(path, file_type)
    ]


def has_file_type(
    directory: Path,
    file_type: FileType,
) -> bool:
    """Return whether a directory contains a file of the given type.

    The search is non-recursive.
    """
    if not directory.is_dir():
        return False

    return any(
        path.is_file() and is_file_type(path, file_type)
        for path in directory.iterdir()
    )


def group_files(
    directory: Path,
) -> dict[FileType, list[Path]]:
    """Group files in a directory by logical file type.

    Unknown file types are ignored. The search is non-recursive.

    Parameters
    ----------
    directory
        Directory to scan.

    Returns
    -------
    dict[FileType, list[Path]]
        Mapping from file type to matching files.
    """
    grouped: defaultdict[FileType, list[Path]] = defaultdict(list)

    if not directory.is_dir():
        return {}

    for path in directory.iterdir():
        if not path.is_file():
            continue

        file_type = get_file_type(path)
        if file_type is not None:
            grouped[file_type].append(path)

    return dict(grouped)
