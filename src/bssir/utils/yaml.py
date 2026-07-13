from pathlib import Path
from typing import Any

import yaml


def load_yaml(path: Path) -> dict[str, Any]:
    """Open and parse a YAML file from package or root directory.

    Handles locating the YAML file based on provided path and
    directory location.

    Parameters
    ----------
    path : Path or str
        Path to YAML file.
    location : str, default "package"
        "package" or "root" directory location.

    Returns
    -------
    dict
        Parsed YAML contents as a dictionary.
    """
    return parse_yaml(path.read_text(encoding="utf-8"), source=path)


def parse_yaml(
    yaml_text: str,
    *,
    source: str | Path = "<string>",
) -> dict[str, Any]:
    """Parse YAML text into a dictionary."""
    if not yaml_text.strip():
        return {}

    content = yaml.safe_load(yaml_text) or {}

    if not isinstance(content, dict):
        raise TypeError(
            f"Invalid YAML structure in '{source}'. "
            f"Expected a mapping (dict) at the root level."
        )

    return content
