from pathlib import Path

from .config import Config


def initialize_package(config: Config) -> None:
    """Prepare the local filesystem for BSSIR."""
    ensure_directory(config.local_dir)

    for _, directory in config.dirs:
        ensure_directory(directory)

    ensure_gitignore(config.local_dir)


def ensure_directory(path: Path) -> None:
    """Create a directory if it does not already exist."""
    path.mkdir(parents=True, exist_ok=True)


def ensure_gitignore(directory: Path) -> None:
    """Create a `.gitignore` file that ignores all contents."""
    gitignore = directory / ".gitignore"

    if gitignore.exists():
        return

    gitignore.write_text(
        "# Created automatically by BSSIR\n*\n",
        encoding="utf-8",
    )
