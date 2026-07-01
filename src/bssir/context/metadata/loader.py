from pathlib import Path

from bssir.context.config import Config
from .models import MetadataSource, MetadataDefinition
from .interpreters import INTERPRETERS


def build_metadata_source(name: str, config: Config) -> MetadataSource:
    return MetadataSource(
        base_package_path=config.base_package_metadata[name],
        package_path=config.package_metadata[name],
        local_path=config.local_metadata[name],
    )

def build_metadata_definition(name: str, config: Config) -> MetadataDefinition:
    return MetadataDefinition(
        source=build_metadata_source(name, config),
        interpreter=INTERPRETERS.get(name),
    )


def extract_comment_block(file_path: Path) -> str | None:
    """Return the leading comment block from a YAML file.

    The comment block must start at the beginning of the file. Consecutive
    comment lines are included, and blank lines within the block are preserved.
    Returns ``None`` if the file does not begin with a comment block.

    Args:
        file_path: Path to the YAML file.

    Returns:
        The extracted comment block without leading ``#`` characters, or
        ``None`` if no leading comment block exists.
    """
    if not file_path.exists():
        return None

    comments: list[str] = []

    with file_path.open(encoding="utf-8") as f:
        for line in f:
            # line = line.rstrip("\n")

            if line.startswith("#"):
                comments.append(line.removeprefix("#").lstrip())

            else:
                break

    if not comments:
        return None

    return "\n".join(comments).strip()


def extract_metadata_description(source: MetadataSource) -> str | None:
    """Extract description from the highest priority metadata layer."""

    for path in (
        source.local_path,
        source.package_path,
        source.base_package_path,
    ):
        if description := extract_comment_block(path):
            return description

    return None
