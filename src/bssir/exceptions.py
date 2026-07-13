from collections.abc import Mapping


class MetadataError(Exception):
    """Base class for metadata-related errors."""


class MetadataResolutionError(MetadataError):
    entity_name: str = "metadata"

    def __init__(
        self,
        name: str,
        year: int,
        resolved: Mapping,
        error: Exception,
    ):
        self.name = name
        self.year = year
        self.resolved = resolved
        self.error = error

        message = [
            f"Failed to resolve {self.entity_name} '{name}' for year {year}."
        ]

        if error is not None:
            message.append(f"\nCause:\n{error}")

        if resolved is not None:
            message.append(f"\nResolved metadata:\n{resolved!r}")

        super().__init__("".join(message))


class TableResolutionError(MetadataResolutionError):
    entity_name = "table"


class ColumnResolutionError(MetadataResolutionError):
    entity_name = "column"


class AttributeResolutionError(MetadataResolutionError):
    entity_name = "attribute"
