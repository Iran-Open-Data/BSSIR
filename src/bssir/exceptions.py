from collections.abc import Mapping

from pydantic import ValidationError


class MetadataError(Exception):
    """Base class for metadata-related errors."""


class MetadataResolutionError(MetadataError):
    """Raised when metadata cannot be resolved into a valid model."""


class TableResolutionError(MetadataResolutionError):
    def __init__(
        self,
        table: str,
        year: int,
        resolved: Mapping,
        error: Exception,
    ):
        self.table = table
        self.year = year
        self.resolved = resolved
        self.error = error

        message = [
            f"Failed to resolve table '{table}' for year {year}."
        ]

        if error is not None:
            message.append(f"\nCause:\n{error}")

        if resolved is not None:
            message.append(f"\nResolved metadata:\n{resolved!r}")

        super().__init__("".join(message))


class ColumnResolutionError(MetadataResolutionError):
    def __init__(
        self,
        column: str,
        year: int,
        resolved: Mapping,
        error: Exception,
    ):
        self.column = column
        self.year = year
        self.resolved = resolved
        self.error = error

        message = [
            f"Failed to resolve column '{column}' for year {year}.",
            f"\nCause:\n{error}",
            f"\nResolved metadata:\n{resolved!r}",
        ]

        super().__init__("".join(message))
