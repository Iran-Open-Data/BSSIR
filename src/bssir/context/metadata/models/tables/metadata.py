from collections.abc import Iterator
from typing import Any
from functools import cached_property

from pydantic import model_validator

from dataforgeir.new_models import Column, Table

from ..common import Metadata


class TablesMetadata(Metadata):
    @model_validator(mode="after")
    def validate_column_resolution(self) -> "TablesMetadata":
        invalid = {
            column: table
            for column, table in self.column_resolution.items()
            if table not in self.tables
        }

        if invalid:
            details = ", ".join(
                f"{column!r} -> {table!r}"
                for column, table in invalid.items()
            )
            raise ValueError(
                f"Invalid column resolution: {details}. "
                "Referenced tables must exist."
            )

        return self

    @cached_property
    def column_resolution(self) -> dict[str, str]:
        return self.content.get("column_resolution", {})

    @cached_property
    def tables(self) -> dict[str, Table]:
        return {
            name: self._create_table(name, metadata)
            for name, metadata in self.content.items()
            if not name.isupper() and name != "column_resolution"
        }

    @staticmethod
    def _create_table(
        name: str,
        metadata: dict[str, Any],
    ) -> Table:
        columns = [
            Column(table=name, name=column_name, **column_metadata)
            for column_name, column_metadata in metadata["columns"].items()
        ]
        return Table(name=name, **{**metadata, "columns": columns})

    @cached_property
    def columns(self) -> dict[tuple[str, str], Column]:
        return {
            (table_name, column.name.lower()): column
            for table_name, table in self.tables.items()
            for column in table.columns
        }

    @cached_property
    def columns_by_name(self) -> dict[str, list[Column]]:
        result: dict[str, list[Column]] = {}

        for column in self.columns.values():
            result.setdefault(column.name, []).append(column)

        return result

    def get_column(self, key: tuple[str, str] | str) -> Column:
        """Return a column by table/column key or by column name.

        A bare column name must resolve to exactly one column unless a
        table is specified in ``column_resolution``.
        """
        if isinstance(key, tuple):
            table_name, column_name = key
            return self.columns[(table_name.lower(), column_name.lower())]

        column_name = key.lower()
        matches = self.columns_by_name.get(column_name, [])

        if not matches:
            raise KeyError(f"Column not found: {key!r}")

        if resolution := self.column_resolution.get(column_name):
            matches = [match for match in matches if match.table == resolution]

            if not matches:
                raise KeyError(
                    f"Column {key!r} is resolved to table {resolution!r}, "
                    "but that table does not contain the column."
                )

        if len(matches) > 1:
            tables = ", ".join(column.table for column in matches)
            raise KeyError(
                f"Column name is ambiguous: {key!r}; "
                f"found in tables: {tables}"
            )

        return matches[0]

    def __getitem__(self, key: str) -> Table:
        return self.tables.__getitem__(key)

    def __iter__(self) -> Iterator[str]:
        return super().__iter__()

    def __contains__(self, key: str) -> bool:
        return key in self.tables

    def get(self, key: str, default: None = None) -> Table | None:
        return self.tables.get(key)
