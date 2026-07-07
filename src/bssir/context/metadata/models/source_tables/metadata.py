from collections.abc import Iterator
from functools import cached_property

import pandas as pd

from ..common import Metadata, collapse_years
from .table import SourceTableSettings, SourceTable



class SourceTablesMetadata(Metadata):
    @property
    def default_settings(self) -> SourceTableSettings:
        return self.content.get("default_settings", {})

    @property
    def table_list(self) -> list[str]:
        return self.content.get("table_list", [])

    @property
    def group_list(self) -> list[str]:
        return self.content.get("group_list", [])

    @cached_property
    def tables(self) -> dict[str, SourceTable]:
        return {
            name: SourceTable(
                name=name,
                merged=value,
                default_settings=self.default_settings,
                config=self.config,
            )
            for name, value in self.content.items()
            if name not in ["default_settings", "table_list", "group_list"]
        }

    def __getitem__(self, key: str) -> SourceTable:
        return self.tables.__getitem__(key)

    def __iter__(self) -> Iterator[str]:
        return super().__iter__()

    def __contains__(self, key: str) -> bool:
        return key in self.tables

    def get(self, key: str, default: None = None) -> SourceTable | None:
        return self.tables.get(key)

    @property
    def table_list_report(self) -> pd.DataFrame:
        actual = set(self.tables)
        expected = set(self.table_list)

        rows = []

        for table in sorted(actual | expected):
            rows.append({
                "table": table,
                "defined": table in actual,
                "listed": table in expected,
            })

        return pd.DataFrame(rows).set_index("table")

    @property
    def table_availability(self) -> dict[str, list[int]]:
        return {table.name: table.availability for table in self.tables.values()}

    @cached_property
    def table_groups(self) -> dict[str, str | None]:
        return {table.name: table.group for table in self.tables.values()}

    @property
    def has_groups(self) -> bool:
        return any(self.table_groups.values())

    @cached_property
    def table_availability_report(self) -> pd.DataFrame:
        report = pd.DataFrame(
            {name: {year: True for year in years}
            for name, years in self.table_availability.items()}
        )
        report = (
            report
            .sort_index()
            .pipe(collapse_years)
            .transpose()
            .rename_axis("Table")
            .join(pd.Series(range(len(self.table_list)), index=self.table_list, name="Table_Index"))
            .sort_values(["Table_Index"], na_position="last")
        )
        if self.has_groups:
            report = (
                report
                .join(
                    pd.Series(self.table_groups, name="Group")
                    .fillna("no_group")
                    .to_frame()
                    .join(
                        pd.Series(range(len(self.group_list)), index=self.group_list, name="Group_Index"),
                        on="Group",
                    )
                )
                .reset_index()
                .set_index(["Group", "Table"])
                .sort_values(["Group_Index", "Table_Index"], na_position="last")
                .drop(columns="Group_Index")
            )
        report = report.drop(columns="Table_Index")
        return report
