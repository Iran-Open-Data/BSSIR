from __future__ import annotations

from typing import Any, Literal

import pandas as pd

from bssir.context import Context
from bssir.core.pipeline import load_pipeline_table

Column = str | tuple[str, str]
Granularity = Literal["household", "individual"]

PRIMARY_KEY = {
    "household": ["year", "household_id"],
    "individual": ["year", "household_id", "member_number"],
}


class Query:
    """Build analytical tables from requested columns."""

    def __init__(self, context: Context) -> None:
        self.context = context

    def gather_table(
        self,
        columns: list[Column],
        year: int,
        *,
        granularity: Granularity = "household",
    ) -> pd.DataFrame:
        """Gather requested columns into a single record-level table.

        Column names may be given as bare names or as explicit
        ``(table, column)`` pairs. When a bare column name is provided,
        the underlying table is resolved from the dataset metadata.

        Parameters
        ----------
        columns:
            Columns to include in the resulting table.
        unit_of_observation:
            The record level of the resulting table.

        Returns
        -------
        pandas.DataFrame
            A record-level table containing the requested columns.
        """
        if not columns:
            raise ValueError("At least one column must be requested.")

        load_params: dict[str, dict[str, Any]] = {}

        for column_key in columns:
            column = self.context.metadata.tables.get_column(column_key)
            params = load_params.setdefault(
                column.table,
                {
                    "table_name": column.table,
                    "columns": self._get_primary_key(column.table),
                },
            )
            params["columns"].append(column.name)

        tables = {
            table: load_pipeline_table(year=year, context=self.context, **params)
            for table, params in load_params.items()
        }

        aligned_tables: dict[str, pd.DataFrame] = {}

        for table_name, table in tables.items():
            table_metadata = self.context.metadata.tables.tables[table_name]

            if table_metadata.granularity == granularity:
                aligned_tables[table_name] = table.set_index(table_metadata.primary_key)

            elif (
                table_metadata.granularity == "individual"
                and granularity == "household"
            ):
                aligned_tables[table_name] = self._aggregate_to_target(
                    table, table_metadata.granularity,
                )

            elif (
                granularity == "household"
                and granularity == "individual"
            ):
                aligned_tables[table_name] = table.set_index(table_metadata.primary_key)

            else:
                raise ValueError(
                    f"Cannot align table {table_name!r} with "
                    f"unit of observation {granularity!r}."
                )

        return pd.concat(aligned_tables.values(), axis="columns")

    def _get_primary_key(self, table_name: str) -> list[str]:
        return self.context.metadata.tables[table_name].primary_key.copy()

    def _aggregate_to_target(
        self,
        table: pd.DataFrame,
        target_granularity: str,
        *,
        aggregation_method: str = "sum",
    ) -> pd.DataFrame:
        return (
            table
            .groupby(PRIMARY_KEY[target_granularity])
            .aggregate(aggregation_method)
        )

    def tabulate(self) -> pd.DataFrame:
        """Create a tabulated/aggregated table from a query."""
        raise NotImplementedError

