from collections.abc import Iterator

import pandas as pd

from .api import API, Years
from .core import data_cleaner
from .calculator import Calculator


class DataStats:
    """Compute descriptive statistics and quality metrics for HBSIR datasets.

    This class provides utilities for exploring raw tables, inspecting data
    quality, and generating summary information that can be used in dataset
    documentation and metadata.

    Parameters
    ----------
    api : API
        HBSIR API instance used to access datasets, metadata, and package
        configuration.

    Notes
    -----
    Methods in this class operate directly on raw source tables and return
    results as pandas DataFrames suitable for inspection, reporting, and
    metadata generation.
    """

    def __init__(self, api: API) -> None:
        """Initialize a statistics helper.

        Parameters
        ----------
        api : API
            HBSIR API instance.
        """
        self.api = api
        self._calculator = Calculator(api)

    def _iter_raw_tables(
        self,
        table_name: str,
        years: Years,
    ) -> Iterator[tuple[int, pd.DataFrame]]:
        """Yield raw tables for the requested years."""
        for year in self.api.utils.parse_years_for_table(
            years,
            table_name=table_name,
        ):
            yield year, data_cleaner.load_raw_table(
                table_name,
                year,
                context=self.api.context,
            )

    def raw_table_summary(
        self,
        table_name: str,
        years: Years,
    ) -> pd.DataFrame:
        """Summarize raw table dimensions and storage requirements.

        Parameters
        ----------
        table_name : str
            Name of the raw table.

        years : Years
            Year or years to summarize.

        Returns
        -------
        pandas.DataFrame
            DataFrame indexed by year with the following columns:

            - rows
            - columns
            - missing_cells
            - memory_bytes
        """
        records = []

        for year, table in self._iter_raw_tables(table_name, years):
            records.append(
                {
                    "year": year,
                    "rows": table.shape[0],
                    "columns": table.shape[1],
                    "missing_cells": int(table.isna().values.sum()),
                    "memory_bytes": int(
                        table.memory_usage(deep=True).sum()
                    ),
                }
            )

        return (
            pd.DataFrame(records)
            .set_index("year")
            .sort_index()
        )

    def raw_column_quality(
        self,
        table_name: str,
        years: Years,
    ) -> pd.DataFrame:
        """Report column-level data quality metrics for a raw table.

        Parameters
        ----------
        table_name : str
            Name of the raw table.

        years : Years
            Year or years to analyze.

        Returns
        -------
        pandas.DataFrame
            DataFrame indexed by year and column with the following columns:

            - dtype: Data type of the column.
            - missing: Number of missing values.
            - missing_ratio: Percentage of missing values.

        Notes
        -----
        Empty strings and strings containing only whitespace are treated as
        missing values when calculating missing counts and percentages.
        """

        frames = []

        for year, table in self._iter_raw_tables(table_name, years):

            missing = (
                table.replace(
                    r"^\s*$",
                    pd.NA,
                    regex=True,
                )
                .isna()
            )

            report = pd.DataFrame(
                {
                    "dtype": table.dtypes.astype(str),
                    "missing": missing.sum(),
                    "missing_ratio": missing.mean() * 100,
                }
            )

            report["year"] = year

            frames.append(
                report.reset_index(names="column")
            )

        return (
            pd.concat(frames, ignore_index=True)
            .set_index(["year", "column"])
            .sort_index()
        )

    def raw_numeric_summary(
        self,
        table_name: str,
        years: Years,
        percentiles: list[float] | None = None,
    ) -> pd.DataFrame:
        """Compute descriptive statistics for numeric columns in a raw table.

        Parameters
        ----------
        table_name : str
            Name of the raw table.

        years : Years
            Year or years to analyze.

        percentiles : list[float], optional
            Additional percentiles to include in the summary statistics.

        Returns
        -------
        pandas.DataFrame
            DataFrame indexed by year, column, and statistic containing
            descriptive statistics for numeric columns.

        Notes
        -----
        All columns are converted to numeric values using
        ``pandas.to_numeric(errors="coerce")``. Values that cannot be
        converted are treated as missing.
        """
        frames = []

        for year, table in self._iter_raw_tables(
            table_name,
            years,
        ):
            numeric = table.apply(
                pd.to_numeric,
                errors="coerce",
            )

            desc = numeric.describe(
                percentiles=percentiles
            )

            desc.loc["count"] = (
                numeric.count()
                .astype(int)
            )

            frames.append(desc)

        return pd.concat(
            frames,
            keys=[
                year
                for year, _ in self._iter_raw_tables(
                    table_name,
                    years,
                )
            ],
            names=["year", "stat"],
        )

    def raw_value_counts(
        self,
        table_name: str,
        column: str,
        years: Years,
        top: int = 10,
    ) -> pd.DataFrame:
        """Summarize the most frequent values in a raw table column.

        Parameters
        ----------
        table_name : str
            Name of the raw table.

        column : str
            Column to summarize.

        years : Years
            Year or years to analyze.

        top : int, default 10
            Number of most frequent values to return.

        Returns
        -------
        pandas.DataFrame
            DataFrame indexed by year and value with a single column:

            - count: Number of occurrences of each value.

        Notes
        -----
        Missing values, including empty strings and whitespace-only strings,
        are included in the frequency counts.
        """

        frames = []

        for year, table in self._iter_raw_tables(table_name, years):

            if column not in table.columns:
                raise KeyError(
                    f"Column '{column}' not found in table "
                    f"'{table_name}' for year {year}"
                )

            values = table[column].replace(
                r"^\s*$",
                pd.NA,
                regex=True,
            )

            counts = (
                values
                .value_counts(dropna=False)
                .head(top)
                .rename("count")
                .to_frame()
            )

            counts["year"] = year

            frames.append(
                counts.reset_index(names="value")
            )

        return (
            pd.concat(frames, ignore_index=True)
            .set_index(["year", "value"])
            .sort_index()
        )

    def weighted_mean(
        self,
        table_name: str,
        column: str,
        groupby: list[str] | str | None = None,
        years: Years = "all",
    ) -> pd.DataFrame:
        years_list = self.api.utils.parse_years_for_table(years, table_name=table_name)
        table = self.api.load_table(table_name, years_list, form="normalized")
        table = self.api.add_weight(table)
        return self._calculator.average_table(
            table=table, columns=[column], groupby=groupby
        )

    def weighted_total(
        self,
        table_name: str,
        column: str,
        groupby: list[str] | str | None = None,
        years: Years = "all",
    ) -> pd.DataFrame:
        years_list = self.api.utils.parse_years_for_table(years, table_name=table_name)
        table = self.api.load_table(table_name, years_list, form="normalized")
        table = self.api.add_weight(table)
        weight_col = self.api.context.config.standard_columns.weight

        if groupby is None:
            groupby = [
                col for col in table.columns if col in self.api.context.config.standard_columns.groupby
            ]
        elif isinstance(groupby, str):
            groupby = [groupby]

        weighted = table[column] * table[weight_col]
        result = table.groupby(groupby, observed=True).agg(
            Unweighted_Total=(column, "sum"),
            Weighted_Total=(column, lambda x: weighted.loc[x.index].sum()),
            Population=(weight_col, "sum"),
        )
        return result

    def share(
        self,
        table_name: str,
        column: str,
        groupby: list[str] | str,
        years: Years = "all",
    ) -> pd.DataFrame:
        years_list = self.api.utils.parse_years_for_table(years, table_name=table_name)
        table = self.api.load_table(table_name, years_list, form="normalized")
        table = self.api.add_weight(table)
        weight_col = self.api.context.config.standard_columns.weight

        if isinstance(groupby, str):
            groupby = [groupby]

        weighted = table[column] * table[weight_col]
        grand_total = weighted.sum()
        result = table.groupby(groupby, observed=True).agg(
            Weighted_Total=(column, lambda x: weighted.loc[x.index].sum()),
        )
        result["Share_%"] = result["Weighted_Total"] / grand_total * 100
        return result

    def quantile(
        self,
        table_name: str,
        value_column: str,
        groupby: list[str] | None = None,
        years: Years = "all",
        bins: int = -1,
    ) -> pd.Series:
        years_list = self.api.utils.parse_years_for_table(years, table_name=table_name)
        table = self.api.load_table(table_name, years_list, form="normalized")
        return self._calculator.quantile(
            table=table,
            on_column=value_column,
            groupby=groupby or [],
            bins=bins,
            years=years_list,
        )

    def grouped_summary(
        self,
        table_name: str,
        groupby: list[str] | str,
        columns: list[str] | None = None,
        years: Years = "all",
    ) -> pd.DataFrame:
        years_list = self.api.utils.parse_years_for_table(years, table_name=table_name)
        table = self.api.load_table(table_name, years_list, form="normalized")
        table = self.api.add_weight(table)
        weight_col = self.api.context.config.standard_columns.weight

        if isinstance(groupby, str):
            groupby = [groupby]

        if columns is None:
            columns = [
                col
                for col in table.select_dtypes("number").columns
                if col not in self.api.context.config.standard_columns.groupby
                and col not in [self.api.context.config.standard_columns.id, weight_col, "index"]
            ]
        elif isinstance(columns, str):
            columns = [columns]

        first_col = table.columns[0]
        agg_dict: dict[str, tuple[str, str]] = {"Count": (first_col, "count")}
        for col in columns:
            weighted_name = f"__w_{col}"
            table[weighted_name] = table[col] * table[weight_col]
            agg_dict[f"{col}_Mean"] = (col, "mean")
            agg_dict[f"{col}_Weighted_Total"] = (weighted_name, "sum")

        result = table.groupby(groupby, observed=True).agg(**agg_dict)
        return result
