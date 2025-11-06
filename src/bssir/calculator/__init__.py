import pandas as pd

from ..api import API

from .quantile import QuantileSettings, Quantiler

from .average import weighted_average, average_table

# pylint: disable=too-many-arguments
# pylint: disable=unused-argument


class Calculator:
    def __init__(self, api: API) -> None:
        self._api = api

    def weighted_average(
        self,
        table: pd.DataFrame,
        weight_col: str | None = None,
        columns: list[str] | None = None,
    ) -> pd.Series:
        return weighted_average(
            table=table,
            defaults=self._api.defaults,
            columns=columns,
            weight_col=weight_col,
        )

    def average_table(
        self,
        table: pd.DataFrame,
        weight_col: str | None = None,
        groupby: list[str] | str | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        return average_table(
            table=table,
            defaults=self._api.defaults,
            columns=columns,
            groupby=groupby,
            weight_col=weight_col,
        )

    def quantile(
        self,
        *,
        table: pd.DataFrame | pd.Series | None = None,
        quantile_column_name: str = "Quantile",
        bins: int = -1,
        **kwargs
    ) -> pd.Series:
        settings = QuantileSettings(api=self._api, **kwargs)
        quantile = Quantiler(table=table, settings=settings).calculate_quantile()
        quantile = quantile.rename(quantile_column_name)
        if bins > 0:
            quantile = (
                quantile.multiply(bins)
                .case_when([
                    (lambda s: s.mod(1).eq(0), lambda s: s),
                    (lambda s: s.mod(1).ne(0), lambda s: s.floordiv(1).add(1)),
                ])
                .astype("Int64")
                .rename(quantile_column_name)
            )
        return quantile

    def add_quantile(
        self, table: pd.DataFrame, quantile_column_name: str = "Quantile", **kwargs
    ) -> pd.DataFrame:
        kwargs.update({"quantile_column_name": quantile_column_name})
        if quantile_column_name in table.columns:
            table = table.drop(columns=quantile_column_name)
        table[quantile_column_name] = self.quantile(table=table, **kwargs)
        return table

    def add_decile(
        self, table: pd.DataFrame, quantile_column_name: str = "Decile", **kwargs
    ) -> pd.DataFrame:
        kwargs.update({"bins": 10, "quantile_column_name": quantile_column_name})
        table = self.add_quantile(table=table, **kwargs)
        return table

    def add_percentile(
        self, table: pd.DataFrame, quantile_column_name: str = "Percentile", **kwargs
    ) -> pd.DataFrame:
        kwargs.update({"bins": 100, "quantile_column_name": quantile_column_name})
        table = self.add_quantile(table=table, **kwargs)
        return table
