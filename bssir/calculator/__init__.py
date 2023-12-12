import pandas as pd

from ..metadata_reader import Defaults

from .quantile import QuantileSettings, Quantiler

from .average import weighted_average, average_table

# pylint: disable=too-many-arguments
# pylint: disable=unused-argument


class Calculator:
    def __init__(self, defaults: Defaults) -> None:
        self.__defaults = defaults

    def weighted_average(
        self,
        table: pd.DataFrame,
        weight_col: str | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        return weighted_average(
            table=table,
            defaults=self.__defaults,
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
            defaults=self.__defaults,
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
        weight_column: str | None = None,
        **kwargs
    ) -> pd.Series:
        if weight_column is None:
            weight_column = self.__defaults.columns.weight
        settings_vars = {
            key: value for key, value in locals().items() if key != "table"
        }
        settings = QuantileSettings(**settings_vars)
        quantile = Quantiler(table=table, settings=settings).calculate_quantile()
        quantile = quantile.rename(quantile_column_name)
        if bins > 0:
            quantile = (
                quantile.multiply(bins)
                .floordiv(1)
                .add(1)
                .clip(1, bins)
                .astype(int)
                .rename(quantile_column_name)
            )
        return quantile

    def add_quantile(
        self, table: pd.DataFrame, quantile_column_name: str = "Quantile", **kwargs
    ) -> pd.DataFrame:
        quantile = self.quantile(**locals())
        quantile.index = table.index
        table[quantile_column_name] = quantile
        return table

    def add_decile(
        self, table: pd.DataFrame, quantile_column_name: str = "Decile", **kwargs
    ) -> pd.DataFrame:
        setting_vars = locals()
        setting_vars.update({"bins": 10})
        quantile = self.quantile(**setting_vars)
        quantile.index = table.index
        table[quantile_column_name] = quantile
        return table

    def add_percentile(
        self, table: pd.DataFrame, quantile_column_name: str = "Percentile", **kwargs
    ) -> pd.DataFrame:
        setting_vars = locals()
        setting_vars.update({"bins": 100})
        quantile = self.quantile(**setting_vars)
        quantile.index = table.index
        table[quantile_column_name] = quantile
        return table
