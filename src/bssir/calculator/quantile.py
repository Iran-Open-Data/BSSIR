from typing import Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict


from ..api import API


_QuantileBase = Literal[
    "Income",
    "Expenditure",
    "Gross_Expenditure",
    "Net_Expenditure",
]


_EquivalenceScale = Literal[
    "Household", "Per_Capita", "OECD", "OECD_Modified", "Square_Root"
]


class QuantileSettings(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    api: API
    weight_column: str | None = None
    on_column: str | None = None
    on_variable: _QuantileBase | None = None
    default_variable: _QuantileBase = "Net_Expenditure"
    weighted: bool = True
    equivalence_scale: _EquivalenceScale = "Household"
    for_all: bool = True
    annual: bool = True
    groupby: list[str] = []
    years: list[int] | None = None


class Quantiler:
    """
    Quantiler Class
    """

    variable_aliases = {
        "Expenditure": "Gross_Expenditure",
    }

    variable_tables: dict[str, str] = {
        "Income": "Total_Income",
        "Gross_Expenditure": "Total_Expenditure",
        "Net_Expenditure": "Total_Expenditure",
    }

    def __init__(
        self,
        settings: QuantileSettings,
        table: pd.DataFrame | pd.Series | None = None,
    ) -> None:
        self.settings = settings.model_copy()

        if table is not None:
            if ("Year" in table.index.names) and ("ID" in table.index.names):
                self.original_index = pd.DataFrame(index=table.index)
            else:
                self.original_index = table[["Year", "ID"]]
            self.table = table.copy().reset_index().set_index(["Year", "ID"])
        else:
            self.table = table
        self.years = self._find_years()
        self.value_table = self.create_value_table()

    def create_value_table(self) -> pd.DataFrame:
        if self.settings.on_column is not None:
            if self.table is None:
                raise ValueError(
                    "The table must be provided for the `on_column` method"
                )
            value_table = self._extract_value_table(self.table)
        elif self.settings.on_variable is not None:
            value_table = self._get_external_value_table(self.settings.on_variable)
        else:
            value_table = self._get_external_value_table(self.settings.default_variable)

        equivalence_scale = (
            self.settings.api.load_table("Equivalence_Scale", years=self.years)
            .set_index(["Year", "ID"])
            .loc[:, self.settings.equivalence_scale]
        )
        value_table["Values"] = value_table["Values"].div(equivalence_scale)
        value_table = value_table.dropna().sort_values("Values")
        return value_table

    def _find_years(self) -> list[int]:
        if self.settings.years is not None:
            return self.settings.years
        if self.table is None:
            raise ValueError("Year is Not available")
        if "Year" in self.table.index.names:
            return self.table.index.get_level_values("Year").unique().to_list()
        if "Year" in self.table.columns:
            return list(self.table["Year"].unique())
        raise ValueError("Year must be provided")

    def _extract_value_table(self, table: pd.DataFrame | pd.Series) -> pd.DataFrame:
        if isinstance(table, pd.Series):
            table = table.to_frame(name="Values")
            index_levels = list(table.index.names)
            index_levels.remove("Year")
            index_levels.remove("ID")
            table.index = table.index.droplevel(index_levels)  # type: ignore
        if isinstance(table, pd.DataFrame):
            table = table.reset_index().set_index(["Year", "ID"])
            if len(table.columns) > 1:
                if self.settings.on_column is None:
                    raise ValueError("Column name must be provided")
                assert self.settings.on_column in table.columns
                table = table[[self.settings.on_column]]
            table.columns = ["Values"]
        assert table.index.duplicated().sum() == 0
        return table

    def _get_external_value_table(self, variable: str) -> pd.DataFrame:
        variable = self.variable_aliases.get(variable, variable)
        table_name = self.variable_tables[variable]
        value_table = (
            self.settings.api.load_table(table_name, self.years)
            .set_index(["Year", "ID"])[[variable]]
            .rename(columns={variable: "Values"})
        )
        if (not self.settings.for_all) and (self.table is not None):
            value_table = value_table.loc[self.table.set_index(["Year", "ID"]).index]
        return value_table

    def calculate_quantile(self) -> pd.Series:
        groupby_columns = ["Year"] if self.settings.annual else []
        groupby_columns.extend(self.settings.groupby)
        quantile = (
            self.value_table
            .pipe(self._add_attributes)
            .pipe(self._add_weights)
            .groupby(groupby_columns, group_keys=False)
            .apply(self._calculate_subgroup_quantile, include_groups=False) # type: ignore
            .pipe(self._align_with_table)
            .loc[:, "Quantile"]
        )
        return quantile

    def _calculate_subgroup_quantile(self, subgroup: pd.DataFrame) -> pd.DataFrame:
        return subgroup.assign(
            CumWeight=lambda df: df["__quantile_weight"].cumsum(),
            Quantile=lambda df: df["CumWeight"] / df["CumWeight"].iloc[-1],
        )

    def _add_weights(self, table: pd.DataFrame) -> pd.DataFrame:
        if self.settings.weight_column is not None:
            assert isinstance(self.table, pd.DataFrame)
            table = (
                table
                .join(self.table[self.settings.weight_column])
                .rename(columns={self.settings.weight_column: "__quantile_weight"})
            )
        elif self.settings.weighted:
            weight_col = self.settings.api.defaults.columns.weight
            table = (
                self.settings.api
                .add_weight(table)
                .rename(columns={weight_col: "__quantile_weight"})
            )
        else:
            table["__quantile_weight"] = 1
        return table

    def _add_attributes(self, table: pd.DataFrame) -> pd.DataFrame:
        for attribute in self.settings.groupby:  # type: ignore
            table = self.settings.api.add_attribute(table, name=attribute)
        return table

    def _align_with_table(self, quantile: pd.DataFrame) -> pd.DataFrame:
        if self.table is None:
            return quantile
        quantile = self.original_index.join(quantile, on=["Year", "ID"])
        return quantile
