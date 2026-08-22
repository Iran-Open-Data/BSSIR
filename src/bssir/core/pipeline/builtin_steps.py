from typing import Literal

import pandas as pd
from pandas.api.types import CategoricalDtype

from .registry import register
from .models import BaseStep, CreateColumnStep, StepContext


@register("add_year")
class AddYearStep(BaseStep):
    action: Literal["add_year"]
    column: str = "Year"

    def run(self, context: StepContext) -> pd.DataFrame:
        return (
            context.get_source()
            .assign(**{self.column: context.year})
        )


@register("add_table_name")
class AddTableNameStep(BaseStep):
    action: Literal["add_table_name"]
    column: str = "Year"

    def run(self, context: StepContext) -> dict[str, pd.DataFrame]:
        return {
            context.output: (
                context.get_source()
                .assign(**{self.column: context.source})
            )
        }


@register("drop_nulls")
class DropNullsStep(BaseStep):
    action: Literal["drop_nulls"]
    subset: str | list[str] | None = None
    how: Literal["any", "all"] = "any"

    def run(self, context: StepContext) -> pd.DataFrame:
        return (
            context.get_source()
            .dropna(subset=self.subset, how=self.how)
        )


@register("merge")
class MergeStep(BaseStep):
    action: Literal["merge"]
    left: str | None
    right: str
    on: list[str]
    how: Literal["left", "right", "outer", "inner", "cross"] = "left"

    def run(self, context: StepContext) -> pd.DataFrame:
        if self.left:
            assert self.left in context.tables
            left = self.left
        else:
            left = context.source
        return pd.merge(
            left=context.tables[left],
            right=context.tables[self.right],
            on=self.on,
            how=self.how,
        )


@register("concat")
class ConcatStep(BaseStep):
    action: Literal["concat"]
    inputs: list[str] | None = None
    add_table_name: bool = False

    def run(self, context: StepContext) -> pd.DataFrame:
        inputs = self.inputs if self.inputs else context.tables.keys()
        return pd.concat([context.tables[t] for t in inputs])


@register("add_attribute")
class AddAttributeStep(BaseStep):
    action: Literal["add_attribute"]
    name: str
    aspects: tuple[str, ...] = ("name",)
    column_names: tuple[str, ...] = ()

    def run(self, context: StepContext) -> pd.DataFrame:
        from bssir.core.attribute import add_attribute

        return add_attribute(
            table=context.get_source(),
            context=context.lib_context,
        )


@register("add_month_column")
class CreateMonthColumnStep(CreateColumnStep):
    action: Literal["add_month_column"]
    column: str = "Month"

    def create_column(self, source: pd.DataFrame) -> pd.Series:
        return source["Interview_Month"].replace({1: 13}).sub(1)


@register("add_season_code")
class CreateSeasonCodeColumnStep(CreateColumnStep):
    action: Literal["add_season_code"]
    column: str = "Season_Code"

    def create_column(self, source: pd.DataFrame) -> pd.Series:
        return source["Month"].sub(1).floordiv(3).add(1)


@register("add_season_name")
class CreateSeasonNameColumnStep(CreateColumnStep):
    action: Literal["add_season_name"]
    column: str = "Season_Name"

    def create_column(self, source: pd.DataFrame) -> pd.Series:
        return (
            source["Season_Code"]
            .astype(CategoricalDtype([1, 2, 3, 4], ordered=True))
            .cat.rename_categories({1: "Spring", 2: "Summer", 3: "Autumn", 4: "Winter"})
        )
