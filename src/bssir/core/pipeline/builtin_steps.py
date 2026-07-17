from typing import Literal

import pandas as pd

from bssir.context.metadata.models.pipelines.step import BaseStep, StepContext
from .registry import register_step


@register_step
class AddYearStep(BaseStep):
    action: Literal["add_year"]
    column: str = "Year"

    def run(self, context: StepContext) -> dict[str, pd.DataFrame]:
        return {
            context.output: (
                context.get_source()
                .assign(**{self.column: context.year})
            )
        }


@register_step
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


@register_step
class DropNullsStep(BaseStep):
    action: Literal["drop_nulls"]
    subset: str | list[str] | None = None
    how: Literal["any", "all"] = "any"

    def run(self, context: StepContext) -> dict[str, pd.DataFrame]:
        return {
            context.output: (
                context.get_source()
                .dropna(
                    subset=self.subset,
                    how=self.how,
                )
            )
        }


@register_step
class MergeStep(BaseStep):
    action: Literal["merge"]
    left: str | None
    right: str
    on: list[str]
    how: Literal["left", "right", "outer", "inner", "cross"] = "left"

    def run(self, context: StepContext) -> dict[str, pd.DataFrame]:
        if self.left:
            assert self.left in context.tables
            left = self.left
        else:
            left = context.source
        return {
            context.output: pd.merge(
                left=context.tables[left],
                right=context.tables[self.right],
                on=self.on,
                how=self.how,
            )
        }


# class ConcatStep(BaseStep):
#     action: Literal["concat"]
#     inputs: list[str]

#     def run(self, context: StepContext) -> dict[str, pd.DataFrame]:
#         result = pd.concat([runner.tables[t] for t in self.inputs])
#         self.set_result(runner=runner, result=result)


# class AddAttributeStep(BaseStep):
#     action: Literal["add_attribute"]
#     name: str
#     aspects: tuple[str, ...] = ("name",)
#     column_names: tuple[str, ...] = ()


# class TransformStep(BaseStep):
#     action: Literal["transform"]
#     name: str
