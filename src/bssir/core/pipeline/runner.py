from functools import cached_property

import pandas as pd

from bssir.context import Context
from bssir.context.metadata.models.pipelines.step import StepContext, BaseStep
from bssir.core.data_cleaner import load_cleaned_table
from . import registry


class PipelineRunner:
    def __init__(
        self,
        table_name: str,
        year: int,
        *,
        context: Context,
    ) -> None:
        self.table_name = table_name
        self.year = year
        self.context = context

        self.metadata = context.metadata.pipelines[table_name].resolve(year)

        self.tables: dict[str, pd.DataFrame] = {}
        self.active_table = self.metadata.sources[0].name

    @cached_property
    def steps(self) -> list[BaseStep]:
        return [registry.create(step) for step in self.metadata.steps]

    def initialize(self) -> None:
        self.load_sources()

    def run(self) -> pd.DataFrame:
        self.initialize()

        for step in self.steps:
            self.run_step(step)

        return self.table

    def load_sources(self) -> None:
        for source in self.metadata.sources:
            if source.form != "cleaned":
                continue

            self.tables[source.name] = load_cleaned_table(
                source.name,
                self.year,
                context=self.context,
                columns=source.columns,
            )

    @property
    def table(self) -> pd.DataFrame:
        return self.tables[self.active_table]

    def _build_context(self, step: BaseStep) -> StepContext:
        source = step.source or self.active_table
        output = step.output or self.active_table

        if source not in self.tables:
            raise KeyError(f"Unknown table '{source}'.")

        return StepContext(
            tables=self.tables,
            source=source,
            output=output,
            year=self.year,
        )

    def run_step(self, step: BaseStep) -> None:
        context = self._build_context(step)
        result = step.run(context)

        if isinstance(result, pd.DataFrame):
            self.tables[context.output] = result
        elif isinstance(result, dict):
            self.tables.update(result)
        else:
            raise TypeError(
                f"{type(step).__name__}.run() returned "
                f"{type(result).__name__}; expected DataFrame or dict[str, DataFrame]."
            )

        self.active_table = context.output
