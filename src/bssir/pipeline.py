# import pandas as pd

# from bssir.context import Context
# from bssir.context.metadata.models.pipelines.step import Step, StepContext
# from bssir.core.data_cleaner import load_cleaned_table


# class PipelineRunner:
#     def __init__(
#         self,
#         table_name: str,
#         year: int,
#         *,
#         context: Context,
#     ) -> None:
#         self.table_name = table_name
#         self.year = year
#         self.context = context

#         self._check_pipeline()

#         self.metadata = context.metadata.pipelines[table_name].resolve(year)

#         self.tables: dict[str, pd.DataFrame] = {}
#         self.active_table = self.metadata.sources[0].name

#     def _check_pipeline(self) -> None:
#         """Raise an informative exception if the pipeline is unavailable."""
#         ...

#     def initialize(self) -> None:
#         self.load_sources()

#     def run(self) -> pd.DataFrame:
#         self.initialize()

#         for step in self.metadata.steps:
#             self.run_step(step)

#         return self.table

#     def load_sources(self) -> None:
#         for source in self.metadata.sources:
#             if source.form != "cleaned":
#                 continue

#             self.tables[source.name] = load_cleaned_table(
#                 source.name,
#                 self.year,
#                 context=self.context,
#             )

#     @property
#     def table(self) -> pd.DataFrame:
#         return self.tables[self.active_table]

#     def _step_context(self, step: Step) -> StepContext:
#         source = step.source or self.active_table
#         output = step.output or self.active_table

#         if source not in self.tables:
#             raise KeyError(f"Unknown table '{source}'.")

#         return StepContext(
#             tables=self.tables,
#             source=source,
#             output=output,
#             year=self.year,
#         )

#     def run_step(self, step: Step) -> None:
#         self.tables.update(step.run(self._step_context(step)))

