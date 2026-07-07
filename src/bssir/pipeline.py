from typing import Literal

import pandas as pd

from bssir.context import Context
from bssir.data_cleaner import load_cleaned_table


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
            self.check_pipeline_availability()
            self.metadata = context.metadata.pipelines[table_name].resolve(year)
            self.current = self.metadata.sources[0].name
            self.tables: dict[str, pd.DataFrame] = {}

    def check_pipeline_availability(self) -> None: ...
        # check and rise if is not available with useful error

    def load_sources(self) -> None:
        for source in self.metadata.sources:
            if source.form == "cleaned":
                self._tables[source.name] = load_cleaned_table(source.name, self.year, context=self.context)

    @property
    def table(self) -> pd.DataFrame:
        return self.tables[self.current]
