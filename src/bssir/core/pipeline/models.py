from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandas as pd
from pydantic import BaseModel

from bssir.context import Context


@dataclass
class StepContext():
    tables: dict[str, pd.DataFrame]
    source: str
    output: str
    year: int
    lib_context: Context

    def get_source(self) -> pd.DataFrame:
        return self.tables[self.source]


class BaseStep(ABC, BaseModel):
    action: str
    source: str | None = None
    output: str | None = None

    def run(self, context: StepContext) -> pd.DataFrame | dict[str, pd.DataFrame]:
        return context.get_source().pipe(self.transform)

    def transform(self, table: pd.DataFrame) -> pd.DataFrame:
        ...


class CreateColumnStep(BaseStep):
    column: str

    def run(self, context: StepContext) -> pd.DataFrame:
        return context.get_source().assign(**{self.column: self.create_column(context)})

    @abstractmethod
    def create_column(self, context: StepContext) -> pd.Series:
        ...
