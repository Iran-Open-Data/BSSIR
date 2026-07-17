from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandas as pd
from pydantic import BaseModel


@dataclass
class StepContext():
    tables: dict[str, pd.DataFrame]
    source: str
    output: str
    year: int

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
