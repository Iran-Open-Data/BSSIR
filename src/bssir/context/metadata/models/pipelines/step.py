from abc import ABC, abstractmethod
from typing import Annotated, Literal

import pandas as pd
from pydantic import BaseModel, Field

from .runner_protocol import RunnerProtocol


class BaseStep(ABC, BaseModel):
    output: str | None = None

    @abstractmethod
    def run(self, runner: RunnerProtocol) -> None:
        ...

    def set_result(self, runner: RunnerProtocol, result: pd.DataFrame) -> None:
        current = self.output or runner.current

        runner.tables[current] = result
        runner.current = current


class AddYearStep(BaseStep):
    type: Literal["add_year"]
    column: str = "Year"

    def run(self, runner: RunnerProtocol) -> None:
        result = runner.tables[runner.current].assign(**{self.column: runner.year})
        self.set_result(runner=runner, result=result)


class MergeStep(BaseStep):
    type: Literal["merge"]
    left: str | None
    right: str
    on: list[str]
    how: Literal["left", "right", "outer", "inner", "cross"] = "left"

    def run(self, runner: RunnerProtocol) -> None:
        left = (
            runner.tables[self.left]
            if self.left is not None
            else runner.tables[runner.current]
        )
        result = pd.merge(
            left=left,
            right=runner.tables[self.right],
            on=self.on,
            how=self.how,
        )
        self.set_result(runner=runner, result=result)



class ConcatStep(BaseStep):
    type: Literal["concat"]
    inputs: list[str]

    def run(self, runner: RunnerProtocol) -> None:
        result = pd.concat([runner.tables[t] for t in self.inputs])
        self.set_result(runner=runner, result=result)


class AddAttributeStep(BaseStep):
    type: Literal["add_attribute"]
    name: str
    aspects: tuple[str, ...] = ("name",)
    column_names: tuple[str, ...] = ()


class TransformStep(BaseStep):
    type: Literal["transform"]
    name: str


PipelineStep = Annotated[
    MergeStep | ConcatStep | TransformStep,
    Field(discriminator="type"),
]
