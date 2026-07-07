from pydantic import BaseModel

from ..common import MetadataNode
from .step import PipelineStep
from .source import Source


class ResolvedPipeline(BaseModel):
    sources: list[Source]
    steps: list[PipelineStep]


class TablePipeline(MetadataNode):
    name: str

    def resolve(
        self,
        year: int,
        categorize: bool = False,
        **optional_settings,
    ) -> ResolvedPipeline:
        resolved = super().resolve(
            year=year,
            categorize=categorize,
            **optional_settings,
        )

        resolved_steps = []

        for step in resolved["steps"]:
            step_name, step_params = next(iter(step.items()))
            step_params = step_params.copy()

            years = step_params.pop("years", None)
            if years is not None and year not in self.parse_argham(years):
                continue

            resolved_steps.append({step_name: step_params})

        resolved["steps"] = resolved_steps
        return ResolvedPipeline(**resolved)
