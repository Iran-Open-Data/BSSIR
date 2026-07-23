from pydantic import BaseModel

from ..common import MetadataNode
from .source import Source


class ResolvedPipeline(BaseModel):
    sources: list[Source]
    steps: list[dict]


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

        resolved_sources = []
        for source in resolved["sources"]:
            if isinstance(source, str):
                resolved_source = {"name": source}
            elif isinstance(source, dict):
                assert len(source) == 1
                resolved_source: dict = next(iter(source.values())) # type: ignore
                resolved_source.update({"name": next(iter(source.keys()))})
            resolved_sources.append(resolved_source)
        resolved["sources"] = resolved_sources

        resolved_steps = []
        for step in resolved.get("steps", {}):
            years = step.pop("years", None)
            if years is not None and year not in self.parse_argham(years):
                continue
            resolved_steps.append(step)

        resolved["steps"] = resolved_steps
        return ResolvedPipeline(**resolved)
