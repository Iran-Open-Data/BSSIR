from collections.abc import Iterator
from functools import cached_property

from ..common import Metadata
from .pipeline import TablePipeline


class PipelineMetadata(Metadata):
    @cached_property
    def tables(self) -> dict[str, TablePipeline]:
        return {
            name: TablePipeline(
                name=name,
                merged=value,
                config=self.config,
            )
            for name, value in self.content.items()
            if name not in []
        }

    def __getitem__(self, key: str) -> TablePipeline:
        return self.tables.__getitem__(key)

    def __iter__(self) -> Iterator[str]:
        return super().__iter__()

    def __contains__(self, key: str) -> bool:
        return key in self.tables

    def get(self, key: str, default: None = None) -> TablePipeline | None:
        return self.tables.get(key)
