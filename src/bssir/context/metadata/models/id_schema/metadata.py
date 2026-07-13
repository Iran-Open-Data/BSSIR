from collections.abc import Iterator
from functools import cached_property

import pandas as pd

from bssir.utils.argham import Argham
from ..common import Metadata, MetadataNode
from .attribute import Attribute
from .mapping import LabelMapping


class IDLengths(MetadataNode):
    def resolve(self, year: int) -> int:
        resolved = super().resolve(year=year)
        if resolved is None:
            raise KeyError(f"No ID length defined for year {year}.")
        return resolved


class IDSchemaMetadata(Metadata):
    RESERVED_KEYS: frozenset = frozenset({"id_lengths"})
    MAPPINGS_SUFFIXES: tuple = tuple(["_mapping", "_translation"])

    @cached_property
    def id_lengths(self) -> IDLengths:
        return IDLengths(
            merged=self.content["id_lengths"],
            config=self.config,
        )

    @property
    def mappings_names(self) -> list[str]:
        return [
            key
            for key in self.content
            if any(suffix in key for suffix in self.MAPPINGS_SUFFIXES)
        ]
    
    @cached_property
    def mappings(self) -> dict[str, LabelMapping]:
        return {
            name: LabelMapping(
                name=name,
                merged=self.content[name],
                config=self.config
            )
            for name in self.mappings_names
        }

    @cached_property
    def attributes(self) -> dict[str, Attribute]:
        reserved = set(self.mappings_names) | self.RESERVED_KEYS

        return {
            name: Attribute(
                name=name,
                merged=self.content[name],
                config=self.config,
                label_mappings=self.mappings,
            )
            for name in self.content
            if name not in reserved
        }

    def __getitem__(self, key: str) -> Attribute:
        return self.attributes[key]
