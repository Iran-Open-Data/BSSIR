from collections.abc import Callable, Mapping, Iterator
from typing import Any
from functools import cached_property
from pathlib import Path

import pandas as pd
from pydantic import BaseModel, ConfigDict

from bssir.context import Config
from bssir.utils.yaml import parse_yaml
from bssir.context.utils.resolver import resolve_metadata
from bssir.utils.argham import Argham


def _read_text(path: Path) -> str | None:
    """Read a UTF-8 encoded text file."""
    if path.exists():
        return path.read_text(encoding="utf-8")
    return None


def collapse_years(table: pd.DataFrame) -> pd.DataFrame:
    first = table.drop_duplicates(keep="first")
    last = table.drop_duplicates(keep="last")

    first.index = [f"{i[0]}-{i[1]}" for i in zip(first.index, last.index)]
    return first


class MetadataSource(BaseModel):
    """Metadata from the three supported source layers."""
    base_package_path: Path
    package_path: Path
    local_path: Path

    @property
    def has_base(self):
        return self.base_package_path.exists()

    @property
    def has_package(self):
        return self.package_path.exists()

    @property
    def has_local(self):
        return self.local_path.exists()

    @property
    def base_loaded(self) -> bool:
        return "base_package" in self.__dict__

    @property
    def package_loaded(self) -> bool:
        return "package" in self.__dict__

    @property
    def local_loaded(self) -> bool:
        return "local" in self.__dict__

    @property
    def loaded(self) -> bool:
        """Whether all available metadata sources have been loaded."""
        return (
            (not self.has_base or self.base_loaded)
            and (not self.has_package or self.package_loaded)
            and (not self.has_local or self.local_loaded)
        )

    @cached_property
    def base_package(self) -> str | None:
        return _read_text(self.base_package_path)

    @cached_property
    def package(self) -> str | None:
        return _read_text(self.package_path)

    @cached_property
    def local(self) -> str | None:
        return _read_text(self.local_path)



class MetadataDefinition(BaseModel):
    source: MetadataSource
    interpreter: Callable[[str, dict], str] | None


class MetadataNode(BaseModel, Mapping):
    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    config: Config
    merged: dict | None = None

    @property
    def content(self) -> dict:
        if self.merged:
            return self.merged
        return {}

    @cached_property
    def _resolved_cache(self):
        return {}

    def resolve(self, year: int, categorize: bool = False, **optional_settings) -> Any:
        """
        Resolve metadata for a specific year.

        Metadata values may vary over time and are stored using year-dependent
        mappings. This method resolves those mappings for the requested year and
        returns the resulting metadata dictionary.

        The resolved metadata is cached, so repeated calls for the same year return
        the previously computed result.

        Parameters
        ----------
        year : int
            The year for which to resolve the metadata.

        Returns
        -------
        dict[str, Any]
            The metadata with all year-dependent values resolved.
        """
        key = (year, categorize)

        if "add_year" not in optional_settings:
            optional_settings["add_year"] = False

        if key not in self._resolved_cache:
            resolved = resolve_metadata(
                self.content,
                year,
                categorize=categorize,
                **optional_settings
            )
            self._resolved_cache[key] = resolved
        return self._resolved_cache[key]

    def __getitem__(self, key):
        return self.content[key]

    def __iter__(self) -> Iterator:
        return iter(self.content)

    def __len__(self) -> int:
        return len(self.content)

    def __contains__(self, key) -> bool:
        return key in self.content

    def get(self, key, default = None):
        return self.content.get(key, default)

    def keys(self):
        return self.content.keys()

    def values(self):
        return self.content.values()

    def items(self):
        return self.content.items()

    def parse_argham(self, argham) -> Argham:
        parsed_argham = Argham(
            argham,
            default_start=min(self.config.coverage_period),
            default_end=max(self.config.coverage_period)+1,
        )
        return parsed_argham


class Metadata(MetadataNode):
    name: str
    definition: MetadataDefinition
    description: str | None = None

    def _merge_metadata(self) -> dict:
        """Merge metadata from all configured source layers."""

        merged: dict = {}

        for text in (
            self.definition.source.base_package,
            self.definition.source.package,
            self.definition.source.local,
        ):
            if not text:
                continue

            if self.definition.interpreter is not None:
                try:
                    text = self.definition.interpreter(text, merged)
                except TypeError:
                    pass

            merged.update(parse_yaml(text))

        merged = {
            k: v for k, v in merged.items()
            if not isinstance(k, str) or not k.isupper()
        }

        return merged

    @cached_property
    def content(self) -> dict:
        return self._merge_metadata()
