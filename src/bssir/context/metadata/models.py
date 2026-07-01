from collections.abc import Callable, Mapping, Iterator
from functools import cached_property
from pathlib import Path
from typing import Literal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from bssir.context.utils.yaml import parse_yaml
from bssir.context.utils.resolver import resolve_metadata
from bssir.context.utils.parser import parse_years


def _read_text(path: Path) -> str | None:
    """Read a UTF-8 encoded text file."""
    if path.exists():
        return path.read_text(encoding="utf-8")
    return None


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
    model_config = ConfigDict(frozen=True)
    merged: dict | None = None

    @property
    def content(self) -> dict:
        if self.merged:
            return self.merged
        else:
            raise

    @cached_property
    def _resolved_cache(self):
        return {}

    def resolve(self, year: int, categorize: bool = False) -> Mapping:
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

        if key not in self._resolved_cache:
            resolved = resolve_metadata(
                self.content,
                year,
                categorize=categorize,
            )
            if not isinstance(resolved, dict):
                raise TypeError(
                    f"Expected resolved metadata to be a dict, got {type(resolved).__name__}."
                )
            self._resolved_cache[key] = resolved
        return self._resolved_cache[key]

    def __getitem__(self, key: Any) -> Any:
        return self.content[key]

    def __iter__(self) -> Iterator[Any]:
        return iter(self.content)

    def __len__(self) -> int:
        return len(self.content)

    def __contains__(self, key: Any) -> bool:
        return key in self.content

    def get(self, key: Any, default: Any = None) -> Any:
        return self.content.get(key, default)

    def keys(self):
        return self.content.keys()

    def values(self):
        return self.content.values()

    def items(self):
        return self.content.items()


class Metadata(MetadataNode):
    name: str
    definition: MetadataDefinition
    description: str | None = None

    def _merge_metadata(self) -> dict[str, Any]:
        """Merge metadata from all configured source layers."""

        merged: dict[str, Any] = {}

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


class DefaultSettings(BaseModel):
    """Default settings applied to table definitions."""

    model_config = ConfigDict(frozen=True)

    missings: Literal["error", "drop", "keep"] = Field(
        "error",
        description="How missing columns/variables are handled",
    )

    encoding: str = Field(
        "utf8",
        description="File encoding used when not overridden by a table.",
    )


class Table(MetadataNode):
    name: str

    @cached_property
    def availability(self) -> list[int] | None:
        availability = self.get("availability")
        if not availability:
            return None
        return parse_years(availability)


class ResolvedTable(BaseModel): ...


class TablesMetadata(Metadata):
    @property
    def default_settings(self) -> DefaultSettings:
        return self.content.get("default_settings", {})
    
    @property
    def table_list(self) -> list[str]:
        return self.content.get("table_list", [])
    
    @cached_property
    def tables(self) -> dict[str, Table]:
        return {
            name: Table(name=name, merged=value)
            for name, value in self.content.items()
            if name not in ["default_settings", "table_list"]
        }

    def __getitem__(self, key: str) -> Table:
        return self.tables.__getitem__(key)

    def __iter__(self) -> Iterator[str]:
        return super().__iter__()

    def __contains__(self, key: str) -> bool:
        return super().__contains__(key)

    def get(self, key: str, default: None = None) -> Table | None:
        return self.tables.get(key)


METADATA_MODELS: dict[str, type[Metadata]] = {
    "instruction": Metadata,
    "raw_files": Metadata,
    "id_schema": Metadata,
    "tables": TablesMetadata,
    "schema": Metadata,
    "commodities": Metadata,
    "occupations": Metadata,
    "industries": Metadata,
    "maps": Metadata,
}
