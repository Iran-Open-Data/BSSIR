from typing import Annotated, Literal, TypeAlias
from functools import cached_property

from pydantic import BaseModel, field_validator, model_validator, Field, TypeAdapter, ValidationError, ConfigDict

from bssir.exceptions import AttributeResolutionError
from bssir.utils.argham import Argham
from ..common import MetadataNode
from .mapping import LabelMapping, ResolvedLabelMapping


type ResolvedAspect = dict[int, str]


class Aspect(MetadataNode):
    name: str
    limit: Argham | None = None

    def resolve(self, year: int, **optional_settings) -> ResolvedAspect:
        if self.limit and year not in self.limit:
            raise ValueError(
                f"Aspect '{self.name}' is not available for year {year}."
            )
        resolved = super().resolve(year=year, **optional_settings)
        return {
            key: value
            for key, value in resolved.items()
            if value is not None
        }


class Position(BaseModel):
    start: int
    end: int

    @property
    def length(self) -> int:
        return self.end - self.start


class AttributeDefinition(BaseModel):
    position: Position | None = None
    external_file: str | None = None

    @model_validator(mode="after")
    def validate_definition(self):
        if (self.position is None) == (self.external_file is None):
            raise ValueError(
                "Exactly one of 'position' or 'external_file' must be specified."
            )
        return self

    @property
    def type(self) -> Literal["positional", "external"]:
        if self.position is not None:
            return "positional"
        return "external"


class MappingDefinition(BaseModel):
    origin: str
    mapping: str
    keep_original_if_missing: bool = True
    fallback_value: str | None = None


class ResolvedAttribute(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    year: int

    definition: AttributeDefinition

    mappings: dict[str, MappingDefinition] = Field(default_factory=dict)

    direct_aspects: dict[str, ResolvedAspect]

    label_mappings: dict[str, ResolvedLabelMapping]

    limits: dict[str, Argham]

    @cached_property
    def aspects(self) -> dict[str, ResolvedAspect]:
        aspects = dict(self.direct_aspects)

        for name, definition in self.mappings.items():
            origin = aspects.get(definition.origin)
            if origin is None:
                raise ValueError(
                    f"Mapping '{name}' refers to unknown aspect "
                    f"'{definition.origin}'."
                )

            label_mapping = self.label_mappings.get(definition.mapping)
            if label_mapping is None:
                raise ValueError(
                    f"Unknown label mapping '{definition.mapping}'."
                )

            aspects[name] = {
                k: label_mapping.get(v, v)
                for k, v in origin.items()
            }

        return aspects


class Attribute(MetadataNode):
    RESERVED_KEYS: frozenset = frozenset({"definition", "limits", "mappings"})

    name: str
    label_mappings: dict[str, LabelMapping]

    @cached_property
    def limits(self) -> dict[str, Argham]:
        return {
            name: self.parse_argham(value)
            for name, value in self.content.get("limits", {}).items()
        }

    @cached_property
    def aspects(self) -> dict[str, Aspect]:
        return {
            name: Aspect(
                name=name,
                merged=value,
                config=self.config,
                limit=self.limits.get(name)
            )
            for name, value in self.content.items()
            if name not in self.RESERVED_KEYS
        }

    def resolve(self, year: int, **optional_settings) -> ResolvedAttribute:
        resolved = super().resolve(year=year, **optional_settings)

        resolved["direct_aspects"] = {
            name: aspect.resolve(year, **optional_settings)
            for name, aspect in self.aspects.items()
        }

        mapping_names = {
            definition["mapping"]
            for definition in resolved.get("mappings", {}).values()
        }
        resolved_mappings = {
            name: self.label_mappings[name].resolve(year)
            for name in mapping_names
        }

        try:
            return ResolvedAttribute(
                year=year,
                **resolved,
                label_mappings=resolved_mappings,
                limits=self.limits,
            )

        except ValidationError as exc:
            raise AttributeResolutionError(
                name=self.name,
                year=year,
                resolved=resolved,
                error=exc,
            ) from exc

    def __getitem__(self, key: str) -> Aspect:
        return self.aspects[key]

    def get(self, key: str, default=None) -> Aspect | None:
        return self.aspects.get(key, default)
