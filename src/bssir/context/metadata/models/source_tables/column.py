from typing import Annotated, Literal

from pydantic import BaseModel, field_validator, Field, TypeAdapter, ValidationError

from bssir.exceptions import ColumnResolutionError
from bssir.utils.argham import Argham
from ..common import MetadataNode


NumericType = Literal[
    "unsigned", "int", "float",
    "UInt8", "UInt16", "UInt32", "UInt64",
    "Int8", "Int16", "Int32", "Int64",
    "Float16", "Float32",
]


class ResolvedColumnBase(BaseModel):
    label: str
    description: str | None = None
    type: Literal["string", "category", "boolean"] | NumericType
    replace: dict = Field(default_factory=dict)
    source: dict = Field(default_factory=dict)

    @field_validator("replace", "source", mode="before")
    @classmethod
    def none_to_empty_dict(cls, value):
        if value is None:
            return {}
        return value


class StringColumn(ResolvedColumnBase):
    type: Literal["string"]


class CategoricalColumn(ResolvedColumnBase):
    type: Literal["category"]
    categories: dict


class BooleanColumn(ResolvedColumnBase):
    type: Literal["boolean"]
    true_condition: str


class NumericalColumn(ResolvedColumnBase):
    type: NumericType


ResolvedColumn = Annotated[
    StringColumn | CategoricalColumn | BooleanColumn | NumericalColumn,
    Field(discriminator="type"),
]

column_adapter = TypeAdapter(ResolvedColumn)


class Column(MetadataNode):
    name: str
    table_availability: list[int]

    @property
    def availability(self) -> list[int]:
        availability = self.get("availability")
        if not availability:
            return self.table_availability
        availability = Argham(
            availability,
            default_start=min(self.table_availability),
            default_end=max(self.table_availability)+1,
        ).get_numbers()
        return sorted(list(availability))

    def resolve(self, year: int, **optional_settings) -> ResolvedColumn:
        resolved = super().resolve(year=year, **optional_settings)

        try:
            return column_adapter.validate_python(resolved)

        except ValidationError as exc:
            raise ColumnResolutionError(
                name=self.name,
                year=year,
                resolved=resolved,
                error=exc,
            ) from exc

