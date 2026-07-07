# from collections.abc import Callable, Mapping, Iterator
# from functools import cached_property
# from pathlib import Path
# from typing import Literal
# from typing import Any, Annotated

# import pandas as pd
# from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, ValidationError, field_validator, ValidationInfo, model_validator

# from bssir.context import Config
# from bssir.context.utils.yaml import parse_yaml
# from bssir.context.utils.resolver import resolve_metadata
# from bssir.context.utils.argham import Argham


# def _read_text(path: Path) -> str | None:
#     """Read a UTF-8 encoded text file."""
#     if path.exists():
#         return path.read_text(encoding="utf-8")
#     return None


# class MetadataSource(BaseModel):
#     """Metadata from the three supported source layers."""
#     base_package_path: Path
#     package_path: Path
#     local_path: Path

#     @property
#     def has_base(self):
#         return self.base_package_path.exists()

#     @property
#     def has_package(self):
#         return self.package_path.exists()

#     @property
#     def has_local(self):
#         return self.local_path.exists()

#     @property
#     def base_loaded(self) -> bool:
#         return "base_package" in self.__dict__

#     @property
#     def package_loaded(self) -> bool:
#         return "package" in self.__dict__

#     @property
#     def local_loaded(self) -> bool:
#         return "local" in self.__dict__

#     @property
#     def loaded(self) -> bool:
#         """Whether all available metadata sources have been loaded."""
#         return (
#             (not self.has_base or self.base_loaded)
#             and (not self.has_package or self.package_loaded)
#             and (not self.has_local or self.local_loaded)
#         )

#     @cached_property
#     def base_package(self) -> str | None:
#         return _read_text(self.base_package_path)

#     @cached_property
#     def package(self) -> str | None:
#         return _read_text(self.package_path)

#     @cached_property
#     def local(self) -> str | None:
#         return _read_text(self.local_path)



# class MetadataDefinition(BaseModel):
#     source: MetadataSource
#     interpreter: Callable[[str, dict], str] | None


# class MetadataNode(BaseModel, Mapping):
#     model_config = ConfigDict(frozen=True)

#     config: Config
#     merged: dict | None = None

#     @property
#     def content(self) -> dict:
#         if self.merged:
#             return self.merged
#         else:
#             raise

#     @cached_property
#     def _resolved_cache(self):
#         return {}

#     def resolve(self, year: int, categorize: bool = False, **optional_settings) -> Mapping:
#         """
#         Resolve metadata for a specific year.

#         Metadata values may vary over time and are stored using year-dependent
#         mappings. This method resolves those mappings for the requested year and
#         returns the resulting metadata dictionary.

#         The resolved metadata is cached, so repeated calls for the same year return
#         the previously computed result.

#         Parameters
#         ----------
#         year : int
#             The year for which to resolve the metadata.

#         Returns
#         -------
#         dict[str, Any]
#             The metadata with all year-dependent values resolved.
#         """
#         key = (year, categorize)

#         if key not in self._resolved_cache:
#             resolved = resolve_metadata(
#                 self.content,
#                 year,
#                 categorize=categorize,
#                 **optional_settings
#             )
#             if not isinstance(resolved, dict):
#                 raise TypeError(
#                     f"Expected resolved metadata to be a dict, got {type(resolved).__name__}."
#                 )
#             self._resolved_cache[key] = resolved
#         return self._resolved_cache[key]

#     def __getitem__(self, key: Any) -> Any:
#         return self.content[key]

#     def __iter__(self) -> Iterator[Any]:
#         return iter(self.content)

#     def __len__(self) -> int:
#         return len(self.content)

#     def __contains__(self, key: Any) -> bool:
#         return key in self.content

#     def get(self, key: Any, default: Any = None) -> Any:
#         return self.content.get(key, default)

#     def keys(self):
#         return self.content.keys()

#     def values(self):
#         return self.content.values()

#     def items(self):
#         return self.content.items()


# class Metadata(MetadataNode):
#     name: str
#     definition: MetadataDefinition
#     description: str | None = None

#     def _merge_metadata(self) -> dict[str, Any]:
#         """Merge metadata from all configured source layers."""

#         merged: dict[str, Any] = {}

#         for text in (
#             self.definition.source.base_package,
#             self.definition.source.package,
#             self.definition.source.local,
#         ):
#             if not text:
#                 continue

#             if self.definition.interpreter is not None:
#                 try:
#                     text = self.definition.interpreter(text, merged)
#                 except TypeError:
#                     pass

#             merged.update(parse_yaml(text))

#         merged = {
#             k: v for k, v in merged.items()
#             if not isinstance(k, str) or not k.isupper()
#         }

#         return merged

#     @cached_property
#     def content(self) -> dict:
#         return self._merge_metadata()


# class SourceTableSettings(BaseModel):
#     """Default settings applied to table definitions."""

#     model_config = ConfigDict(frozen=True)

#     missings: Literal["error", "drop", "keep"] = Field(
#         "error",
#         description="How missing columns/variables are handled",
#     )

#     encoding: str = Field(
#         "utf8",
#         description="File encoding used when not overridden by a table.",
#     )


# NumericType = Literal[
#     "unsigned", "int", "float",
#     "UInt8", "UInt16", "UInt32", "UInt64",
#     "Int8", "Int16", "Int32", "Int64",
#     "Float16", "Float32",
# ]


# class ResolvedColumnBase(BaseModel):
#     label: str
#     description: str | None = None
#     type: Literal["string", "category", "boolean"] | NumericType
#     replace: dict = Field(default_factory=dict)
#     source: dict = Field(default_factory=dict)

#     @field_validator("replace", "source", mode="before")
#     @classmethod
#     def none_to_empty_dict(cls, value):
#         if value is None:
#             return {}
#         return value


# class StringColumn(ResolvedColumnBase):
#     type: Literal["string"]


# class CategoricalColumn(ResolvedColumnBase):
#     type: Literal["category"]
#     categories: dict


# class BooleanColumn(ResolvedColumnBase):
#     type: Literal["boolean"]
#     true_condition: str


# class NumericalColumn(ResolvedColumnBase):
#     type: NumericType


# ResolvedColumn = Annotated[
#     StringColumn | CategoricalColumn | BooleanColumn | NumericalColumn,
#     Field(discriminator="type"),
# ]

# column_adapter = TypeAdapter(ResolvedColumn)


# class ResolvedTable(BaseModel):
#     year: int
#     file_patterns: str | list[str]
#     settings: SourceTableSettings
#     default_settings: SourceTableSettings
#     columns: dict[str, ResolvedColumn | Literal["drop"] | None]
#     config: Config

#     @model_validator(mode="before")
#     @classmethod
#     def merge_default_settings(cls, data: dict) -> dict:
#         default = data["default_settings"]
#         if default is None:
#             return data

#         settings = data.get("settings", {})

#         data["settings"] = {
#             **default.model_dump(),
#             **settings,
#         }

#         return data

#     @field_validator("file_patterns", mode="before")
#     @classmethod
#     def normalize_patterns(cls, value):
#         if isinstance(value, list):
#             return value
#         if isinstance(value, str):
#             return [value]
#         raise ValueError(
#             "Expected a string or a list of strings."
#         )

#     @property
#     def extracted_directory(self) -> Path:
#         """Directory containing the extracted raw files for this year."""
#         return self.config.dirs.extracted / str(self.year)

#     @property
#     def files(self) -> list[Path]:
#         """Raw data files matching the resolved file patterns.

#         Returns
#         -------
#         list[Path]
#             Matching files sorted alphabetically. Returns an empty list if the
#             table has no associated file patterns.
#         """
#         if not self.file_patterns:
#             return []

#         files: set[Path] = set()

#         for pattern in self.file_patterns:
#             files.update(
#                 self.extracted_directory.glob(
#                     pattern,
#                     case_sensitive=False,
#                 )
#             )

#         return sorted(files)

#     def __getitem__(self, key: str) -> ResolvedColumn | None:
#         column = self.columns[key]
#         return None if column is None or column == "drop" else column

#     def get(
#         self,
#         key: str,
#         default: ResolvedColumn | None = None,
#     ) -> ResolvedColumn | None:
#         column = self.columns.get(key)

#         if column is None:
#             return default

#         return None if column == "drop" else column


# class Column(MetadataNode):
#     name: str
#     table_availability: list[int]

#     @property
#     def availability(self) -> list[int]:
#         availability = self.get("availability")
#         if not availability:
#             return self.table_availability
#         availability = Argham(
#             availability,
#             default_start=min(self.table_availability),
#             default_end=max(self.table_availability)+1,
#         ).get_numbers()
#         return sorted(list(availability))

#     def resolve(self, year: int, **optional_settings) -> ResolvedColumn:
#         resolved = super().resolve(year=year, **optional_settings)

#         try:
#             return column_adapter.validate_python(resolved)

#         except ValidationError as exc:
#             raise ColumnResolutionError(
#                 column=self.name,
#                 year=year,
#                 resolved=resolved,
#                 error=exc,
#             ) from exc


# class SourceTable(MetadataNode):
#     name: str
#     default_settings: SourceTableSettings

#     @property
#     def availability(self) -> list[int]:
#         availability = self.content.get("availability")
#         if not availability:
#             return self.config.coverage_period
#         availability = Argham(
#             availability,
#             default_start=min(self.config.coverage_period),
#             default_end=max(self.config.coverage_period)+1,
#         ).get_numbers()
#         return sorted(list(availability))

#     @property
#     def group(self) -> str | None:
#         return self.content.get("group")

#     @cached_property
#     def columns(self) -> dict[str, Column | None]:
#         return {
#             name: Column(
#                 name=name,
#                 merged=value,
#                 table_availability=self.availability,
#                 config=self.config,
#             )
#             if value is not None and value != "drop" else None
#             for name, value in self.content.get("columns", {}).items()
#         }

#     def resolve(self, year: int, **optional_settings) -> ResolvedTable:
#         resolved = super().resolve(year=year, **optional_settings)

#         try:
#             return ResolvedTable(
#                 **resolved,
#                 config=self.config,
#                 default_settings=self.default_settings,
#             )

#         except MetadataResolutionError as exc:
#             # A nested object (typically a column) already produced a rich error.
#             raise TableResolutionError(
#                 table=self.name,
#                 year=year,
#                 resolved=resolved,
#                 error=exc,
#             ) from exc

#         except ValidationError as exc:
#             # Validation of the table itself failed.
#             raise TableResolutionError(
#                 table=self.name,
#                 year=year,
#                 resolved=resolved,
#                 error=exc,
#             ) from exc

#     def __getitem__(self, key: str) -> Column | None:
#         return self.columns[key]

#     def get(self, key: str, default: None = None) -> Column | None:
#         return self.columns.get(key)

#     @cached_property
#     def column_labels(self) -> dict[str, dict[int, str]]:
#         mapping = {}
#         for name, column in self.columns.items():
#             if not column:
#                 continue
#             mapping[name] = {
#                 year: column.resolve(year).label
#                 for year in column.availability
#             }
#         return mapping

#     @cached_property
#     def lable_columns(self) -> dict[str, dict[int, str]]:
#         mapping: dict[str, dict[int, str]] = {}

#         for column_name, labels in self.column_labels.items():
#             for year, label in labels.items():
#                 label_mapping = mapping.setdefault(label, {})

#                 if year in label_mapping:
#                     raise ValueError(
#                         f"Duplicate label {label!r} for year {year}: "
#                         f"{label_mapping[year]!r} and {column_name!r}"
#                     )

#                 label_mapping[year] = column_name

#         return mapping

#     @property
#     def column_labels_report(self) -> pd.DataFrame:
#         return collapse_years(pd.DataFrame(self.column_labels))

#     @property
#     def label_columns_report(self) -> pd.DataFrame:
#         return collapse_years(pd.DataFrame(self.lable_columns))

#     @property
#     def files(self) -> dict[int, list[Path]]:
#         """Raw data files grouped by survey year.

#         Returns
#         -------
#         dict[int, list[Path]]
#             Mapping from survey years to the matching raw data files. Only years
#             with one or more matching files are included.
#         """
#         files: dict[int, list[Path]] = {}

#         for year in self.availability:
#             files[year] = self.resolve(year).files

#         return files

#     def _repr_html_(self) -> str:
#         from bssir import rendering

#         return rendering.html_repr("source_table", self)


# def collapse_years(table: pd.DataFrame) -> pd.DataFrame:
#     first = table.drop_duplicates(keep="first")
#     last = table.drop_duplicates(keep="last")

#     first.index = [f"{i[0]}-{i[1]}" for i in zip(first.index, last.index)]
#     return first


# class SourceTablesMetadata(Metadata):
#     @property
#     def default_settings(self) -> SourceTableSettings:
#         return self.content.get("default_settings", {})

#     @property
#     def table_list(self) -> list[str]:
#         return self.content.get("table_list", [])

#     @property
#     def group_list(self) -> list[str]:
#         return self.content.get("group_list", [])

#     @cached_property
#     def tables(self) -> dict[str, SourceTable]:
#         return {
#             name: SourceTable(
#                 name=name,
#                 merged=value,
#                 default_settings=self.default_settings,
#                 config=self.config,
#             )
#             for name, value in self.content.items()
#             if name not in ["default_settings", "table_list", "group_list"]
#         }

#     def __getitem__(self, key: str) -> SourceTable:
#         return self.tables.__getitem__(key)

#     def __iter__(self) -> Iterator[str]:
#         return super().__iter__()

#     def __contains__(self, key: str) -> bool:
#         return key in self.tables

#     def get(self, key: str, default: None = None) -> SourceTable | None:
#         return self.tables.get(key)

#     @property
#     def table_list_report(self) -> pd.DataFrame:
#         actual = set(self.tables)
#         expected = set(self.table_list)

#         rows = []

#         for table in sorted(actual | expected):
#             rows.append({
#                 "table": table,
#                 "defined": table in actual,
#                 "listed": table in expected,
#             })

#         return pd.DataFrame(rows).set_index("table")

#     @property
#     def table_availability(self) -> dict[str, list[int]]:
#         return {table.name: table.availability for table in self.tables.values()}

#     @cached_property
#     def table_groups(self) -> dict[str, str | None]:
#         return {table.name: table.group for table in self.tables.values()}

#     @property
#     def has_groups(self) -> bool:
#         return any(self.table_groups.values())

#     @cached_property
#     def table_availability_report(self) -> pd.DataFrame:
#         report = pd.DataFrame(
#             {name: {year: True for year in years}
#             for name, years in self.table_availability.items()}
#         )
#         report = (
#             report
#             .sort_index()
#             .pipe(collapse_years)
#             .transpose()
#             .rename_axis("Table")
#             .join(pd.Series(range(len(self.table_list)), index=self.table_list, name="Table_Index"))
#             .sort_values(["Table_Index"], na_position="last")
#         )
#         if self.has_groups:
#             report = (
#                 report
#                 .join(
#                     pd.Series(self.table_groups, name="Group")
#                     .fillna("no_group")
#                     .to_frame()
#                     .join(
#                         pd.Series(range(len(self.group_list)), index=self.group_list, name="Group_Index"),
#                         on="Group",
#                     )
#                 )
#                 .reset_index()
#                 .set_index(["Group", "Table"])
#                 .sort_values(["Group_Index", "Table_Index"], na_position="last")
#                 .drop(columns="Group_Index")
#             )
#         report = report.drop(columns="Table_Index")
#         return report


# METADATA_MODELS: dict[str, type[Metadata]] = {
#     "instruction": Metadata,
#     "raw_files": Metadata,
#     "id_schema": Metadata,
#     "source_tables": SourceTablesMetadata,
#     "schema": Metadata,
#     "commodities": Metadata,
#     "occupations": Metadata,
#     "industries": Metadata,
#     "maps": Metadata,
# }
