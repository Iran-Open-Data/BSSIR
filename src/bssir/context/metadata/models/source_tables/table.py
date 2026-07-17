from pathlib import Path
from functools import cached_property
from typing import Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict, field_validator, Field, model_validator, ValidationError

from bssir.exceptions import MetadataResolutionError, TableResolutionError
from bssir.context.config import Config
from bssir.context.states import CleanState
from bssir.utils.argham import Argham
from ..common import MetadataNode, collapse_years
from .column import Column, ResolvedColumn


class SourceTableSettings(BaseModel):
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


class ResolvedTable(BaseModel):
    name: str
    year: int
    file_patterns: str | list[str]
    settings: SourceTableSettings
    default_settings: SourceTableSettings
    columns: dict[str, ResolvedColumn | Literal["drop"] | None]
    config: Config

    @model_validator(mode="before")
    @classmethod
    def merge_default_settings(cls, data: dict) -> dict:
        default = data["default_settings"]
        if default is None:
            return data

        settings = data.get("settings", {})

        data["settings"] = {
            **default.model_dump(),
            **settings,
        }

        return data

    @field_validator("file_patterns", mode="before")
    @classmethod
    def normalize_patterns(cls, value):
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            return [value]
        raise ValueError("Expected a string or a list of strings.")

    @property
    def extracted_directory(self) -> Path:
        """Directory containing the extracted raw files for this year."""
        return self.config.dirs.extracted / str(self.year)

    @property
    def files(self) -> list[Path]:
        """Raw data files matching the resolved file patterns.

        Returns
        -------
        list[Path]
            Matching files sorted alphabetically. Returns an empty list if the
            table has no associated file patterns.
        """
        if not self.file_patterns:
            return []

        files: set[Path] = set()

        for pattern in self.file_patterns:
            files.update(
                self.extracted_directory.glob(
                    pattern,
                    case_sensitive=False,
                )
            )

        return sorted(files)

    @property
    def cleaned_path(self) -> Path:
        directory = self.config.dirs.cleaned / str(self.year)
        directory.mkdir(exist_ok=True, parents=True)
        return directory / f"{self.name}.parquet"

    @property
    def clean_state_path(self) -> Path:
        return self.cleaned_path.parent / ".bssir" / f"{self.name}.json"

    @property
    def saved_clean_state(self) -> CleanState | None:
        if not self.clean_state_path.exists():
            return None

        try:
            return CleanState.model_validate_json(
                self.clean_state_path.read_text(encoding="utf-8")
            )
        except Exception:
            return None

    @property
    def current_clean_state(self) -> CleanState:
        return CleanState.from_paths(
            output_path=self.cleaned_path,
            source_paths=self.files,
            config=self.config,
        )

    def save_clean_state(self, state: CleanState) -> None:
        self.clean_state_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.clean_state_path.write_text(
            state.model_dump_json(indent=4),
            encoding="utf-8",
        )

    def update_clean_state(self) -> None:

        self.save_clean_state(self.current_clean_state)

    def is_cleaned(self) -> bool:
        saved = self.saved_clean_state
        if saved is None:
            return False

        current = self.current_clean_state
        return (
            current.output_file == saved.output_file
            and current.source_files == saved.source_files
            and current.source_tables_metadata == saved.source_tables_metadata
        )

    def __getitem__(self, key: str) -> ResolvedColumn | None:
        column = self.columns[key]
        return None if column is None or column == "drop" else column

    def get(
        self,
        key: str,
        default: ResolvedColumn | None = None,
    ) -> ResolvedColumn | None:
        column = self.columns.get(key)

        if column is None:
            return default

        return None if column == "drop" else column


class SourceTable(MetadataNode):
    name: str
    default_settings: SourceTableSettings

    @property
    def availability(self) -> list[int]:
        availability = self.content.get("availability")
        if not availability:
            return self.config.coverage_period
        availability = Argham(
            availability,
            default_start=min(self.config.coverage_period),
            default_end=max(self.config.coverage_period)+1,
        ).get_numbers()
        return sorted(list(availability))

    @property
    def group(self) -> str | None:
        return self.content.get("group")

    @cached_property
    def columns(self) -> dict[str, Column | None]:
        return {
            name: Column(
                name=name,
                merged=value,
                table_availability=self.availability,
                config=self.config,
            )
            if value is not None and value != "drop" else None
            for name, value in self.content.get("columns", {}).items()
        }

    def resolve(self, year: int, **optional_settings) -> ResolvedTable:
        resolved = super().resolve(year=year, add_year=True, **optional_settings)

        try:
            return ResolvedTable(
                name=self.name,
                **resolved,
                config=self.config,
                default_settings=self.default_settings,
            )

        except MetadataResolutionError as exc:
            raise TableResolutionError(
                name=self.name,
                year=year,
                resolved=resolved,
                error=exc,
            ) from exc

        except ValidationError as exc:
            raise TableResolutionError(
                name=self.name,
                year=year,
                resolved=resolved,
                error=exc,
            ) from exc

    def __getitem__(self, key: str) -> Column | None:
        return self.columns[key]

    def get(self, key: str, default: None = None) -> Column | None:
        return self.columns.get(key)

    @cached_property
    def column_labels(self) -> dict[str, dict[int, str]]:
        mapping = {}
        for name, column in self.columns.items():
            if not column:
                continue
            mapping[name] = {
                year: column.resolve(year).label
                for year in column.availability
            }
        return mapping

    @cached_property
    def lable_columns(self) -> dict[str, dict[int, str]]:
        mapping: dict[str, dict[int, str]] = {}

        for column_name, labels in self.column_labels.items():
            for year, label in labels.items():
                label_mapping = mapping.setdefault(label, {})

                if year in label_mapping:
                    raise ValueError(
                        f"Duplicate label {label!r} for year {year}: "
                        f"{label_mapping[year]!r} and {column_name!r}"
                    )

                label_mapping[year] = column_name

        return mapping

    @property
    def column_labels_report(self) -> pd.DataFrame:
        return collapse_years(pd.DataFrame(self.column_labels))

    @property
    def label_columns_report(self) -> pd.DataFrame:
        return collapse_years(pd.DataFrame(self.lable_columns))

    @property
    def files(self) -> dict[int, list[Path]]:
        """Raw data files grouped by survey year.

        Returns
        -------
        dict[int, list[Path]]
            Mapping from survey years to the matching raw data files. Only years
            with one or more matching files are included.
        """
        files: dict[int, list[Path]] = {}

        for year in self.availability:
            files[year] = self.resolve(year).files

        return files

    def _repr_html_(self) -> str:
        from bssir import rendering

        return rendering.html_repr("source_table", self)
