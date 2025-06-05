"""HBSIR library utility functions"""
from concurrent.futures import ThreadPoolExecutor
from typing import Literal, Iterable
from pathlib import Path

from ..metadata_reader import Defaults, Metadata, _Years

from .archive_utils import extract
from .download_utils import download, download_map
from .parsing_utils import parse_years, create_table_year_pairs
from .metadata_utils import (
    resolve_metadata,
    extract_column_metadata,
    exteract_code_metadata,
)
from .argham import Argham


__all__ = [
    "parse_years",
    "download",
    "resolve_metadata",
    "Argham",
    "Utils",
]


class Utils:
    def __init__(self, defaults: Defaults, metadata: Metadata):
        self._defautls = defaults
        self._metadata = metadata

    def extract(self, compressed_file: Path, output_directory: Path) -> None:
        extract(
            compressed_file=compressed_file,
            output_directory=output_directory,
            seven_zip_directory=self._defautls.base_package_dir,
        )

    def parse_years(
        self,
        years: _Years,
        *,
        table_name: str | None = None,
        form: Literal["raw", "cleaned", "normalized"] = "raw",
    ) -> list[int]:
        table_availability = self._metadata.tables["table_availability"].copy()
        if form == "normalized":
            table_availability.update(
                self._metadata.schema.get("table_availability", {})
            )
        return parse_years(
            years=years,
            table_name=table_name,
            available_years=self._defautls.years,
            tables_availability=table_availability,
        )

    def create_table_year_pairs(
        self, table_names: str | Iterable[str], years: _Years
    ) -> list[tuple[str, int]]:
        if table_names == "all":
            table_names = list(self._metadata.tables["table_availability"].keys())
        return create_table_year_pairs(
            table_names=table_names,
            years=years,
            available_years=self._defautls.years,
            tables_availability=self._metadata.tables["table_availability"],
        )

    def download_cleaned_tables(
        self,
        years: list[int],
        source: Literal["mirror"] | str = "mirror",
    ) -> None:
        table_years = self.create_table_year_pairs("all", years)
        futures = []
        with ThreadPoolExecutor(6) as executer:
            for table_name, year in table_years:
                futures.append(
                    executer.submit(
                        self._download_cleaned_table,
                        year=year,
                        table_name=table_name,
                        source=source,
                    )
                )
        list(future.result() for future in futures)

    def _download_cleaned_table(
        self,
        year: int,
        table_name: str,
        source: Literal["mirror"] | str = "mirror",
    ) -> None:
        file_name = f"{year}_{table_name}.parquet"
        path = self._defautls.dir.cleaned.joinpath(file_name)
        index = 0 if source == "mirror" else self._defautls.get_mirror_index(source)
        url = f"{self._defautls.online_dirs[index].cleaned}/{file_name}"
        download(url, path)

    def download_map(
        self, map_name: str, source: Literal["original"] = "original"
    ) -> None:
        download_map(
            map_name=map_name,
            source=source,
            map_metadata=self._metadata.maps,
            maps_directory=self._defautls.dir.maps,
        )

    def resolve_metadata(
        self,
        versioned_metadata: dict,
        year: int,
        categorize: bool = False,
        **optional_settings,
    ):
        return resolve_metadata(
            versioned_metadata, year, categorize, **optional_settings
        )

    def extract_column_metadata(
        self,
        column_name: str,
        table_name: str,
    ) -> dict:
        table_metadata = self._metadata.tables[table_name]
        return extract_column_metadata(
            column_name=column_name,
            table_metadata=table_metadata,
            lib_defaults=self._defautls,
        )

    def exteract_code_metadata(self, column_code: str, table_name: str) -> dict:
        table_metadata = self._metadata.tables[table_name]
        return exteract_code_metadata(
            column_code=column_code,
            table_metadata=table_metadata,
            lib_defaults=self._defautls,
        )
