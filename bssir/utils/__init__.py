"""HBSIR library utility functions"""
from typing import Literal
from pathlib import Path

from ..metadata_reader import Defaults, Metadata, _Years

from .seven_zip_utils import extract as sevenzip
from .download_utils import download, download_map
from .parsing_utils import parse_years, create_table_year_pairs
from .metadata_utils import resolve_metadata, extract_column_metadata
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
        self.__defautls = defaults
        self.__metadata = metadata

    def sevenzip(self, compressed_file: Path, output_directory: Path) -> None:
        sevenzip(
            compressed_file=compressed_file,
            output_directory=output_directory,
            seven_zip_directory=self.__defautls.base_package_dir,
        )

    def parse_years(self, years: _Years, *, table_name: str | None = None) -> list[int]:
        return parse_years(
            years=years,
            table_name=table_name,
            available_years=self.__defautls.years,
            tables_availability=self.__metadata.tables["table_availability"],
        )

    def create_table_year_pairs(
        self, table_names: str, years: _Years
    ) -> list[tuple[str, int]]:
        return create_table_year_pairs(
            table_names=table_names,
            years=years,
            available_years=self.__defautls.years,
            tables_availability=self.__metadata.tables["table_availability"],
        )

    def download_map(
        self, map_name: str, source: Literal["original"] = "original"
    ) -> None:
        download_map(
            map_name=map_name,
            source=source,
            map_metadata=self.__metadata.maps,
            maps_directory=self.__defautls.dirs.maps,
        )

    def resolve_metadata(
        self,
        versioned_metadata: dict,
        year: int,
        categorize: bool = False,
        **optional_settings
    ):
        return resolve_metadata(
            versioned_metadata, year, categorize, **optional_settings
        )

    def extract_column_metadata(
        self,
        column_name: str,
        table_name: str,
    ) -> dict:
        table_metadata = self.__metadata.tables[table_name]
        return extract_column_metadata(
            column_name=column_name,
            table_metadata=table_metadata,
            lib_defaults=self.__defautls,
        )
