"""HBSIR library utility functions"""
from concurrent.futures import ThreadPoolExecutor
from typing import Literal
from pathlib import Path

from ..metadata_reader import Defaults, Metadata, _Years

from .seven_zip_utils import extract as sevenzip
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

    def download_cleaned_tables(self, years: list[int]) -> None:
        table_years = self.create_table_year_pairs("all", years)
        with ThreadPoolExecutor(4) as executer:
            for table_year in table_years:
                executer.submit(self.__download_cleaned_table, *table_year)

    def __download_cleaned_table(self, year: int, table_name: str) -> None:
        file_name = f"{year}_{table_name}.parquet"
        path = self.__defautls.dirs.cleaned.joinpath(file_name)
        url = f"{self.__defautls.online_dirs.cleaned}/{file_name}"
        download(url, path)

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
        table_metadata = self.__metadata.tables[table_name]
        return extract_column_metadata(
            column_name=column_name,
            table_metadata=table_metadata,
            lib_defaults=self.__defautls,
        )

    def exteract_code_metadata(self, column_code: str, table_name: str) -> dict:
        table_metadata = self.__metadata.tables[table_name]
        return exteract_code_metadata(
            column_code=column_code,
            table_metadata=table_metadata,
            lib_defaults=self.__defautls,
        )
