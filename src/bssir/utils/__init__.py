"""BSSIR library utility functions"""
from concurrent.futures import ThreadPoolExecutor
from typing import Literal, Iterable
from pathlib import Path

# from ..metadata_reader import Defaults, Metadata, Years
from bssir.context import Context
from bssir.context.types import Years

from .archive_utils import extract
from .download_utils import download #, download_map
from ..context.utils.parser import parse_years
from .argham import Argham
from .injection import import_module_from_path

__all__ = [
    "parse_years",
    "import_module_from_path",
    "download",
    "Argham",
    "Utils",
]


class Utils:
    def __init__(self, context: Context):
        self.context = context

    def extract(self, compressed_file: Path, output_directory: Path) -> None:
        extract(
            compressed_file=compressed_file,
            output_directory=output_directory,
            config=self.context.config,
        )

    def parse_years(self, years: Years) -> list[int]:
        return parse_years(
            years=years,
            available_years=self.context.config.coverage_period,
        )

    def parse_years_for_table(self, years: Years, table_name: str) -> list[int]:
        table_metadata = self.context.metadata.tables[table_name]
        if table_metadata.availability:
            available_years = table_metadata.availability
        else:
            available_years=self.context.config.coverage_period
        return parse_years(years=years, available_years=available_years)

    def create_table_year_pairs(
        self, table_names: str | Iterable[str], years: Years
    ) -> list[tuple[str, int]]:
        if table_names == "all":
            table_names = self.context.metadata.tables.table_list
        table_names = [table_names] if isinstance(table_names, str) else table_names
        table_year = []
        for table_name in table_names:
            table_years = self.parse_years_for_table(
                years,
                table_name=table_name,
            )
            table_year.extend([(table_name, year) for year in table_years])
        return table_year


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
        path = self.context.config.dirs.cleaned.joinpath(file_name)
        url = (
            f"{self.context.config.get_mirror(source).bucket_address}/"
            f"{self.context.config.directory_names.cleaned}/"
            f"{file_name}"
        )
        download(url, path)

    # def download_map(
    #     self, map_name: str, source: Literal["original"] = "original"
    # ) -> None:
    #     download_map(
    #         map_name=map_name,
    #         source=source,
    #         map_metadata=self._metadata.maps,
    #         maps_directory=self._defautls.dir.maps,
    #     )

    # def resolve_metadata(
    #     self,
    #     versioned_metadata: dict,
    #     year: int,
    #     categorize: bool = False,
    #     **optional_settings,
    # ):
    #     return resolve_metadata(
    #         versioned_metadata, year, categorize, **optional_settings
    #     )

    # def extract_column_metadata(
    #     self,
    #     column_name: str,
    #     table_name: str,
    # ) -> dict:
    #     table_metadata = self._metadata.tables[table_name]
    #     return extract_column_metadata(
    #         column_name=column_name,
    #         table_metadata=table_metadata,
    #         lib_defaults=self._defautls,
    #     )

    # def exteract_code_metadata(self, column_code: str, table_name: str) -> dict:
    #     table_metadata = self._metadata.tables[table_name]
    #     return exteract_code_metadata(
    #         column_code=column_code,
    #         table_metadata=table_metadata,
    #         lib_defaults=self._defautls,
    #     )
