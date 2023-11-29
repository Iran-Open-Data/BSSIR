from typing import Literal

import pandas as pd

from .metadata_reader import Defaults, Metadata, _Years, LoadTableSettings
from . import archive_handler, data_cleaner, external_data, data_engine
from .utils import Utils

_DataSource = Literal["SCI", "CBI"]
_Frequency = Literal["Annual", "Monthly"]
_SeparateBy = Literal["Urban_Rural", "Province"]


class API:
    def __init__(self, defaults: Defaults, metadata: Metadata):
        self.defautls = defaults
        self.metadata = metadata
        self.utils = Utils(defaults, metadata)

    def setup_raw_data(
        self,
        years: _Years,
        *,
        replace: bool = False,
        download_source: Literal["original", "mirror"] = "mirror"
    ):
        years = self.utils.parse_years(years)
        archive_handler.download(
            years,
            replace=replace,
            source=download_source,
            lib_metadata=self.metadata,
            lib_defaults=self.defautls,
        )
        archive_handler.unpack(years, replace=replace, lib_defaults=self.defautls)
        archive_handler.extract(years, replace=replace, lib_defaults=self.defautls)

    def load_table(
        self,
        table_name,
        years: _Years,
        form: Literal["raw", "cleaned", "processed"],
        **kwargs
    ) -> pd.DataFrame:
        settings = self.defautls.functions.load_table
        settings = settings.model_copy(update=kwargs)
        years = self.utils.parse_years(years, table_name=table_name)
        if form == "raw":
            table = self.__load_raw_table(table_name, years)
        elif form == "cleaned":
            table = self.__load_cleaned_table(table_name, years, settings)
        return table

    def __load_raw_table(self, table_name: str, years: list[int]) -> pd.DataFrame:
        table = pd.concat(
            [
                data_cleaner.load_raw_table(
                    table_name=table_name,
                    year=year,
                    lib_defaults=self.defautls,
                    lib_metadata=self.metadata,
                )
                for year in years
            ],
            ignore_index=True,
        )
        return table

    def __load_cleaned_table(
        self, table_name: str, years: list[int], settings: LoadTableSettings
    ):
        table = pd.concat(
            [
                data_engine.TableHandler(
                    [table_name],
                    year,
                    lib_defaults=self.defautls,
                    lib_metadata=self.metadata,
                    settings=settings,
                )[table_name]
                for year in years
            ],
            ignore_index=True,
        )
        return table

    def load_external_table(
        self,
        table_name,
        years: _Years,
        data_source: _DataSource | None = None,
        frequency: _Frequency | None = None,
        separate_by: _SeparateBy | None = None,
        reset_index: bool = True,
        **kwargs
    ) -> pd.DataFrame:
        years = self.utils.parse_years(years, table_name=table_name)
        external_data.load_table(
            table_name=table_name,
            lib_defaults=self.defautls,
            data_source=data_source,
            frequency=frequency,
            separate_by=separate_by,
            reset_index=reset_index,
            **kwargs,
        )
