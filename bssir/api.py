from typing import Any, Callable, Literal, Iterable
from types import ModuleType
import shutil
import importlib

import pandas as pd

from .metadata_reader import Defaults, Metadata, _Years, LoadTableSettings
from . import archive_handler, data_cleaner, external_data, data_engine, decoder
from .utils import Utils

_DataSource = Literal["SCI", "CBI"]
_Frequency = Literal["Annual", "Monthly"]
_SeparateBy = Literal["Urban_Rural", "Province"]


class API:
    def __init__(self, defaults: Defaults, metadata: Metadata):
        self.defaults = defaults
        self.metadata = metadata
        self.utils = Utils(defaults, metadata)

    def setup(
        self,
        years: _Years,
        *,
        table_names: str | Iterable[str] = None,
        replace: bool = False,
        method: Literal["create_from_raw", "download_cleaned"] = "download_cleaned",
        download_source: Literal["original", "mirror"] = "mirror",
    ) -> None:
        """Download, extract, and clean survey data."""
        years = self.utils.parse_years(years)
        if method == "create_from_raw":
            self.setup_raw_data(years, replace=replace, download_source=download_source)
            self._create_cleaned_files(years=years, table_names=table_names)
        else:
            self.utils.download_cleaned_tables(years)

    def setup_raw_data(
        self,
        years: _Years,
        *,
        replace: bool = False,
        download_source: Literal["original", "mirror"] = "mirror",
    ) -> None:
        """Download and extract raw survey data."""
        years = self.utils.parse_years(years)
        archive_handler.download(
            years,
            replace=replace,
            source=download_source,
            lib_metadata=self.metadata,
            lib_defaults=self.defaults,
        )
        archive_handler.unpack(years, replace=replace, lib_defaults=self.defaults)
        archive_handler.extract(years, replace=replace, lib_defaults=self.defaults)

    def setup_config(self, replace=False) -> None:
        """Copy default config file to data directory."""
        src = self.defaults.base_package_dir.joinpath("config", "settings_sample.yaml")
        dst = self.defaults.root_dir.joinpath(self.defaults.local_settings)
        dst.parent.mkdir(parents=True, exist_ok=True)
        if (not dst.exists()) or replace:
            shutil.copy(src, dst)

    def load_table(self, table_name: str, years: _Years, **kwargs) -> pd.DataFrame:
        """Load a table for the given table name and year(s)."""
        settings = self.defaults.functions.load_table
        settings = settings.model_copy(update=kwargs)
        years = self.utils.parse_years(years, table_name=table_name)
        if settings.form == "raw":
            table = self._load_raw_table(table_name, years)
        elif settings.form == "cleaned":
            table = self._load_cleaned_table(table_name, years, settings)
        elif settings.form == "normalized":
            table = data_engine.create_normalized_table(
                table_name=table_name,
                years=years,
                settings=settings,
                lib_defaults=self.defaults,
                lib_metadata=self.metadata,
            )
        return table

    def _load_raw_table(self, table_name: str, years: list[int]) -> pd.DataFrame:
        table = pd.concat(
            [
                data_cleaner.load_raw_table(
                    table_name=table_name,
                    year=year,
                    lib_defaults=self.defaults,
                    lib_metadata=self.metadata,
                )
                for year in years
            ],
            ignore_index=True,
        )
        return table

    def _load_cleaned_table(
        self,
        table_name: str,
        years: list[int],
        settings: LoadTableSettings | None = None,
    ):
        table = pd.concat(
            [
                data_engine.TableHandler(
                    [table_name],
                    year,
                    lib_defaults=self.defaults,
                    lib_metadata=self.metadata,
                    settings=settings,
                )[table_name]
                for year in years
            ],
            ignore_index=True,
        )
        return table

    def _create_cleaned_files(
        self, years: list[int], table_names: str | list[str] = None
    ) -> None:
        if table_names is None:
            table_names = "all"
        table_year_pairs = self.utils.create_table_year_pairs(
            table_names=table_names, years=years
        )
        for table_name, year in table_year_pairs:
            self.__create_cleaned_file(table_name, year)

    def __create_cleaned_file(self, table_name: str, year: int) -> None:
        table = self._load_raw_table(table_name=table_name, years=[year])
        table = data_cleaner.clean_table(
            table, table_name=table_name, year=year, lib_metadata=self.metadata
        )
        file_name = f"{year}_{table_name}.parquet"
        table.to_parquet(self.defaults.dirs.cleaned.joinpath(file_name))

    def load_external_table(
        self,
        table_name: str,
        data_source: _DataSource | None = None,
        frequency: _Frequency | None = None,
        separate_by: _SeparateBy | None = None,
        reset_index: bool = True,
        **kwargs,
    ) -> pd.DataFrame:
        """Load an external table for the given table name and year(s)."""
        return external_data.load_table(
            table_name=table_name,
            lib_defaults=self.defaults,
            data_source=data_source,
            frequency=frequency,
            separate_by=separate_by,
            reset_index=reset_index,
            **kwargs,
        )

    def load_knowledge(self, name: str, years: _Years | None = None, **kwargs) -> Any:
        if years is not None:
            kwargs["years"] = self.utils.parse_years(years)
        module: ModuleType = importlib.import_module(
            f"{self.defaults.package_name.lower()}.knowledge_base.{name}"
        )
        main_function: Callable = getattr(module, "main")
        return main_function(**kwargs)

    def add_attribute(self, table: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Add attributes to table based on ID column."""
        kwargs.update({"lib_defaults": self.defaults, "lib_metadata": self.metadata})
        settings = decoder.IDDecoderSettings(**kwargs)
        table = decoder.IDDecoder(table=table, settings=settings).add_attribute()
        return table

    def add_classification(self, table: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Add industry, occupation, or commodity classification to table."""
        if "target" not in kwargs:
            potential_targets = []
            for column_name in table.columns:
                if self._is_potential_target(column_name):
                    potential_targets.append(column_name)
            if len(potential_targets) == 1:
                kwargs["target"] = potential_targets[0]
            else:
                raise ValueError("Target column not specified.")
        kwargs.update({"lib_defaults": self.defaults, "lib_metadata": self.metadata})
        settings = decoder.DecoderSettings(**kwargs)
        table = decoder.Decoder(table=table, settings=settings).add_classification()
        return table

    def add_weight(self, table: pd.DataFrame) -> pd.DataFrame:
        """Add sampling weight to table"""
        years = (
            decoder.extract_column(table, self.defaults.columns.year).unique().tolist()
        )
        weights = self.load_table("Weight", years)
        return table.merge(
            weights,
            how="left",
            on=[self.defaults.columns.year, self.defaults.columns.id],
        )

    def _is_potential_target(self, column_name) -> bool:
        for keywords in [
            self.defaults.columns.commodity_code,
            self.defaults.columns.industry_code,
            self.defaults.columns.occupation_code,
        ]:
            for keyword in keywords:
                if keyword.lower() in column_name.lower():
                    return True
        return False
