import logging
from typing import Any, Callable, Literal, Iterable, overload, Optional
from types import ModuleType
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

    def setup(self, **kwargs) -> None:
        """Download, extract, and clean survey data."""
        settings_model = self.defaults.functions.setup
        try:
            settings = settings_model.model_copy(update=kwargs)
        except Exception as exc:
            logging.error("Invalid settings for setup: %s", exc)
            raise
        logging.debug(f"Setup with settings: {settings}")
        years = self.utils.parse_years(settings.years)
        if settings.method == "create_from_raw":
            self.setup_raw_data(**settings.model_dump())
            self._create_cleaned_files(years=years, table_names=settings.table_names)
        elif settings.method == "download_cleaned":
            if (not settings.download_source) or (settings.download_source == "original"):
                source = "mirror"
            else:
                source = settings.download_source
            self.utils.download_cleaned_tables(years, source=source)
        else:
            raise ValueError

    def setup_raw_data(self, **kwargs) -> None:
        """Download and extract raw (unprocessed) survey tables for the requested years.

        This method:
        - Normalises and validates the requested years.
        - Merges runtime overrides from kwargs into the package default settings for
            the setup_raw_data function.
        - Calls archive_handler.setup(...) to download and extract archives into the
            configured "unpacked" / "extracted" directories.

        Parameters
        ----------
        years : _Years
            Year or iterable of years (see self.utils.parse_years for accepted forms).
        **kwargs
            Optional overrides for the defaults.functions.setup_raw_data model.
            Typical keys: replace (bool), download_source (str). Unknown keys will
            be passed to model_copy(update=...) and may raise if invalid.

        Raises
        ------
        ValueError
            If no years are resolved from the provided input.
        Exception
            Any exceptions raised by settings.model_copy(...) or archive_handler.setup
            are logged and re-raised to the caller.
        """
        settings_model = self.defaults.functions.setup_raw_data
        try:
            settings = settings_model.model_copy(update=kwargs)
        except Exception as exc:
            logging.error("Invalid settings for setup_raw_data: %s", exc)
            raise
        years = self.utils.parse_years(settings.years)

        archive_handler.setup(
            years,
            lib_metadata=self.metadata,
            lib_defaults=self.defaults,
            replace=settings.replace,
            download_source=settings.download_source,
        )

    def setup_config(
        self,
        mode: Literal["Standard", "Colab"] = "Standard",
        replace: bool = False,
    ) -> None:
        """Copy default config file to data directory."""
        if mode == "Standard":
            src_file_name = "settings_sample.yaml"
        else:
            src_file_name = f"settings_sample_{mode.lower()}.yaml"
        src = self.defaults.base_package_dir.joinpath("config", src_file_name)
        dst = self.defaults.root_dir.joinpath(self.defaults.local_settings)
        dst.parent.mkdir(parents=True, exist_ok=True)
        if (not dst.exists()) or replace:
            local_dir = str(self.defaults.root_dir.joinpath(self.defaults.local_dir))
            with open(src, mode="r", encoding="utf-8") as file:
                setup_text = file.read()
            setup_text = setup_text.replace("{{local_dir}}", local_dir)
            with open(dst, mode="w", encoding="utf-8") as file:
                file.write(setup_text)

    def load_table(self, table_name: str, years: _Years, **kwargs) -> pd.DataFrame:
        """Load a table for the given table name and year(s)."""
        settings = self.defaults.functions.load_table
        settings = settings.model_copy(update=kwargs)
        years = self.utils.parse_years(years, table_name=table_name, form=settings.form)
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
        else:
            raise ValueError
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
        self, years: list[int], table_names: str | Iterable[str] | None = None
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
        self.defaults.dir.cleaned.mkdir(exist_ok=True, parents=True)
        table.to_parquet(self.defaults.dir.cleaned.joinpath(file_name))

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
        _index = [self.defaults.columns.year, self.defaults.columns.id]
        weights = self.load_table("Weight", years).set_index(_index)
        return table.join(weights, how="left", on=_index)

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
