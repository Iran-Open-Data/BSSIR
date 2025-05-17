"""Main module for loading and transforming HBSIR data.

This module provides the central interfaces for loading raw Iranian 
household data tables, transforming them, and constructing cleaned
derivative tables for analysis.

Key functions:

- extract_dependencies - Get dependencies for building a table 
- TableHandler - Loads multiple dependency tables
- Pipeline - Applies a sequence of transform steps to a table
- TableFactory - Loads and builds tables from different sources
- create_table - Constructs a table by loading multiple years  
- load_weights - Loads sample weights for a given year
- add_weights - Adds weights to a table

The module focuses on ETL (Extract, Transform, Load) functions to go 
from raw provided data tables to cleaned analytic tables.

Relies on metadata schema and configuration for how to process tables.

"""

import re
from pathlib import Path
from typing import Iterable
from types import ModuleType
import importlib
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import yaml

from . import decoder

from . import utils, data_cleaner
from .metadata_reader import Defaults, Metadata, LoadTableSettings


class TableHandler:
    """Handles loading multiple tables from parquet.

    Loads a set of tables by reading from local parquet files or downloading
    or generating if missing. Provides access to the tables via indexing.

    Attributes
    ----------
    table_list : list of str
        List of table names to load
    year : int
        Year for tables
    settings : LoadTable
        Settings for how to load the tables
    tables : dict of DataFrames
        Loaded tables keyed by table name

    """

    def __init__(
        self,
        table_list: Iterable[str],
        year: int,
        lib_defaults: Defaults,
        lib_metadata: Metadata,
        settings: LoadTableSettings | None = None,
    ) -> None:
        self.table_list = table_list
        self.year = year
        default_settings = lib_defaults.functions.load_table

        self.settings = default_settings if settings is None else settings
        self.lib_defaults = lib_defaults
        self.lib_metadata = lib_metadata

        self.tables: dict[str, pd.DataFrame] = self.setup()

    def __getitem__(self, table_name: str) -> pd.DataFrame:
        """Get a table by name.

        Parameters
        ----------
        table_name : str
            Name of table to retrieve

        Returns
        -------
        table : DataFrame
            Table loaded for the given name

        """
        return self.tables[table_name]

    def get(self, names: str | Iterable[str]) -> list[pd.DataFrame]:
        """Get multiple tables by name.

        Parameters
        ----------
        names : str or list of str
            Name(s) of tables to retrieve

        Returns
        -------
        tables : list of DataFrames
            Requested tables loaded

        """
        names = [names] if isinstance(names, str) else names
        return [self[name] for name in names]

    def setup(self) -> dict[str, pd.DataFrame]:
        """Set up the handler by loading all tables.

        Loads all of the configured tables in parallel using a
        ThreadPoolExecutor.

        Returns
        -------
        tables : dict of DataFrames
            Dictionary of the loaded tables by name.

        """
        with ThreadPoolExecutor(max_workers=6) as executer:
            tables = zip(
                self.table_list, executer.map(self.read_table, self.table_list)
            )
        return dict(tables)

    def read_table(self, table_name: str) -> pd.DataFrame:
        """Read a single table by name.

        Parameters
        ----------
        table_name : str
            Name of the table to load

        Returns
        -------
        table : DataFrame
            Loaded table data

        """

        if self.settings.recreate:
            table = self._create_table(table_name)
        elif self.settings.redownload:
            table = self._download_table(table_name)
        elif self.get_local_path(table_name).exists():
            table = self._load_table(table_name)
        elif self.settings.on_missing == "create":
            table = self._create_table(table_name)
        elif self.settings.on_missing == "download":
            table = self._download_table(table_name)
        else:
            raise FileNotFoundError

        table.attrs["table_name"] = table_name
        table.attrs["year"] = self.year
        return table

    def get_local_path(self, table_name) -> Path:
        file_name = f"{self.year}_{table_name}.parquet"
        local_path = self.lib_defaults.dirs.cleaned.joinpath(file_name)
        return local_path

    def _create_table(self, table_name: str) -> pd.DataFrame:
        table = data_cleaner.load_raw_table(
            table_name,
            self.year,
            lib_defaults=self.lib_defaults,
            lib_metadata=self.lib_metadata,
        )
        table = data_cleaner.clean_table(
            table, table_name=table_name, year=self.year, lib_metadata=self.lib_metadata
        )
        if self.settings.save_created:
            table.to_parquet(self.get_local_path(table_name))
        return table

    def _download_table(self, table_name: str) -> pd.DataFrame:
        table = pd.read_parquet(
            f"{self.lib_defaults.online_dirs.cleaned}/{self.year}_{table_name}.parquet"
        )
        if self.settings.save_downloaded:
            table.to_parquet(self.get_local_path(table_name))
        return table

    def _load_table(self, table_name: str) -> pd.DataFrame:
        return pd.read_parquet(self.get_local_path(table_name))


class Pipeline:
    """Applies a sequence of transformation steps to a DataFrame.

    This class allows chaining together a set of predefined steps
    for cleaning, transforming, and processing a DataFrame representing
    a table of data. The steps are configured by passing a list of
    operations which are applied in sequence.

    Attributes
    ----------
    table : DataFrame
        The input DataFrame that steps are applied to
    steps : list
        The sequence of step functions to apply
    properties : dict
        Additional properties passed to steps

    """

    def __init__(
        self,
        table: pd.DataFrame,
        *,
        steps: list,
        pipeline_params: dict,
        settings: LoadTableSettings,
    ) -> None:
        self.table = table
        self.steps = steps
        self.pipeline_params = pipeline_params
        self.settings = settings
        self.modules: dict[str, ModuleType] = {}

    def run(self) -> pd.DataFrame:
        """Run the pipeline on the table.

        Iterates through the step functions in the pipeline
        and applies them sequentially to transform the table.

        Returns
        -------
        table : DataFrame
            The transformed table after applying all steps.
        """
        for step in self.steps:
            if step is None:
                continue
            method_name, method_input = self._extract_method_name(step)
            if method_input is None:
                getattr(self, f"_{method_name}")()
            else:
                getattr(self, f"_{method_name}")(method_input)
        return self.table

    def _extract_method_name(self, instruction):
        if isinstance(instruction, str):
            method_name = instruction
            method_input = None
        elif isinstance(instruction, dict):
            method_name, method_input = list(instruction.items())[0]
        else:
            raise TypeError
        return method_name, method_input

    def _add_year(self) -> None:
        self.table["Year"] = self.pipeline_params["year"]

    def _filter_year(self) -> None:
        filt = self.table["Year"] == self.pipeline_params["year"]
        self.table = self.table.loc[filt]

    def _add_table_name(self) -> None:
        self.table["Table_Name"] = self.pipeline_params["table_name"]

    def _add_classification(self, method_input: dict | None = None) -> None:
        if method_input is None:
            return
        method_input["lib_defaults"] = self.pipeline_params["lib_defaults"]
        method_input["lib_metadata"] = self.pipeline_params["lib_metadata"]
        settings = decoder.DecoderSettings(**method_input)
        self.table = decoder.Decoder(self.table, settings).add_classification()

    def _add_attribute(self, method_input: dict | None = None) -> None:
        if method_input is None:
            return
        method_input["lib_defaults"] = self.pipeline_params["lib_defaults"]
        method_input["lib_metadata"] = self.pipeline_params["lib_metadata"]
        settings = decoder.IDDecoderSettings(**method_input)
        self.table = decoder.IDDecoder(self.table, settings).add_attribute()

    def _apply_order(self, method_input: list):
        new_order = [
            column if isinstance(column, str) else list(column.keys())[0]
            for column in method_input
        ]
        types = {
            list(column.keys())[0]: list(column.values())[0]
            for column in method_input
            if isinstance(column, dict)
        }

        self.table = self.table[list(new_order)].astype(types)

    def _rename(self, method_input: dict | None = None) -> None:
        if method_input is None:
            return
        self.table = self.table.rename(columns=method_input)

    def _create_column(self, method_input: dict | None = None) -> None:
        if method_input is None:
            return
        column_name = method_input["name"]
        if method_input["type"] == "numerical":
            expression = method_input["expression"]
            self.__apply_numerical_instruction(column_name, expression)
        elif method_input["type"] == "categorical":
            categories = method_input["categories"]
            self.__apply_categorical_instruction(column_name, categories)

    def __apply_numerical_instruction(self, column_name, expression: int | str) -> None:
        if isinstance(expression, int):
            self.table.loc[:, column_name] = expression
        else:
            self.table[column_name] = self.table.eval(expression, engine="python")

    def __apply_categorical_instruction(
        self, column_name: str, categories: dict
    ) -> None:
        categorical_column = pd.Series(
            index=self.table.index,
            dtype=pd.CategoricalDtype(list(categories.keys()))
        )

        for category, condition in categories.items():
            filt = self.__construct_filter(column_name, condition)
            categorical_column.loc[filt] = category

        self.table[column_name] = categorical_column

    def __construct_filter(self, column_name, condition) -> pd.Series:
        if condition is None:
            filt = self.table.index.to_series()
        elif isinstance(condition, str):
            filt = self.table[column_name] == condition
        elif isinstance(condition, list):
            filt = self.table[column_name].isin(condition)
        elif isinstance(condition, dict):
            filts = []
            for other_column, value in condition.items():
                if isinstance(value, (bool, str)):
                    filts.append(self.table[other_column] == value)
                elif isinstance(value, list):
                    filts.append(self.table[other_column].isin(value))
                else:
                    raise KeyError
            filt_sum = pd.concat(filts, axis="columns").sum(axis="columns")
            filt = filt_sum == len(condition)
        else:
            raise KeyError
        return filt

    def _apply_filter(self, conditions: str | list[str] | None = None) -> None:
        if conditions is None:
            return
        conditions = [conditions] if isinstance(conditions, str) else conditions
        for condition in conditions:
            self.table = self.table.query(condition)

    def _apply_pandas_function(self, method_input: str | None = None) -> None:
        if method_input is None:
            return
        method_input = "self.table" + method_input.replace("\n", "")
        table = pd.eval(method_input, target=self.table, engine="python")
        assert isinstance(table, pd.DataFrame)
        self.table = table

    def _apply_function(self, method_input: str | None = None) -> None:
        if method_input is None:
            return
        module_name, func_name = method_input.rsplit(".", 1)
        self.__load_module(module_name)
        func = getattr(self.modules[module_name], func_name)
        self.table = func(self.table)

    def __load_module(self, module_name: str) -> None:
        if module_name not in self.modules:
            self.modules[module_name] = importlib.import_module(module_name)

    def _join(self, method_input: dict | str | None = None):
        if method_input is None:
            return
        if isinstance(method_input, str):
            table_name = method_input
            columns = ["Year", "ID"]
            years = None
        elif isinstance(method_input, dict):
            table_name = method_input["table_name"]
            columns = method_input["columns"]
            years = method_input.get("year", None)
        else:
            raise TypeError
        years = list(self.table["Year"].unique())
        other_table = create_normalized_table(
            table_name,
            years,
            lib_defaults=self.pipeline_params["lib_defaults"],
            lib_metadata=self.pipeline_params["lib_metadata"],
            settings=self.settings,
        )
        self.table = self.table.merge(other_table, on=columns)

    def _dropna(self, method_input: str | list | None = None) -> None:
        if method_input is None:
            return
        self.table.dropna(subset=method_input, inplace=True)

    def _fillna(self, method_input: str | list | dict | None = None) -> None:
        if method_input is None:
            return
        if isinstance(method_input, str):
            method_input = [method_input]
        if isinstance(method_input, list):
            method_input = {column: 0 for column in method_input}
        for column, replacement in method_input.items():
            self.table[column] = self.table[column].fillna(replacement)
    

class TableFactory:
    """Builds DataFrames representing tables of data.

    This class handles loading or constructing DataFrames representing
    different tables of data. It builds tables either from original
    source parquet files, by querying cached results, or by dynamically
    constructing the table from other tables based on a schema.

    """

    def __init__(
        self,
        table_name: str,
        year: int,
        *,
        lib_defaults: Defaults,
        lib_metadata: Metadata,
        settings: LoadTableSettings,
    ):
        self.table_name = table_name
        self.year = year
        self.lib_defaults = lib_defaults
        self.lib_metadata = lib_metadata
        self.settings = settings

        schema = utils.resolve_metadata(lib_metadata.schema, year)
        if isinstance(schema, dict):
            self.schema = dict(schema)
        else:
            raise ValueError("Invalid Schema")

        if table_name in self.schema:
            table_schema = self.schema.get(table_name)
            assert isinstance(table_schema, dict)
            self.table_schema = table_schema
        else:
            self.table_schema = {}

        dependencies = self.extract_dependencies(table_name, year)
        original_table_list = [
            table
            for table, props in dependencies.items()
            if ("size" in props) and ("external." not in table)
        ]
        self.table_handler = TableHandler(
            original_table_list,
            year,
            lib_defaults=self.lib_defaults,
            lib_metadata=self.lib_metadata,
            settings=self.settings,
        )

    def load(self, table_name: str | None = None) -> pd.DataFrame:
        """Load the table.

        Builds the table according to the configured settings.
        Will attempt to read from cache first before building
        dynamically from a schema.

        Parameters
        ----------
        table_name : str, optional
            Table to load, will use instance table_name if not specified

        Returns
        -------
        table : DataFrame
            The loaded table data

        """
        table_name = self.table_name if table_name is None else table_name

        if all(
            [
                table_name not in self.lib_metadata.tables["table_availability"],
                table_name not in self.schema,
            ]
        ):
            raise ValueError
        if self.schema.get(table_name, {}).get("cache_result", False):
            try:
                table = self.read_cached_table(table_name)
            except FileNotFoundError:
                table = self._construct_schema_based_table(table_name)
                self.save_cache(table, table_name)
        elif "table_list" in self.schema.get(table_name, {}):
            table = self._construct_schema_based_table(table_name)
        elif table_name in self.lib_metadata.tables["table_availability"]:
            table = self.table_handler[table_name]
            if not table.empty and (table_name in self.schema):
                table = self._apply_schema(table, table_name)
        else:
            raise ValueError
        return table

    def extract_dependencies(
        self,
        table_name: str,
        year: int,
    ) -> dict:
        """Extract the dependencies of a table based on the metadata schema.

        For the given table name and year, traverses the schema metadata to find
        all upstream dependencies that are required to construct the table.

        Recursively extracts dependencies of dependencies until only base tables
        remain. Base tables have their file size stored instead of further dependencies.

        Parameters
        ----------
        table_name : str
            Name of the target table to extract dependencies for

        year : int
            Year to extract schema dependencies for

        Returns
        -------
        dependencies : dict
            Dictionary with dependencies in the format:
            {table_name: {"dependencies": {dep1: {}, dep2: {}}},
            table_name2: {"size": 1024}}
        """
        table_list = [table_name]
        dependencies: dict[str, dict] = {}
        while len(table_list) > 0:
            table = table_list.pop(0)
            if table.split(".")[0] == "external":
                file_name = f"{table.split('.')[1]}.parquet"
                local_path = self.lib_defaults.dirs.external.joinpath(file_name)
                size = local_path.stat().st_size if local_path.exists() else None
                dependencies[table] = {"size": size}
            elif "table_list" in self.lib_metadata.schema[table]:
                dependencies[table] = self.lib_metadata.schema[table]
                upstream_tables = utils.resolve_metadata(
                    self.lib_metadata.schema[table]["table_list"], year=year
                )
                if isinstance(upstream_tables, str):
                    upstream_tables = [upstream_tables]
                assert isinstance(upstream_tables, list)
                table_list.extend(upstream_tables)
            elif table in self.lib_metadata.tables["table_availability"]:
                file_name = f"{year}_{table}.parquet"
                local_path = self.lib_defaults.dirs.cleaned.joinpath(file_name)
                size = local_path.stat().st_size if local_path.exists() else None
                dependencies[table] = {"size": size}
            else:
                raise ValueError
        return dependencies

    def read_cached_table(
        self,
        table_name: str,
    ) -> pd.DataFrame:
        """Read a cached table if dependencies are unchanged.

        Checks that the dependencies of the cached table match the
        current dependencies before reading from the cached parquet file.

        Raises FileNotFoundError if dependencies have changed.

        Parameters
        ----------
        table_name : str
            Name of table to read from cache

        Returns
        -------
        table : DataFrame
            The cached table data

        Raises
        ------
        FileNotFoundError
            If cached dependencies are out of date

        """
        if not self.check_table_dependencies(table_name):
            raise FileNotFoundError
        file_name = f"{table_name}_{self.year}.parquet"
        file_path = self.lib_defaults.dirs.cached.joinpath(file_name)
        table = pd.read_parquet(file_path)
        return table

    def check_table_dependencies(self, table_name: str) -> bool:
        """Check if cached dependencies match current dependencies.

        Compares the dependencies recorded in the cache metadata file
        to the currently extracted dependencies for the table.

        Parameters
        ----------
        table_name : str
            Table name to check dependencies for

        Returns
        -------
        match : bool
            True if dependencies match, False otherwise

        """
        file_name = f"{table_name}_{self.year}_metadata.yaml"
        cach_metadata_path = self.lib_defaults.dirs.cached.joinpath(file_name)
        with open(cach_metadata_path, encoding="utf-8") as file:
            cach_metadata = yaml.safe_load(file)
        file_dependencies = cach_metadata["dependencies"]
        current_dependencies = self.extract_dependencies(table_name, self.year)
        return file_dependencies == current_dependencies

    def save_cache(
        self,
        table: pd.DataFrame,
        table_name: str,
    ) -> None:
        """Save table to cache along with metadata.

        Saves the table to a parquet file and saves metadata about
        dependencies to a yaml file.

        Parameters
        ----------
        table : DataFrame
            Table data to cache
        table_name : str
            Name of table being cached

        """
        file_name = f"{table_name}_{self.year}.parquet"
        file_path = self.lib_defaults.dirs.cached.joinpath(file_name)
        file_name = f"{table_name}_{self.year}_metadata.yaml"
        cach_metadata_path = self.lib_defaults.dirs.cached.joinpath(file_name)
        file_metadata = {
            "dependencies": self.extract_dependencies(table_name, self.year)
        }
        with open(cach_metadata_path, mode="w", encoding="utf-8") as file:
            yaml.safe_dump(file_metadata, file)
        table.to_parquet(file_path, index=False)

    def _apply_schema(
        self,
        table: pd.DataFrame,
        table_name: str,
    ):
        if "instructions" not in self.schema[table_name]:
            return table

        steps = self.schema[table_name]["instructions"]
        assert isinstance(steps, list)
        pipeline_params = {
            "table_name": table_name,
            "year": self.year,
            "lib_defaults": self.lib_defaults,
            "lib_metadata": self.lib_metadata,
        }
        table = Pipeline(
            table=table,
            steps=steps,
            pipeline_params=pipeline_params,
            settings=self.settings,
        ).run()
        return table

    def _construct_schema_based_table(self, table_name: str) -> pd.DataFrame:
        if table_name not in self.schema:
            raise KeyError(f"Table name {table_name} is not available in schema")
        table_names = self.schema[table_name]["table_list"]
        assert isinstance(table_names, (str, list))

        table_list = self._collect_schema_tables(table_names)

        table = self._create_concat_table(
            table_list,
            self.schema[table_name].get("concat_options", {}),
        )

        table = self._apply_schema(table, table_name)
        return table

    def _create_concat_table(
        self,
        table_list: list[pd.DataFrame],
        concat_options: dict,
    ) -> pd.DataFrame:
        if "merge_on" in concat_options:
            concat_options["left_on"] = concat_options["merge_on"]
            concat_options["right_on"] = concat_options["merge_on"]
            del concat_options["merge_on"]

        if len(concat_options) == 0:
            table = pd.concat(table_list)
        elif "on_columns" in concat_options:
            concat_options["axis"] = "columns"
            table_list = [table.set_index(concat_options["on_columns"]) for table in table_list]
            del concat_options["on_columns"]
            table = pd.concat(table_list, **concat_options).reset_index()
        elif ("left_on" in concat_options) and ("right_on" in concat_options):
            assert len(table_list) == 2
            table = pd.merge(table_list[0], table_list[1], **concat_options)
        else:
            raise ValueError

        return table

    def _collect_schema_tables(
        self, table_names: str | list[str]
    ) -> list[pd.DataFrame]:
        api_file: ModuleType = importlib.import_module(
            f"{self.lib_defaults.package_name.lower()}.api"
        )
        api = getattr(api_file, "api")
        table_names = [table_names] if isinstance(table_names, str) else table_names
        table_list = [
            self.load(name)
            if name.split(".")[0] != "external"
            else api.load_external_table(name.split(".")[1])
            for name in table_names
        ]
        table_list = [table for table in table_list if not table.empty]
        return table_list


def create_normalized_table(
    table_name: str,
    years: list[int],
    lib_defaults: Defaults,
    lib_metadata: Metadata,
    settings: LoadTableSettings,
) -> pd.DataFrame:
    """Construct a table by loading it for multiple years.

    Loads the specified table for each year in the provided
    range of years. Concatenates the individual tables into
    one table indexed by year.

    Parameters
    ----------
    table_name : str
        Name of table to load
    years : int or list of int
        Years to load table for
    settings : LoadTable, optional
        Settings for how to load each table

    Returns
    -------
    table : DataFrame
        Table concatenated across specified years

    """
    table_list = []
    for year in years:
        table = TableFactory(
            table_name,
            year,
            lib_defaults=lib_defaults,
            lib_metadata=lib_metadata,
            settings=settings,
        ).load()
        table_list.append(table)
    table = pd.concat(table_list, ignore_index=True)
    return table
