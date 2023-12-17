"""
Module for cleaning raw data into proper format
"""

from typing import Literal

import pandas as pd

from .metadata_reader import Defaults, Metadata
from . import utils


pandas_numerical_data_types = [
    "UInt8",
    "UInt16",
    "UInt32",
    "UInt64",
    "Int8",
    "Int16",
    "Int32",
    "Int64",
    "Float32",
    "Float64",
]


def load_raw_table(
    table_name: str,
    year: int,
    *,
    lib_defaults: Defaults,
    lib_metadata: Metadata,
) -> pd.DataFrame:
    """Reads CSV file(s) and returns DataFrame for table and year.

    Parameters
    ----------
    table_name : str
        Name of the table to read.

    year : int
        Year of the data to read.

    Returns
    -------
    DataFrame
        Concatenated table data from the CSV file(s).

    Raises
    ------
    FileNotFoundError
        If CSV file(s) not found.

    ValueError
        If invalid table name, year, or corrupt metadata.

    """
    file_code = utils.resolve_metadata(
        lib_metadata.tables[table_name]["file_code"], year
    )

    if isinstance(file_code, list):
        files = [
            lib_defaults.dirs.extracted.joinpath(str(year), f"{file}.csv")
            for file in list(file_code)
        ]
    elif isinstance(file_code, str) and (file_code.count("*") == 0):
        files = [lib_defaults.dirs.extracted.joinpath(str(year), f"{file_code}.csv")]
    elif isinstance(file_code, str):
        files = lib_defaults.dirs.extracted.joinpath(str(year)).glob(file_code)
    else:
        raise ValueError(f"Table {table_name} is not available for year {year}")

    table = pd.concat(
        [pd.read_csv(file, low_memory=False) for file in files],
        ignore_index=True,
    )

    return table


def clean_table(
    table: pd.DataFrame,
    *,
    table_name: str,
    year: int,
    lib_metadata: Metadata,
) -> pd.DataFrame:
    """Cleans table data using metadata transformations.

    Loads raw table data, applies cleaning ops based on metadata,
    and concatenates urban and rural tables.

    Useful as a preprocessing step before further analysis.
    Called by save_processed_tables() to clean each table.

    Parameters
    ----------
    table_name : _OriginalTable
        Name of table to clean.

    year : int
        Year of data to clean.

    Returns
    -------
    DataFrame
        Cleaned concatenated table data.

    """
    table_metadata = utils.resolve_metadata(lib_metadata.tables[table_name], year)
    default_settings = lib_metadata.tables["default_settings"]
    table = _apply_metadata_to_table(table, table_metadata, default_settings)
    return table


def _apply_metadata_to_table(
    table: pd.DataFrame, table_metadata: dict, default_settings: str
) -> pd.DataFrame:
    table_settings: dict = default_settings.copy()
    table_settings.update(table_metadata.get("settings", {}))
    cleaned_table = pd.DataFrame()
    for column_name, column in table.items():
        assert isinstance(column_name, str)
        column_metadata = _get_column_metadata(
            table_metadata=table_metadata,
            column_name=column_name,
            table_settings=table_settings,
        )
        if column_metadata == "drop":
            continue
        if column_metadata == "error":
            raise ValueError(
                f"Error: The column '{column_name}' was not found in the metadata."
            )
        column = _apply_metadata_to_column(column, column_metadata)
        cleaned_table[column_metadata["new_name"]] = column
    return cleaned_table


def _get_column_metadata(
    *, table_metadata: dict, column_name: str, table_settings: dict
) -> dict | Literal["drop", "error"]:
    column_name = column_name.upper()
    columns_metadata = table_metadata["columns"]
    if not isinstance(columns_metadata, dict):
        raise ValueError(
            f"Unvalid metadata for column {column_name}: \n {columns_metadata}"
        )
    columns_metadata = {key.upper(): value for key, value in columns_metadata.items()}
    if column_name in columns_metadata:
        column_metadata = columns_metadata[column_name]
        if not (isinstance(column_metadata, dict) or column_metadata == "drop"):
            print(column_name, table_metadata)
            raise ValueError(f"Metadata for column {column_name} is not valid")
    else:
        column_metadata: Literal["drop", "error"] = table_settings["missings"]
        if column_metadata not in ["drop", "error"]:
            raise ValueError("Missing treatment is not valid")
    return column_metadata


def _apply_metadata_to_column(column: pd.Series, column_metadata: dict) -> pd.Series:
    if ("replace" in column_metadata) and (column_metadata["replace"] is not None):
        column = column.replace(column_metadata["replace"])
    column = _apply_type_to_column(column, column_metadata)
    return column


def _apply_type_to_column(column: pd.Series, column_metadata: dict) -> pd.Series:
    column = _general_cleaning(column)
    new_column = pd.Series(None, index=column.index)
    if ("type" not in column_metadata) or (column_metadata["type"] == "string"):
        new_column = column.copy()
    elif column_metadata["type"] == "boolean":
        new_column = column == column_metadata["true_condition"]
        new_column = new_column.astype("boolean")
    elif column_metadata["type"] in ("unsigned", "integer", "float"):
        new_column = pd.to_numeric(column, downcast=column_metadata["type"])
    elif column_metadata["type"] in pandas_numerical_data_types:
        new_column = column.astype(column_metadata["type"])
    elif column_metadata["type"] == "category":
        new_column = column.astype("category")
        new_column = new_column.cat.rename_categories(column_metadata["categories"])
    else:
        raise ValueError("Type is not valid")
    return new_column


def _general_cleaning(column: pd.Series):
    if pd.api.types.is_numeric_dtype(column.dtype):
        return column
    chars_to_remove = r"\n\r\,\@\+\*\[\]\_\?\&"
    column = column.fillna("__missing__").astype("string").replace("__missing__", None)
    column = column.str.replace(chr(183), ".").str.rstrip(".")
    column = column.str.replace(f"[{chars_to_remove}]+", "", regex=True)
    column = column.str.replace(r"\b\-", "", regex=True)
    column = column.replace(r"^[\s\.\-]*$", None, regex=True)
    try:
        column = column.astype("Float64")
        column = column.astype("Int64")
    except ValueError:
        pass
    return column
