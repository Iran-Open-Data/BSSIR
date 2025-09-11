"""
Module for cleaning raw data into proper format
"""
import logging
from typing import Any, Literal

import pandas as pd

from .metadata_reader import Defaults, Metadata
from . import utils


PANDAS_NUMERICALS = [
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
    """Reads raw CSV file(s) for a specific table and year into a DataFrame.

    This function locates the directory for the given year, resolves the file
    name patterns from metadata, finds all matching CSV files, and concatenates
    them. All data is read as strings to prevent automatic type inference.

    Parameters
    ----------
    table_name : str
        The name of the table to load, as defined in the metadata.
    year : int
        The year of the data to load.
    lib_defaults : Defaults
        A configuration object providing directory paths.
    lib_metadata : Metadata
        A metadata object with file codes and table definitions.

    Returns
    -------
    pd.DataFrame
        A DataFrame with the concatenated data.

    Raises
    ------
    FileNotFoundError
        If the data directory for the year does not exist, or if no raw CSV
        files matching the metadata's patterns are found.
    KeyError
        If the `table_name` is not found in the metadata.
    TypeError
        If the resolved 'file_code' from metadata is not a string or a list
        of strings.
    """
    year_directory = lib_defaults.dir.extracted.joinpath(str(year))
    if not year_directory.is_dir():
        msg = f"Extracted data directory not found for year {year}: {year_directory}"
        logging.error(msg)
        raise FileNotFoundError(msg)

    file_code = utils.resolve_metadata(
        lib_metadata.tables[table_name]["file_code"], year
    )
    file_patterns = _normalize_file_patterns(file_code)

    file_paths = [
        path for pattern in file_patterns for path in year_directory.glob(pattern)
    ]

    if not file_paths:
        msg = f"No raw files found for table '{table_name}' in year {year}."
        logging.error(msg)
        raise FileNotFoundError(msg)

    logging.info(
        f"Loading {len(file_paths)} file(s) for table '{table_name}' in year {year}."
    )

    tables_to_concat = [pd.read_csv(path, dtype=str) for path in file_paths]
    return pd.concat(tables_to_concat, ignore_index=True)


def _normalize_file_patterns(file_code: Any) -> list[str]:
    """Validates and normalizes the file_code into a list of strings."""
    if isinstance(file_code, str):
        return [file_code]
    if isinstance(file_code, list):
        if not all(isinstance(item, str) for item in file_code):
            raise TypeError("If 'file_code' is a list, all items must be strings.")
        return file_code
    raise TypeError(
        f"Expected 'file_code' as str or list[str], got {type(file_code).__name__}."
    )


def clean_table(
    table: pd.DataFrame,
    *,
    table_name: str,
    year: int,
    lib_metadata: Metadata,
) -> pd.DataFrame:
    """Applies metadata-driven cleaning transformations to a DataFrame.

    This function orchestrates the cleaning of a given table by resolving the
    appropriate metadata for the specified year and applying the cleaning rules.
    It acts as a high-level wrapper around the `_apply_metadata_to_table` function.

    Parameters
    ----------
    table : pd.DataFrame
        The raw pandas DataFrame to be cleaned.
    table_name : str
        The name of the table, used to look up cleaning rules in the metadata.
    year : int
        The year of the data, used for resolving year-specific metadata.
    lib_metadata : Metadata
        An object containing all table definitions and cleaning rules.

    Returns
    -------
    pd.DataFrame
        A new, cleaned DataFrame with all transformations applied.

    Raises
    ------
    KeyError
        If the `table_name` or "default_settings" are not found in the metadata.
    """
    table_metadata = utils.resolve_metadata(lib_metadata.tables[table_name], year)

    if not isinstance(table_metadata, dict):
        actual_type = type(table_metadata).__name__
        msg = f"Metadata must be a dictionary, but got {actual_type} for year {year}."
        raise TypeError(msg)

    default_settings = lib_metadata.tables["default_settings"]

    cleaned_table = _apply_metadata_to_table(
        table=table,
        table_metadata=table_metadata,
        default_settings=default_settings,
    )
    return cleaned_table


def _apply_metadata_to_table(
    table: pd.DataFrame, table_metadata: dict, default_settings: dict
) -> pd.DataFrame:
    """Applies metadata rules to clean all columns in a table.

    This function iterates through the input table's columns, applies metadata-driven
    transformations like renaming and type casting, and returns a new, cleaned
    DataFrame.

    Parameters
    ----------
    table : pd.DataFrame
        The raw pandas DataFrame to be cleaned.
    table_metadata : dict
        A dictionary of cleaning rules specific to this table.
    default_settings : dict
        A dictionary of fallback settings.

    Returns
    -------
    pd.DataFrame
        A new pandas DataFrame with all cleaning rules applied.

    Raises
    ------
    ValueError
        If a column is found that is not defined in the metadata
        and the table's 'missings' setting is 'error'.
    """
    table_settings = default_settings.copy()
    table_settings.update(table_metadata.get("settings", {}))

    processed_columns = {}
    for column_name, column_data in table.items():
        if not isinstance(column_name, str):
            actual_type = type(column_name).__name__
            msg = (
                "Column names must be strings, but got a column of type "
                f"'{actual_type}'."
            )
            raise TypeError(msg)
        column_metadata = _get_column_metadata(
            table_metadata=table_metadata,
            column_name=column_name,
            table_settings=table_settings,
        )

        if column_metadata == "drop":
            continue
        elif column_metadata == "error":
            raise ValueError(
                f"Column '{column_name}' not in metadata and policy is 'error'."
            )

        cleaned_column = _apply_metadata_to_column(column_data, column_metadata)
        new_name = column_metadata["new_name"]
        processed_columns[new_name] = cleaned_column

    return pd.DataFrame(processed_columns)


def _get_column_metadata(
    *, table_metadata: dict, column_name: str, table_settings: dict
) -> dict | Literal["drop", "error"]:
    """Retrieves and validates metadata for a single column.

    This function finds the metadata for a given column name, handling case-
    insensitivity. If the column is not found in the metadata, it returns a
    fallback behavior ('drop' or 'error') defined in the table settings.

    Parameters
    ----------
    table_metadata : dict
        The metadata dictionary for the entire table.
    column_name : str
        The name of the column to look up.
    table_settings : dict
        A dictionary of settings for the table, including fallback rules.

    Returns
    -------
    dict | Literal["drop", "error"]
        A dictionary of metadata for the column, or a string indicating
        the action to take ('drop' or 'error').

    Raises
    ------
    ValueError
        If the metadata is malformed (e.g., 'columns' is not a dict),
        or if a column's metadata or the fallback setting is invalid.
    KeyError
        If 'columns' or 'missings' keys are not found where expected.
    """
    all_columns_meta = table_metadata["columns"]
    if not isinstance(all_columns_meta, dict):
        raise ValueError(
            f"'columns' key in table metadata must be a dict, but got "
            f"{type(all_columns_meta).__name__}."
        )

    # Normalize keys to uppercase for case-insensitive lookup
    normalized_columns_meta = {str(k).upper(): v for k, v in all_columns_meta.items()}

    # Look for the column's specific metadata entry
    column_meta_entry = normalized_columns_meta.get(column_name.upper())

    if column_meta_entry is not None:
        # Column metadata was found, so validate and return it.
        if isinstance(column_meta_entry, dict) or column_meta_entry == "drop":
            return column_meta_entry
        raise ValueError(
            f"Invalid metadata for column '{column_name}'. "
            f"Expected a dict or 'drop', got {type(column_meta_entry).__name__}."
        )
    else:
        # Column not found, so use the fallback behavior from settings.
        fallback_behavior = table_settings["missings"]
        if fallback_behavior not in ("drop", "error"):
            raise ValueError(
                "Invalid 'missings' setting. Must be 'drop' or 'error', "
                f"but got '{fallback_behavior}'."
            )
        return fallback_behavior


def _apply_metadata_to_column(
    column: pd.Series, column_metadata: dict
) -> pd.Series:
    """Applies value replacement and type conversion to a single column.

    This function performs two main operations based on the provided metadata:
    1. Replaces values in the column using a mapping dictionary.
    2. Applies a final data type to the column via a helper function.

    Parameters
    ----------
    column : pd.Series
        The raw pandas Series (column data) to be cleaned.
    column_metadata : dict
        A dictionary containing cleaning rules, potentially including a
        'replace' dictionary and a 'type' definition.

    Returns
    -------
    pd.Series
        A new, cleaned pandas Series with transformations applied.
    """
    if replace_map := column_metadata.get("replace"):
        column = column.replace(replace_map)

    column = _apply_type_to_column(column, column_metadata)

    return column


def _apply_type_to_column(column: pd.Series, column_metadata: dict) -> pd.Series:
    """
    Applies cleaning and type conversion to a column based on metadata.

    This function first applies a general string cleaning routine and then
    converts the column to the target data type specified in the metadata.
    This ensures consistent data cleaning and robust type casting.

    Parameters
    ----------
    column : pd.Series
        The pandas Series (column data) to be transformed.
    column_metadata : dict
        A dictionary of metadata rules for the column, including the target 'type'.

    Returns
    -------
    pd.Series
        A new, cleaned, and correctly-typed pandas Series.

    Raises
    ------
    KeyError
        If required metadata keys (e.g., 'true_condition' for booleans or
        'categories' for categories) are missing.
    ValueError
        If the 'type' specified in the metadata is not valid or supported.
    """
    target_type = column_metadata.get("type", "string")

    if target_type == "string":
        return column

    cleaned_column = _general_cleaning(column.copy())

    if target_type == "boolean":
        true_condition = column_metadata.get("true_condition")
        if true_condition is None:
            raise KeyError(
                f"Column '{column.name}' with type 'boolean' is missing the "
                "'true_condition' key in its metadata."
            )
        return (cleaned_column == true_condition).astype("boolean")

    if target_type in ("unsigned", "integer", "float"):
        return pd.to_numeric(cleaned_column, downcast=target_type)

    if target_type in PANDAS_NUMERICALS:
        return cleaned_column.astype(target_type)

    if target_type == "category":
        categories_map = column_metadata.get("categories")
        if categories_map is None:
            raise KeyError(
                f"Column '{column.name}' with type 'category' is missing the "
                "'categories' map in its metadata."
            )
        return (
            cleaned_column
            .astype("category")
            .cat.rename_categories(categories_map)
        )

    raise ValueError(
        f"Metadata for column '{column.name}' contains an invalid type: "
        f"'{target_type}'."
    )


def _general_cleaning(column: pd.Series) -> pd.Series:
    """
    Cleans a pandas Series by removing unwanted characters and standardizing format.
    """
    # Define a regex pattern for characters to be completely removed.
    chars_to_remove = r"\n\r\,\@\+\*\[\]\_\?\&\s"

    cleaned_column = (
        column

        # Replace middle dot '·' (ASCII 183) with a standard period.
        .str.replace(chr(183), ".", regex=False)

        # Remove any trailing periods.
        .str.rstrip(".")
        
        # Remove all characters defined in the chars_to_remove set.
        .str.replace(f"[{chars_to_remove}]+", "", regex=True)
        
        # Replace fields containing only whitespace/periods/hyphens with None.
        .replace(r"^[\s\.\-]*$", None, regex=True)
    )
    
    return cleaned_column
