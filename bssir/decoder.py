"""Decodes household data using metadata mappings.

Provides functionality to resolve metadata versions, map codes 
to metadata attributes, and join decoded attributes onto data tables.

Classes
-------
CommodityDecoderSettings - Settings for commodity code decoding.
CommodityDecoder - Decodes commodity codes using metadata.
IDDecoderSettings - Settings for household ID decoding.  
IDDecoder - Decodes household IDs using metadata.

Functions
--------- 
read_classification_info - Reads classified metadata by name.
create_classification_table - Creates table from classified metadata.

The decoders resolve metadata versions, map codes to attributes, 
and add decoded columns to the input tables.

The functions provide helpers for resolving metadata and reading
classification info from the raw metadata.

"""
from itertools import product
from typing import Callable, Iterable, Literal, Annotated, Any, Optional

import pandas as pd
from pydantic import BaseModel, ConfigDict, BeforeValidator, Field

from .metadata_reader import Defaults, Metadata

from . import utils, external_data


def maybe_to_tuple(_input: Any) -> tuple:
    """Converts input to a tuple if needed.

    Parameters
    ----------
    _input : Any
        Input to convert.

    Returns
    -------
    tuple
        Input converted to a tuple.

    Notes
    -----
    If _input is already a tuple, returns it unchanged.
    If _input is a single value, returns it in a 1-tuple.
    If _input is an iterable, converts it to a tuple.
    """
    if isinstance(_input, tuple):
        return _input
    if isinstance(_input, str):
        return (_input,)
    if isinstance(_input, Iterable):
        return tuple(_input)
    return (_input,)


def extract_column(table: pd.DataFrame, column_name: str) -> pd.Series:
    """Extracts a column from a DataFrame as a Series.

    Checks table columns, index name(s) for the given column
    name and returns the matching column as a Series.

    Raises KeyError if column not found.

    Parameters
    ----------
    table : DataFrame
        DataFrame to extract column from.

    column_name : str
        Name of column to extract.

    Returns
    -------
    Series
        Extracted column as a Series.

    Raises
    ------
    KeyError
        If column not found in DataFrame.

    """
    if column_name in table.columns:
        column = table.loc[:, column_name].copy()
    elif isinstance(table.index, pd.Index) and table.index.name == column_name:
        column = table.index.to_series()
    elif isinstance(table.index, pd.MultiIndex) and column_name in table.index.names:
        column = table.index.to_frame().loc[:, column_name].copy()
    else:
        raise KeyError
    return column


_Aspects = Annotated[tuple[str, ...], BeforeValidator(maybe_to_tuple)]
_Levels = Annotated[tuple[int, ...], BeforeValidator(maybe_to_tuple)]
_ColumnNames = Annotated[tuple[str, ...], BeforeValidator(maybe_to_tuple)]


class DecoderSettings(BaseModel):
    """Settings for decoding classification codes.

    Attributes
    ----------
    target : str
        Column to decode.

    classification_type : Literal["commodity", "occupation", "industry"], optional
        Type of classification. Inferred automatically if not provided.

    name : str, default "original"
        Name of classification metadata to use.

    year_col : str, optional
        Column name for year. Inferred automatically if not provided.

    versioned_info : dict
        Classification metadata. Populated automatically based on name and
        classification_type.

    defaults : dict
        Default settings. Merged with configured settings.

    aspects : tuple[str]
        Labels to extract as output columns.

    levels : tuple[int]
         Hierarchy levels to extract.

    drop_value : bool, default False
        Whether to drop rows when value is missing.

    column_names : tuple[str]
        Names for extracted output columns.

    missing_value_replacements : dict
        Replacements for missing values in output columns.

    See Also
    --------
    Decoder : Uses these settings to decode codes.

    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    lib_defaults: Defaults
    lib_metadata: Metadata
    target: str
    classification_type: Optional[Literal["commodity", "occupation", "industry"]] = None
    name: str = "original"
    year_col: str = Field(None, validate_default=False)
    versioned_info: dict = {}
    defaults: dict = {}
    aspects: _Aspects = ()
    levels: _Levels = ()
    drop_value: bool = False
    column_names: _ColumnNames = ()
    missing_value_replacements: dict[str, str] = {}

    def model_post_init(self, __contex=None) -> None:
        if self.classification_type is None:
            self.classification_type = self._resolve_classification_type()

        if self.year_col is None:
            self.year_col = self.lib_defaults.columns.year

        if self.classification_type == "commodity":
            self.versioned_info = self.lib_metadata.commodities[self.name]
        elif self.classification_type == "industry":
            self.versioned_info = self.lib_metadata.industries[self.name]
        elif self.classification_type == "occupation":
            self.versioned_info = self.lib_metadata.occupations[self.name]
        else:
            raise ValueError(f"{self.classification_type} is not valid type")

        if "defaults" in self.versioned_info:
            self.defaults = self.versioned_info["defaults"]
        for key, value in self.defaults.items():
            if isinstance(value, list):
                value = tuple(value)
            if (getattr(self, key) is None) or (len(getattr(self, key)) == 0):
                setattr(self, key, value)
        if len(self.aspects) == 0:
            self.aspects = ("item_key",)
        if len(self.levels) == 0:
            self.levels = (1,)
        self._resolve_column_names()

    def _resolve_classification_type(
        self,
    ) -> Literal["commodity", "occupation", "industry"]:
        """Resolve the classification type based on keywords."""
        for name, keywords in [
            ("commodity", self.lib_defaults.columns.commodity_code),
            ("industry", self.lib_defaults.columns.industry_code),
            ("occupation", self.lib_defaults.columns.occupation_code),
        ]:
            for keyword in keywords:
                if keyword.lower() in self.target.lower():
                    return name
        raise ValueError("Cannot specify classification type")

    def _resolve_column_names(self) -> None:
        if len(self.column_names) == 0:
            names = [
                f"{label}_{level}"
                for label, level in product(self.aspects, self.levels)
            ]
            self.column_names = tuple(names)
        elif len(self.column_names) == len(self.aspects) * len(self.levels):
            pass
        elif len(self.column_names) == len(self.aspects):
            names = [
                f"{label}_{level}"
                for label, level in product(self.column_names, self.levels)
            ]
            self.column_names = tuple(names)

    @property
    def rename_dict(self):
        """Mapping of label-level keys to output column names.

        Returns a dictionary mapping each combination of labels
        and levels to the corresponding output column name.

        This can be used to rename the multi-level columns in the
        decoded mapping table to the configured output names.

        Returns
        -------
        dict
            Mapping of label-level tuples to output column names.

        """
        label_level = product(self.aspects, self.levels)
        return dict(zip(label_level, self.column_names))


class Decoder:
    """Decodes commodity codes using classification metadata.

    Parameters
    ----------
    table : DataFrame
        Table with code and year columns to decode.

    settings : DecoderSettings
        Decoding configuration settings.

    Attributes
    ----------
    classification_table : DataFrame
        Resolved classification metadata.

    year_code_pairs : DataFrame
        Unique year and code combinations.

    Methods
    -------
    create_mapping_table()
        Maps codes to metadata based on year.

    add_classification()
        Adds decoded columns to the input table.

    See Also
    --------
    DecoderSettings : Decoding configuration.

    """

    def __init__(self, table: pd.DataFrame, settings: DecoderSettings) -> None:
        self.table = table
        self.settings = settings
        self.code_column = extract_column(table, settings.target)
        self.year_column = extract_column(table, settings.year_col)
        self.classification_table = self.create_classification_table(
            years=self.year_column.drop_duplicates().to_list(),
        )
        self.year_code_pairs = self._create_year_code_pairs()

    def create_classification_table(
        self,
        years: Iterable[int],
    ) -> pd.DataFrame:
        """Creates classification table for given years.

        Loops through the provided years, reads the classification metadata,
        converts to annual DataFrames, concatenates the results, and returns
        the final classification table.

        Parameters
        ----------

        years : Iterable[int]
            Years to include in the resulting table.

        Returns
        -------
        DataFrame
            Classification table with a row for each year.

        See Also
        --------
        _create_annual_classification_table : Converts metadata to DataFrame.

        """
        table_list = []
        for year in years:
            classification_info = utils.resolve_metadata(
                self.settings.versioned_info, year, categorize=True
            )
            assert isinstance(classification_info, dict)
            annual_table = pd.DataFrame(classification_info.get("items"))
            annual_table["code_range"] = annual_table["code"].apply(
                utils.Argham,  # type: ignore
                default_start=self.settings.lib_defaults.years[0],
                default_end=self.settings.lib_defaults.years[-1] + 1,
                keywords=["code"],
            )
            annual_table = annual_table.drop(columns=["code"])
            annual_table.loc[:, "Year"] = year
            table_list.append(annual_table)
        table = pd.concat(table_list, ignore_index=True)
        return table

    def _create_year_code_pairs(self) -> pd.DataFrame:
        years = self.year_column.drop_duplicates()
        yc_pair_list = []
        for year in years:
            filt = self.year_column == year
            codes = self.code_column.loc[filt].drop_duplicates()
            yc_pair = codes.to_frame()
            yc_pair[self.settings.year_col] = year
            yc_pair_list.append(yc_pair)
        return pd.concat(yc_pair_list, ignore_index=True)

    def _build_year_code_table(
        self, year_code_pairs: pd.DataFrame, row: pd.Series
    ) -> pd.DataFrame:
        filt = (
            year_code_pairs[self.settings.target]
            .apply(lambda x: x in row["code_range"])
        )
        filt = filt & (
            year_code_pairs[self.settings.year_col] == row[self.settings.year_col]
        )
        matched_codes = year_code_pairs.loc[filt].set_index(
            [self.settings.year_col, self.settings.target]
        )
        columns = row.drop(["code_range", self.settings.year_col]).index
        code_table = pd.DataFrame(
            data=[row.loc[columns]] * len(matched_codes.index),
            index=matched_codes.index,
            columns=columns,
        )
        return code_table

    def create_mapping_table(self) -> pd.DataFrame:
        """Creates code mapping table from metadata.

        Loops through classification table and builds a mapping
        table linking codes to metadata based on year.

        Multi-index columns are renamed using the settings.
        Table is validated before returning.

        Returns
        -------
        DataFrame
            Mapping table with year, code and decoded columns.

        See Also
        --------
        _build_year_code_table : Builds table for a single row.

        _validate_mapping_table : Validates mapping integrity.

        """
        code_table_list = []
        for _, row in self.classification_table.iterrows():
            code_table = self._build_year_code_table(self.year_code_pairs, row)
            if not code_table.empty:
                code_table_list.append(code_table)
        mapping_table = pd.concat(code_table_list)
        mapping_table = mapping_table.set_index("level", append=True)
        self._validate_mapping_table(mapping_table)
        mapping_table = mapping_table.unstack(-1)
        mapping_table = mapping_table.loc[:, self.settings.rename_dict.keys()]  # type: ignore
        mapping_table.columns = self.settings.rename_dict.values()
        return mapping_table

    def _validate_mapping_table(self, mapping_table: pd.DataFrame):
        filt = mapping_table.index.duplicated(keep=False)
        if filt.sum() > 0:
            invalid_case_sample = (
                mapping_table.loc[filt]
                .sort_values([self.settings.target, "level"])
                .head(10)
            )
            raise ValueError(f"Classification is not valid \n{invalid_case_sample}")

    def _fill_missing_values(self):
        if "missing_value_replacements" not in self.settings.defaults:
            return
        for column, default in self.settings.defaults[
            "missing_value_replacements"
        ].items():
            if column not in self.table.columns:
                continue
            filt = self.table.loc[:, column].isna()
            self.table.loc[filt, column] = default  # type: ignore

    def add_classification(self):
        """Adds decoded columns to the input table.

        Joins the mapping table to the input table using
        the year and code columns.

        Also fills in any missing values based on defaults.

        Returns
        -------
        DataFrame
            Input table with decoded columns added.

        """
        mapping = self.create_mapping_table()
        self.table = self.table.join(
            mapping, on=[self.settings.year_col, self.settings.target]
        )
        self._fill_missing_values()
        return self.table


class IDDecoderSettings(BaseModel):
    """Settings for decoding household IDs.

    Attributes
    ----------
    name : Attribute
        Name of household attribute to decode.

    id_col : str
        Column name for household IDs.

    year_col : str
        Column name for year.

    fields : tuple[str]
        Labels to extract as output columns.

    column_names : tuple[str]
        Names of columns to add to output table.

    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    aspects: _Aspects = ("name",)
    column_names: _ColumnNames = ()
    lib_defaults: Defaults
    lib_metadata: Metadata

    id_col: str | None = None
    year_col: str | None = None

    def model_post_init(self, __contex=None) -> None:
        if self.id_col is None:
            self.id_col = self.lib_defaults.columns.id
        if self.year_col is None:
            self.year_col = self.lib_defaults.columns.year
        self._resolve_column_names()

    def _resolve_column_names(self) -> None:
        if len(self.column_names) != len(self.aspects):
            if len(self.aspects) == 1:
                names = [self.name]
            else:
                names = [f"{self.name}_{label}" for label in self.aspects]
            self.column_names = tuple(names)


class IDDecoder:
    """Decodes household IDs using metadata mappings.

    Parameters
    ----------
    table : DataFrame
        Table with ID and year columns to decode.

    settings : IDDecoderSettings
        Configuration settings for decoding.

    Methods
    -------
    construct_mapping_table()
        Builds mapping table from metadata.

    add_attribute()
        Adds decoded columns to the input table.
    """

    def __init__(
        self,
        table: pd.DataFrame,
        settings: IDDecoderSettings,
    ) -> None:
        self.table = table
        self.settings = settings
        self.id_series = extract_column(table, settings.id_col)
        self.year_series = extract_column(table, settings.year_col)

    def construct_mapping_table(self) -> pd.DataFrame:
        """Constructs metadata mapping table for household IDs.

        Maps ID column to decoded labels based on year. Concatenates
        the ID, year, and label columns into a mapping table.

        Returns
        -------
        DataFrame
            Mapping table with year, ID and decoded columns.

        """
        mapped_columns = [self.year_series, self.id_series]
        for label in self.settings.aspects:
            mapped_column = self._map_id_to_label(label)
            mapped_columns.append(mapped_column)
        year_and_id = [self.settings.year_col, self.settings.id_col]
        columns = year_and_id + list(self.settings.column_names)
        mapping_table = pd.concat(mapped_columns, axis="columns", keys=columns)
        mapping_table = mapping_table.drop_duplicates().set_index(year_and_id)
        return mapping_table

    def _create_code_builder(
        self, household_metadata: dict
    ) -> Callable[[pd.Series], pd.Series]:
        ld_len = household_metadata["ID_Length"]
        attr_dict = household_metadata[self.settings.name]["code"]

        if ("position" in attr_dict) and attr_dict["position"] is not None:
            start, end = attr_dict["position"]["start"], attr_dict["position"]["end"]

            def builder(household_id_column: pd.Series) -> pd.Series:
                return (
                    household_id_column
                    % pow(10, (ld_len - start))
                    // pow(10, (ld_len - end))
                )

        elif "external_file" in attr_dict:
            file_name = attr_dict["external_file"]
            code_builer_file = external_data.load_table(
                file_name, self.settings.lib_defaults, reset_index=False
            )
            code_series = code_builer_file.loc[household_metadata["year"]].iloc[:, 0]
            assert isinstance(code_series, pd.Series)
            mapping_dict = code_series.to_dict()

            def builder(household_id_column: pd.Series) -> pd.Series:
                codes = household_id_column.map(mapping_dict)
                return codes

        else:
            raise ValueError("Code position is not available")

        return builder

    def _check_limits(self, attribute_metadata: dict, label: str, year: int) -> None:
        if "limits" not in attribute_metadata:
            return
        label_limit = attribute_metadata["limits"].get(label, None)
        default_limit = attribute_metadata["limits"].get("default", None)
        if label_limit is None:
            label_limit = default_limit
        if label_limit is None:
            return
        last_year = self.settings.lib_defaults.years[-1]
        label_limit = utils.Argham(label_limit, default_end=last_year)
        if year not in label_limit:
            raise ValueError(f"Year {year} is out of the defined limits ({label_limit})")
        return

    def _create_code_mapper(
        self, label: str, year: int
    ) -> Callable[[pd.Series], pd.Series]:
        id_metadata = utils.resolve_metadata(
            self.settings.lib_metadata.id_information, year
        )

        self._check_limits(
            attribute_metadata=id_metadata[self.settings.name],
            label=label,
            year=year,
        )

        if label == "code":
            return self._create_code_builder(id_metadata)

        if not isinstance(id_metadata, dict):
            raise ValueError
        # pylint: disable=unsubscriptable-object
        mapping = {}
        while label in id_metadata[self.settings.name].get("mappings", {}):
            mapping_info = id_metadata[self.settings.name]["mappings"][label]
            label = mapping_info["origin"]
            mapping_part = self.settings.lib_metadata.id_information[
                mapping_info["mapping"]
            ]
            for key, value in mapping.items():
                mapping_part = {
                    k: (value if v == key else v)
                    for k, v in mapping_part.items()
                }
            mapping.update(
                {
                    key: value for key, value
                    in mapping_part.items()
                    if key not in mapping
                }
            )

        labeles = id_metadata[self.settings.name][label]
        code_builder = self._create_code_builder(id_metadata)

        def mapper(household_id_column: pd.Series) -> pd.Series:
            mapped = code_builder(household_id_column).map(labeles)
            mapped = mapped.replace(mapping)
            mapped = mapped.astype("category")
            mapped.name = label
            return mapped

        return mapper

    def _map_id_to_label(self, label: str):
        years = self.year_series.drop_duplicates()
        attribute_column = pd.Series(index=self.table.index, dtype="object")
        for year in years:
            filt = self.year_series == year
            attribute_column.loc[filt] = self._create_code_mapper(label, year)(
                self.id_series.loc[filt]
            )
        return attribute_column

    def add_attribute(self):
        """Adds decoded columns to the input table.

        Joins the mapping table to the input table using the ID and
        year columns.

        Returns
        -------
        DataFrame
            Input table with decoded columns added.

        """
        mapping_table = self.construct_mapping_table()
        year_and_id = [self.settings.year_col, self.settings.id_col]
        self.table = self.table.join(mapping_table, year_and_id)
        return self.table
 