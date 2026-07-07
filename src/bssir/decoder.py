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
from collections.abc import Mapping, Callable, Iterable
from functools import cached_property
from itertools import product
from typing import Literal, Annotated, Any

import pandas as pd
from pydantic import BaseModel, ConfigDict, BeforeValidator, Field

from bssir.context import Context
from bssir.context.utils.resolver import resolve_metadata

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


Aspects = Annotated[tuple[str, ...], BeforeValidator(maybe_to_tuple)]
Levels = Annotated[tuple[int, ...], BeforeValidator(maybe_to_tuple)]
ColumnNames = Annotated[tuple[str, ...], BeforeValidator(maybe_to_tuple)]
ClassificationType = Literal["commodity", "occupation", "industry"]


class DecoderSettings(BaseModel):
    """Configuration for decoding hierarchical classification codes.

    This model specifies how classification codes in a table should be
    translated into descriptive attributes using the classification metadata.
    It also resolves metadata defaults and determines the output column names.

    Parameters
    ----------
    context : Context
        BSSIR context used to access configuration and metadata.

    target_column : str
        Name of the column containing the classification codes to decode.

    year_column : str, optional
        Name of the column containing the survey year. If omitted, the
        standard year column configured in the context is used.

    classification_name : str, default="original"
        Name of the classification metadata to use.

    classification_type : {"commodity", "industry", "occupation"}, optional
        Classification type. If omitted, it is inferred from
        ``target_column``.

    aspects : tuple[str], default=("item_key",)
        Classification attributes to extract for each level.

    levels : tuple[int], default=(1,)
        Hierarchical levels to extract.

    output_columns : tuple[str], optional
        Names of the output columns. If omitted, names are generated from
        ``aspects`` and ``levels``. If one name is provided for each aspect,
        the level number is appended automatically.

    missing_replacements : dict[str, str], optional
        Replacement values applied after decoding for missing classification
        attributes.

    See Also
    --------
    Decoder
        Performs the decoding using these settings.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    context: Context

    available_columns: list[str] | None = None
    target_column: str | None = None
    year_column: str | None = None

    classification_name: str = "original"
    classification_type: ClassificationType | None = None
    aspects: Aspects = ()
    levels: Levels = ()
    output_columns: ColumnNames = ()
    missing_replacements: dict[str, str] = Field(default_factory=dict)

    @property
    def resolved_year_column(self) -> str:
        """Return the effective year column name.

        Returns the user-specified year column if provided; otherwise returns
        the default year column configured in the context.
        """
        return self.year_column or self.context.config.standard_columns.year

    @property
    def resolved_target_column(self) -> str:
        if self.target_column is not None:
            return self.target_column

        if self.available_columns is None:
            raise ValueError(
                "target_column was not provided and no available columns were supplied."
            )

        candidates = [
            column
            for column in self.available_columns
            if self._is_potential_target(column)
        ]

        if len(candidates) == 1:
            return candidates[0]

        if len(candidates) == 0:
            raise ValueError(
                "Could not infer the target column. "
                f"Specify target_column explicitly.\n{self.available_columns}\n{self.classification_keywords}"
            )

        raise ValueError(
            "Multiple candidate target columns were found: "
            f"{', '.join(candidates)}. "
            "Specify target_column explicitly."
        )

    def _is_potential_target(self, column: str) -> bool:
        column = column.lower()

        return any(
            keyword.lower() in column
            for keywords in self.classification_keywords.values()
            for keyword in keywords
        )

    @property
    def resolved_classification_type(self) -> ClassificationType:
        """Return the effective classification type.

        If ``classification_type`` was not specified, the type is inferred from
        the target column name using the configured classification keywords.

        Raises
        ------
        ValueError
            If the classification type cannot be inferred.
        """
        if self.classification_type is not None:
            return self.classification_type

        target = self.resolved_target_column.lower()

        for classification, keywords in self.classification_keywords.items():
            if any(keyword.lower() in target for keyword in keywords):
                return classification

        raise ValueError(
            f"Could not infer the classification type from "
            f"target_column={self.resolved_target_column!r}. "
            "Specify classification_type explicitly."
        )

    @property
    def classification_keywords(self) -> dict[ClassificationType, list[str]]:
        """Keywords used to infer the classification type.

        Returns
        -------
        dict
            Mapping from classification type to the keywords that identify
            columns containing that classification.
        """
        return {
            "commodity": self.context.config.standard_columns.commodity_code,
            "industry": self.context.config.standard_columns.industry_code,
            "occupation": self.context.config.standard_columns.occupation_code,
        }

    # @property
    # def resolved_aspects(self) -> Aspects:
    #     if self.aspects:
    #         return self.aspects
    #     if "aspects" in self.defaults:
    #         return tuple(self.defaults["aspects"])
    #     return ("item_key",)

    # @property
    # def resolved_levels(self) -> Levels:
    #     if self.levels:
    #         return self.levels
    #     if "levels" in self.defaults:
    #         return tuple(self.defaults["levels"])
    #     return (1,)

    @property
    def resolved_missing_replacements(self) -> dict[str, str]:
        replacements = self.defaults.get("missing_replacements", {}).copy()
        replacements.update(self.missing_replacements)
        return replacements

    def model_post_init(self, __contex=None) -> None:
        """Finalize the decoder settings.

        Applies configuration values in the following order of precedence:

        1. User-specified values.
        2. Defaults defined by the selected classification metadata.
        3. Library defaults.

        Finally, normalizes the output column names based on the resolved
        aspects and hierarchy levels.
        """
        self._apply_metadata_defaults()
        self._apply_library_defaults()
        self._normalize_output_columns()

    def _apply_metadata_defaults(self):
        for key, value in self.defaults.items():
            if isinstance(value, list):
                value = tuple(value)

            current = getattr(self, key)

            if current is None:
                setattr(self, key, value)
            elif isinstance(current, (tuple, list, dict)) and not current:
                setattr(self, key, value)

    def _apply_library_defaults(self):
        if not self.aspects:
            self.aspects = ("item_key",)

        if not self.levels:
            self.levels = (1,)

    def _normalize_output_columns(self) -> None:
        expected = len(self.aspects) * len(self.levels)

        if not self.output_columns:
            self.output_columns = tuple(
                f"{aspect}_{level}"
                for aspect, level in product(self.aspects, self.levels)
            )
            return

        if len(self.output_columns) == len(self.aspects):
            self.output_columns = tuple(
                f"{name}_{level}"
                for name, level in product(self.output_columns, self.levels)
            )
            return

        if len(self.output_columns) != expected:
            raise ValueError(
                f"Expected {len(self.aspects)} or {expected} output columns, "
                f"got {len(self.output_columns)}."
            )

    @cached_property
    def versioned_info(self) -> dict:
        """Resolved classification metadata.

        Returns the metadata corresponding to the selected classification type
        and classification name.
        """
        metadata_lookup = {
            "commodity": self.context.metadata.commodities,
            "industry": self.context.metadata.industries,
            "occupation": self.context.metadata.occupations,
        }

        return metadata_lookup[self.resolved_classification_type][self.classification_name]

    @cached_property
    def defaults(self) -> dict:
        """Default decoder settings defined by the classification metadata."""
        return self.versioned_info.get("defaults", {})

    @property
    def output_column_mapping(self):
        """Map decoded attributes to output column names.

        Returns
        -------
        dict
            Mapping from ``(aspect, level)`` pairs to the corresponding output
            column names.

        Notes
        -----
        This mapping is primarily used to rename the columns produced by the
        decoded classification table.
        """
        label_level = product(self.aspects, self.levels)
        return dict(zip(label_level, self.output_columns))


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

    def __init__(self, table: pd.DataFrame, settings: DecoderSettings, context: Context) -> None:
        self.table = table
        self.settings = settings
        self.context = context
        self.code_col = extract_column(table, settings.resolved_target_column)
        self.year_col = extract_column(table, settings.resolved_year_column)
        self.classification_table = self.create_classification_table(
            years=self.year_col.drop_duplicates().to_list(),
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
            classification_info = resolve_metadata(
                self.settings.versioned_info, year, categorize=True
            )
            assert isinstance(classification_info, dict)
            annual_table = pd.DataFrame(classification_info.get("items"))
            annual_table["code_range"] = annual_table["code"].apply(
                utils.Argham,  # type: ignore
                default_start=min(self.context.config.coverage_period),
                default_end=max(self.context.config.coverage_period)+1,
                keywords=["code"],
            )
            annual_table = annual_table.drop(columns=["code"])
            annual_table.loc[:, "Year"] = year
            table_list.append(annual_table)
        table = pd.concat(table_list, ignore_index=True)
        return table

    def _create_year_code_pairs(self) -> pd.DataFrame:
        years = self.year_col.drop_duplicates()
        yc_pair_list = []
        for year in years:
            filt = self.year_col == year
            codes = self.code_col.loc[filt].drop_duplicates()
            yc_pair = codes.to_frame()
            yc_pair[self.settings.resolved_year_column] = year
            yc_pair_list.append(yc_pair)
        return pd.concat(yc_pair_list, ignore_index=True)

    def _build_year_code_table(
        self, year_code_pairs: pd.DataFrame, row: pd.Series
    ) -> pd.DataFrame:
        year_column = self.settings.resolved_year_column
        filt = (
            year_code_pairs[self.settings.resolved_target_column]
            .apply(lambda x: x in row["code_range"])
        )
        filt = filt & (year_code_pairs[year_column] == row[year_column])
        matched_codes = year_code_pairs.loc[filt].set_index(
            [year_column, self.settings.resolved_target_column]
        )
        columns = row.drop(["code_range", year_column]).index
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
        mapping_table = mapping_table.loc[:, self.settings.output_column_mapping.keys()]  # type: ignore
        mapping_table.columns = self.settings.output_column_mapping.values()
        return mapping_table

    def _validate_mapping_table(self, mapping_table: pd.DataFrame):
        filt = mapping_table.index.duplicated(keep=False)
        if filt.sum() > 0:
            invalid_case_sample = (
                mapping_table.loc[filt]
                .sort_values([self.settings.resolved_target_column, "level"])
                .head(10)
            )
            raise ValueError(f"Classification is not valid \n{invalid_case_sample}")

    def _fill_missing_values(self) -> None:
        for column, default in self.settings.resolved_missing_replacements.items():
            if column in self.table.columns:
                self.table[column] = self.table[column].fillna(default)

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
            mapping, on=[self.settings.resolved_year_column, self.settings.resolved_target_column]
        )
        self._fill_missing_values()
        return self.table


class IDDecoderSettings(BaseModel):
    """Configuration for decoding household ID attributes.

    This model specifies how household IDs should be decoded into one or
    more descriptive attributes using the household ID metadata.

    Parameters
    ----------
    context : Context
        BSSIR context used to access configuration and metadata.

    name : str
        Name of the household attribute to decode.

    aspects : tuple[str], default=("name",)
        Attribute aspects to extract.

    output_columns : tuple[str], optional
        Names of the output columns. If omitted, names are generated
        automatically from ``name`` and ``aspects``.

    id_column : str, optional
        Name of the household ID column. If omitted, the standard ID
        column configured in the context is used.

    year_column : str, optional
        Name of the survey year column. If omitted, the standard year
        column configured in the context is used.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    context: Context

    attribute_name: str

    aspects: Aspects = ("name",)
    output_columns: ColumnNames = ()

    id_column: str | None = None
    year_column: str | None = None

    @property
    def resolved_id_column(self) -> str:
        """Return the effective household ID column name."""
        return self.id_column or self.context.config.standard_columns.id

    @property
    def resolved_year_column(self) -> str:
        """Return the effective survey year column name."""
        return self.year_column or self.context.config.standard_columns.year

    @property
    def resolved_output_columns(self) -> tuple[str, ...]:
        """Return the effective output column names.

        If no output columns are provided, names are generated from
        the decoded attribute name and selected aspects.
        """
        if not self.output_columns:
            if len(self.aspects) == 1:
                return (self.attribute_name,)
            return tuple(
                f"{self.attribute_name}_{aspect}"
                for aspect in self.aspects
            )

        if len(self.output_columns) != len(self.aspects):
            raise ValueError(
                f"Expected {len(self.aspects)} output columns, "
                f"got {len(self.output_columns)}."
            )

        return self.output_columns

    @property
    def join_columns(self) -> list[str]:
        """Columns used to join decoded attributes onto the input table."""
        return [
            self.resolved_year_column,
            self.resolved_id_column,
        ]


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
        context: Context,
    ) -> None:
        self.table = table
        self.settings = settings
        self.context = context
        self.id_series = extract_column(table, settings.resolved_id_column)
        self.year_series = extract_column(table, settings.resolved_year_column)

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
        columns = (
            list(self.settings.join_columns)
            + list(self.settings.resolved_output_columns)
        )
        mapping_table = pd.concat(mapped_columns, axis="columns", keys=columns)
        mapping_table = mapping_table.drop_duplicates().set_index(self.settings.join_columns)
        return mapping_table

    def _create_code_builder(
        self, household_metadata: Mapping
    ) -> Callable[[pd.Series], pd.Series]:
        ld_len = household_metadata["ID_Length"]
        attr_dict = household_metadata[self.settings.attribute_name]["code"]

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
                file_name, context=self.context, reset_index=False
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
        last_year = max(self.context.config.coverage_period) + 1
        label_limit = utils.Argham(label_limit, default_end=last_year)
        if year not in label_limit:
            raise ValueError(f"Year {year} is out of the defined limits ({label_limit})")
        return

    def _create_code_mapper(
        self, label: str, year: int
    ) -> Callable[[pd.Series], pd.Series]:
        id_schema = self.context.metadata.id_schema.resolve(year)

        self._check_limits(
            attribute_metadata=id_schema[self.settings.attribute_name],
            label=label,
            year=year,
        )

        if label == "code":
            return self._create_code_builder(id_schema)

        if not isinstance(id_schema, dict):
            raise ValueError
        # pylint: disable=unsubscriptable-object
        mapping = {}
        while label in id_schema[self.settings.attribute_name].get("mappings", {}):
            mapping_info = id_schema[self.settings.attribute_name]["mappings"][label]
            label = mapping_info["origin"]
            mapping_part = self.context.metadata.id_schema[mapping_info["mapping"]]
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

        labeles = id_schema[self.settings.attribute_name][label]
        code_builder = self._create_code_builder(id_schema)

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
        self.table = self.table.join(mapping_table, on=self.settings.join_columns)
        return self.table
 