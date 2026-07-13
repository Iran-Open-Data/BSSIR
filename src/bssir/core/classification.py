"""Decode hierarchical classification codes.

This module provides the core classes for translating classification
codes, such as commodity, industry, and occupation codes, into
descriptive metadata using the BSSIR classification metadata.

Classes
-------
ClassificationSettings
    Configuration for decoding classification codes.

ClassificationDecoder
    Decodes classification codes and joins the decoded attributes onto
    a table.
"""

from collections.abc import Iterable, Sequence
from functools import cached_property
from itertools import product
from typing import Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from bssir.context import Context
from bssir.context.utils.resolver import resolve_metadata

from .. import utils


ClassificationType = Literal["commodity", "occupation", "industry"]


class ClassificationSettings(BaseModel):
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
    ClassificationDecoder
        Performs the decoding using these settings.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    context: Context

    available_columns: Iterable[str] | None = None
    target_column: str | None = None
    year_column: str | None = None

    classification_name: str = "original"
    classification_type: ClassificationType | None = None
    aspects: str | Sequence[str] = ()
    levels: int | Sequence[int] = ()
    output_columns: str | Sequence[str] = ()
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

    @property
    def resolved_aspects(self) -> list[str]:
        return [self.aspects] if isinstance(self.aspects, str) else list(self.aspects)

    @property
    def resolved_levels(self) -> list[int]:
        return [self.levels] if isinstance(self.levels, int) else list(self.levels)

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
        expected = len(self.resolved_aspects) * len(self.resolved_levels)

        if not self.output_columns:
            self.output_columns = tuple(
                f"{aspect}_{level}"
                for aspect, level in product(self.resolved_aspects, self.resolved_levels)
            )
            return

        if len(self.output_columns) == len(self.resolved_aspects):
            self.output_columns = tuple(
                f"{name}_{level}"
                for name, level in product(self.output_columns, self.resolved_levels)
            )
            return

        if len(self.output_columns) != expected:
            raise ValueError(
                f"Expected {len(self.resolved_aspects)} or {expected} output columns, "
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
        label_level = product(self.resolved_aspects, self.resolved_levels)
        return dict(zip(label_level, self.output_columns))


class ClassificationDecoder:
    """Decode hierarchical classification codes.

    Builds a mapping between classification codes and metadata attributes,
    then joins the decoded attributes onto an input table.

    Parameters
    ----------
    table : DataFrame
        Input table containing classification codes and survey years.

    settings : ClassificationSettings
        Configuration controlling how classification codes are decoded.

    context : Context
        BSSIR context providing metadata and configuration.

    Methods
    -------
    build_mapping_table()
        Construct the mapping between classification codes and decoded
        attributes.

    add()
        Join the decoded attributes onto the input table.
    """

    def __init__(
            self,
            table: pd.DataFrame,
            settings: ClassificationSettings,
            context: Context,
        ) -> None:
        self.original_table = table
        self.table = table.reset_index().copy()
        self.settings = settings
        self.context = context
        self.classification_table = self.build_classification_table(
            years=self.year_col.drop_duplicates().to_list(),
        )
        self.year_code_pairs = self._create_year_code_pairs()

    @property
    def code_col(self) -> pd.Series:
        return self.table[self.settings.resolved_target_column]

    @property
    def year_col(self) -> pd.Series:
        return self.table[self.settings.resolved_year_column]

    def build_classification_table(
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

    def build_mapping_table(self) -> pd.DataFrame:
        """Build the classification mapping table.

        Constructs a lookup table that maps each classification code and survey
        year to the requested metadata attributes.

        Returns
        -------
        DataFrame
            Mapping table indexed by survey year and classification code.
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

    def add(self):
        """Add decoded classification attributes to the input table.

        Builds the classification mapping table, joins the decoded attributes
        onto the input table, and fills missing values using the configured
        replacement values.

        Returns
        -------
        DataFrame
            Input table with decoded classification columns added.
        """
        mapping = self.build_mapping_table()
        self.table = self.table.join(
            mapping, on=[self.settings.resolved_year_column, self.settings.resolved_target_column]
        )
        self._fill_missing_values()
        return self.table


def add_classification(
    table: pd.DataFrame,
    *,
    context: Context,
    **kwargs,
) -> pd.DataFrame:
    settings = ClassificationSettings(context=context, **kwargs)
    return ClassificationDecoder(table, settings, context).add()
