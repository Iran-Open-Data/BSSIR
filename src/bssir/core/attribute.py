"""Decode household ID attributes.

This module provides the core classes for extracting household
attributes from household IDs using the ID schema metadata.

Classes
-------
AttributeSettings
    Configuration for decoding a household ID attribute.

AttributeDecoder
    Decodes household IDs and joins decoded attributes onto a table.
"""

from collections.abc import Sequence
from functools import cached_property

import pandas as pd
from pydantic import BaseModel, ConfigDict

from bssir.context import Context

from bssir.context.metadata.models.id_schema.attribute import Attribute, ResolvedAttribute
from bssir.context.metadata.models.id_schema.metadata import IDLengths


class AttributeSettings(BaseModel):
    """Configuration for decoding a household ID attribute.

    This model specifies which household ID attribute should be decoded,
    which attribute aspects should be returned, and how the resulting
    columns should be named.

    Parameters
    ----------
    context : Context
        BSSIR context used to access metadata and configuration.

    attribute : str
        Name of the household attribute to decode.

    aspects : str or Sequence[str], default=("name",)
        Attribute aspects to extract.

    output_columns : str or Sequence[str], optional
        Names of the decoded output columns. If omitted, names are
        generated automatically from ``attribute`` and ``aspects``.

    id_column : str, optional
        Name of the household ID column. If omitted, the standard ID
        column configured in the context is used.

    year_column : str, optional
        Name of the survey year column. If omitted, the standard year
        column configured in the context is used.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    context: Context

    attribute: str

    aspects: str | Sequence[str] = ("name",)
    output_columns: str | Sequence[str] | None = None

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
    def resolved_aspects(self) -> list[str]:
        return [self.aspects] if isinstance(self.aspects, str) else list(self.aspects)

    @property
    def resolved_output_columns(self) -> list[str]:
        """Return the effective output column names.

        If no output columns are provided, names are generated from
        the decoded attribute name and selected aspects.
        """
        if not self.output_columns:
            if len(self.resolved_aspects) == 1:
                return [self.attribute]
            return list(
                f"{self.attribute}_{aspect}"
                for aspect in self.resolved_aspects
            )

        output_columns = [self.output_columns] if isinstance(self.output_columns, str) else list(self.output_columns)
        if len(output_columns) != len(self.resolved_aspects):
            raise ValueError(
                f"Expected {len(self.resolved_aspects)} output columns, "
                f"got {len(output_columns)}."
            )

        return output_columns

    @property
    def join_columns(self) -> list[str]:
        """Columns used to join decoded attributes onto the input table."""
        return [
            self.resolved_year_column,
            self.resolved_id_column,
        ]

    @property
    def columns(self) -> list[str]:
        return self.join_columns + self.resolved_output_columns


class AttributeDecoder:
    """Decode household ID attributes.

    Extracts an attribute from household IDs using the ID schema metadata
    and joins the decoded values onto a data table.

    Parameters
    ----------
    table : DataFrame
        Input table containing household IDs and survey years.

    settings : AttributeSettings
        Configuration controlling which attribute is decoded.

    context : Context
        BSSIR context providing metadata and configuration.

    Methods
    -------
    build_mapping_table()
        Construct a mapping between household IDs and decoded attributes.

    add()
        Join decoded attributes onto the input table.
    """

    def __init__(
        self,
        table: pd.DataFrame,
        settings: AttributeSettings,
        context: Context,
    ) -> None:
        self.settings = settings
        self.context = context

        self._validate_required_columns(table)

        self.original_table = table
        self.table = table.copy().reset_index()

    def _validate_required_columns(self, table: pd.DataFrame) -> None:
        """Validate that the required ID and year columns are available.

        The columns may exist either as DataFrame columns or as index levels.
        """
        available_targets = set(table.columns).union(table.index.names)
        
        missing_cols = [
            col for col in self.settings.join_columns if col not in available_targets
        ]
        
        if missing_cols:
            raise ValueError(
                f"The following required columns/indices were not found in the input table: {missing_cols}. "
                f"Available columns/indices: {list(available_targets)}"
            )

    @cached_property
    def id_col(self) -> pd.Series:
        return self.table[self.settings.resolved_id_column]

    @cached_property
    def year_col(self) -> pd.Series:
        return self.table[self.settings.resolved_year_column]

    @cached_property
    def years(self) -> list[int]:
        return sorted(self.year_col.unique())

    @cached_property
    def attribute_metadata(self) -> Attribute:
        """Metadata definition for the requested household attribute."""
        return (
            self.context.metadata.id_schema
            .attributes[self.settings.attribute]
        )

    @cached_property
    def id_lengths(self) -> IDLengths:
        """Household ID length metadata."""
        return self.context.metadata.id_schema.id_lengths

    @cached_property
    def attribute_code(self) -> pd.Series:
        """Extracted attribute codes.

        Returns
        -------
        Series
            The portion of each household ID corresponding to the requested
            attribute. Depending on the metadata definition, the values are
            extracted either from the ID digits or from an external lookup table.
        """
        parts = []

        for year in self.years:
            metadata = self.attribute_metadata.resolve(year)
            id_part = self._extract_id_part(metadata)
            parts.append(id_part)

        return pd.concat(parts).sort_index()

    def _extract_id_part(
        self,
        attribute: ResolvedAttribute,
    ) -> pd.Series:
        """Extract attribute codes for a single survey year."""
        if attribute.definition.type == "external":
            return self._extract_external_id_part(attribute)
        return self._extract_positional_id_part(attribute)

    def _extract_external_id_part(
        self,
        attribute: ResolvedAttribute,
    ) -> pd.Series:
        """Extract attribute codes using an external lookup table."""
        import dataforgeir as dfir

        external_file = attribute.definition.external_file
        assert external_file is not None
        id_part_table = dfir.get_dataset(external_file)

        required_columns = {
            self.settings.resolved_id_column,
            self.settings.resolved_year_column,
        }
        missing = required_columns - set(id_part_table.columns)
        if missing:
            raise ValueError(
                "External mapping table is missing required columns: "
                f"{', '.join(sorted(missing))}."
            )

        id_part_column_list = [
            col
            for col in id_part_table.columns
            if col not in {
                self.settings.resolved_id_column,
                self.settings.resolved_year_column,
            }
        ]

        if len(id_part_column_list) != 1:
            raise ValueError(
                "External mapping table must contain exactly one attribute column.\n"
                f"got {id_part_column_list}"
            )
        id_part_column = id_part_column_list[0]

        filt = id_part_table[self.settings.resolved_year_column].eq(attribute.year)
        id_part_table = id_part_table.loc[filt]
        lookup = id_part_table.set_index(self.settings.resolved_id_column)[id_part_column]

        id_part = (
            self.id_col.loc[self.year_col == attribute.year]
            .map(lookup)
        )

        return id_part

    def _extract_positional_id_part(self, attribute: ResolvedAttribute) -> pd.Series:
        """Extract attribute codes from digit positions within household IDs."""
        id_length = self.id_lengths.resolve(attribute.year)
        position = attribute.definition.position
        assert position is not None
        start = position.start
        end = position.end

        divisor = 10 ** (id_length - end)
        mask = 10 ** (end - start)

        id_part = (
            self.id_col.loc[self.year_col == attribute.year]
            .mod(mask * divisor)
            .floordiv(divisor)
        )
        
        return id_part

    def decode_aspect(self, aspect: str) -> pd.Series:
        """Decode one aspect of the requested attribute.

        Parameters
        ----------
        aspect : str
            Name of the attribute aspect to decode.

        Returns
        -------
        Series
            Values of the requested aspect for each household in the table.
        """
        parts = []

        for year in self.years:
            parts.append(
                self.attribute_code.loc[self.year_col == year]
                .map(self.attribute_metadata.resolve(year).aspects[aspect])
                .rename(aspect)
            )

        return pd.concat(parts).sort_index()

    def build_mapping_table(self) -> pd.DataFrame:
        """Build a household ID mapping table.

        Creates a table containing one row per unique household ID and survey
        year, together with the requested decoded attribute aspects.

        Returns
        -------
        DataFrame
            Mapping table suitable for joining onto the input table.
        """
        cols = [self.year_col, self.id_col]
        for aspect in self.settings.resolved_aspects:
            cols.append(self.decode_aspect(aspect))
        table = pd.concat(cols, axis="columns")
        table.columns = self.settings.columns
        table = table.drop_duplicates(subset=self.settings.join_columns)
        return table

    def add(self) -> pd.DataFrame:
        """Decode the requested household attribute.

        Returns
        -------
        DataFrame
            Input table with the decoded attribute columns added.
        """
        mapping_table = self.build_mapping_table()

        joined = self.table.merge(mapping_table, on=self.settings.join_columns)

        joined.index = self.original_table.index

        columns_to_drop = [
            col for col in self.table.columns 
            if col not in self.original_table.columns and col in joined.columns
        ]
        joined = joined.drop(columns=columns_to_drop)

        self.original_table = joined
        return self.original_table


def add_attribute(
    table: pd.DataFrame,
    *,
    context: Context,
    **kwargs,
) -> pd.DataFrame:
    """Decode a household ID attribute and add it to a table."""
    settings = AttributeSettings(context=context, **kwargs)
    return AttributeDecoder(
        table=table,
        settings=settings,
        context=context,
    ).add()
