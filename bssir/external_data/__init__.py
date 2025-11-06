from typing import Literal, Any

import pandas as pd

from .external_data_cleaner import ExternalDataCleaner
from ..metadata_reader import LoadExternalTableSettings, Defaults

# pylint: disable=too-many-arguments
# pylint: disable=unused-argument
# pylint: disable=too-many-locals

__all__ = ["load_table"]


_DataSource = Literal["SCI", "CBI"]
_Frequency = Literal["Annual", "Quarterly", "Monthly"]
_SeparateBy = Literal["Urban_Rural", "Province"]


def _extract_parameters(local_variables: dict) -> dict:
    return {key: value for key, value in local_variables.items() if value is not None}


def load_table(
    table_name: str,
    lib_defaults: Defaults,
    data_source: _DataSource | None = None,
    frequency: _Frequency | None = None,
    separate_by: _SeparateBy | None = None,
    reset_index: bool = True,
    **kwargs,
) -> pd.DataFrame:
    name_parts = [data_source, table_name, frequency, separate_by]
    name_parts = [part for part in name_parts if part is not None]
    name = ".".join(name_parts).lower()
    table = ExternalDataCleaner(
        name,
        lib_defaults,
        **kwargs,
    ).read_table()
    if reset_index:
        table = table.reset_index()
    return table
