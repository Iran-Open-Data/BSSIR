from typing import Any, Iterable, Literal, overload, Optional
from pathlib import Path

import pandas as pd

from .metadata_reader import config
from .api import API, _DataSource, _Frequency, _SeparateBy


_ExternalTable = Literal[
    "CPI",
]


defaults, metadata = config.set_package_config(Path(__file__).parent)
api = API(defaults=defaults, metadata=metadata)


def __get_optional_params(local_variables: dict) -> dict:
    return {key: value for key, value in local_variables.items() if value is not None}


def load_external_table(
    table_name: _ExternalTable | str,
    data_source: Optional[_DataSource] = None,
    frequency: Optional[_Frequency] = None,
    separate_by: Optional[_SeparateBy] = None,
    form: Optional[Literal["cleaned", "original"]]  = None,
    on_missing: Optional[Literal["error", "download", "create"]]  = None,
    save_downloaded: Optional[bool]  = None,
    redownload: Optional[bool]  = None,
    save_created: Optional[bool]  = None,
    recreate: Optional[bool]  = None,
    reset_index: bool = True,
) -> pd.DataFrame:
    parameters = __get_optional_params(locals())
    return api.load_external_table(**parameters)


def setup_external_data() -> None:
    for table in [
        "sci.cpi_1400.monthly",
        "sci.cpi_1400.quarterly",
        "sci.cpi_1400.annual",
        "sci.cpi_1400_rural.monthly",
        "sci.cpi_1400_rural.quarterly",
        "sci.cpi_1400_rural.annual",
        "sci.cpi_1400_urban.monthly",
        "sci.cpi_1400_urban.quarterly",
        "sci.cpi_1400_urban.annual",
        "sci.cpi_1400.monthly.urban_rural",
        "sci.cpi_1400.quarterly.urban_rural",
        "sci.cpi_1400.annual.urban_rural",
        "wb.ppp_conversion_factor",
        "imf.ppp_conversion_factor",
    ]:
        load_external_table(table, recreate=True, redownload=True)
