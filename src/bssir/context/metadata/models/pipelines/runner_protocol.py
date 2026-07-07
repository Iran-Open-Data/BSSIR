from typing import Protocol

import pandas as pd

# from bssir.context import Context


class RunnerProtocol(Protocol):
    table_name: str
    year: int
    # context: Context

    tables: dict[str, pd.DataFrame]
    current: str
