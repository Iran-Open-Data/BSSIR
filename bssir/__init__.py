import importlib.metadata

from .external_data_manager import load_external_table


__version__ = importlib.metadata.version("bssir")

__all__ = [
    "load_external_table",
]
