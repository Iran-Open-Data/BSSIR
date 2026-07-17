from collections.abc import Mapping
from typing import Any

import pandas as pd

from bssir.context import Context


class API:
    def __init__(self, context: Context):
        self.context = context

    def setup(self, **kwargs) -> None:
        """Prepare survey data for analysis."""
        self.context.tools.ensure_extractor()
        settings = self._resolve_settings("setup", kwargs)
        method = settings["method"]

        match method:
            case "create_from_raw":
                self._setup_from_raw(settings)
            case "download_cleaned":
                self._setup_from_download(settings)
            case _:
                raise ValueError(f"Unknown setup method: {method}")

    def _resolve_settings(self, name: str, overrides: dict) -> dict[str, Any]:
        if "years" in overrides:
            overrides["years"] = self.context.tools.parse_years(overrides["years"])

        model = getattr(self.context.config.functions, name)

        settings = model.model_copy(update=self._omit_none(overrides)).model_dump()
        return settings

    @staticmethod
    def _omit_none(mapping: Mapping[str, Any]) -> dict[str, Any]:
        return {k: v for k, v in mapping.items() if v is not None}

    def _setup_from_raw(self, settings) -> None:
        from bssir.core.data_cleaner import build_tables

        self.setup_raw_data(**settings)
        build_tables(
            years=settings["years"],
            recreate=settings["replace"],
            tables=settings["table_names"],
            context=self.context,
        )

    def _setup_from_download(self, settings) -> None:
        from bssir.core.cleaned_data import download_years

        source = settings.download_source
        if source in (None, "original"):
            source = "mirror"

        download_years(
            years=settings["years"],
            source=source,
            context=self.context,
        )

    def _load_raw_table(self, table_name: str, year: int) -> pd.DataFrame:
        from bssir.core.data_cleaner import load_raw_table

        return load_raw_table(
            table_name=table_name,
            year=year,
            context=self.context,
        )

    def _load_cleaned_table(self, table_name: str, year: int) -> pd.DataFrame:
        from bssir.core.data_cleaner import load_cleaned_table

        return load_cleaned_table(
            table_name=table_name,
            year=year,
            context=self.context,
        )

    def setup_raw_data(self, **kwargs) -> None:
        """Download and extract raw survey data."""
        from bssir.core.resource_handler import setup

        settings = self._resolve_settings("setup_raw_data", kwargs)

        setup(context=self.context, **settings)
