from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal

import pandas as pd

from bssir.context import Context
from bssir.types import Years

TableKind = Literal["raw", "cleaned", "normalized"]

class API:
    def __init__(self, context: Context):
        self.context = context

    def initialize_config(
        self,
        mode: Literal["Standard", "Colab"] = "Standard",
        replace: bool = False,
    ) -> None:
        """Create the local settings file from a packaged template."""
        src = self._config_template(mode)
        dst = self._local_settings_path()

        if dst.exists() and not replace:
            return

        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_text(
            self._render_config_template(src),
            encoding="utf-8",
        )

    def _config_template(
        self,
        mode: Literal["Standard", "Colab"],
    ) -> Path:
        templates = {
            "Standard": "settings_sample.yaml",
            "Colab": "settings_sample_colab.yaml",
        }

        try:
            filename = templates[mode]
        except KeyError:
            raise ValueError(f"Unknown config mode: {mode!r}") from None

        return (
            self.context.config.base_package_dir
            / "config"
            / filename
        )

    def _local_settings_path(self) -> Path:
        return (
            self.context.config.root_dir
            / self.context.config.local_settings
        )

    def _render_config_template(self, template: Path) -> str:
        local_dir = (
            self.context.config.root_dir
            / self.context.config.local_dir
        )

        return template.read_text(encoding="utf-8").replace(
            "{{local_dir}}",
            str(local_dir),
        )

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

    def load_table(
            self,
            table_name: str,
            years: Years,
            **kwargs
        ) -> pd.DataFrame:
        """Load a table at the requested processing stage.

        Args:
            table_name: Name of the table to load.
            year: Survey year of the table.
            stage: Processing stage of the table:

                - ``"raw"``: Original source table with minimal processing.
                - ``"cleaned"``: Table after cleaning and standardization.
                - ``"normalized"``: Table after the normalization pipeline.

        Returns:
            The requested table as a pandas DataFrame.

        Raises:
            ValueError: If ``stage`` is not one of ``"raw"``, ``"cleaned"``,
                or ``"normalized"``.
        """
        settings = self._resolve_settings("load_table", kwargs)

        loaders = {
            "raw": self._load_raw_table,
            "cleaned": self._load_cleaned_table,
            "normalized": self._load_normlized_table,
        }
        kind = settings["kind"]
        try:
            loader = loaders[kind]
        except KeyError:
            valid_stages = ", ".join(repr(value) for value in loaders)
            raise ValueError(
                f"Invalid table stage: {kind!r}. "
                f"Expected one of: {valid_stages}."
            ) from None

        return pd.concat(
            [
                loader(table_name, year)
                for year in self.context.tools.parse_years(years)
            ],
            axis="index",
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

    def _load_normlized_table(self, table_name: str, year: int) -> pd.DataFrame:
        from bssir.core.pipeline import load_pipeline_table

        return load_pipeline_table(
            table_name=table_name,
            year=year,
            context=self.context,
        )

    def setup_raw_data(self, **kwargs) -> None:
        """Download and extract raw survey data."""
        from bssir.core.resource_handler import setup

        settings = self._resolve_settings("setup_raw_data", kwargs)

        setup(context=self.context, **settings)

    def add_attribute(self, table: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Add attributes to table based on ID column."""
        from bssir.core.attribute import add_attribute

        return add_attribute(
            table,
            context=self.context,
            **self._omit_none(kwargs),
        )
