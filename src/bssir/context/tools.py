from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
import platform
import os
from typing import TYPE_CHECKING
from zipfile import ZipFile

from bssir import utils
from bssir.types import Years
from .config import Config

if TYPE_CHECKING:
    from .metadata import MetadataCollection


class Tools:
    def __init__(
        self,
        config: Config,
        metadata: MetadataCollection | None = None,
    ) -> None:
        self.config = config
        self.metadata = metadata

    def _require_metadata(self) -> MetadataCollection:
        if self.metadata is None:
            raise RuntimeError(
                "This operation requires metadata, but no metadata is configured."
            )
        return self.metadata

    def extract(
        self,
        source: Path,
        destination: Path,
    ) -> None:
        utils.archive.extract(
            source=source,
            destination=destination,
            tools=self.config.tools.model_dump(),
        )

    def parse_years(self, years: Years) -> list[int]:
        return utils.years.parse(years, self.config.coverage_period)

    def parse_years_for_table(
        self,
        years: Years,
        table_name: str,
    ) -> list[int]:
        metadata = self._require_metadata()

        table_metadata = metadata.source_tables[table_name]

        available_years = (
            table_metadata.availability
            or self.config.coverage_period
        )

        return utils.years.parse(
            years,
            available_years,
        )

    def create_table_year_pairs(
        self,
        table_names: str | Iterable[str],
        years: Years,
    ) -> list[tuple[str, int]]:
        metadata = self._require_metadata()

        if table_names == "all":
            table_names = metadata.source_tables.table_list
        elif isinstance(table_names, str):
            table_names = [table_names]

        return [
            (table_name, year)
            for table_name in table_names
            for year in self.parse_years_for_table(years, table_name)
        ]

    def resolve_metadata(
            self,
            versioned_metadata: dict,
            year: int,
            categorize: bool = False,
            **optional_settings,
        ):
        return utils.metadata.resolve(
            versioned_metadata=versioned_metadata,
            year=year,
            categorize=categorize,
            **optional_settings
        )

    def resolve_source(
        self,
        source: str | None,
        *,
        data_type: str,
    ) -> str:
        """
        Resolve a download source for a data type.

        Parameters
        ----------
        source : str, optional
            Requested download source. If ``None``, the configured default source is
            used.
        data_type : str
            Type of data being accessed (e.g. ``"raw"`` or ``"cleaned"``).

        Returns
        -------
        str
            Name of the resolved download source.
        """
        source = source or self.config.default_download_source

        if data_type == "cleaned" and source == "original":
            return "mirror"

        return source

    def ensure_extractor(self) -> None:
        """Return a usable 7-Zip executable, installing it if necessary."""
        if self._has_extractor():
            return

        exe = self.resolve_extractor()
        if not exe.exists():
            self._install_extractor()

        self._add_to_path(exe.parent)

    def _has_extractor(self) -> bool:
        """Return whether a supported extractor is already available."""
        import rarfile

        try:
            rarfile.tool_setup(unar=True, sevenzip=True, sevenzip2=True)
        except Exception:
            return False
        return True

    def _install_extractor(self) -> Path:
        """Download and install the bundled 7-Zip executable."""
        archive = self._download_extractor_archive()

        try:
            install_dir = self._extract_extractor_archive(archive)
            return self._prepare_extractor(install_dir)
        finally:
            archive.unlink(missing_ok=True)

    def _download_extractor_archive(self, mirror_name: str | None = None) -> Path:
        """Download the platform-specific 7-Zip archive."""
        system = platform.system()
        architecture = platform.architecture()[0]

        filename = f"{system}-{architecture}.zip"
        archive = self.config.root_dir / filename
        self.config.get_mirror(mirror_name).download(
            f"/7-Zip/{filename}",
            self.config.root_dir / filename
        )

        return archive

    def _extract_extractor_archive(self, archive: Path) -> Path:
        """Extract the downloaded archive."""
        install_dir = self.config.root_dir / "7-Zip"

        with ZipFile(archive) as zip_file:
            zip_file.extractall(self.config.root_dir)

        return install_dir

    def _prepare_extractor(self, install_dir: Path) -> Path:
        """Finalize the installation and return the executable."""
        gitignore = install_dir / ".gitignore"
        gitignore.write_text(
            "# Automatically created by BSSIR\n*\n",
            encoding="utf-8",
        )

        executable = self.resolve_extractor()

        if platform.system() == "Linux":
            executable.chmod(0o771)

        return executable

    def resolve_extractor(self) -> Path:
        system = platform.system()

        if system == "Windows":
            return self.config.root_dir / "7-Zip" / "7z.exe"

        return self.config.root_dir / "7-Zip" / "7zz"

    def _add_to_path(self, directory: Path) -> None:
        _directory = str(directory)

        paths = os.environ.get("PATH", "").split(os.pathsep)
        if _directory not in paths:
            os.environ["PATH"] = os.pathsep.join([_directory, *paths])
