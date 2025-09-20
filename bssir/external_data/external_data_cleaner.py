from typing import Callable, Literal
import importlib
from pathlib import Path

import pandas as pd

from .. import utils
from ..metadata_reader import Defaults, read_yaml


class ExternalDataCleaner:
    def __init__(
        self,
        name: str,
        lib_defaults: Defaults,
        source: Literal["mirror"] | str = "mirror",
        **kwargs,
    ) -> None:
        self.name = name
        self.source = source
        settings = lib_defaults.functions.load_external_table
        self.settings = settings.model_copy(update=kwargs)
        self.lib_defaults = lib_defaults
        self.metadata = self._get_metadata()
        self.metadata_type = self._extract_type()

    def read_table(self) -> pd.DataFrame:
        """Read a single table by name.

        Parameters
        ----------
        table_name : str
            Name of the table to load

        Returns
        -------
        table : DataFrame
            Loaded table data

        """
        local_file = self.lib_defaults.dir.external.joinpath(f"{self.name}.parquet")

        if self.metadata_type == "alias":
            name = self.metadata["alias"]
            if name.count(".") == 0:
                name = f"{self.name}.{name}"
            table = ExternalDataCleaner(
                name=name,
                lib_defaults=self.lib_defaults,
                **self.settings.model_dump(),
            ).read_table()
        elif self.settings.form == "original":
            table = self._load_raw_file()
        elif self.settings.recreate:
            table = self._create_table()
        elif self.settings.redownload:
            table = self._download_table()
        elif local_file.exists():
            table = pd.read_parquet(local_file)
        elif self.settings.on_missing == "create":
            table = self._create_table()
        elif self.settings.on_missing == "download":
            table = self._download_table()
        else:
            raise FileNotFoundError

        return table

    def _create_table(self) -> pd.DataFrame:
        if self.metadata_type == "manual":
            table = self._download_table()
        elif self.metadata_type in ["file", "url"]:
            table = self._clean_raw_file()
        elif self.metadata_type == "from":
            table = self._collect_and_clean()
        else:
            raise ValueError(f"{self.metadata_type} is not a valid type")
        if self.settings.save_created:
            self.save_table(table)
        return table

    def _get_metadata(self) -> dict:
        metadata = read_yaml(Path(__file__).parent.joinpath("metadata.yaml"))
        name_parts = self.name.split(".")
        while len(name_parts) > 0:
            part = name_parts.pop(0)
            metadata = metadata[part]
            if "goto" in metadata:
                new_address: str = metadata["goto"]
                self.name = ".".join(new_address.split(".") + name_parts)
                metadata = self._get_metadata()
                break
        return metadata

    def _extract_type(self) -> Literal["manual", "file", "url", "from", "alias"]:
        for metadata_type in ("manual", "file", "url", "from", "alias"):
            if (metadata_type in self.metadata) or (self.metadata == metadata_type):
                return metadata_type
        raise ValueError(f"Metadata type is missing for {self.name}")

    def _find_extension(self) -> str:
        available_extentions = ["xlsx"]
        extension = self.metadata.get("extension", None)

        if (extension is None) and (self.url is not None):
            try:
                extension = self.url.rsplit(".", maxsplit=1)[1]
            except IndexError:
                raise ValueError(f"URL '{self.url}' does not have a valid file extension.")

        if extension not in available_extentions:
            raise ValueError(
                f"Unsupported file extension '{extension}'. "
                f"Supported extensions are: {', '.join(available_extentions)}"
            )

        return extension

    def _open_cleaned_data(self) -> pd.DataFrame:
        return pd.read_parquet(
            self.lib_defaults.dir.external.joinpath(f"{self.name}.parquet")
        )

    @property
    def raw_file_path(self) -> Path:
        raw_folder_path = self.lib_defaults.dir.external.joinpath("_raw")
        raw_folder_path.mkdir(exist_ok=True, parents=True)
        extension = self._find_extension()
        if self.metadata_type == "file":
            name = self.metadata["file"]["name"]
        else:
            name = self.name
        return raw_folder_path.joinpath(f"{name}.{extension}")

    @property
    def url(self) -> str:
        if "file" in self.metadata:
            assert "url" in self.metadata["file"]
            url = self.metadata["file"]["url"]
        else:
            url = self.metadata["url"]
        return url

    def _load_raw_file(self) -> pd.DataFrame:
        assert self.raw_file_path is not None
        if (not self.raw_file_path.exists()) or self.settings.redownload:
            utils.download(self.url, self.raw_file_path)
        if self.raw_file_path.suffix in [".xlsx"]:
            sheet_name = self.metadata.get("sheet_name", 0)
            table = pd.read_excel(
                self.raw_file_path, header=None, sheet_name=sheet_name
            )
        else:
            raise ValueError("Format not supported yet")
        return table

    def _clean_raw_file(self, table: pd.DataFrame | None = None) -> pd.DataFrame:
        if table is None:
            table = self._load_raw_file()
        try:
            table = self.cleaning_function(table)
        except AttributeError:
            print(f"Cleaning function {self.name.replace('.', '_')} do not exist")
        return table

    def _collect_and_clean(self) -> pd.DataFrame:
        data_list = self.metadata["from"]
        data_list = data_list if isinstance(data_list, list) else [data_list]
        table_list = [
            ExternalDataCleaner(table, self.lib_defaults).read_table()
            for table in data_list
        ]
        table = self.cleaning_function(table_list)
        return table

    @property
    def cleaning_function(
        self,
    ) -> Callable[[pd.DataFrame | list[pd.DataFrame]], pd.DataFrame]:
        cleaning_module = importlib.import_module(
            "bssir.external_data.cleaning_scripts"
        )
        return getattr(cleaning_module, self.name.replace(".", "_"))

    def save_table(self, table: pd.DataFrame) -> None:
        self.lib_defaults.dir.external.mkdir(exist_ok=True, parents=True)
        table.to_parquet(
            self.lib_defaults.dir.external.joinpath(f"{self.name}.parquet")
        )

    def _download_table(self) -> pd.DataFrame:
        url = (
            f"{self.lib_defaults.get_mirror(self.source).bucket_address}/"
            f"{self.lib_defaults.get_online_dir(self.source).external}/"
            f"{self.name}.parquet"
        )
        table = pd.read_parquet(url)
        if self.settings.save_downloaded:
            self.save_table(table)
        return table
