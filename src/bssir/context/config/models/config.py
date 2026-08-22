from pathlib import Path
import tomllib
from typing import Annotated, Literal

from pydantic import BaseModel, BeforeValidator, model_validator

from bssir import utils

from .credential import credential_adapter
from .directory import Directories, DirectoriesNames
from .mirror import Mirror


Years = Annotated[list[int], BeforeValidator(utils.years.parse)]


class StandardColumns(BaseModel):
    year: str
    id: str
    weight: str

    commodity_code: list[str]
    industry_code: list[str]
    occupation_code: list[str]

    groupby: list


class SetupSettings(BaseModel):
    years: Years
    table_names: str
    replace: bool
    method: str
    download_source: str | None


class SetupRawDataSettings(BaseModel):
    years: Years
    replace: bool
    download_source: str | None


class LoadTableSettings(BaseModel):
    years: Years
    kind: Literal["raw", "cleaned", "normalized"]
    on_missing: str
    download_source: str | None
    save_downloaded: bool
    redownload: bool
    save_created: bool
    recreate: bool


class LoadExternalTableSettings(BaseModel):
    kind: str
    on_missing: str
    save_downloaded: bool
    redownload: bool
    save_created: bool
    recreate: bool


class FunctionsConfig(BaseModel):
    setup: SetupSettings
    setup_raw_data: SetupRawDataSettings
    load_table: LoadTableSettings
    # add_attribute: 
    load_external_table: LoadExternalTableSettings


class ToolsConfig(BaseModel):
    unrar: str | None = None
    sevenzip: str | None = None


class Docs(BaseModel):
    csv: Path
    raw_tables: Path
    cleaned_tables: Path


class Config(BaseModel):
    package_name: str

    coverage_period: Years

    default_download_source: str

    tools: ToolsConfig

    mirrors: list[Mirror]
    default_mirror: str | None = None

    credentials_file: Path

    base_package_dir: Path
    package_dir: Path
    root_dir: Path
    local_settings: str

    local_dir: Path
    local_dir_in_root: bool

    directory_names: DirectoriesNames
    dirs: Directories

    standard_columns: StandardColumns
    functions: FunctionsConfig

    base_package_metadata: dict
    package_metadata: dict
    local_metadata: dict
    docs: Docs

    @model_validator(mode="after")
    def load_private_mirror_credentials(self) -> "Config":
        if not self.credentials_file.exists():
            return self

        with open(self.credentials_file, "rb") as f:
            tokens_data = tomllib.load(f)

        for mirror in self.mirrors:
            if mirror.name in tokens_data:
                raw_creds = tokens_data[mirror.name]
                cred_instance = credential_adapter.validate_python(raw_creds)
                mirror._credentials = cred_instance

        return self

    def get_mirror(self, name: str | Literal["default", "mirror"] | None = None) -> Mirror:
        """
        Return a configured mirror.

        Parameters
        ----------
        name : str | Literal["default"] | None, default None
            Mirror to retrieve.

            - ``None`` returns the first configured mirror.
            - ``"default"`` returns the configured default mirror.
            - Any other string is treated as a mirror name.

        Returns
        -------
        Mirror
            The requested mirror.

        Raises
        ------
        LookupError
            If no matching mirror is found.
        """
        if isinstance(name, str):
            name = name.lower()

        if name in ["default", "mirror"]:
            name = self.default_mirror

        if name is None:
            return self.mirrors[0]

        for mirror in self.mirrors:
            if mirror.name == name:
                return mirror

        available = ", ".join(mirror.name for mirror in self.mirrors)
        raise LookupError(
            f"Unknown mirror {name!r}. Available mirrors: {available}"
        )
