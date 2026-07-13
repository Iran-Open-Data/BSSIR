from pathlib import Path
import tomllib
from typing import Annotated, Literal

from pydantic import BaseModel, BeforeValidator, model_validator

from bssir.utils.parse import parse_years, Years

from .credential import credential_adapter
from .directory import Directories, DirectoriesNames
from .mirror import Mirror

CoveragePeriod = Annotated[list[int], BeforeValidator(parse_years)]


# class Mirror(BaseModel):
#     name: str
#     bucket_name: str

#     endpoint: str | None = None
#     region_name: str | None = None
#     url_format: str | None = None
#     directory_names: DirectoriesNames

#     @model_validator(mode="after")
#     def validate_address_configuration(self) -> "Mirror":
#         if self.endpoint is None:
#             missing = []

#             if self.region_name is None:
#                 missing.append("region_name")

#             if self.url_format is None:
#                 missing.append("url_format")

#             if missing:
#                 raise ValueError(
#                     "Mirror configuration is invalid. "
#                     f"Missing {', '.join(missing)} when endpoint is not provided."
#                 )

#         return self

#     @property
#     def bucket_address(self) -> str:
#         if self.endpoint:
#             return urllib.parse.urljoin(f"{self.endpoint}/", self.bucket_name)

#         return self.url_format.format(**self.model_dump()) # type: ignore

#     @cached_property
#     def dirs(self) -> RemoteDirectories:
#         return self._create_remote_dirs()

#     def _create_remote_dirs(self) -> RemoteDirectories:
#         return RemoteDirectories(
#             **{
#                 k: f"{self.bucket_address}/{v}"
#                 for k, v in self.directory_names.model_dump().items()
#             }
#         )


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
    download_source: str


class SetupRawDataSettings(BaseModel):
    years: Years
    replace: bool
    download_source: str


class LoadTableSettings(BaseModel):
    years: Years
    form: Literal["raw", "cleaned", "normalized"]
    on_missing: str
    download_source: str
    save_downloaded: bool
    redownload: bool
    save_created: bool
    recreate: bool


class LoadExternalTableSettings(BaseModel):
    form: str
    on_missing: str
    save_downloaded: bool
    redownload: bool
    save_created: bool
    recreate: bool


class FunctionsConfig(BaseModel):
    setup: SetupSettings
    setup_raw_data: SetupRawDataSettings
    load_table: LoadTableSettings
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

    coverage_period: CoveragePeriod

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
