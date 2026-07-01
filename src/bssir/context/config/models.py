from pathlib import Path
from functools import cached_property
from typing import Annotated, Literal
import urllib.parse

from pydantic import BaseModel, BeforeValidator, model_validator

from bssir.context.utils.parser import parse_years


CoveragePeriod = Annotated[list[int], BeforeValidator(parse_years)]


class DirectoriesNames(BaseModel):
    original: str
    unpacked: str
    extracted: str
    cleaned: str
    external: str
    maps: str
    cached: str


class Directories(BaseModel):
    original: Path
    unpacked: Path
    extracted: Path
    cleaned: Path
    external: Path
    maps: Path
    cached: Path


class OnlineDirectories(BaseModel):
    original: str
    unpacked: str
    extracted: str
    cleaned: str
    external: str
    maps: str


class Mirror(BaseModel):
    name: str
    bucket_name: str

    endpoint: str | None = None
    region_name: str | None = None
    url_format: str | None = None
    directory_names: DirectoriesNames

    @model_validator(mode="after")
    def validate_address_configuration(self) -> "Mirror":
        if self.endpoint is None:
            missing = []

            if self.region_name is None:
                missing.append("region_name")

            if self.url_format is None:
                missing.append("url_format")

            if missing:
                raise ValueError(
                    "Mirror configuration is invalid. "
                    f"Missing {', '.join(missing)} when endpoint is not provided."
                )

        return self

    @property
    def bucket_address(self) -> str:
        if self.endpoint:
            return urllib.parse.urljoin(f"{self.endpoint}/", self.bucket_name)

        return self.url_format.format(**self.model_dump()) # type: ignore

    @cached_property
    def dirs(self) -> OnlineDirectories:
        return self._create_online_dirs()

    def _create_online_dirs(self) -> OnlineDirectories:
        return OnlineDirectories(
            **{
                k: f"{self.bucket_address}/{v}"
                for k, v in self.directory_names.model_dump()
            }
        )


class StandardColumns(BaseModel):
    year: str
    id: str
    weight: str

    commodity_code: list[str]
    industry_code: list[str]
    occupation_code: list[str]

    groupby: list


class SetupSettings(BaseModel):
    years: str
    table_names: str
    replace: bool
    method: str
    download_source: str


class SetupRawDataSettings(BaseModel):
    years: str
    replace: bool
    download_source: str


class LoadTableSettings(BaseModel):
    years: str
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


class Docs(BaseModel):
    csv: Path
    raw_tables: Path
    cleaned_tables: Path


class Config(BaseModel):
    package_name: str

    coverage_period: CoveragePeriod

    default_download_source: str
    private_data: bool

    mirrors: list[Mirror]
    default_mirror: str | None = None

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
