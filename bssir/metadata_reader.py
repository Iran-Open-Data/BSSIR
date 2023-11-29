"""
Metadata module
"""
import functools
import re

from pathlib import Path
from typing import Any, Annotated, Literal, Callable, Iterable, Optional

from pydantic import BaseModel, BeforeValidator
import yaml

BASE_PACKAGE_DIRECTORY = Path(__file__).parents[0]
ROOT_DIRECTORT = Path().absolute()

_Years = int | Iterable[int] | str | Literal["all", "last"]


def read_yaml(
    path: Path,
    interpreter: Callable[[str], str] | None = None,
):
    """Open and parse a YAML file from package or root directory.

    Handles locating the YAML file based on provided path and
    directory location. Runs an optional string interpreter
    function before loading the YAML.

    Parameters
    ----------
    path : Path or str
        Path to YAML file.
    location : str, default "package"
        "package" or "root" directory location.
    interpreter : callable, optional
        Function to preprocess YAML string before loading.

    Returns
    -------
    dict
        Parsed YAML contents as a dictionary.
    """
    if interpreter is not None:
        with open(path, mode="r", encoding="utf8") as yaml_file:
            yaml_text = yaml_file.read()
        if yaml_text == "":
            yaml_content = {}
        else:
            yaml_text = interpreter(yaml_text)
            yaml_content = yaml.safe_load(yaml_text)
    else:
        with open(path, mode="r", encoding="utf8") as yaml_file:
            yaml_content = yaml.safe_load(yaml_file)
    yaml_content = {} if yaml_content is None else yaml_content
    return yaml_content


def flatten_dict(dictionary: dict) -> dict[tuple[Any, ...], Any]:
    """Flatten a nested dictionary into a flattened dictionary.

    Converts a nested dictionary into a flattened version where the keys
    are tuples that preserve the structure of the original nested keys.

    For example:

        {
            'a': 1,
            'b': {
                'c': 2,
                'd': {
                    'e': 3
                }
            }
        }

    would flatten to:

        {
            ('a',): 1,
            ('b','c'): 2,
            ('b','d','e'): 3
        }

    Parameters
    ----------
    dictionary : dict
        Nested dictionary to flatten.

    Returns
    -------
    dict
        Flattened dictionary.

    """
    flattened_dict = {}
    for key, value in dictionary.items():
        if isinstance(value, dict):
            flattend_value = flatten_dict(value)
            for sub_key, sub_value in flattend_value.items():
                flattened_dict[(key,) + sub_key] = sub_value
        else:
            flattened_dict[(key,)] = value
    return flattened_dict


def unflatten_dict(flattened_dict: dict[tuple[Any, ...], Any]) -> dict:
    unflattened_dict = {}
    for key, value in flattened_dict.items():
        dict_part = unflattened_dict
        for key_part in key[:-1]:
            if key_part not in dict_part:
                dict_part[key_part] = {}
            dict_part = dict_part[key_part]
        dict_part[key[-1]] = value
    return unflattened_dict


def update_settings(base_settings: dict, new_settings: dict) -> dict:
    base_settings = flatten_dict(base_settings)
    new_settings = flatten_dict(new_settings)
    for key, value in new_settings.items():
        base_settings[key] = value
    base_settings = unflatten_dict(base_settings)
    return base_settings


def read_available_years(years: list[int] | str) -> list[int]:
    if isinstance(years, list):
        return years
    if isinstance(years, str):
        years = list(range(*[int(year.strip()) for year in years.split("-")]))
        years.append(years[-1] + 1)
        return years
    raise TypeError


_DefaultYears = Annotated[list[int], BeforeValidator(read_available_years)]


class DefaultColumns(BaseModel):
    year: str
    id: str
    weight: str

    commodity_code: Optional[str] = None
    industry_code: Optional[str] = None
    occupation_code: Optional[str] = None


class DefaultFolderName(BaseModel):
    compressed: str
    unpacked: str
    extracted: str
    cleaned: str
    external: str
    maps: str
    cached: str


class DefaultDirectories(BaseModel):
    compressed: Path
    unpacked: Path
    extracted: Path
    cleaned: Path
    external: Path
    maps: Path
    cached: Path


class DefaultOnlineDirectories(BaseModel):
    root: str
    compressed: str
    unpacked: str
    extracted: str
    cleaned: str
    external: str
    maps: str
    cached: str


class LoadTableSettings(BaseModel):
    form: Literal["processed", "cleaned", "raw"]
    on_missing: Literal["error", "download", "create"]
    save_downloaded: bool
    redownload: bool
    save_created: bool
    recreate: bool


class LoadExternalTableSettings(BaseModel):
    form: Literal["processed", "original"]
    on_missing: Literal["error", "download", "create"]
    save_downloaded: bool
    redownload: bool
    save_created: bool
    recreate: bool


class DefaultFunctions(BaseModel):
    load_table: LoadTableSettings
    load_external_table: LoadExternalTableSettings


class DefaultDocs(BaseModel):
    csv: Path
    raw_tables: Path
    cleaned_tables: Path


class Defaults(BaseModel):
    package_name: str
    bucket_address: str
    online_dirs: Optional[DefaultOnlineDirectories] = None

    years: _DefaultYears

    base_package_dir: Path
    package_dir: Path
    root_dir: Path

    local_dir: Path
    in_root: bool

    folder_names: DefaultFolderName
    dirs: Optional[DefaultDirectories] = None

    columns: DefaultColumns
    functions: DefaultFunctions

    base_package_metadata: dict
    package_metadata: dict
    local_metadata: dict
    docs: DefaultDocs

    def model_post_init(self, __context=None) -> None:
        self.create_local_dir()
        self.create_dirs()
        self.create_online_dir()
        self.create_meta_paths()

    def create_local_dir(self) -> None:
        if self.local_dir.is_absolute():
            pass
        elif self.in_root:
            self.local_dir = self.root_dir.joinpath(*self.local_dir.parts)
        else:
            self.local_dir = self.package_dir.joinpath(*self.local_dir.parts)

    def create_dirs(self) -> None:
        path_dict = {}
        for key, value in self.folder_names.model_dump().items():
            path_dict[key] = (
                Path(value)
                if Path(value).is_absolute()
                else self.local_dir.joinpath(value)
            )
            path_dict[key].mkdir(exist_ok=True, parents=True)
        self.dirs = DefaultDirectories(**path_dict)

    def create_online_dir(self):
        root = f"{self.bucket_address}/{self.package_name}"
        online_dict = {"root": root}
        for key, value in self.folder_names.model_dump().items():
            online_dict[key] = f"{root}/{value}"
        self.online_dirs = DefaultOnlineDirectories(**online_dict)

    def create_meta_paths(self):
        for key, value in self.base_package_metadata.items():
            self.base_package_metadata[key] = self.base_package_dir.joinpath(value)
        for key, value in self.package_metadata.items():
            self.package_metadata[key] = self.package_dir.joinpath(value)
        for key, value in self.local_metadata.items():
            self.local_metadata[key] = self.root_dir.joinpath(value)


class Metadata:
    """
    A dataclass for accessing metadata used in other parts of the project.

    """

    instruction: dict[str, Any]
    raw_files: dict[str, Any]
    tables: dict[str, Any]
    maps: dict[str, Any]
    id_information: dict[str, Any]
    commodities: dict[str, Any]
    occupations: dict[str, Any]
    industries: dict[str, Any]
    schema: dict[str, Any]

    def __init__(self, _defaults: Defaults) -> None:
        self.defaults = _defaults
        self.metadata_files = list(_defaults.base_package_metadata.keys())
        self.reload()

    def reload(self):
        for file_name in self.metadata_files:
            self.reload_file(file_name)

    def reload_file(self, file_name):
        base_package_meta = self.defaults.base_package_metadata[file_name]
        package_meta = self.defaults.package_metadata[file_name]
        local_meta = self.defaults.local_metadata[file_name]
        interpreter = self.get_interpreter(file_name)
        _metadata: dict = read_yaml(base_package_meta, interpreter=interpreter)
        interpreter = self.get_interpreter(file_name, _metadata)
        if package_meta.exists():
            _metadata.update(read_yaml(package_meta, interpreter=interpreter))
        interpreter = self.get_interpreter(file_name, _metadata)
        if local_meta.exists():
            _metadata.update(read_yaml(local_meta, interpreter=interpreter))
        setattr(self, file_name, _metadata)

    def get_interpreter(
        self, file_name: str, context: dict | None = None
    ) -> Callable[[str], str] | None:
        context = context or {}
        if f"{file_name}_interpreter" in dir(self):
            interpreter = getattr(self, f"{file_name}_interpreter")
            interpreter = functools.partial(interpreter, context=context)
        else:
            interpreter = None
        return interpreter

    @staticmethod
    def commodities_interpreter(yaml_text: str, context: dict) -> str:
        context.update(yaml.safe_load(re.sub("{{.*}}", "", yaml_text)))
        placeholders_list: list[str] = re.findall(r"{{\s*(.*)\s*}}", yaml_text)
        mapping = {}
        for placeholder in placeholders_list:
            parts = placeholder.split(".")
            if len(parts) == 1:
                mapping[placeholder] = context[parts[0]]["items"]
            elif len(parts) == 2:
                mapping[placeholder] = context[parts[0]]["items"][parts[1]]
            else:
                raise ValueError
        for placeholder, value in mapping.items():
            yaml_text = yaml_text.replace("{{" + placeholder + "}}", str(value))
        return yaml_text


class Config:
    def __init__(self) -> None:
        self.base_package_dir = Path(__file__).parents[0]
        self.root_dir = Path().absolute()
        self.settings = self.get_base_config()

    def get_base_config(self) -> dict[str, Any]:
        base_settings_path = self.base_package_dir.joinpath("config", "settings.yaml")
        return read_yaml(base_settings_path)

    def get_package_config(self, package_path: Path) -> dict[str, Any]:
        package_setting_path = package_path.joinpath(self.settings["package_settings"])
        return read_yaml(package_setting_path)

    def get_root_config(self) -> dict[str, Any]:
        root_setting_path = self.root_dir.joinpath(self.settings["package_settings"])
        try:
            return read_yaml(root_setting_path)
        except FileNotFoundError:
            return {}

    def __setup_docs(self):
        root_dir: Path = self.settings["root_dir"]
        for key, value in self.settings["docs"].items():
            root_dir.joinpath("docs", value).mkdir(parents=True, exist_ok=True)
            self.settings["docs"][key] = root_dir.joinpath("docs", value)
        gitignore = root_dir.joinpath("docs", "temp", ".gitignore")
        if not gitignore.exists():
            with gitignore.open(mode="w") as file:
                file.write("# This file created automatically by BSSIR\n*\n")

    def set_package_config(self, package_path: Path) -> tuple[Defaults, Metadata]:
        for new_setting in [
            self.get_package_config(package_path),
            self.get_root_config(),
        ]:
            self.settings = update_settings(self.settings, new_setting)
        self.settings["base_package_dir"] = self.base_package_dir
        self.settings["package_dir"] = package_path
        self.settings["root_dir"] = self.root_dir
        self.__setup_docs()
        _defaults = Defaults(**self.settings)
        _metadata = Metadata(_defaults)
        return _defaults, _metadata


config = Config()
defaults, metadata = config.set_package_config(BASE_PACKAGE_DIRECTORY)
