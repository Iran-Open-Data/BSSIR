"""
This module provides utility functions for downloading, unpacking, and extracting
household budget survey data from archive files. 

Key functions:

- setup() - Downloads, unpacks, and extracts data for specified years
- download() - Downloads archive files for given years 
- unpack() - Unpacks archive files into directories
- extract() - Extracts tables from Access DBs as CSVs

The key functions allow:

- Downloading survey data archive files for specified years from an online directory.

- Unpacking the downloaded archive files (which are in .rar format) into directories.
  Nested archives are extracted recursively.
  
- Connecting to the MS Access database file contained in each archive.

- Extracting all tables from the Access database as CSV files.

This enables access to the raw underlying survey data tables extracted directly 
from the archive Access database files, before any cleaning or processing is applied. 

The extracted CSV table data can then be loaded and cleaned as needed by the
data_cleaner module. 

Typical usage often only requires the cleaned processed data from data_engine.  
However, this module is useful for development and checking details in the original
raw data tables before cleaning.
"""

from contextlib import contextmanager
from typing import Generator, Literal, Optional
import shutil
import platform
from pathlib import Path

from dbfread import DBF
import pandas as pd
import pyodbc

from . import utils
from .metadata_reader import Defaults, Metadata


def setup(
    years: list[int],
    *,
    lib_metadata: Metadata,
    lib_defaults: Defaults,
    replace: bool = False,
    download_source: Literal["original", "mirror"] | str = "original",
) -> None:
    """Download, unpack, and extract survey data for the specified years.

    This function executes the full workflow to download, unpack, and
    extract the raw survey data tables for the given years.

    It calls the download(), unpack(), and extract() functions internally.

    The years can be specified as:

    - int: A single year
    - Iterable[int]: A list or range of years
    - str: A string range like "1390-1400"
    - "all": All available years (default)
    - "last": Just the last year

    Years are parsed and validated by the `parse_years()` helper.

    Existing files are skipped unless `replace=True`.

    Parameters
    ----------
    years : _Years, optional
        Years to setup data for. Default is "all".

    replace : bool, optional
        Whether to re-download and overwrite existing files.

    Returns
    -------
    None

    Examples
    --------
    >>> setup(1393) # Setup only 1393 skip if files already exist

    >>> setup("1390-1400") # Setup 1390 to 1400

    >>> setup("last", replace=True) # Setup last year, replace if already exists

    Notes
    -----
    This function is intended for development use to access the raw data.

    For analysis you likely only need the cleaned dataset.

    Warnings
    --------
    Setting up the full range of years will download and extract
    approximately 12 GB of data.

    See Also
    --------
    download : Download archive files.
    unpack : Unpack archive files.
    extract : Extract tables from Access DBs.
    parse_years : Validate and parse year inputs.
    """
    download(
        years,
        replace=replace,
        source=download_source,
        lib_metadata=lib_metadata,
        lib_defaults=lib_defaults,
    )
    unpack(years, replace=replace, lib_defaults=lib_defaults)
    extract(years, replace=replace, lib_defaults=lib_defaults)


def download(
    years: list[int],
    *,
    lib_metadata: Metadata,
    lib_defaults: Defaults,
    replace: bool = False,
    source: Literal["original", "mirror"] | str = "original",
) -> None:
    """Download archive files for the specified years.

    This downloads the archive files for the given years from the
    online directory to local storage.

    Years are parsed and validated by the `parse_years()` helper.

    Parameters
    ----------
    years : _Years, optional
        Years to download archives for. Default is "all".

    replace : bool, optional
        Whether to re-download existing files.

    Returns
    -------
    None

    Raises
    ------
    HTTPError
        If the URL cannot be reached or the file not found.

    See Also
    --------
    setup : Download, unpack, and extract data for given years.
    parse_years : Parse and validate different year representations.
    """

    def _download_file(url: str, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists() and not replace:
            return
        utils.download(url, path)

    for year in years:
        files = lib_metadata.raw_files[year]

        if source == "original":
            for file in files.get("compressed_files", []):
                file_path = lib_defaults.dir.compressed.joinpath(
                    str(year), file["name"]
                )
                url = file[source]
                _download_file(url, file_path)
            for file in files.get("unpacked_files", []):
                file_path = lib_defaults.dir.unpacked.joinpath(str(year), file["name"])
                url = file[source]
                _download_file(url, file_path)

        else: 
            index = 0 if source == "mirror"  else lib_defaults.get_mirror_index(source)
            for file in files.get("compressed_files", []):
                file_str_path = f"{year}/{file['name']}"
                url = f"{lib_defaults.online_dirs[index].compressed}/{file_str_path}"
                file_path = lib_defaults.dir.compressed.joinpath(file_str_path)
                _download_file(url, file_path)
            for file in files.get("unpacked_files", []):
                file_str_path = f"{year}/{file['name']}"
                url = f"{lib_defaults.online_dirs[index].unpacked}/{file_str_path}"
                file_path = lib_defaults.dir.unpacked.joinpath(file_str_path)
                _download_file(url, file_path)


def unpack(years: list[int], *, lib_defaults: Defaults, replace: bool = False) -> None:
    """Extract archive files for the specified years.

    This extracts the RAR archive for each given year from
    defaults.archive_files into a directory under defaults.unpacked_data.

    Nested ZIP/RAR archives found within the extracted files are also
    recursively unpacked.

    The years can be specified as:

    - int: A single year
    - Iterable[int]: A list or range of years
    - str: A string range like "1390-1400"
    - "all": Extract all available years
    - "last": Extract just the last year

    Years are parsed and validated by parse_years().

    Parameters
    ----------
    years : _Years, optional
        Years to extract archives for. Default is "all".

    replace : bool, optional
        Whether to re-extract if directories already exist.

    Returns
    -------
    None

    See Also
    --------
    setup : Download, unpack, and extract data for given years.
    parse_years : Parse and validate year inputs.
    """

    for year in years:
        _unpack_yearly_data_archive(year, lib_defaults=lib_defaults, replace=replace)


def _unpack_yearly_data_archive(
    year: int, *, lib_defaults: Defaults, replace: bool = True
):
    """Extract the RAR archive for the given year.

    See Also
    --------
    unpack: Unpack archive files for the specified years.
    _unpack_archives_recursive : Recursively extracts nested archives.
    """
    zip_files = lib_defaults.dir.compressed.joinpath(str(year))
    year_directory = lib_defaults.dir.unpacked.joinpath(str(year))
    year_directory.mkdir(parents=True, exist_ok=True)
    data_files = [
        file
        for file in year_directory.iterdir()
        if file.suffix.lower in [".dbf", ".mdb", ".accdb"]
    ]
    if len(data_files) > 0:
        if not replace:
            return
        for file in data_files:
            file.unlink()
    for file in zip_files.iterdir():
        utils.extract(file, year_directory)
    _unpack_archives_recursive(year_directory)


def _unpack_archives_recursive(target_directory: Path):
    """Recursively extract nested archives under the given directory.

    This searches the given directory for any ZIP/RAR files, and extracts
    them using 7zip. It calls itself recursively on any nested archives found
    within the extracted directories.

    Stops recursing once no more archives are found.

    Parameters
    ----------
    directory : Path
        The directory under which to recursively extract archives.

    Returns
    -------
    None
    """
    while True:
        for directory in [d for d in target_directory.iterdir() if d.is_dir()]:
            for path in directory.iterdir():
                if path.is_dir():
                    shutil.copytree(path, path.parents[1])
                else:
                    shutil.copy(path, path.parents[1])
            shutil.rmtree(directory)
        archive_files = [
            file for file in target_directory.iterdir() if file.suffix in (".zip", ".rar")
        ]
        if len(archive_files) == 0:
            break
        for file in archive_files:
            utils.extract(file, target_directory)
            Path(file).unlink()


def extract(
    years: list[int],
    *,
    lib_defaults: Defaults,
    replace: bool = False,
) -> None:
    """Extract tables from Access DBs into CSV files for the given years.

    This connects to the Access database file for each specified year,
    extracts all the tables, and saves them as CSV files under
    defaults.extracted_data.

    Parameters
    ----------
    years: _Years, optional
        Years to extract tables for. Default is "all".

    replace: bool, optional
        Whether to overwrite existing extracted CSV files.

    Returns
    -------
    None

    See Also
    --------
    setup : Download, unpack, and extract data for given years.
    parse_years : Parse and validate year inputs.
    """
    for year in years:
        year_directory = lib_defaults.dir.unpacked.joinpath(str(year))
        access_files = [
            file
            for file in year_directory.iterdir()
            if file.suffix.lower() in [".mdb", ".accdb"]
        ]
        if replace:
            _remove_extracted_directory(year, lib_defaults=lib_defaults)
        for file in access_files:
            add_prefix = len(access_files) > 1
            _extract_tables_from_access_file(
                year,
                file,
                lib_defaults=lib_defaults,
                replace=replace,
                add_prefix=add_prefix,
            )

        dbf_files = [
            file for file in year_directory.iterdir() if file.suffix.lower() == ".dbf"
        ]
        for file in dbf_files:
            _extract_tables_from_dbf_file(
                year, file, lib_defaults=lib_defaults, replace=replace
            )


def _remove_extracted_directory(
    year: int,
    *,
    lib_defaults: Defaults,
) -> None:
    extracted_directory = lib_defaults.dir.extracted.joinpath(str(year))
    if not extracted_directory.exists():
        return
    for file in extracted_directory.iterdir():
        file.unlink()
    extracted_directory.rmdir()


def _extract_tables_from_access_file(
    year: int,
    file_path: Path,
    *,
    lib_defaults: Defaults,
    replace: bool = True,
    add_prefix: bool = False,
) -> None:
    with _create_cursor(file_path) as cursor:
        table_list = _get_access_table_list(cursor)
        name_prefix = file_path.stem if add_prefix else None
        for table_name in table_list:
            _extract_table(
                cursor,
                year,
                table_name=table_name,
                lib_defaults=lib_defaults,
                replace=replace,
                name_prefix=name_prefix,
            )


@contextmanager
def _create_cursor(file_path: Path) -> Generator[pyodbc.Cursor, None, None]:
    connection_string = _make_connection_string(file_path)
    connection = pyodbc.connect(connection_string)
    try:
        yield connection.cursor()
    finally:
        connection.close()


def _make_connection_string(file_path: Path):
    if platform.system() == "Windows":
        driver = "Microsoft Access Driver (*.mdb, *.accdb)"
    else:
        driver = "MDBTools"
    conn_str = f"DRIVER={{{driver}}};" f"DBQ={file_path};"
    return conn_str


def _get_access_table_list(cursor: pyodbc.Cursor) -> list:
    table_list = []
    access_tables = cursor.tables()
    for table in access_tables:
        table_list.append(table.table_name)
    table_list = [table for table in table_list if "MSys" not in table]
    return table_list


def _extract_table(
    cursor: pyodbc.Cursor,
    year: int,
    table_name: str,
    *,
    lib_defaults: Defaults,
    replace: bool = True,
    name_prefix: Optional[str] = None
):
    year_directory = lib_defaults.dir.extracted.joinpath(str(year))
    year_directory.mkdir(parents=True, exist_ok=True)
    file_name = table_name if name_prefix is None else f"{name_prefix}_{table_name}"
    file_path = year_directory.joinpath(f"{file_name}.csv")
    if (file_path.exists()) and (not replace):
        return
    try:
        table = _get_access_table(cursor, table_name)
    except pyodbc.Error:
        print(f"table {table_name} from {year} failed to extract")
        return
    table.to_csv(file_path, index=False)


def _get_access_table(cursor: pyodbc.Cursor, table_name: str) -> pd.DataFrame:
    rows = cursor.execute(f"SELECT * FROM [{table_name}]").fetchall()
    headers = [c[0] for c in cursor.description]
    table = pd.DataFrame.from_records(rows, columns=headers)
    return table


def _extract_tables_from_dbf_file(
    year: int, file_path: Path, *, lib_defaults: Defaults, replace: bool = True
) -> None:
    try:
        table = pd.DataFrame(iter(DBF(file_path)))
    except UnicodeDecodeError:
        table = pd.DataFrame(iter(DBF(file_path, encoding="cp720")))
    year_directory = lib_defaults.dir.extracted.joinpath(str(year))
    year_directory.mkdir(parents=True, exist_ok=True)
    csv_file_path = year_directory.joinpath(f"{file_path.stem}.csv")
    if csv_file_path.exists() and not replace:
        return
    table.to_csv(csv_file_path, index=False)
