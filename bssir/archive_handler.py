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
import logging
from contextlib import contextmanager
from typing import Generator, Literal, Optional
import shutil
import platform
from pathlib import Path

from tqdm.auto import tqdm
from dbfread import DBF
import pandas as pd
import pyodbc

from . import utils
from .metadata_reader import Defaults, Metadata


ARCHIVE_EXTENSIONS = {".zip", ".rar"}
DATA_FILE_EXTENSIONS = {".dbf", ".mdb", ".accdb"}


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
    """Downloads data archives for a list of specified years.

    This function iterates through a list of years and calls a helper
    to download the corresponding data files. A progress bar is displayed
    to show the overall progress.

    Parameters
    ----------
    years : list[int]
        A list of integer years to download.
    lib_metadata : _Metadata
        A configuration object containing metadata about the files.
    lib_defaults : _Defaults
        A configuration object with default settings, like directory paths.
    replace : bool, optional
        If True, existing files will be re-downloaded, by default False.
    source : str, optional
        The download source, either "original" or a mirror name,
        by default "original".
    """
    for year in tqdm(
        years,
        desc="Downloading annual data",
        bar_format=lib_defaults.bar_format,
        unit="Year",
    ):
        _download_year(
            year,
            lib_metadata=lib_metadata,
            lib_defaults=lib_defaults,
            replace=replace,
            source=source,
        )


def _download_year(
    year: int,
    *,
    lib_metadata: Metadata,
    lib_defaults: Defaults,
    replace: bool,
    source: str,
) -> None:
    """Downloads all files for a single year from the specified source.

    This helper function constructs the appropriate URLs and local file paths
    based on the download source and then downloads each file.
    """
    files_to_download: list[dict] = lib_metadata.raw_files.get(year, {}).get("files", [])
    if not files_to_download:
        logging.warning(f"No files listed in metadata for year {year}.")
        return

    # Determine the base URL and path based on the source once
    if source == "original":
        base_url_template = None  # URL is directly in file metadata
    else:
        mirror_index = lib_defaults.get_mirror_index(source)
        base_url_template = lib_defaults.online_dirs[mirror_index].original
    base_path = lib_defaults.dir.original

    for file_info in tqdm(
        files_to_download,
        desc=f"Downloading files for {year}",
        bar_format=lib_defaults.bar_format,
        unit="File",
        leave=False,
    ):
        file_name: str = file_info["name"]
        relative_path = Path(str(year), file_name)
        local_path = base_path / relative_path

        if source == "original":
            url = file_info.get("original")
            if not url:
                logging.error(f"Missing 'original' URL for {file_name} in year {year}.")
                continue
        else:
            url = f"{base_url_template}/{relative_path.as_posix()}"

        if local_path.exists() and not replace:
            logging.info(f"Skipping existing file: {local_path}")
            continue

        utils.download(url, local_path)


def unpack(years: list[int], *, lib_defaults: Defaults, replace: bool = False) -> None:
    """Extracts data archives for a list of specified years.

    This function serves as the main entry point for the unpacking process.
    It iterates through a list of years and calls a helper function to
    handle the unpacking for each individual year. A progress bar is
    displayed to show the overall progress.

    Parameters
    ----------
    years : list[int]
        A list of integer years to process.
    lib_defaults : _Defaults
        A configuration object with directory paths.
    replace : bool, optional
        If True, existing unpacked data will be deleted before
        extraction, by default False.

    See Also
    --------
    setup: The main workflow function that calls this unpacker.
    _unpack_year: The helper function that performs the actual unpacking
                  for a single year.
    """
    for year in tqdm(
        years,
        desc="Unpacking annual archives",
        bar_format=lib_defaults.bar_format,
        unit="Year",
    ):
        _unpack_year(year, lib_defaults=lib_defaults, replace=replace)


def _unpack_year(year: int, *, lib_defaults: Defaults, replace: bool = True) -> None:
    """Unpacks all archive and data files for a single year.

    This function manages the unpacking process for a given year's data. It
    prepares a destination directory, handling existing data based on the
    `replace` flag. It then iterates through the source directory,
    extracting any archives and copying over individual files. Finally,
    it triggers a process to handle any nested archives.

    Parameters
    ----------
    year : int
        The year to unpack data for.
    lib_defaults : _Defaults
        A configuration object containing source and destination directory paths.
    replace : bool, optional
        If True, any existing unpacked data for the year will be deleted
        before unpacking. If False, the function will skip the year if
        data already exists.

    See Also
    --------
    unpack : The public function that calls this helper for each year.
    _unpack_nested_archives : Handles archives found inside other archives.
    """
    source_dir = lib_defaults.dir.original.joinpath(str(year))
    dest_dir = lib_defaults.dir.unpacked.joinpath(str(year))

    # --- 1. Prepare the destination directory: skip or clean as needed. ---
    if dest_dir.exists():
        if not replace:
            logging.info(f"Skipping year {year}: Unpacked data already exists.")
            return
        logging.warning(f"Replacing existing data for year {year}.")
        shutil.rmtree(dest_dir)
    dest_dir.mkdir(exist_ok=True, parents=True)

    # --- 2. Ensure the source directory exists before proceeding. ---
    if not source_dir.exists():
        logging.error(f"Source directory not found for year {year}: {source_dir}")
        return

    # --- 3. Perform the initial extraction from the source directory. ---
    for item in source_dir.iterdir():
        if item.suffix.lower() in ARCHIVE_EXTENSIONS:
            utils.extract(item, dest_dir)
        elif item.is_file():
            shutil.copy(item, dest_dir)

    # --- 4. After the initial extraction, find and unpack any nested archives. ---
    _unpack_nested_archives(dest_dir)


def _unpack_nested_archives(target_dir: Path) -> None:
    """Iteratively finds and extracts nested archives within a directory.

    This function performs two main actions in a loop until no archives remain:
    1.  Flattens subdirectories: Moves contents of any subdirectory up into
        the target directory, then removes the now-empty subdirectory. This
        handles cases where an archive unpacks into its own folder.
    2.  Extracts archives: Finds and extracts all archives in the
        target directory, then deletes the original archive file.

    Parameters
    ----------
    target_dir
        The directory in which to search for and unpack nested archives.
    """
    while True:
        # --- 1. Flatten any subdirectories created by previous extractions ---
        sub_dirs = [d for d in target_dir.iterdir() if d.is_dir()]
        for sub_dir in sub_dirs:
            for item in sub_dir.iterdir():
                try:
                    shutil.move(item, target_dir)
                except shutil.Error as e:
                    logging.warning(f"Could not move '{item.name}': {e}. It may be a duplicate.")
            shutil.rmtree(sub_dir)
        sub_dirs = [d for d in target_dir.iterdir() if d.is_dir()]

        # --- 2. Find and extract any archives at the current level ---
        archive_files = [
            f for f in target_dir.iterdir()
            if f.is_file() and f.suffix.lower() in ARCHIVE_EXTENSIONS
        ]
        logging.info(f"Found {len(archive_files)} nested archives to unpack.")
        for archive in archive_files:
            utils.extract(archive, target_dir)
            archive.unlink()  # Clean up the archive file after extraction.
        archive_files = [
            f for f in target_dir.iterdir()
            if f.is_file() and f.suffix.lower() in ARCHIVE_EXTENSIONS
        ]

        # If no archives or subdirectories are left to extract, our work is done.
        if (not archive_files) and (not sub_dirs):
            break


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
    for year in tqdm(
        years,
        desc="Extracting annual archives",
        bar_format=lib_defaults.bar_format,
        unit="Year",
    ):
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
