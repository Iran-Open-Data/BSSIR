"""
Utilities to download, unpack and extract raw survey tables from archive files.

This module provides a small pipeline for obtaining raw household budget survey
data and exporting raw tables as CSVs. It is intentionally low-level: it
downloads archive files, unpacks them (including nested archives), locates
MS Access (.mdb/.accdb) and DBF (.dbf) data files, then extracts each table
into a CSV under the configured "extracted" directory.

Primary functions
- setup(years, lib_metadata, lib_defaults, replace, download_source)
  Orchestrates download -> unpack -> extract for the requested years.
- download(years, lib_metadata, lib_defaults, replace, source)
  Downloads archive files listed in metadata.
- unpack(years, lib_defaults, replace)
  Unpacks archive files and flattens nested archives.
- extract(years, lib_defaults, replace)
  Finds Access and DBF files in unpacked directories and writes CSVs.

Design notes
- Access extraction uses pyodbc; DBF extraction uses dbfread; pandas is used to
  create CSVs.
- The module expects Metadata and Defaults objects (from metadata_reader) to
  provide file lists, local directories and UI settings (progress bar format).
- When multiple Access files exist for a year, CSV filenames may be prefixed
  with the Access filename stem to avoid collisions.
- Extraction attempts to avoid leaving partial CSVs (atomic or .part -> replace
  patterns are used where appropriate) and logs errors per table so a single
  failure does not stop processing other files.

Platform & dependency notes
- On Windows the code expects an appropriate MS Access ODBC driver (used by
  pyodbc). On non-Windows systems an MDBTools-based driver may be required.
- Required third-party packages: pyodbc, dbfread, pandas, tqdm.

This module is intended for developers and reproducible processing workflows
that need access to original raw tables before any cleaning. Higher-level
consumers should prefer the cleaned outputs produced by the project's
data_cleaner / data_engine modules.
"""
import logging
from contextlib import contextmanager
from typing import Generator, Literal, Optional, Iterable
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
MS_ACCESS_FILE_EXTENSIONS = {".mdb", ".accdb"}
DBF_FILE_EXTENSIONS = {".dbf"}


def setup(
    years: list[int],
    *,
    lib_metadata: Metadata,
    lib_defaults: Defaults,
    replace: bool,
    download_source: Literal["original", "mirror"] | str,
) -> None:
    """Download, unpack, and extract survey data for the specified years.

    This function orchestrates the entire data setup pipeline by sequentially
    calling the download, unpack, and extract functions. It is the primary
    function for preparing the raw data.

    Parameters
    ----------
    years : list[int]
        A list of integer years for which to set up the data.
    lib_metadata : Metadata
        An instance of the `Metadata` class. It provides structured access to all
        the metadata required for the setup process, such as the list of raw
        files to download for each year, table schemas, and processing instructions.
    lib_defaults
        An instance of the `Defaults` class. It serves as the central
        configuration hub, providing all necessary settings like local directory
        paths for storing data, online mirror URLs, and other default values.
    replace : bool
        If True, any existing files will be overwritten.
    download_source : str
        The source from which to download data, e.g., "original" or a mirror.

    See Also
    --------
    download : Downloads the raw archive files.
    unpack : Unpacks the downloaded archives.
    extract : Extracts data tables from databases into CSV format.
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
    replace: bool,
    source: Literal["original", "mirror"] | str,
) -> None:
    """Downloads data archives for a list of specified years.

    This function iterates through a list of years and calls a helper
    to download the corresponding data files. A progress bar is displayed
    to show the overall progress.

    Parameters
    ----------
    years : list[int]
        A list of integer years to download.
    lib_metadata : Metadata
        A configuration object containing metadata about the files.
    lib_defaults : Defaults
        A configuration object with default settings, like directory paths.
    replace : bool
        If True, existing files will be re-downloaded.
    source : str
        The download source, either "original" or a mirror name.
    """
    for year in tqdm(
        years,
        desc="Downloading annual data",
        bar_format=lib_defaults.bar_format,
        unit="Year",
    ):
        if lib_defaults.private_data:
            _download_year_private_data(
                year,
                lib_metadata=lib_metadata,
                lib_defaults=lib_defaults,
                replace=replace,
                source=source,
            )
        else:
            _download_year_public_data(
                year,
                lib_metadata=lib_metadata,
                lib_defaults=lib_defaults,
                replace=replace,
                source=source,
            )


def _download_year_private_data(
    year: int,
    *,
    lib_metadata: Metadata,
    lib_defaults: Defaults,
    replace: bool,
    source: str,
) -> None:
    from .utils.s3 import get_bucket
    index = lib_defaults.get_mirror_index(source)
    mirror = lib_defaults.mirrors[index]
    bucket = get_bucket(mirror)
    for file_info in tqdm(
        _gets_files_to_download(year, lib_metadata=lib_metadata),
        desc=f"Downloading files for {year}",
        bar_format=lib_defaults.bar_format,
        unit="File",
        leave=False,
    ):
        target_path = lib_defaults.dir.original.joinpath(file_info["name"])
        if target_path.exists() and not replace:
            continue
        bucket.download_file(
            f'{lib_defaults.folder_names.original}/{str(year)}/{file_info["name"]}',
            str(target_path.absolute()),
        )


def _download_year_public_data(
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
    # Determine the base URL and path based on the source once
    if source == "original":
        base_url_template = None  # URL is directly in file metadata
    else:
        mirror_index = lib_defaults.get_mirror_index(source)
        base_url_template = lib_defaults.online_dirs[mirror_index].original
    base_path = lib_defaults.dir.original

    for file_info in tqdm(
        _gets_files_to_download(year, lib_metadata=lib_metadata),
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


def _gets_files_to_download(
    year: int,
    *,
    lib_metadata: Metadata,
) -> list[dict]:
    files_to_download: list[dict] = lib_metadata.raw_files.get(year, {}).get("files", [])
    if not files_to_download:
        logging.warning(f"No files listed in metadata for year {year}.")
    return files_to_download


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
        archive_files = _find_files_with_extensions(target_dir, ARCHIVE_EXTENSIONS)
        logging.info(f"Found {len(archive_files)} nested archives to unpack.")
        for archive in archive_files:
            utils.extract(archive, target_dir)
            archive.unlink()  # Clean up the archive file after extraction.
        archive_files = _find_files_with_extensions(target_dir, ARCHIVE_EXTENSIONS)

        # If no archives or subdirectories are left to extract, our work is done.
        if (not archive_files) and (not sub_dirs):
            break


def extract(
    years: list[int],
    *,
    lib_defaults: Defaults,
    replace: bool,
) -> None:
    """Extract raw tables from unpacked data into CSV files.

    For each year in `years` this function:
    - Scans lib_defaults.dir.unpacked/<year> for MS Access (.mdb/.accdb) and DBF (.dbf) files.
    - For each Access file, opens a connection, reads every table and writes a CSV to
      lib_defaults.dir.extracted/<year>. If multiple Access files exist for a year,
      CSV filenames are prefixed with the Access filename stem to avoid name collisions.
    - For each DBF file, reads the DBF and writes a CSV with the DBF filename stem.

    Parameters
    ----------
    years : list[int]
        Years to extract tables for.
    lib_defaults : Defaults
        Configuration object providing directory paths (unpacked, extracted) and UI settings.
    replace : bool
        If True, overwrite existing extracted CSV files; if False, skip existing files.

    Notes
    -----
    - Missing unpacked directories are skipped (a warning is logged).
    - Access extraction uses pyodbc; DBF extraction uses dbfread. Errors extracting individual
      tables are logged and do not stop the overall extraction process.

    Returns
    -------
    None
    """
    for year in tqdm(
        years,
        desc="Extracting annual archives",
        bar_format=lib_defaults.bar_format,
        unit="Year",
    ):
        source_dir = lib_defaults.dir.unpacked.joinpath(str(year))
        access_files = _find_files_with_extensions(source_dir, MS_ACCESS_FILE_EXTENSIONS)
        if replace:
            shutil.rmtree(lib_defaults.dir.extracted / str(year))
        for file in access_files:
            add_prefix = len(access_files) > 1
            _extract_tables_from_access_file(
                year,
                file,
                lib_defaults=lib_defaults,
                replace=replace,
                add_prefix=add_prefix,
            )

        dbf_files = _find_files_with_extensions(source_dir, DBF_FILE_EXTENSIONS)
        for file in dbf_files:
            _extract_tables_from_dbf_file(
                year, file, lib_defaults=lib_defaults, replace=replace
            )


def _extract_tables_from_access_file(
    year: int,
    file_path: Path,
    *,
    lib_defaults: Defaults,
    replace: bool,
    add_prefix: bool,
) -> None:
    """Extract all non-system tables from an Access file into CSVs.

    Behaviour
    - If the Access file does not exist, logs and returns.
    - Opens a DB cursor using _create_cursor(). Connection errors are caught
      and logged so extraction can continue for other files/years.
    - Obtains a list of table names via _get_access_table_list() and extracts
      each table using _extract_table(). When multiple Access files exist for
      a year, `add_prefix` controls whether the Access filename stem is added
      as a prefix to avoid name collisions.
    - Uses a nested progress bar for per-table progress (respecting
      lib_defaults.bar_format) and logs if no tables were found.

    Parameters
    ----------
    year : int
        Year being processed (used to determine destination directory).
    file_path : Path
        Path to the .mdb/.accdb file to read.
    lib_defaults : Defaults
        Defaults/configuration object (provides extracted dir and UI settings).
    replace : bool
        If True, overwrite existing CSVs; if False, skip existing files.
    add_prefix : bool
        If True, prefix CSV filenames with the Access file stem to avoid collisions.

    Returns
    -------
    None
    """
    if not file_path.exists():
        logging.error(f"Access file not found: {file_path}")
        return

    try:
        with _create_cursor(file_path) as cursor:
            table_list = _get_access_table_list(cursor)
            if not table_list:
                logging.info(f"No user tables found in Access DB: {file_path}")
                return

            name_prefix = file_path.stem if add_prefix else None
            for table_name in tqdm(
                table_list,
                desc=f"Extracting tables from {file_path.name}",
                bar_format=lib_defaults.bar_format,
                unit="Table",
                leave=False,
            ):
                _extract_table(
                    cursor,
                    year,
                    table_name=table_name,
                    lib_defaults=lib_defaults,
                    replace=replace,
                    name_prefix=name_prefix,
                )
    except pyodbc.Error as exc:
        logging.error(f"Failed to open Access DB '{file_path}': {exc}")
    except Exception as exc:
        logging.exception(f"Unexpected error extracting from Access DB '{file_path}': {exc}")


@contextmanager
def _create_cursor(file_path: Path) -> Generator[pyodbc.Cursor, None, None]:
    """Context manager that yields a pyodbc cursor for an Access database file.

    Builds an ODBC connection string with _make_connection_string(), opens a
    pyodbc.Connection and yields a cursor. The connection is always closed when
    the context exits. Connection errors are logged and re-raised so callers
    can handle them.

    Parameters
    ----------
    file_path : Path
        Path to the .mdb/.accdb file.

    Yields
    ------
    pyodbc.Cursor
        A cursor object from an open pyodbc connection.

    Raises
    ------
    pyodbc.Error
        If opening the connection fails.
    """
    connection_string = _make_connection_string(file_path)
    try:
        connection = pyodbc.connect(connection_string)
    except pyodbc.Error as exc:
        logging.error(f"Failed to connect to Access DB '{file_path}': {exc}")
        raise

    try:
        yield connection.cursor()
    finally:
        try:
            connection.close()
        except Exception as exc:
            logging.warning(f"Error closing connection to '{file_path}': {exc}")


def _make_connection_string(path: Path):
    """Return an ODBC connection string for an Access database file.

    Chooses a driver based on the host platform:
    - Windows: "Microsoft Access Driver (*.mdb, *.accdb)"
    - Non-Windows: "MDBTools" (common unixODBC/MDBTools setup)

    Parameters
    ----------
    path : Path
        Path to the .mdb/.accdb file. The file does not have to exist, 
        but a warning is logged if it is missing.

    Returns
    -------
    str
        A connection string suitable for pyodbc.connect(), e.g.
        "DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:\\path\\to\\file.mdb;"

    Notes
    -----
    - The caller must ensure an appropriate ODBC driver is installed on the system.
    - This function does not open any connections; it only builds the string.
    """
    if not path.exists():
        logging.warning(f"Access file not found: {path}")

    if platform.system() == "Windows":
        driver = "Microsoft Access Driver (*.mdb, *.accdb)"
    else:
        driver = "MDBTools"

    conn_str = f"DRIVER={{{driver}}};" f"DBQ={path};"
    return conn_str


def _get_access_table_list(cursor: pyodbc.Cursor) -> list[str]:
    """Return non-system table names from an Access database cursor.

    Calls cursor.tables() and returns a deduplicated list of table names.
    Includes objects of type TABLE (when table type is available)
    and excludes Access system tables (names starting with "MSys").

    The implementation is defensive about the shape of rows returned by
    pyodbc (attribute names or positional fields) and logs errors instead
    of raising so callers can continue processing other files.
    """
    table_names: list[str] = []
    try:
        for row in cursor.tables():
            # Robustly extract the table name (attribute, uppercase key, or positional)
            name = getattr(row, "table_name", None) or getattr(row, "TABLE_NAME", None)
            if not name:
                try:
                    name = row[2]
                except Exception:
                    continue
            name = str(name)

            # If the row provides a type, prefer only TABLE
            ttype = getattr(row, "table_type", None) or getattr(row, "TABLE_TYPE", None)
            if ttype:
                if str(ttype).upper() != "TABLE":
                    continue

            # Skip Access system tables
            if name.startswith("MSys"):
                continue

            table_names.append(name)
    except Exception as exc:
        logging.error(f"Error listing tables from Access DB: {exc}")

    return table_names


def _extract_table(
    cursor: pyodbc.Cursor,
    year: int,
    table_name: str,
    *,
    lib_defaults: Defaults,
    replace: bool,
    name_prefix: Optional[str] = None
):
    """Read a table from an Access cursor and write it atomically to CSV.

    Behaviour
    - Ensures the destination directory exists.
    - Skips writing if the target CSV already exists and `replace` is False.

    Parameters
    ----------
    cursor
        Open pyodbc cursor for the Access database.
    year
        The year (used to select the destination subdirectory).
    table_name
        Name of the table to extract.
    lib_defaults
        Defaults object providing the extracted dir path and UI settings.
    replace
        If True, overwrite existing CSVs; if False, skip existing files.
    name_prefix
        Optional prefix to prepend to the CSV filename (used when multiple
        Access files for a year would otherwise produce name collisions).

    Returns
    -------
    None
    """
    dest_dir = lib_defaults.dir.extracted.joinpath(str(year))
    dest_dir.mkdir(parents=True, exist_ok=True)

    file_name = table_name if name_prefix is None else f"{name_prefix}_{table_name}"
    file_path = dest_dir.joinpath(f"{file_name}.csv")

    if (file_path.exists()) and (not replace):
        logging.info(f"Skipping existing extracted table: {file_path}")
        return

    # Read table (propagates pyodbc.Error or other exceptions to be handled here)
    try:
        table = _get_access_table(cursor, table_name)
    except pyodbc.Error as exc:
        logging.error(f"Failed to read table '{table_name}' for year {year}: {exc}")
        return
    except Exception as exc:
        logging.error(f"Unexpected error reading table '{table_name}' for year {year}: {exc}", exc_info=True)
        return

    table.to_csv(file_path, index=False)


def _get_access_table(cursor: pyodbc.Cursor, table_name: str) -> pd.DataFrame:
    """Fetch an Access table into a pandas DataFrame.

    Executes "SELECT * FROM [table_name]" and returns a DataFrame.

    Parameters
    ----------
    cursor : pyodbc.Cursor
        Open cursor against an Access database.
    table_name : str
        Table name to read (will be quoted with square brackets).

    Returns
    -------
    pd.DataFrame
        DataFrame containing the table rows (may be empty).

    Raises
    ------
    pyodbc.Error
        Propagated from the underlying ODBC call if the query fails.
    """
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


def _find_files_with_extensions(
    directory: Path, extensions: Iterable[str]
) -> list[Path]:
    """Finds all files in a directory that match a given set of extensions.

    This function searches the specified directory (non-recursively) and
    returns a list of `Path` objects for files whose extensions are in the
    provided set. The comparison is case-insensitive.

    Parameters
    ----------
    directory
        The `Path` object representing the directory to search.
    extensions
        An iterable of string extensions to look for (e.g., {".zip", ".rar"}).
        The leading dot is required.

    Returns
    -------
    list[Path]
        A list of `Path` objects for the matching files, or an empty list if
        the directory does not exist.
    """
    if not directory.is_dir():
        logging.warning(f"Directory not found: {directory}")
        return []

    # Ensure extensions are lowercase for case-insensitive comparison
    extensions_set = {ext.lower() for ext in extensions}
    return [
        file
        for file in directory.iterdir()
        if file.is_file() and file.suffix.lower() in extensions_set
    ]
