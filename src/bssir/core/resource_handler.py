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
from typing import Generator, Literal
import shutil
import platform
from pathlib import Path

from tqdm.auto import tqdm
from dbfread import DBF
import pandas as pd
import pyodbc

from .. import utils
from bssir.utils.files import has_file_type, find_files, get_file_type
from bssir.context import Context
from bssir.context.metadata.models.resources.resource import BaseResource


def setup(
    years: list[int],
    *,
    context: Context,
    replace: bool,
    download_source: Literal["original", "mirror"] | str | None = None,
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
        files to download for each year, table schemas, and processing pipelines.
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
        context=context,
    )
    unpack(years, replace=replace, context=context)
    extract(years, replace=replace, context=context)


def download(
    years: list[int],
    *,
    context: Context,
    replace: bool = False,
    source: Literal["original", "mirror"] | str | None = None,
) -> None:
    """Ensure original resource files are available locally.

    For each requested year, downloads any missing original resource files
    defined in the metadata. If ``replace`` is ``True``, existing local files
    are replaced regardless of their recorded state. Otherwise, only files
    that are missing or not considered ready are downloaded.

    Downloaded files are recorded in the corresponding file state so future
    operations can determine whether the local resources are unchanged.

    Parameters
    ----------
    years : list[int]
        Survey years whose original resource files should be available.
    context : Context
        Library context providing configuration and resource metadata.
    replace : bool, default=True
        Whether to force re-downloading existing files.
    source : str | None, default=None
        Name of the download source. If ``None``, the configured default
        download source is used.
    """
    for year in tqdm(
        years,
        desc="Downloading annual data",
        unit="Year",
        disable=True,
    ):
        resource = context.metadata.resources[year]
        resource.download(source_name=source, replace=replace)


def unpack(years: list[int], *, context: Context, replace: bool = False) -> None:
    """Ensure unpacked resource files are available for the given years.

    For each requested year, extracts the original resource files into the
    configured unpacked directory. The unpack operation is skipped when the
    existing unpacked files match the recorded unpack state, unless
    ``replace`` is ``True``.

    After a successful unpack, the unpack state is updated to record the
    relationship between the original input files and the generated unpacked
    files.

    Parameters
    ----------
    years : list[int]
        Survey years whose original resources should be unpacked.
    context : Context
        Library context providing configuration and resource metadata.
    replace : bool, default=True
        Whether to discard any existing unpacked files and perform the unpack
        operation again.
    """
    for year in tqdm(
        years,
        desc="Unpacking annual archives",
        unit="Year",
        disable=True,
    ):
        resource = context.metadata.resources[year]
        _unpack_resource(resource, context=context, replace=replace)


def _unpack_resource(
    resource: BaseResource,
    *,
    context: Context,
    replace: bool = False,
) -> None:
    if not resource.has_original_files():
        raise FileNotFoundError(
            f"Original files are missing for resource: {resource}"
        )

    if resource.is_unpacked() and not replace:
        logging.info(
            f"Skipping {resource}: already unpacked."
        )
        return

    if replace and resource.unpacked_path.exists():
        shutil.rmtree(resource.unpacked_path)

    resource.unpacked_path.mkdir(
        parents=True,
        exist_ok=True,
    )

    for item in resource.original_path.iterdir():
        if has_file_type(item, "archive"):
            context.tools.extract(item, resource.unpacked_path)
        elif item.is_file():
            shutil.copy(
                item,
                resource.unpacked_path,
            )

    _unpack_nested_archives(
        resource.unpacked_path,
        context,
    )

    resource.update_unpack_state()


def _unpack_nested_archives(target_dir: Path, context: Context) -> None:
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
        sub_dirs = [d for d in target_dir.iterdir() if d.is_dir() if not d.name.startswith(".")]
        for sub_dir in sub_dirs:
            for item in sub_dir.iterdir():
                try:
                    shutil.move(item, target_dir)
                except shutil.Error as e:
                    logging.warning(f"Could not move '{item.name}': {e}. It may be a duplicate.")
            shutil.rmtree(sub_dir)
        sub_dirs = [d for d in target_dir.iterdir() if d.is_dir()]

        archive_files = find_files(target_dir, "archive")
        logging.info(f"Found {len(archive_files)} nested archives to unpack.")
        for archive in archive_files:
            context.tools.extract(archive, target_dir)
            archive.unlink()
        archive_files = find_files(target_dir, "archive")

        if (not archive_files) and (not sub_dirs):
            break


def extract(
    years: list[int],
    *,
    context: Context,
    replace: bool = False,
) -> None:
    for year in tqdm(
        years,
        desc="Extracting annual archives",
        unit="Year",
        disable=True,
    ):
        resource = context.metadata.resources[year]
        if replace or not resource.is_extracted():
            _extract_resource(resource)


def _extract_resource(resource: BaseResource) -> None:

    if not resource.is_unpacked():
        raise RuntimeError("Resource must be unpacked before extraction.")

    target = resource.extracted_path
    shutil.rmtree(target, ignore_errors=True)
    target.mkdir(exist_ok=True, parents=True)

    for file in resource.unpacked_path.iterdir():
        _extract_file(file, target)

    resource.update_extract_state()


def _extract_file(source: Path, target: Path) -> None:
    file_type = get_file_type(source)
    if not file_type:
        return None
    
    EXTRACTORS[file_type](source, target)



def _extract_access(
    source: Path,
    target: Path,
) -> None:
    """Extract all non-system tables from an Access file into CSVs.

    Returns
    -------
    None
    """
    try:
        with _create_cursor(source) as cursor:
            table_list = _list_tables(cursor)
            if not table_list:
                logging.warning(f"No user tables found in Access DB: {source}")
                return

            for table_name in tqdm(
                table_list,
                desc=f"Extracting tables from {source.name}",
                # bar_format=lib_defaults.bar_format,
                unit="Table",
                leave=False,
                disable=True,
            ):
                _extract_table(
                    source=source,
                    target=target,
                    cursor=cursor,
                    table_name=table_name,
                )
    except pyodbc.Error as exc:
        logging.error(f"Failed to open Access DB '{source}': {exc}")
    except Exception as exc:
        logging.exception(f"Unexpected error extracting from Access DB '{source}': {exc}")


@contextmanager
def _create_cursor(path: Path) -> Generator[pyodbc.Cursor, None, None]:
    """Yield a cursor connected to an Access database.

    Opens an ODBC connection to the database at ``path`` and yields a
    ``pyodbc.Cursor`` for executing SQL statements. The cursor and its
    underlying connection are closed automatically when the context exits,
    even if an exception occurs.

    Parameters
    ----------
    path
        Path to the Access database file.

    Yields
    ------
    pyodbc.Cursor
        Cursor for executing SQL statements.

    Raises
    ------
    pyodbc.Error
        If the database connection cannot be established.
    """
    connection = pyodbc.connect(_make_connection_string(path))
    cursor = connection.cursor()

    try:
        yield cursor
    finally:
        try:
            cursor.close()
        finally:
            connection.close()


def _make_connection_string(path: Path) -> str:
    """Build an ODBC connection string for an Access database.

    Selects the appropriate ODBC driver for the current platform and
    returns a connection string suitable for ``pyodbc.connect()``.

    Parameters
    ----------
    path
        Path to the Access database file.

    Returns
    -------
    str
        ODBC connection string for the specified database.

    Notes
    -----
    An appropriate ODBC driver must be installed on the host system.
    Windows uses the Microsoft Access ODBC driver, while other platforms
    assume an ``MDBTools``-compatible driver.
    """
    driver = (
        "Microsoft Access Driver (*.mdb, *.accdb)"
        if platform.system() == "Windows"
        else "MDBTools"
    )

    return f"DRIVER={{{driver}}};DBQ={path};"


def _list_tables(cursor: pyodbc.Cursor) -> list[str]:
    """Return the names of user tables in an Access database."""
    names: list[str] = []

    for row in cursor.tables():
        if getattr(row, "table_type", None) not in (None, "TABLE"):
            continue

        name = getattr(row, "table_name", row[2])

        if name.startswith("MSys"):
            continue

        names.append(name)

    return names


def _extract_table(
    cursor: pyodbc.Cursor,
    source: Path,
    target: Path,
    table_name: str,
) -> None:
    """Extract a database table to a CSV file."""
    try:
        table = _read_table(cursor, table_name)

        name_prefix = source.name.replace(".", "_")
        file_name = f"{name_prefix}_{table_name}.csv"

        table.to_csv(target / file_name, index=False)
    except pyodbc.Error as exc:
        logging.error(
            "Failed to read table '%s' from '%s': %s",
            table_name,
            source,
            exc,
        )
    except Exception:
        logging.exception(
            "Failed to extract table '%s' from '%s'.",
            table_name,
            source,
        )

def _read_table(cursor: pyodbc.Cursor, table_name: str) -> pd.DataFrame:
    """Read a database table into a DataFrame.

    Parameters
    ----------
    cursor
        Open database cursor.
    table_name
        Name of the table to read.

    Returns
    -------
    pd.DataFrame
        Contents of the table.
    """
    result = cursor.execute(f"SELECT * FROM [{table_name}]")
    rows = [tuple(row) for row in result.fetchall()]
    columns = [column[0] for column in result.description]

    table = pd.DataFrame.from_records(rows, columns=columns)
    return table


def _extract_dbf(source: Path, target:Path) -> None:
    file_name = f"{source.name.replace(".", "_")}.csv"
    try:
        table = pd.DataFrame(iter(DBF(source)))
    except UnicodeDecodeError:
        table = pd.DataFrame(iter(DBF(source, encoding="cp720")))
    table.to_csv(target / file_name, index=False)


def _extract_stata(source: Path, target:Path) -> None:
    file_name = f"{source.name.replace(".", "_")}.csv"
    table = pd.read_stata(source)
    table.to_csv(target / file_name, index=False)


def _move_file(source: Path, target:Path) -> None:
    shutil.copy(source, target)


EXTRACTORS = {
    "access": _extract_access,
    "dbf": _extract_dbf,
    "stata": _extract_stata,
    "csv": _move_file,
}
