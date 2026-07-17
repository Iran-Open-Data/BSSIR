import logging
from contextlib import contextmanager
from typing import Generator
import shutil
import platform
from pathlib import Path

from tqdm.auto import tqdm
import pandas as pd
import pyodbc

from bssir.utils.files import get_file_type


def extract(source: Path, target: Path) -> None:
    file_type = get_file_type(source)
    if not file_type:
        return None
    
    _EXTRACTORS[file_type](source, target)


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
    from dbfread import DBF

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


_EXTRACTORS = {
    "access": _extract_access,
    "dbf": _extract_dbf,
    "stata": _extract_stata,
    "csv": _move_file,
}
