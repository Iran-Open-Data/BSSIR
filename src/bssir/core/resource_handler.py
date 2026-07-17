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
from typing import Literal
import shutil
from pathlib import Path

from tqdm.auto import tqdm

from bssir import utils
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
        if utils.files.has_file_type(item, "archive"):
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
    """Recursively unpack nested directories and archives."""
    while True:
        flattened = _flatten_nested_dirs(target_dir)
        extracted = _extract_nested_archives(target_dir, context)
        if not flattened and not extracted:
            break


def _flatten_nested_dirs(target_dir: Path) -> bool:
    """Move files from nested directories into ``target_dir``.

    Returns
    -------
    bool
        True if any nested directories were processed.
    """
    sub_dirs = get_dirs(target_dir)
    logging.info("Found %d nested directories to unpack.", len(sub_dirs))

    for sub_dir in sub_dirs:
        for item in sub_dir.iterdir():
            try:
                shutil.move(item, target_dir)
            except shutil.Error as exc:
                logging.warning(
                    "Could not move '%s': %s. It may be a duplicate.",
                    item.name,
                    exc,
                )
        shutil.rmtree(sub_dir)

    return bool(sub_dirs)


def _extract_nested_archives(target_dir: Path, context: Context) -> bool:
    """Extract archives found directly in ``target_dir``.

    Returns
    -------
    bool
        True if any archives were extracted.
    """
    archives = utils.files.find(target_dir, "archive")
    logging.info("Found %d nested archives to unpack.", len(archives))

    for archive in archives:
        context.tools.extract(archive, target_dir)
        archive.unlink()

    return bool(archives)


def get_dirs(path: Path) -> list[Path]:
    return [
        d for d in path.iterdir()
        if d.is_dir() and not d.name.startswith(".")
    ]


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
        utils.extract.extract(file, target)

    resource.update_extract_state()
