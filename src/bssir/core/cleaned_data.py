"""Utilities for managing cleaned survey data."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from bssir.context import Context
from bssir.context.config.models import Mirror


def download_years(
    *,
    context: Context,
    years: list[int],
    source: str = "mirror",
) -> None:
    """Download cleaned survey tables for the requested years.

    Parameters
    ----------
    context : Context
        BSSIR execution context.
    years : list[int]
        Years to download.
    source : str, default="mirror"
        Name of the configured download source.
    """
    mirror = context.config.get_mirror(source)
    cleaned_directory = context.config.directory_names.cleaned
    destination_directory = context.config.dirs.cleaned

    table_years = context.tools.create_table_year_pairs("all", years)

    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = [
            executor.submit(
                download_table,
                mirror=mirror,
                cleaned_directory=cleaned_directory,
                destination_directory=destination_directory,
                table_name=table_name,
                year=year,
            )
            for table_name, year in table_years
        ]

        for future in futures:
            future.result()


def download_table(
    *,
    mirror: Mirror,
    cleaned_directory: str,
    destination_directory: Path,
    table_name: str,
    year: int,
) -> None:
    """Download a single cleaned table."""
    filename = f"{year}_{table_name}.parquet"

    mirror.download(
        source=f"{cleaned_directory}/{filename}",
        destination=destination_directory / filename,
    )
