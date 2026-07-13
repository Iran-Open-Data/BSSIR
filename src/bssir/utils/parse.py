from typing import Iterable

from bssir.types import Years


def parse_years(
    years: Years,
    available_years: list[int] | None = None,
) -> list[int]:
    """Convert different year representations to a list of integer years.

    This function handles converting various input types representing
    years into a standardized list of integer years.

    The input `years` can be specified as:

    - int: A single year
    - Iterable[int]: A collection of years like [94, 95, 96] or range(1390, 1400)
    - str: A comma-separated string of years or ranges like '86-90, 1396-1400'
    - "all": All available years
    - "last": Just the last year

    Years are validated before returning.

    Parameters
    ----------
    years : Years
        The input years to parse

    Returns
    -------
    list[int]
        The converted years as a list of integer values

    Examples
    --------
    >>> parse_years(1399)
    [1399]

    >>> parse_years([98, 99, 1400])
    [1398, 1399, 1400]

    >>> parse_years(range(1380, 1390))
    [1380, 1381, 1382, 1383, 1384, 1385, 1386, 1387, 1388, 1389]

    >>> parse_years('1365, 80-83, 99')
    [1365, 1380, 1381, 1382, 1383, 1399]
    """
    if isinstance(years, str):
        if years == "all":
            if available_years is None:
                raise ValueError
            years = available_years
        elif years == "last":
            if available_years is None:
                raise ValueError
            years = max(available_years)

    year_list = _parse_years_param(years)

    if available_years is not None:
        year_list = [year for year in year_list if year in available_years]

    return year_list


def _parse_years_param(years: int | Iterable[int] | str) -> list[int]:
    if isinstance(years, int):
        year_list = [_check_year_validity(years)]
    elif isinstance(years, str):
        year_list = _parse_year_str(years)
    elif isinstance(years, Iterable):
        year_list = [_check_year_validity(year) for year in years]
    else:
        raise TypeError
    year_list.sort()
    return year_list


def _check_year_validity(year: str | int) -> int:
    if isinstance(year, str):
        output_year = int(year.strip())
    else:
        output_year = year

    if output_year <= 60:
        output_year += 1400
    elif output_year < 100:
        output_year += 1300

    return output_year


def _parse_year_str(year: str) -> list[int]:
    year_list = []
    year_parts = year.split(",")
    for part in year_parts:
        if part.find("-") >= 0:
            year_interval = part.split("-")
            if len(year_interval) != 2:
                raise ValueError(f"Interval Not Valid {part}")
            start_year, end_year = year_interval
            start_year = _check_year_validity(start_year)
            end_year = _check_year_validity(end_year)
            year_list.extend(list(range(start_year, end_year + 1)))
        else:
            year_list.append(_check_year_validity(part))
    return year_list
