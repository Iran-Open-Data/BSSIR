from collections.abc import Mapping
from typing import Any


def flatten_dict(
    dictionary: Mapping[str, Any],
) -> dict[tuple[str, ...], Any]:
    """Flatten a nested dictionary into tuple-keyed paths.

    Nested dictionary keys are converted into tuples representing the
    original path. This is useful for comparing, merging, or updating
    nested metadata structures.

    Examples
    --------
    >>> flatten_dict(
    ...     {
    ...         "a": 1,
    ...         "b": {
    ...             "c": 2,
    ...             "d": {
    ...                 "e": 3,
    ...             },
    ...         },
    ...     }
    ... )
    {
        ("a",): 1,
        ("b", "c"): 2,
        ("b", "d", "e"): 3,
    }

    Parameters
    ----------
    dictionary:
        Nested dictionary to flatten.

    Returns
    -------
    dict
        A dictionary where keys are tuples representing nested paths and
        values are the original leaf values.
    """
    flattened: dict[tuple[str, ...], Any] = {}

    for key, value in dictionary.items():
        if isinstance(value, Mapping):
            for sub_key, sub_value in flatten_dict(value).items():
                flattened[(key, *sub_key)] = sub_value
        else:
            flattened[(key,)] = value

    return flattened


def unflatten_dict(
    dictionary: Mapping[tuple[str, ...], Any],
) -> dict[str, Any]:
    """Convert a tuple-keyed dictionary back into a nested dictionary.

    This is the inverse operation of :func:`flatten_dict`.

    Examples
    --------
    >>> unflatten_dict(
    ...     {
    ...         ("a",): 1,
    ...         ("b", "c"): 2,
    ...     }
    ... )
    {
        "a": 1,
        "b": {
            "c": 2,
        },
    }

    Parameters
    ----------
    dictionary:
        Dictionary with tuple keys representing nested paths.

    Returns
    -------
    dict
        Nested dictionary reconstructed from flattened paths.
    """
    unflattened: dict[str, Any] = {}

    for key, value in dictionary.items():
        current = unflattened

        for part in key[:-1]:
            current = current.setdefault(part, {})

        current[key[-1]] = value

    return unflattened


def update_dict(
    base: Mapping[str, Any],
    updates: Mapping[str, Any],
) -> dict[str, Any]:
    """Update a nested dictionary while preserving existing values.

    Nested dictionaries are merged recursively. Values provided in
    ``updates`` replace values at the same path in ``base``.

    This function is useful for combining metadata configurations where
    a small override dictionary modifies only selected fields.

    Examples
    --------
    >>> update_dict(
    ...     {
    ...         "table": {
    ...             "description": "old",
    ...             "unit": "kg",
    ...         }
    ...     },
    ...     {
    ...         "table": {
    ...             "description": "new",
    ...         }
    ...     },
    ... )
    {
        "table": {
            "description": "new",
            "unit": "kg",
        }
    }

    Parameters
    ----------
    base:
        Original nested dictionary.

    updates:
        Values to apply on top of ``base``.

    Returns
    -------
    dict
        A new merged dictionary.
    """
    flattened = flatten_dict(base)
    flattened.update(flatten_dict(updates))

    return unflatten_dict(flattened)
