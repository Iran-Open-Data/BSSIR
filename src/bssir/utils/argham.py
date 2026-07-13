from typing import Iterable
import warnings

from pydantic import BaseModel, Field


class ArghamDefaults(BaseModel):
    keywords: list[str] = Field(default_factory=list)
    default_start: int | None = None
    default_end: int | None = None
    default_step: int = 1
    default_range: tuple[int, int] | None = None


class Argham:
    """Flexible argument handler for numbers and ranges.

    Accepts numbers, ranges, lists, and dicts to define a set
    of numbers and numeric ranges. Parses the input into
    number_list and range_list.

    Implements membership testing via 'in' operator based
    on the contained numbers and ranges.

    Can be used to flexibly defined sets of numbers/ranges
    and perform set-like membership tests on them.

    Parameters
    ----------
    argham : list, dict, int
        Input content. Can mix numbers, ranges, lists, and dicts.

    keywords : list of str, optional
        Keys in dict argham to parse as ranges.

    default_start : int, optional
        Default start for ranges when not specified.

    default_end : int, optional
        Default end for ranges when not specified.

    Attributes
    ----------
    number_list : list of int
        Parsed individual number values.

    range_list : list of range
        Parsed numeric ranges.

    Methods
    -------
    __contains__(value)
        Implements 'in' membership check.

    check_contained(values)
        Check membership for single or multiple values.

    Examples
    --------
    >>> arg = Argham([1, 2, 3, 4, {'start':6, 'end':10}")
    >>> 2 in arg
    True

    >>> arg.check_contained([4, 5, 6])
    [True, False, True]
    """

    def __init__(self, argham: list | dict | int | None = None, **kwargs):
        self.range_set = set()
        self.defaults = ArghamDefaults(**kwargs)
        self._min: int | None = None
        self._max: int | None = None
        self._parse_argham(argham)

    def check_contained(self, values: int | Iterable[int]) -> bool | list[bool]:
        """Check membership of values in container.

        For a single value, checks whether the value is a member
        of the container using `in` and returns a bool result.

        For multiple values given as an iterable, checks each value
        individually and returns a list of boolean results.

        Parameters
        ----------
        values: int or iterable of int
            Single value or iterable of values to check

        Returns
        -------
        bool or list of bool
            If single value, bool indicating membership.
            If multiple values, list of bool indicating membership
            for each value.

        Examples
        --------
        >>> container.check_membership(2)
        True

        >>> container.check_membership([1, 2, 3])
        [False, True, False]
        """
        if isinstance(values, int):
            return values in self
        result = []
        for value in values:
            result.append(value in self)
        return result

    def get_numbers(self) -> set[int]:
        numbers = set()
        for rng in self.range_set:
            numbers = numbers.union(set(rng))
        return numbers

    def __repr__(self) -> str:
        integers = []
        ranges = []

        for rng in self.range_set:
            if rng.stop - rng.start == 1:
                integers.append(str(rng.start))
            else:
                ranges.append((rng.start, rng.stop))
        representation_list = []
        if len(integers) > 0:
            representation_list.append(f"[{', '.join(integers)}]")
        for rng in ranges:
            representation_list.append(f"({rng[0]} - {rng[1]})")
        return ", ".join(representation_list)

    def __contains__(self, value: int):
        if self._min is None:
            return False
        if (value < self._min) or (value > self._max):  # type: ignore
            return False
        for number_range in self.range_set:
            if value in number_range:
                return True
        return False

    def __eq__(self, value: object) -> bool:
        if isinstance(value, int):
            if (len(self.range_set) == 1) and (value in next(iter(self.range_set))):
                return True
        if isinstance(value, range):
            if (len(self.range_set) == 1) and (next(iter(self.range_set)) == value):
                return True
        if isinstance(value, Argham):
            if self.range_set == value.range_set:
                return True
            if self.get_numbers() == value.get_numbers():
                return True
        return False

    def __add__(self, other: "Argham") -> "Argham":
        if self.defaults != other.defaults:
            warnings.warn(
                (
                    "Adding Argham objects with different defaults. "
                    f"Using {self.defaults!r} and {other.defaults!r}."
                ),
                UserWarning,
                stacklevel=2,
            )
        result = Argham()
        result.defaults = self.defaults
        result.range_set = self.range_set.union(other.range_set)

        if (self._min is None) and (other._min is None):
            result._min = None
        elif self._min is None:
            result._min = other._min
        elif other._min is None:
            result._min = self._min
        else:
            result._min = min(self._min, other._min)

        if (self._max is None) and (other._max is None):
            result._max = None
        elif self._max is None:
            result._max = other._max
        elif other._max is None:
            result._max = self._max
        else:
            result._max = max(self._max, other._max)
        return result

    def _parse_argham(self, argham) -> None:
        if isinstance(argham, list):
            for ragham in argham:
                self._parse_argham(ragham)
        elif isinstance(argham, dict):
            self._parse_dict(argham)
        elif isinstance(argham, int):
            drng = self.defaults.default_range
            if (drng is not None) and ((argham < drng[0]) or (argham > drng[1])):
                return
            self.range_set.add(range(argham, argham + 1))
            self._update_min(argham)
            self._update_max(argham)
        else:
            pass

    def _parse_dict(self, dictionary: dict) -> None:
        if len(self.defaults.keywords) > 0:
            for word in self.defaults.keywords:
                if word in dictionary:
                    self._parse_argham(dictionary[word])
                    return
        if ("start" in dictionary) or ("end" in dictionary):
            self._parse_start_end_dict(dictionary)
        else:
            for value in dictionary.values():
                self._parse_argham(value)

    def _parse_start_end_dict(self, dictionary: dict) -> None:
        start = self.defaults.default_start
        end = self.defaults.default_end
        step = self.defaults.default_step

        if "start" in dictionary:
            start = dictionary["start"]
        if "end" in dictionary:
            end = dictionary["end"]
        if "step" in dictionary:
            step = dictionary["step"]

        if start is None:
            raise ValueError("Start must be specified")
        if end is None:
            raise ValueError("End must be specified")

        self.range_set.add(range(start, end, step))
        self._update_min(start)
        self._update_max(end - 1)

    def _update_min(self, number) -> None:
        if self._min is None:
            self._min = number
        elif number < self._min:
            self._min = number

    def _update_max(self, number) -> None:
        if self._max is None:
            self._max = number
        elif self._max < number:
            self._max = number
