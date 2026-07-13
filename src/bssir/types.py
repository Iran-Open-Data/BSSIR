from collections.abc import Iterable
from typing import Literal, TypeAlias


Years: TypeAlias = int | Iterable[int] | str | Literal["all", "last"]
