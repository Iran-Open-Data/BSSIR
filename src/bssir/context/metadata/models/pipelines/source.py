from typing import Literal

from pydantic import BaseModel


class Source(BaseModel):
    name: str
    form: Literal["cleaned", "normalized"] = "cleaned"
    columns: list[str] | None = None
