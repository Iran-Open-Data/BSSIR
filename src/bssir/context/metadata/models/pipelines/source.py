from typing import Annotated, Literal

from pydantic import BaseModel, Field


class Source(BaseModel):
    name: str
    form: Literal["cleaned", "normalized"]
    columns: list[str] | None = None
