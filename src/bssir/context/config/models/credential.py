from typing import Annotated, Literal

from pydantic import BaseModel, Field, TypeAdapter


class BaseCredential(BaseModel):
    """Credentials required to access an external resource."""
    type: str


class S3Credential(BaseCredential):
    type: Literal["s3"] = "s3"

    access_key: str
    secret_key: str
    session_token: str | None = None


Credential = Annotated[
    S3Credential,
    Field(discriminator="type"),
]

credential_adapter = TypeAdapter(Credential)
