from pathlib import Path
import tomllib

from pydantic import RootModel, BaseModel


class Credential(BaseModel):
    """Credentials required to access an external resource."""

    access_key: str
    secret_key: str
    session_token: str | None = None


class CredentialStore(RootModel[dict[str, Credential]]):
    """Collection of named credentials."""

    def get(self, name: str) -> Credential:
        return self.root[name]

    def __getitem__(self, key: str) -> Credential:
        return self.root[key]


def get_credential_store(path: str | Path) -> CredentialStore:
    """Load and cache credentials from a TOML file."""
    with Path(path).open("rb") as file:
        data = tomllib.load(file)

    return CredentialStore.model_validate(data)
