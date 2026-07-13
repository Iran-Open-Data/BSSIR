from abc import ABC, abstractmethod
from functools import cached_property
from pathlib import Path

from bssir.context.config import Config
from bssir.context.config.models import Mirror
from bssir.context.credential import load_credentials, CredentialStore


class BaseStorage(ABC):
    """Storage backend for reading and writing resources."""

    def __init__(self, config: Config, mirror_name: str | None = None) -> None:
        self.config = config
        self.mirror_name = mirror_name

    @cached_property
    def credentials(self) -> CredentialStore:
        return load_credentials(self.config.credentials_file)

    @abstractmethod
    def download(
        self,
        source: str,
        target: Path,
    ) -> Path:
        """Download a resource."""

    @abstractmethod
    def upload(
        self,
        source: Path,
        target: str,
    ) -> None:
        """Upload a resource."""


class BaseS3Storage(BaseStorage):

    def __init__(self, config: Config, mirror_name: str | None = None) -> None:
        super().__init__(config)
        self.mirror_name = mirror_name

    @cached_property
    def mirror(self) -> Mirror:
        return self.config.get_mirror(self.mirror_name)

    @cached_property
    def credentials(self) -> CredentialStore:
        return load_credentials(self.config.credentials_file)
