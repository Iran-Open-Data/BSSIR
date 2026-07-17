from pathlib import Path

from ._base import BaseS3Storage
from bssir.utils import download


class PublicStorage(BaseS3Storage):
    """Public S3-compatible storage."""

    def download(
        self,
        source: str,
        target: Path,
    ) -> Path:
        """Download a resource from the public storage."""

        target.parent.mkdir(parents=True, exist_ok=True)

        url = (
            f"{self.mirror.address.rstrip('/')}/"
            f"{source.lstrip('/')}"
        )

        download(url, target)

        return target
