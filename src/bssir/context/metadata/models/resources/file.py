from pathlib import Path
from functools import cached_property

from pydantic import Field

from bssir import utils
from bssir.context.config.models import Mirror
from ..common import MetadataNode
from ....states import FileState


class FileMetadata(MetadataNode):
    """Metadata describing a resource file."""

    name: str | None = Field(
        default=None,
        description="Resource identifier."
    )

    filename: str = Field(
        description="Original filename of the resource."
    )

    collection: str | int

    sources: dict[str, str] = Field(
        default_factory=dict,
        description="Named source locations (original, mirrors, etc.)."
    )

    @property
    def path(self) -> Path:
        return self.config.dirs.original / str(self.collection) / self.filename

    @property
    def state_directory(self) -> Path:
        return (
            self.config.dirs.original
            / str(self.collection)
            / ".bssir"
        )

    @property
    def state_path(self) -> Path:
        return self.state_directory / f"{self.filename}.json"

    @property
    def saved_state(self) -> FileState | None:
        if not self.state_path.is_file():
            return None

        try:
            return FileState.model_validate_json(
                self.state_path.read_bytes()
            )
        except Exception:
            return None

    @cached_property
    def current_state(self) -> FileState:
        return FileState.from_path(self.path)

    def save_state(self, state: FileState) -> None:
        self.state_directory.mkdir(parents=True, exist_ok=True)

        self.state_path.write_text(
            state.model_dump_json(indent=4),
            encoding="utf-8"
        )

    def update_state(self, source_name: str) -> None:
        self.save_state(FileState.from_path(filepath=self.path, source=source_name))

    def is_ready(self) -> bool:
        state = self.saved_state

        if state is None or not self.local_exists():
            return False

        stat = self.path.stat()

        return (
            stat.st_size == state.size
            and stat.st_mtime_ns == state.modified_ns
        )

    def is_verified(self) -> bool:
        if not self.is_ready():
            return False

        state = self.saved_state
        assert state is not None

        return self.current_state.checksum == state.checksum

    def get_file_key(self, mirror: Mirror) -> str:
        return f"{mirror.dirs.original}/{self.collection}/{self.filename}"

    def remote_exists(self, mirror_name: str | None = None) -> bool:
        """Check if the file exists on the remote mirror."""
        mirror = self.config.get_mirror(mirror_name)
        return mirror.exists(self.get_file_key(mirror))

    def local_exists(self) -> bool:
        """Check if the file exists locally."""
        return self.path.is_file()

    def is_synced(self, source_name: str | None = None) -> bool:
        """Check if the local file perfectly matches the remote file.
        
        Compares file sizes first (fast), then falls back to MD5 checksums (ETags).
        """
        if not self.local_exists():
            return False

        mirror = self.config.get_mirror(source_name)
        if not mirror.exists(self.get_file_key(mirror)):
            return False
        remote_size = mirror.get_content_length(self.get_file_key(mirror))

        local_size = self.path.stat().st_size
        if local_size != remote_size:
            return False

        return True

    def sync(self, mirror_name: str | None = None, strategy: str = "download_prefer") -> str:
        """Smart sync method depending on state.
        
        Strategies:
            'download_prefer': Downloads if missing/out-of-sync, uploads only if missing remotely.
            'upload_prefer': Uploads if local exists and is out-of-sync.
        """
        local = self.local_exists()
        remote = self.remote_exists(mirror_name)

        if local and remote:
            if self.is_synced(mirror_name):
                return "Already in sync."
            
            if strategy == "upload_prefer":
                self.upload(mirror_name)
                return "Updated remote copy (Upload)."
            else:
                self.download(mirror_name)
                return "Updated local copy (Download)."
        
        elif local and not remote:
            self.upload(mirror_name)
            return "Uploaded missing remote file."
        
        elif not local and remote:
            self.download(mirror_name)
            return "Downloaded missing local file."
        
        else:
            raise FileNotFoundError("File does not exist locally or remotely.")

    def download(self, source_name: str | None = None) -> Path:
        if not source_name:
            source_name = self.config.default_download_source

        if source_name in self.sources:
            path = utils.download.download(
                url=self.sources[source_name],
                path=self.path,
            )
        else:
            mirror = self.config.get_mirror(source_name)
            path = mirror.download(
                source=self.get_file_key(mirror),
                destination=self.path,
            )

        self.update_state(source_name)

        return path

    def upload(self, mirror_name: str | None = None) -> str:
        mirror = self.config.get_mirror(mirror_name)
        return mirror.upload(
            source=self.path,
            destination=self.get_file_key(mirror),
        )
