from abc import ABC, abstractmethod
from functools import cached_property
from pathlib import Path

from ..common import MetadataNode
from .file import FileMetadata
from ....states import ExtractState, ResourceState, UnpackState


class BaseResource(ABC, MetadataNode):

    @property
    @abstractmethod
    def files(self) -> list[FileMetadata]: ...

    @property
    @abstractmethod
    def original_path(self) -> Path: ...

    @property
    @abstractmethod
    def unpacked_path(self) -> Path: ...

    @property
    @abstractmethod
    def extracted_path(self) -> Path: ...

    @property
    def state(self) -> ResourceState:
        return ResourceState(
            files={
                file.filename: file.saved_state
                for file in self.files
                if file.saved_state is not None
            }
        )

    def has_original_files(self) -> bool:
        return all(file.is_ready() for file in self.files)

    @property
    def unpack_state_path(self) -> Path:
        return self.unpacked_path / ".bssir" / "unpack.json"

    @property
    def saved_unpack_state(self) -> UnpackState | None:
        if not self.unpack_state_path.exists():
            return None

        try:
            return UnpackState.model_validate_json(
                self.unpack_state_path.read_text(encoding="utf-8")
            )
        except Exception:
            return None

    @property
    def current_unpack_state(self) -> UnpackState:
        return UnpackState.from_paths(
            source_dir=self.original_path,
            output_dir=self.unpacked_path,
        )

    def save_unpack_state(self, state: UnpackState) -> None:
        self.unpack_state_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.unpack_state_path.write_text(
            state.model_dump_json(indent=4),
            encoding="utf-8",
        )

    def update_unpack_state(self) -> None:

        self.save_unpack_state(self.current_unpack_state)

    def is_unpacked(self) -> bool:
        saved = self.saved_unpack_state
        if saved is None:
            return False

        current = self.current_unpack_state

        return (
            saved.source_files == current.source_files
            and saved.output_files == current.output_files
        )

    @property
    def extract_state_path(self) -> Path:
        return self.extracted_path / ".bssir" / "extract.json"

    @property
    def saved_extract_state(self) -> ExtractState | None:
        if not self.extract_state_path.is_file():
            return None

        try:
            return ExtractState.model_validate_json(
                self.extract_state_path.read_bytes()
            )
        except Exception:
            return None

    def save_extract_state(self, state: ExtractState) -> None:
        self.extract_state_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.extract_state_path.write_text(
            state.model_dump_json(indent=4),
            encoding="utf-8",
        )

    def update_extract_state(self) -> None:
        state = ExtractState.from_paths(
            source_dir=self.unpacked_path,
            output_dir=self.extracted_path,
        )

        self.save_extract_state(state)

    def is_extracted(self) -> bool:
        saved = self.saved_extract_state
        if saved is None:
            return False

        current = ExtractState.from_paths(
            self.unpacked_path,
            self.extracted_path,
        )
        return (
            saved.source_files == current.source_files
            and saved.output_files == current.output_files
        )

    def is_synced(self, source_name: str | None = None) -> bool:
        return all(file.is_synced(source_name) for file in self.files)

    def download(self, source_name: str | None = None, replace: bool = True) -> None:
        for file in self.files:
            if replace or not file.is_ready():
                file.download(source_name)

    def upload(self, source_name: str | None = None, replace: bool = True) -> None:
        for file in self.files:
            if replace or not file.is_synced():
                file.upload(source_name)


class YearResource(BaseResource):
    """Resources associated with a specific data release year."""
    year: int

    @property
    def original_path(self) -> Path:
        return self.config.dirs.original / str(self.year)

    @property
    def unpacked_path(self) -> Path:
        return self.config.dirs.unpacked / str(self.year)

    @property
    def extracted_path(self) -> Path:
        return self.config.dirs.extracted / str(self.year)

    @cached_property
    def files(self) -> list[FileMetadata]:
        return [
            FileMetadata(collection=self.year, **file, config=self.config)
            for file in self.content["files"]
        ]


class CommonResource(BaseResource):
    """Resources shared across multiple years."""
    title: str

    @property
    def original_path(self) -> Path:
        return self.config.dirs.original / self.title

    @property
    def unpacked_path(self) -> Path:
        return self.config.dirs.unpacked / self.title

    @property
    def extracted_path(self) -> Path:
        return self.config.dirs.extracted / self.title

    @cached_property
    def files(self) -> list[FileMetadata]:
        return [
            FileMetadata(collection=self.title, **file, config=self.config)
            for file in self.content["files"]
        ]
