from __future__ import annotations

from datetime import datetime
from hashlib import file_digest
from pathlib import Path

from pydantic import BaseModel


class FileState(BaseModel):
    filename: str

    source: str | None = None

    size: int
    modified_ns: int
    checksum: str

    recorded_at: datetime

    @classmethod
    def from_file(
        cls,
        filepath: Path,
        *,
        source: str | None = None,
        algorithm: str = "md5",
    ) -> "FileState":
        stat = filepath.stat()

        with filepath.open("rb") as file:
            checksum = file_digest(file, algorithm).hexdigest()

        return cls(
            filename=filepath.name,
            source=source,
            size=stat.st_size,
            modified_ns=stat.st_mtime_ns,
            checksum=checksum,
            recorded_at=datetime.now(),
        )


class ResourceState(BaseModel):
    files: dict[str, FileState] = {}

    def get(self, filename: str) -> FileState | None:
        return self.files.get(filename)

    def update(self, state: FileState) -> None:
        self.files[state.filename] = state


class FileSnapshot(BaseModel):
    size: int
    modified_ns: int

    @classmethod
    def from_file(cls, path: Path) -> "FileSnapshot":
        stat = path.stat()
        return cls(
            size=stat.st_size,
            modified_ns=stat.st_mtime_ns,
        )


class UnpackState(BaseModel):
    source_files: dict[str, FileSnapshot]
    output_files: dict[str, FileSnapshot]

    created_at: datetime

    @classmethod
    def from_dirs(
        cls,
        source_dir: Path,
        output_dir: Path,
    ) -> "UnpackState":
        return cls(
            source_files={
                file.name: FileSnapshot.from_file(file)
                for file in sorted(source_dir.iterdir())
                if file.is_file()
                and ".bssir" not in file.parts
            },
            output_files={
                file.name: FileSnapshot.from_file(file)
                for file in sorted(output_dir.iterdir())
                if file.is_file()
                and ".bssir" not in file.parts
            },
            created_at=datetime.now(),
        )


class ExtractState(BaseModel):
    source_files: dict[str, FileSnapshot]
    output_files: dict[str, FileSnapshot]

    created_at: datetime

    @classmethod
    def from_dirs(
        cls,
        source_dir: Path,
        output_dir: Path,
    ) -> "ExtractState":
        return cls(
            source_files={
                str(file.relative_to(source_dir)): FileSnapshot.from_file(file)
                for file in sorted(source_dir.iterdir())
                if file.is_file()
                and ".bssir" not in file.parts
            },
            output_files={
                str(file.relative_to(output_dir)): FileSnapshot.from_file(file)
                for file in sorted(output_dir.iterdir())
                if file.is_file()
                and ".bssir" not in file.parts
            },
            created_at=datetime.now(),
        )
