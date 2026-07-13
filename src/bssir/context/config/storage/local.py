from pathlib import Path

from ._base import BaseStorage

class LocalStorage(BaseStorage):
    def download(self, source: str, target: Path) -> Path:
        path = Path(source)
        target.write_bytes(path.read_bytes())
        return target

    def upload(self, source: Path, target: str) -> None:
        Path(target).write_bytes(source.read_bytes())
