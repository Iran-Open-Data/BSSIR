from pathlib import Path

from pydantic import BaseModel


class DirectoriesNames(BaseModel):
    original: str
    unpacked: str
    extracted: str
    cleaned: str
    external: str
    maps: str
    cached: str


class Directories(BaseModel):
    original: Path
    unpacked: Path
    extracted: Path
    cleaned: Path
    external: Path
    maps: Path
    cached: Path


class RemoteDirectories(BaseModel):
    original: str
    unpacked: str
    extracted: str
    cleaned: str
    external: str
    maps: str
