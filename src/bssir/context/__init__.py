from os import PathLike

from pydantic import BaseModel, ConfigDict

from .config import Config, load_config
from .metadata import MetadataCollection, load_metadata
from .initialize import initialize_package
from .tools import Tools


class Context(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    config: Config
    tools: Tools
    metadata: MetadataCollection


def load_context(
    package_dir: PathLike | None = None,
    *,
    base_package_dir: PathLike | None = None,
    root_dir: PathLike | None = None,
) -> Context:
    """
    Load the complete BSSIR context.

    This function loads the package configuration, prepares the required local
    filesystem, and initializes the metadata collection. It is the recommended
    entry point for creating a fully initialized BSSIR context.

    Parameters
    ----------
    base_package_dir : path-like, optional
        Path to the base BSSIR package. Intended primarily for testing and
        development.
    package_dir : path-like, optional
        Path to the active data package. If omitted, the default package is
        used.
    root_dir : path-like, optional
        Root directory for local configuration, metadata overrides, and data
        storage. If omitted, the current working directory is used.

    Returns
    -------
    Context
        A fully initialized context containing the package configuration and
        metadata collection.
    """
    config = load_config(
        base_package_dir=base_package_dir,
        package_dir=package_dir,
        root_dir=root_dir,
    )
    initialize_package(config)
    metadata = load_metadata(config)
    tools = Tools(config=config)
    return Context(config=config, tools=tools, metadata=metadata)
