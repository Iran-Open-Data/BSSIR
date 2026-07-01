from os import PathLike

from .loader import ConfigLoader
from .models import Config


def load_config(
    package_dir: PathLike | None = None,
    *,
    base_package_dir: PathLike | None = None,
    root_dir: PathLike | None = None,
) -> Config:
    """Load and validate the package configuration."""
    loader = ConfigLoader(
        base_package_dir=base_package_dir,
        package_dir=package_dir,
        root_dir=root_dir,
    )
    return Config.model_validate(loader.build_config())


__all__ = ["Config", "load_config"]
