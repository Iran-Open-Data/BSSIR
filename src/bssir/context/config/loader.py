from copy import deepcopy
from functools import cached_property
from pathlib import Path
from os import PathLike
import tomllib
from typing import Any

from bssir.utils.yaml import load_yaml
from bssir.utils.transformers import update_dict


class ConfigLoader:
    """Build the runtime configuration for a BSSIR package.

    Configuration is assembled from multiple sources, in order of increasing
    precedence:

    1. BSSIR's default configuration.
    2. The package-specific configuration.
    3. Local configuration files found between the filesystem root and
       ``root_dir``, where deeper directories override parent directories.

    The resulting configuration is augmented with runtime paths such as the
    package directory and project root.
    """

    def __init__(
            self,
            base_package_dir: PathLike | None = None,
            package_dir: PathLike | None = None,
            root_dir: PathLike | None = None,
        ) -> None:
        if base_package_dir is None:
            self.base_package_dir = Path(__file__).parents[2]
        else:
            self.base_package_dir = Path(base_package_dir)

        if package_dir is None:
            self.package_dir = self.base_package_dir
        else:
            self.package_dir = Path(package_dir)

        if root_dir is None:
            self.root_dir = Path.cwd()
        else:
            self.root_dir = Path(root_dir)

    def build_config(self) -> dict[str, Any]:
        """Build the complete runtime configuration.

        Returns
        -------
        dict[str, Any]
            The merged configuration composed of the default, package, and local
            configuration sources, with runtime path information added.
        """
        settings = deepcopy(self.base_config)
        settings = update_dict(settings, self.package_config)
        settings = update_dict(settings, self.local_config)

        settings.update(
            {
                "base_package_dir": self.base_package_dir,
                "package_dir": self.package_dir,
                "root_dir": self.root_dir,
            }
        )
        settings["local_dir"] = self._resolve_local_dir(settings)
        settings["dirs"] = self._resolve_dirs(settings)
        settings = self._resolve_metadata_paths(settings)
        settings = self._resolve_mirrors(settings)

        return settings

    @cached_property
    def base_config(self) -> dict[str, Any]:
        """BSSIR's default configuration."""
        path = self.base_package_dir / "config" / "settings.yaml"
        return load_yaml(path)

    @cached_property
    def package_config(self) -> dict[str, Any]:
        """Package-specific configuration."""
        path = self.package_dir / self.base_config["package_settings"]
        return load_yaml(path)

    @cached_property
    def local_config(self) -> dict[str, Any]:
        """Load and merge local configuration files from the filesystem hierarchy.

        Configuration files are searched from the filesystem root down to
        `root_dir`, with deeper directories overriding parent settings.
        """
        directories = (*reversed(self.root_dir.parents), self.root_dir)
        local_config = {}

        local_settings = self.package_config.get(
            "local_settings", self.base_config["local_settings"]
        )

        for directory in directories:
            path = directory / local_settings
            if path.exists():
                local_config = update_dict(local_config, load_yaml(path))

        return local_config

    def _resolve_local_dir(self, settings: dict) -> Path:
        """Resolve the local directory path against the configured root."""
        local_dir = Path(settings["local_dir"])
        if local_dir.is_absolute():
            return local_dir
        elif settings["local_dir_in_root"]:
            return self.root_dir / local_dir
        else:
            return self.package_dir / local_dir

    def _resolve_dirs(self, settings: dict) -> dict[str, Path]:
        """Resolve configured directory names into absolute paths.

        Relative directory names are resolved against the configured local
        directory. Absolute paths are preserved unchanged.

        Args:
            settings: Configuration dictionary containing directory names and
                the local directory.

        Returns:
            A mapping of directory names to resolved ``Path`` objects.
        """
        dirs = {}
        for name, value in settings["directory_names"].items():
            path = Path(value)
            dirs[name] = path if path.is_absolute() else settings["local_dir"] / path

        return dirs

    def _resolve_metadata_paths(self, settings: dict[str, Any]) -> dict[str, Any]:
        """Resolve configured metadata file paths into absolute paths.

        Metadata paths defined as relative paths are resolved against their
        corresponding package directory. Absolute paths are preserved unchanged.

        The following metadata groups are resolved:

        - ``base_package_metadata``: resolved against the base package directory.
        - ``package_metadata``: resolved against the installed package directory.
        - ``local_metadata``: resolved against the local project directory.

        Args:
            settings: Configuration dictionary containing metadata path mappings.

        Returns:
            A copy of the configuration with metadata paths replaced by resolved
            ``Path`` objects.
        """
        path_mappings = {
            "base_package_metadata": self.base_package_dir,
            "package_metadata": self.package_dir,
            "local_metadata": self.root_dir,
        }

        resolved = deepcopy(settings)

        for key, base_dir in path_mappings.items():
            metadata = settings.get(key, {})

            resolved[key] = {
                name: (
                    path
                    if (path := Path(value)).is_absolute()
                    else base_dir / path
                )
                for name, value in metadata.items()
            }

        return resolved

    def _resolve_credentials_path(self, settings: dict) -> dict:
        resolved = deepcopy(settings)

        path = Path(settings.get("credential_file", "tokens.toml"))

        if not path.is_absolute():
            path = self.root_dir / path

        resolved["credentials_file"] = path

        return resolved

    def _resolve_mirrors(self, settings: dict) -> dict:
        resolved = deepcopy(settings)
        for mirror in resolved["mirrors"]:
            mirror["directory_names"] = settings["directory_names"]
        return resolved
