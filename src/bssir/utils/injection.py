from os import PathLike
from types import ModuleType
import importlib.util
import sys


def import_module_from_path(module_name: str, file_path: PathLike) -> ModuleType:
    """Performs a dynamic import of a Python file from a direct file system path.

    If the module name is already cached in sys.modules, the existing module is returned.

    Args:
        module_name: The name to assign to the imported module within sys.modules.
        file_path: The file path to the Python script to import.

    Returns:
        ModuleType: The dynamically loaded module object.

    Raises:
        ImportError: If the module spec or loader cannot be created from the given path.
    """
    if module_name in sys.modules:
        return sys.modules[module_name]

    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if not (spec and spec.loader):
        raise ImportError(f"Could not load spec for {file_path}")
        
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module
