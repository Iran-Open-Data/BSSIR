from __future__ import annotations

from pathlib import Path
from typing import Any

from bssir.utils import import_module_from_path


_REPRS_DIR = Path(__file__).parent.joinpath("reprs")


def html_repr(name: str, obj: Any) -> str:
    """Return the HTML representation of an object.

    Parameters
    ----------
    name
        Renderer module name (without ``.py``), e.g.
        ``"metadata_collection"`` or ``"metadata"``.
    obj
        Object to render.
    """
    module = import_module_from_path(
        module_name=f"bssir.reprs.{name}",
        file_path=_REPRS_DIR / f"{name}.py",
    )

    try:
        renderer = module.html_repr
    except AttributeError as exc:
        raise ImportError(
            f"Renderer '{name}' does not define a 'html_repr' function."
        ) from exc

    return renderer(obj)
