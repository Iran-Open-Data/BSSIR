from __future__ import annotations

from functools import cache
from importlib.resources import files


_PACKAGE = __package__


@cache
def load_template(name: str) -> str:
    """Load an HTML template from the package."""

    path = files(_PACKAGE).joinpath("templates", name)
    return path.read_text(encoding="utf-8")


@cache
def load_css() -> str:
    """Return the shared CSS theme."""

    path = files(_PACKAGE).joinpath("static", "theme.css")
    return path.read_text(encoding="utf-8")


@cache
def load_js() -> str:
    """Return the shared notebook JavaScript."""

    path = files(_PACKAGE).joinpath("static", "notebook.js")
    return path.read_text(encoding="utf-8")
