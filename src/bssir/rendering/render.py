from pathlib import Path
import re
from functools import cache
from typing import Any


_TEMPLATE_DIR = Path(__file__).parent / "templates"
_STYLE_PATH = Path(__file__).parent / "static/style.css"

_PLACEHOLDER_PATTERN = re.compile(r"\{\{\s*(.*?)\s*\}\}")


@cache
def _stylesheet() -> str:
    """Return the global BSSIR stylesheet."""
    css = _STYLE_PATH.read_text(encoding="utf-8")
    return f"<style>\n{css}\n</style>"


def render_template(
    template: str,
    context: dict[str, Any],
) -> str:
    """Render an HTML template."""

    html = (_TEMPLATE_DIR / template).read_text(encoding="utf-8")

    def replace(match: re.Match[str]) -> str:
        return str(context.get(match.group(1), ""))

    html = _PLACEHOLDER_PATTERN.sub(replace, html)

    return _stylesheet() + html
