from __future__ import annotations

from html import escape


def badge(text: str, kind: str) -> str:
    return (
        f'<span class="bssir-badge bssir-{kind}">'
        f"{escape(text)}"
        "</span>"
    )


def stat_card(label: str, value: int | str) -> str:
    return f"""
    <div class="bssir-card">
        <div class="bssir-card-value">{escape(str(value))}</div>
        <div class="bssir-card-label">{escape(label)}</div>
    </div>
    """


def section(title: str, body: str) -> str:
    return f"""
    <section class="bssir-section">
        <div class="bssir-section-title">{escape(title)}</div>
        {body}
    </section>
    """


def footer(text: str) -> str:
    return f"""
    <footer class="bssir-footer">
    {text}
    </footer>
    """
