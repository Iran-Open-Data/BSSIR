from typing import Any
from html import escape

import pandas as pd

from bssir.context.metadata.collection import MetadataCollection
from bssir.rendering.render import render_template


def _source_dot(value: str) -> str:
    """Render source availability."""

    if value == "🟢":
        return '<span class="bssir-dot loaded"></span>'

    if value == "⚪":
        return '<span class="bssir-dot deferred"></span>'

    if value == "🔴":
        return '<span class="bssir-dot error"></span>'

    return ""


def _status_badge(value: str) -> str:
    """Render a metadata loading status."""

    if value.startswith("🟢"):
        cls = "loaded"
        text = "Loaded"

    elif value.startswith("⚪"):
        cls = "deferred"
        text = "Not Loaded"

    elif value.startswith("🔴"):
        cls = "error"
        text = "Error"

    else:
        cls = "neutral"
        text = value

    return (
        f'<span class="bssir-status {cls}">'
        f'<span class="bssir-dot {cls}"></span>'
        f"{escape(text)}"
        "</span>"
    )


from html import escape

def report_html(df: pd.DataFrame) -> str:
    """Render the metadata report as HTML."""

    html = [
        '<div class="bssir-report-wrapper">',
        '<table class="bssir-report">',
        "<thead>",
        "<tr>",
    ]

    source_columns = {"Base", "Package", "Local"}

    for column in df.columns:

        classes = []

        if column in source_columns:
            classes.append("center")

        cls = f' class="{" ".join(classes)}"' if classes else ""

        html.append(f"<th{cls}>{escape(column)}</th>")

    html.extend([
        "</tr>",
        "</thead>",
        "<tbody>",
    ])

    for _, row in df.iterrows():

        html.append("<tr>")

        for column in df.columns:

            value = str(row[column])

            classes = []

            if column == "Name":
                classes.append("left")
                cell = f'<span class="bssir-name">{escape(value)}</span>'

            elif column == "Description":
                classes.append("left")
                cell = f'<span class="bssir-description-cell">{escape(value)}</span>'

            elif column == "State":
                classes.append("left")
                cell = _status_badge(value)

            elif column in source_columns:
                classes.append("center")
                cell = _source_dot(value)

            else:
                cell = escape(value)

            cls = f' class="{" ".join(classes)}"' if classes else ""

            html.append(f"<td{cls}>{cell}</td>")

        html.append("</tr>")

    html.extend([
        "</tbody>",
        "</table>",
        "</div>",
    ])

    return "\n".join(html)


def context(
    metadata: MetadataCollection,
) -> dict[str, Any]:
    """Build the rendering context for a MetadataCollection."""

    report = metadata.report()

    total = len(report)
    loaded = (report["State"] == "🟢 Loaded").sum()
    deferred = total - loaded

    return {
        # ------------------------------------------------------------------
        # Hero
        # ------------------------------------------------------------------
        "title": "Metadata",

        "subtitle": "Package metadata definitions",

        "meta": (
            f"{total} definitions"
            " &nbsp;&nbsp;•&nbsp;&nbsp; "
            f"{loaded} loaded"
            " &nbsp;&nbsp;•&nbsp;&nbsp; "
            f"{deferred} deferred"
        ),

        "description": (
            "Metadata provides the structural information used throughout "
            "the package, including dataset definitions, schemas, "
            "classifications, identifiers, and other supporting resources."
        ),

        # ------------------------------------------------------------------
        # Overview
        # ------------------------------------------------------------------
        "overview_subtitle": "How metadata is organized",

        "summary": """
            <p>
            This collection is the entry point for all metadata definitions available
            in the current package. Each definition is represented by a dedicated
            metadata object that exposes a consistent interface for accessing,
            resolving, and querying metadata.
            </p>

            <p>
            Metadata is loaded lazily. Definitions are created and read only when they
            are first accessed, allowing the package to start quickly while avoiding
            unnecessary file I/O. The report below shows which metadata definitions
            are available, the source layers they inherit from (<strong>Base</strong>,
            <strong>Package</strong>, and <strong>Local</strong>), and whether they
            have already been loaded into memory.
            </p>
        """,

        # ------------------------------------------------------------------
        # Report
        # ------------------------------------------------------------------
        "report": report_html(report),

        # ------------------------------------------------------------------
        # Quick Start
        # ------------------------------------------------------------------
        "quickstart": """
            <div class="bssir-example">
                <div class="bssir-example-title">
                    Access a metadata definition
                </div>
                <pre><code>metadata.source_tables</code></pre>
            </div>

            <div class="bssir-example">
                <div class="bssir-example-title">
                    Resolve year-dependent metadata
                </div>
                <pre><code>metadata.source_tables.resolve(1403)</code></pre>
            </div>

            <div class="bssir-example">
                <div class="bssir-example-title">
                    Access metadata content
                </div>
                <pre><code>metadata.commodities["rice"]</code></pre>
            </div>
        """,

        # ------------------------------------------------------------------
        # Footer
        # ------------------------------------------------------------------
        "footer": """
            <div class="bssir-report-footer">

                <span>
                    <span class="bssir-dot loaded"></span>
                    Loaded
                </span>

                <span>
                    <span class="bssir-dot deferred"></span>
                    Deferred
                </span>

                <span>
                    Base, Package and Local indicate the metadata source layers used
                    to build each definition.
                </span>

            </div>
        """,
    }


def html_repr(metadata: MetadataCollection) -> str:
    """Return the HTML representation of a metadata collection."""
    return render_template(
        "metadata.html",
        context(metadata),
    )
