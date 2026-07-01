from typing import Any
from html import escape

import pandas as pd

from bssir.context.metadata.collection import MetadataCollection
from bssir.rendering.render import render_template


def _status_badge(text: str) -> str:
    """Render a colored status badge."""

    if text.startswith("🟢"):
        cls = "loaded"
    elif text.startswith("⚪"):
        cls = "deferred"
    elif text.startswith("🔴"):
        cls = "error"
    else:
        cls = "neutral"

    return (
        f'<span class="bssir-status {cls}">'
        f"{escape(text)}"
        "</span>"
    )


def _source_dot(value: str) -> str:
    """Render source availability."""

    if value == "🟢":
        return '<span class="bssir-dot loaded"></span>'

    if value == "⚪":
        return '<span class="bssir-dot deferred"></span>'

    if value == "🔴":
        return '<span class="bssir-dot error"></span>'

    return ""


def report_html(df: pd.DataFrame) -> str:
    """
    Render the metadata report as HTML.

    Parameters
    ----------
    df:
        Output of MetadataCollection.report().
    """

    html = [
        '<div class="bssir-report-wrapper">',
        '<table class="bssir-report">',
        "<thead>",
        "<tr>",
    ]

    for column in df.columns:
        html.append(f"<th>{escape(column)}</th>")

    html.extend([
        "</tr>",
        "</thead>",
        "<tbody>",
    ])

    source_columns = {"Base", "Package", "Local"}

    for _, row in df.iterrows():

        html.append("<tr>")

        for column in df.columns:

            value = row[column]

            if column == "State":
                cell = _status_badge(str(value))

            elif column in source_columns:
                cell = _source_dot(str(value))

            else:
                cell = escape(str(value))

            html.append(f"<td>{cell}</td>")

        html.append("</tr>")

    html.extend([
        "</tbody>",
        "</table>",
        """
        <div class="bssir-report-footer">
            <span><span class="bssir-dot loaded"></span> Loaded</span>
            <span><span class="bssir-dot deferred"></span> Not Loaded</span>
        </div>
        """,
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
            <div class="bssir-code">

            <pre><code># Access a metadata definition
            metadata.tables</code></pre>

            <pre><code># Resolve year-dependent metadata
            metadata.tables.resolve(1403)</code></pre>

            <pre><code># Access metadata content
            metadata.commodities["rice"]</code></pre>

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
