from __future__ import annotations

from pathlib import Path
from html import escape
from typing import Any

import pandas as pd

from bssir.context.metadata.models.source_tables.table import SourceTable
from bssir.rendering.render import render_template


def _format_years(years: list[int]) -> str:
    """Format a list of years into compact ranges."""

    if not years:
        return ""

    years = sorted(years)

    ranges: list[str] = []
    start = end = years[0]

    for year in years[1:]:
        if year == end + 1:
            end = year
            continue

        ranges.append(
            f"{start}" if start == end else f"{start}–{end}"
        )
        start = end = year

    ranges.append(
        f"{start}" if start == end else f"{start}–{end}"
    )

    return ", ".join(ranges)


def report(table: SourceTable) -> pd.DataFrame:
    """Create a summary report of table columns."""

    # latest = max(table.availability)

    rows = []

    for name, column in table.columns.items():

        if column is None:
            rows.append(
                {
                    "Column": name,
                    "Label": "Dropped",
                    "Type": "—",
                    "Availability": "—",
                }
            )
            continue

        resolved = column.resolve(max(column.availability))

        rows.append(
            {
                "Column": name,
                "Label": resolved.name,
                "Type": resolved.type,
                "Availability": _format_years(column.availability),
            }
        )

    return pd.DataFrame(rows)


def report_html(df: pd.DataFrame) -> str:
    """Render the column report."""

    html = [
        '<div class="bssir-report-wrapper">',
        '<table class="bssir-report">',
        "<thead>",
        "<tr>",
    ]

    for column in df.columns:
        cls = ' class="left"' if column in {"Column", "Label"} else ""
        html.append(f"<th{cls}>{escape(column)}</th>")

    html.extend(
        [
            "</tr>",
            "</thead>",
            "<tbody>",
        ]
    )

    for _, row in df.iterrows():

        html.append("<tr>")

        for column in df.columns:

            value = escape(str(row[column]))

            cls = ' class="left"' if column in {"Column", "Label"} else ""

            if column == "Column":
                value = f'<span class="bssir-name">{value}</span>'

            html.append(f"<td{cls}>{value}</td>")

        html.append("</tr>")

    html.extend(
        [
            "</tbody>",
            "</table>",
            "</div>",
        ]
    )

    return "\n".join(html)


def dataframe_html(df: pd.DataFrame, **kwargs) -> str:
    """Render a DataFrame inside a scrollable wrapper."""

    table = df.to_html(
        classes="bssir-report",
        na_rep="",
        border=0,
        **kwargs,
    )

    return f'<div class="bssir-report-wrapper">{table}</div>'


def render_files_report(files: dict[int, list[Path]]) -> str:
    """Render a report of source files grouped by survey year."""

    rows = []

    for year, paths in files.items():
        badges = "".join(
            f'<span class="bssir-file-badge" title="{path.as_posix()}">{path.name}</span>'
            for path in paths
        )

        rows.append(
            f"""
            <tr>
                <td>{year}</td>
                <td class="left">{badges or "—"}</td>
            </tr>
            """
        )

    return f"""
    <h3>Source Files</h3>

    <div class="bssir-report-wrapper">
        <table class="bssir-report">
            <thead>
                <tr>
                    <th>Year</th>
                    <th>Matching Files</th>
                </tr>
            </thead>
            <tbody>
                {"".join(rows)}
            </tbody>
        </table>
    </div>
    """


def context(table: SourceTable) -> dict[str, Any]:
    latest = max(table.availability)

    column_labels = dataframe_html(table.column_labels_report)
    label_columns = dataframe_html(table.label_columns_report)

    files_report = render_files_report(table.files)

    return {
        "title": table.name,

        "subtitle": "Raw source table",

        "meta": (
            f"{len(table.columns)} columns"
            " &nbsp;&nbsp;•&nbsp;&nbsp; "
            f"{table.availability[0]}–{table.availability[-1]}"
        ),

        "description": (
            "This metadata defines the raw table together with the evolution "
            "of variable names, labels, and source files over time."
        ),

        "overview_subtitle": "Overview",

        "summary": f"""
        <p>
        <strong>Availability:</strong>
        {table.availability[0]}–{table.availability[-1]}
        </p>

        <p>
        <strong>Columns:</strong>
        {len(table.columns)}
        </p>

        <p>
        The reports below summarize how column names, labels, and source files
        evolve through time. Each row corresponds to a period during which the
        metadata remains unchanged.
        </p>
        """,

        "report": f"""
        <h3>Columns → Labels</h3>
        {column_labels}

        <br><br>

        <h3>Labels → Columns</h3>
        {label_columns}

        <br><br>

        {files_report}
        """,

        "quickstart": f"""
        <div class="bssir-example">
            <div class="bssir-example-title">
                Resolve metadata
            </div>
            <pre><code>table.resolve({latest})</code></pre>
        </div>

        <div class="bssir-example">
            <div class="bssir-example-title">
                Access a column
            </div>
            <pre><code>table["COLUMN"]</code></pre>
        </div>

        <div class="bssir-example">
            <div class="bssir-example-title">
                View label history
            </div>
            <pre><code>table.column_labels</code></pre>
        </div>
        """,

        "footer": """
        <div class="bssir-report-footer">
            Metadata is grouped into periods where the mapping is unchanged.
            Source files reflect the current workspace and may be empty if the
            corresponding setup steps have not yet been executed. Resolve the
            table for a specific year using <code>table.resolve(year)</code>.
        </div>
        """,
    }


def html_repr(table: SourceTable) -> str:
    """HTML representation of a SourceTable."""

    return render_template(
        "metadata.html",
        context(table),
    )
