from typing import cast

import pandas as pd

from bssir.context.config import Config
from .models import Metadata, MetadataSource, SourceTablesMetadata, METADATA_MODELS
from .loader import build_metadata_definition, extract_metadata_description


class MetadataCollection:
    """Access to package metadata."""

    def __init__(self, config: Config):
        self._config = config
        self._cache: dict[str, Metadata] = {}

    def _get(self, name: str) -> Metadata:
        if name not in self._cache:
            self._cache[name] = self.build(name)

        return self._cache[name]

    def build(self, name: str) -> Metadata:
        """Build a metadata object from configured sources."""

        try:
            model = METADATA_MODELS[name]
        except KeyError:
            raise KeyError(
                f"Unknown metadata type: '{name}'. "
                f"Available metadata: {list(METADATA_MODELS)}"
            ) from None

        definition = build_metadata_definition(name, self._config)
        description = extract_metadata_description(definition.source)

        return model(
            name=name,
            definition=definition,
            description=description,
            config=self._config,
        )

    @property
    def instruction(self) -> Metadata:
        return self._get("instruction")

    @property
    def raw_files(self) -> Metadata:
        return self._get("raw_files")

    @property
    def id_schema(self) -> Metadata:
        return self._get("id_schema")

    @property
    def source_tables(self) -> SourceTablesMetadata:
        return cast(SourceTablesMetadata, self._get("source_tables"))

    @property
    def schema(self) -> Metadata:
        return self._get("schema")

    @property
    def commodities(self) -> Metadata:
        return self._get("commodities")

    @property
    def occupations(self) -> Metadata:
        return self._get("occupations")

    @property
    def industries(self) -> Metadata:
        return self._get("industries")

    @property
    def maps(self) -> Metadata:
        return self._get("maps")

    def report(self) -> pd.DataFrame:
        """Return a summary of available metadata."""

        def source_state(source: MetadataSource, layer: str) -> str:
            has = getattr(source, f"has_{layer}")
            if not has:
                return ""

            loaded = getattr(source, f"{layer}_loaded")
            return "🟢" if loaded else "⚪"

        rows = []

        for name in METADATA_MODELS:
            metadata = getattr(self, name)

            description = (metadata.description or "").split("\n", 1)[0]
            source = metadata.definition.source

            rows.append({
                "Name": metadata.name,
                "Description": description,
                "State": "🟢 Loaded" if source.loaded else "⚪ Not Loaded",
                "Base": source_state(source, "base"),
                "Package": source_state(source, "package"),
                "Local": source_state(source, "local"),
            })

        return pd.DataFrame(rows)

    def _repr_html_(self) -> str:
        from bssir import rendering

        return rendering.html_repr("metadata_collection", self)

    # def __rich__(self) -> Panel:
    #     """Render a summary of the metadata collection."""

    #     report = self.report()

    #     table = Table(show_header=True, expand=True)
    #     table.add_column("Metadata", style="cyan", no_wrap=True)
    #     table.add_column("Description")
    #     table.add_column("Base", justify="center")
    #     table.add_column("Package", justify="center")
    #     table.add_column("Local", justify="center")

    #     for row in report.itertuples(index=False):
    #         table.add_row(
    #             str(row.name),
    #             str(row.description) if row.description else "",
    #             yes_no(bool(row.base_package)),
    #             yes_no(bool(row.package)),
    #             yes_no(bool(row.local)),
    #         )

    #     summary = Text(
    #         f"{len(report)} metadata definitions",
    #         style="dim",
    #     )

    #     footer = Text.from_markup(
    #         "Access metadata with [cyan]metadata.source_tables[/], "
    #         "[cyan]metadata.schema[/], etc.",
    #         style="dim",
    #     )

    #     return Panel.fit(
    #         Group(summary, table, footer),
    #         title="Metadata Collection",
    #         border_style="blue",
    #     )


    # def __repr__(self) -> str:
    #     console = Console(
    #         record=True,
    #         force_terminal=False,
    #         width=100,
    #         color_system="truecolor",
    #     )
    #     with console.capture() as capture:
    #         console.print(self.__rich__())

    #     return capture.get()



    # def _repr_html_(self) -> str:
    #     """HTML representation for Jupyter notebooks."""

    #     df = self.report()

    #     loaded = len(self._cache)
    #     total = len(df)

    #     styler = (
    #         df.style
    #         .hide(axis="index")
    #         .set_properties(
    #             subset=["Metadata"],
    #             **{
    #                 "font-weight": "600",
    #                 "font-family": "monospace",
    #                 "white-space": "nowrap",
    #             },
    #         )
    #         .set_properties(
    #             subset=["Base", "Package", "Local"],
    #             **{
    #                 "text-align": "center",
    #                 "font-weight": "bold",
    #                 "width": "60px",
    #             },
    #         )
    #         .set_properties(
    #             subset=["Description"],
    #             **{
    #                 "text-align": "left",
    #             },
    #         )
    #         .map(
    #             lambda v: (
    #                 "color:#2e7d32;font-weight:bold;"
    #                 if v == "✓"
    #                 else "color:#d0d0d0;"
    #             ),
    #             subset=["Base", "Package", "Local"],
    #         )
    #         .set_table_styles(
    #             [
    #                 {
    #                     "selector": "table",
    #                     "props": [
    #                         ("border-collapse", "collapse"),
    #                         ("width", "100%"),
    #                         ("font-family", "system-ui, sans-serif"),
    #                     ],
    #                 },
    #                 {
    #                     "selector": "thead th",
    #                     "props": [
    #                         ("background-color", "#f6f8fa"),
    #                         ("font-weight", "600"),
    #                         ("padding", "6px 10px"),
    #                         ("border-bottom", "2px solid #ddd"),
    #                     ],
    #                 },
    #                 {
    #                     "selector": "tbody td",
    #                     "props": [
    #                         ("padding", "6px 10px"),
    #                         ("border-bottom", "1px solid #eee"),
    #                     ],
    #                 },
    #                 {
    #                     "selector": "tbody tr:hover",
    #                     "props": [
    #                         ("background-color", "#fafafa"),
    #                     ],
    #                 },
    #             ]
    #         )
    #     )

    #     html_table = styler.to_html()

    #     return f"""
    #         <div style="font-family:system-ui,sans-serif;max-width:1000px">

    #         <h2 style="margin-bottom:0.2em;">
    #         Metadata Collection
    #         </h2>

    #         <p style="margin-top:0;color:#666;">
    #         Lazy access to metadata definitions used throughout the package.
    #         </p>

    #         <table style="margin-bottom:1em">
    #         <tr><td><b>Available definitions</b></td><td>{total}</td></tr>
    #         <tr><td><b>Loaded</b></td><td>{loaded}</td></tr>
    #         <tr><td><b>Resolution order</b></td><td>Local → Package → Base</td></tr>
    #         </table>

    #         {html_table}

    #         <div style="
    #         margin-top:1.5em;
    #         padding:0.8em;
    #         background:#f8f8f8;
    #         border-left:4px solid #3b82f6;
    #         ">

    #         <b>Legend</b>

    #         <ul style="margin-top:0.5em;margin-bottom:1em;">
    #         <li><b>Base</b> — Built into BSSIR.</li>
    #         <li><b>Package</b> — Distributed with the selected package.</li>
    #         <li><b>Local</b> — User or project-specific overrides.</li>
    #         </ul>

    #         <b>Usage</b>

    #         <pre style="margin-top:0.5em;">
    #         metadata.source_tables
    #         metadata.schema
    #         metadata.raw_files
    #         metadata.maps
    #         metadata.commodities
    #         </pre>

    #         Each metadata object is loaded lazily and cached after its first access.

    #         </div>

    #         </div>
    #     """
