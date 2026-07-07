from .common import Metadata, MetadataSource, MetadataDefinition
from .source_tables import (
    SourceTableSettings,
    SourceTablesMetadata,
)
from .pipelines import PipelineMetadata

METADATA_MODELS: dict[str, type[Metadata]] = {
    "instruction": Metadata,
    "raw_files": Metadata,
    "id_schema": Metadata,
    "source_tables": SourceTablesMetadata,
    "schema": Metadata,
    "pipelines": PipelineMetadata,
    "commodities": Metadata,
    "occupations": Metadata,
    "industries": Metadata,
    "maps": Metadata,
}


__all__ = [
    "Metadata",
    "MetadataSource",
    "MetadataDefinition",
    "SourceTableSettings",
    "SourceTablesMetadata",
    "PipelineMetadata",
    "METADATA_MODELS",
]

