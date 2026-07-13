from .common import Metadata, MetadataSource, MetadataDefinition
from .source_tables import (
    SourceTableSettings,
    SourceTablesMetadata,
)
from .pipelines import PipelineMetadata
from .id_schema import IDSchemaMetadata
from .resources import ResourcesMetadata


METADATA_MODELS: dict[str, type[Metadata]] = {
    "instruction": Metadata,
    "resources": ResourcesMetadata,
    "id_schema": IDSchemaMetadata,
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
    "ResourcesMetadata",
    "SourceTableSettings",
    "SourceTablesMetadata",
    "PipelineMetadata",
    "METADATA_MODELS",
]

