from .common import Metadata, MetadataSource, MetadataDefinition
from .source_tables import (
    SourceTableSettings,
    SourceTablesMetadata,
)
from .pipelines import PipelinesMetadata
from .id_schema import IDSchemaMetadata
from .resources import ResourcesMetadata
from .tables import TablesMetadata


METADATA_MODELS: dict[str, type[Metadata]] = {
    "instruction": Metadata,
    "resources": ResourcesMetadata,
    "id_schema": IDSchemaMetadata,
    "source_tables": SourceTablesMetadata,
    "schema": Metadata,
    "tables": TablesMetadata,
    "pipelines": PipelinesMetadata,
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
    "TablesMetadata",
    "PipelinesMetadata",
    "METADATA_MODELS",
]

