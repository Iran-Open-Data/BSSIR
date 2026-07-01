from bssir.context.config import Config
from .collection import MetadataCollection



def load_metadata(config: Config) -> MetadataCollection:
    """
    Load and validate the package metadata.

    This function creates a :class:`MetadataCollection` using the current
    package configuration and returns the fully initialized metadata object.
    The returned collection provides access to all metadata resources, such
    as tables, schemas, maps, ID information, commodities, and instructions.

    Returns
    -------
    MetadataCollection
        A validated metadata collection initialized from the current package
        configuration.
    """
    metadata = MetadataCollection(config)
    return metadata
