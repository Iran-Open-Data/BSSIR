from pathlib import Path
import shutil

from bssir.metadata_reader import config


def test_import_package():
    _, metadata = config.set_package_config(Path(__file__).parent)
    metadata.industries
    metadata.occupations
    metadata.commodities
    shutil.rmtree(Path(__file__).parent.joinpath("Data_test"))
