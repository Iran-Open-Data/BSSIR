from pathlib import Path
import shutil

from bssir.metadata_reader import config


def test_import_package():
    config.set_package_config(Path(__file__).parent)
    shutil.rmtree(Path(__file__).parent.joinpath("Data_test"))
