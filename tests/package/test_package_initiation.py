from pathlib import Path
import shutil

from bssir.context import load_context


def test_import_package():
    load_context(package_dir=Path(__file__).parent)
    shutil.rmtree(Path(__file__).parent.joinpath("Data_test"))
