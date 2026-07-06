from pathlib import Path

from bssir.context import load_context


PACKAGE_DIR = Path(__file__).parent


def test_import_package(tmp_path):
    context = load_context(package_dir=PACKAGE_DIR, root_dir=tmp_path)

    assert context.config.package_name == "test"
    assert context.config.root_dir == tmp_path
    assert context.config.local_dir == tmp_path / "Data_Test"
    assert context.config.local_dir.exists()
