from pathlib import Path

from bssir.context import load_context


PACKAGE_DIR = Path(__file__).parent


def test_dependent_package_metadata_is_loaded(tmp_path):
    context = load_context(package_dir=PACKAGE_DIR, root_dir=tmp_path)

    assert context.metadata.source_tables.definition.source.has_package
    assert "Sample" in context.metadata.source_tables
    assert context.metadata.source_tables.table_list == ["Sample", "StartOnly"]


def test_dependent_package_table_metadata_is_available(tmp_path):
    context = load_context(package_dir=PACKAGE_DIR, root_dir=tmp_path)

    sample = context.metadata.source_tables["Sample"]

    assert sample["description"] == "Sample table supplied by a dependent package."
    assert sample.availability == [1380, 1381, 1382]
    assert sample["settings"]["missings"] == "keep"
    assert sample["settings"]["encoding"] == "cp1256"


def test_dependent_package_schema_metadata_is_available(tmp_path):
    context = load_context(package_dir=PACKAGE_DIR, root_dir=tmp_path)

    assert context.metadata.schema["Sample"]["columns"]["ID"]["type"] == "string"
    assert context.metadata.schema["Sample"]["columns"]["Value"]["type"] == "integer"


def test_dependent_package_raw_file_metadata_is_available(tmp_path):
    context = load_context(package_dir=PACKAGE_DIR, root_dir=tmp_path)

    assert context.metadata.resources["sample"] == {
        "pattern": "{year}_sample.csv",
        "format": "csv",
    }
