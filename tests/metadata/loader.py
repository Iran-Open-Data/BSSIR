from pathlib import Path

import pytest

from bssir.utils.yaml import read_yaml


def test_read_yaml_reads_mapping(tmp_path: Path):
    yaml_file = tmp_path / "metadata.yaml"
    yaml_file.write_text(
        """
        name: household
        version: 1
        """,
        encoding="utf-8",
    )

    result = read_yaml(yaml_file)

    assert result == {
        "name": "household",
        "version": 1,
    }


def test_read_yaml_returns_empty_dict_for_empty_file(tmp_path: Path):
    yaml_file = tmp_path / "empty.yaml"
    yaml_file.write_text("", encoding="utf-8")

    result = read_yaml(yaml_file)

    assert result == {}


def test_read_yaml_returns_empty_dict_for_null_yaml(tmp_path: Path):
    yaml_file = tmp_path / "empty.yaml"
    yaml_file.write_text(
        "null",
        encoding="utf-8",
    )

    result = read_yaml(yaml_file)

    assert result == {}


def test_read_yaml_applies_interpreter(tmp_path: Path):
    yaml_file = tmp_path / "metadata.yaml"
    yaml_file.write_text(
        """
        name: ${DATASET_NAME}
        """,
        encoding="utf-8",
    )

    def interpreter(text: str) -> str:
        return text.replace(
            "${DATASET_NAME}",
            "household",
        )

    result = read_yaml(
        yaml_file,
        interpreter=interpreter,
    )

    assert result == {
        "name": "household",
    }


def test_read_yaml_rejects_non_mapping_yaml(tmp_path: Path):
    yaml_file = tmp_path / "invalid.yaml"
    yaml_file.write_text(
        """
        - item1
        - item2
        """,
        encoding="utf-8",
    )

    with pytest.raises(TypeError):
        read_yaml(yaml_file)
