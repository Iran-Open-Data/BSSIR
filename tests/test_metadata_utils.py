import pytest

from bssir import metadata_reader, utils

inst = metadata_reader.metadata.instruction

simple_metadata = {"key": 1380, "other_key": "other_value"}

simple_versioned = {
    1363: {"key": 1363},
    1383: {"key": 1383},
}
simple_versioned_63_82 = {"key": 1363}
simple_versioned_83 = {"key": 1383}


keyword_versioned = {
    "shared_key": "shared_value",
    "overwritten_key": "old_value",
    "versions": {
        1363: {"key": 1363},
        1383: {"key": 1383, "overwritten_key": "new_value"},
    },
}
keyword_versioned_50_62 = {
    "shared_key": "shared_value",
    "overwritten_key": "old_value",
}
keyword_versioned_63_82 = {
    "shared_key": "shared_value",
    "overwritten_key": "old_value",
    "key": 1363,
}
keyword_versioned_83 = {
    "shared_key": "shared_value",
    "overwritten_key": "new_value",
    "key": 1383,
}

categorized_metadata = {
    "shared_key": "shared_value",
    "overwritten_key": "old_value",
    "categories": {
        1: {"key": "first_value"},
        2: {"key": "second_value", "overwritten_key": "new_value"},
    },
}


class TestLocalCases:
    def test_simple_versioned(self):
        for year in range(1350, 1363):
            assert utils.resolve_metadata(simple_versioned, year) is None
        for year in range(1363, 1383):
            assert (
                utils.resolve_metadata(simple_versioned, year, add_year=False)
                == simple_versioned_63_82
            )
        for year in range(1383, 1395):
            assert (
                utils.resolve_metadata(simple_versioned, year, add_year=False)
                == simple_versioned_83
            )

    def test_keyword_versioned(self):
        for year in range(1350, 1363):
            assert (
                utils.resolve_metadata(keyword_versioned, year, add_year=False)
                == keyword_versioned_50_62
            )
        for year in range(1363, 1383):
            assert (
                utils.resolve_metadata(keyword_versioned, year, add_year=False)
                == keyword_versioned_63_82
            )
        for year in range(1383, 1395):
            assert (
                utils.resolve_metadata(keyword_versioned, year, add_year=False)
                == keyword_versioned_83
            )


class TestInstruction:
    def test_simple_versioned(self):
        for year in range(1350, 1360):
            assert (
                utils.resolve_metadata(inst["simple_versioned"], year, add_year=False)
                == inst["simple_versioned_0"]
            )
        for year in range(1360, 1380):
            assert (
                utils.resolve_metadata(inst["simple_versioned"], year, add_year=False)
                == inst["simple_versioned_1360"]
            )
        for year in range(1380, 1400):
            assert (
                utils.resolve_metadata(inst["simple_versioned"], year, add_year=False)
                == inst["simple_versioned_1380"]
            )

    def test_keyword_versioned(self):
        for year in range(1350, 1360):
            assert (
                utils.resolve_metadata(inst["keyword_versioned"], year, add_year=False)
                == inst["keyword_versioned_0"]
            )
        for year in range(1360, 1380):
            assert (
                utils.resolve_metadata(inst["keyword_versioned"], year, add_year=False)
                == inst["keyword_versioned_1360"]
            )
        for year in range(1380, 1400):
            assert (
                utils.resolve_metadata(inst["keyword_versioned"], year, add_year=False)
                == inst["keyword_versioned_1380"]
            )

    def test_sample_1(self):
        for year in range(1350, 1360):
            assert (
                utils.resolve_metadata(inst["sample_1"], year, add_year=False)
                == inst["sample_1_0"]
            )
        for year in range(1360, 1370):
            assert (
                utils.resolve_metadata(inst["sample_1"], year, add_year=False)
                == inst["sample_1_1360"]
            )
        for year in range(1370, 1380):
            assert (
                utils.resolve_metadata(inst["sample_1"], year, add_year=False)
                == inst["sample_1_1370"]
            )
        for year in range(1380, 1400):
            assert (
                utils.resolve_metadata(inst["sample_1"], year, add_year=False)
                == inst["sample_1_1380"]
            )

    def test_sample_2(self):
        for year in range(1350, 1360):
            assert (
                utils.resolve_metadata(inst["sample_2"], year, add_year=False)
                == inst["sample_2_0"]
            )
        for year in range(1360, 1370):
            assert (
                utils.resolve_metadata(inst["sample_2"], year, add_year=False)
                == inst["sample_2_1360"]
            )
        for year in range(1370, 1380):
            assert (
                utils.resolve_metadata(inst["sample_2"], year, add_year=False)
                == inst["sample_2_1370"]
            )
        for year in range(1380, 1400):
            assert (
                utils.resolve_metadata(inst["sample_2"], year, add_year=False)
                == inst["sample_2_1380"]
            )

    def test_sample_3(self):
        for year in range(1350, 1360):
            assert (
                utils.resolve_metadata(inst["sample_3"], year, add_year=False)
                == inst["sample_3_0"]
            )
        for year in range(1360, 1370):
            assert (
                utils.resolve_metadata(inst["sample_3"], year, add_year=False)
                == inst["sample_3_1360"]
            )
        for year in range(1370, 1375):
            assert (
                utils.resolve_metadata(inst["sample_3"], year, add_year=False)
                == inst["sample_3_1370"]
            )
        for year in range(1375, 1380):
            assert (
                utils.resolve_metadata(inst["sample_3"], year, add_year=False)
                == inst["sample_3_1375"]
            )
        for year in range(1380, 1400):
            assert (
                utils.resolve_metadata(inst["sample_3"], year, add_year=False)
                == inst["sample_3_1380"]
            )
