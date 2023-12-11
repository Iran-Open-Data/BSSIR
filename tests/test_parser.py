from bssir.utils.parsing_utils import parse_years, create_table_year_pairs


AVAILABLE_YEARS = list(range(1370, 1391))
TABLES_AVAILABILITY = {
    "a": {"start": 1380, "end": 1385},
}


class TestParseYears:
    def test_without_meta(self):
        for year, result in [
            (1394, [1394]),
            ("1388", [1388]),
            ([1380, 1390], [1380, 1390]),
            ("1385-1395", list(range(1385, 1396))),
        ]:
            assert result == parse_years(year)

    def test_with_availability(self):
        for year, result in [
            (1375, [1375]),
            (1370, [1370]),
            (1369, []),
            (1390, [1390]),
            (1391, []),
            ("1360-1400", AVAILABLE_YEARS),
        ]:
            assert result == parse_years(year, available_years=AVAILABLE_YEARS)

    def test_with_table_info(self):
        for year, result in [
            (1380, [1380]),
            (1379, []),
        ]:
            assert result == parse_years(
                year,
                available_years=AVAILABLE_YEARS,
                table_name="a",
                tables_availability=TABLES_AVAILABILITY,
            )
