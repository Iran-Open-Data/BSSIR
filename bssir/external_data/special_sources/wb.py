import pandas as pd
import requests


class WBIndicator:
    URL_FORMAT = (
        "https://api.worldbank.org/v2"
        "/countries/{country}"
        "/indicators/{indicator}"
        "?format=json"
        "&page={page}"
        "&frequency=Y"
    )

    def __init__(self, indicator: str, country: str = "IRN") -> None:
        self.indicator = indicator
        self.country = country
        self.pages = {}

    def _get_page(self, page: int):
        response = requests.get(
            self.URL_FORMAT.format(
                indicator=self.indicator,
                country=self.country,
                page=page,
            )
        )
        return response.json()

    def get_indicator(self) -> pd.DataFrame:
        self.pages[1] = self._get_page(1)

        for page in range(2, self.pages[1][0]["pages"]+1):
            self.pages[page] = self._get_page(page)

        rows = []
        for page in self.pages.values():
            rows.extend([(r["date"], r["value"]) for r in page[1]])
        table = (
            pd.DataFrame(rows, columns=["date", "value"])
            .dropna()
            .sort_values("date")
            .reset_index(drop=True)
        )
        return table
