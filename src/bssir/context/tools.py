from pathlib import Path

from pydantic import BaseModel

from bssir import utils
from bssir.types import Years
from .config import Config


class Tools(BaseModel):
    config: Config

    def extract(
        self,
        source: Path,
        destination: Path,
    ) -> None:
        utils.archive.extract(
            source=source,
            destination=destination,
            tools=self.config.tools.model_dump(),
        )

    def parse_years(self, years: Years) -> list[int]:
        return utils.parse.parse_years(years, self.config.coverage_period)
