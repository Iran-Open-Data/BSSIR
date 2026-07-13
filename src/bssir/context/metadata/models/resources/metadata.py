from typing import Literal, overload

from ..common import Metadata
from .resource import YearResource, CommonResource


class ResourcesMetadata(Metadata):
    """Resource registry for a dataset.

    Contains resources that are shared across years and resources
    associated with specific release years.
    """
    @overload
    def __getitem__(self, key: int) -> YearResource: ...

    @overload
    def __getitem__(self, key: Literal["metadata"]) -> CommonResource: ...

    def __getitem__(
        self, key: int | Literal["metadata"]
    ) -> YearResource | CommonResource:
        if isinstance(key, int):
            return YearResource(year=key, merged=self.content[key], config=self.config)
        if key == "metadata":
            return CommonResource(title=key, merged=self.content[key], config=self.config)

    def __contains__(self, key: int | Literal["metadata"]) -> bool:
        return key in self.content

    def get(self, key: int | Literal["metadata"], default = None):
        return self.content.get(key, default)    
