from ..common import MetadataNode


type ResolvedLabelMapping = dict[str, str]


class LabelMapping(MetadataNode):
    name: str

    def resolve(self, year: int, **optional_settings) -> ResolvedLabelMapping:
        resolved = super().resolve(year=year, **optional_settings)
        return {
            key: value
            for key, value in resolved.items()
            if value is not None
        }
