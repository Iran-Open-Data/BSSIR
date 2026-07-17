from bssir.context.metadata.models.pipelines.step import BaseStep


STEP_REGISTRY: dict[str, type[BaseStep]] = {}


def register_step(cls: type[BaseStep]) -> type[BaseStep]:
    STEP_REGISTRY[cls.model_fields["action"].default] = cls
    return cls


def create(data: dict) -> BaseStep:
    action = data["action"]

    cls = STEP_REGISTRY[action]

    return cls.model_validate(data)
