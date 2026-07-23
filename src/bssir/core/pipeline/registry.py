from collections.abc import Callable
from collections import defaultdict

from .models import BaseStep


STEP_REGISTRY: dict[str, dict[str, type[BaseStep]]] = defaultdict(dict)


def register(
    action_name: str,
    table_name: str = "_default",
) -> Callable[[type[BaseStep]], type[BaseStep]]:
    def decorator(cls: type[BaseStep]) -> type[BaseStep]:
        STEP_REGISTRY.setdefault(table_name, {})[action_name] = cls
        return cls

    return decorator


def resolve(
    action: str,
    table_name: str | None = None,
) -> type[BaseStep]:
    steps = STEP_REGISTRY.get("_default", {})
    if table_name is not None:
        steps.update(STEP_REGISTRY[table_name])
    if action not in steps:
        raise ValueError(
            f"Unknown pipeline action {action!r} for table {table_name!r}."
        )
    cls = steps[action]
    return cls


def create(
    data: dict,
    table_name: str | None = None,
) -> BaseStep:
    cls = resolve(data["action"], table_name)
    return cls.model_validate(data)
