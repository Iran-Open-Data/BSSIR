from collections.abc import Callable
import re

import yaml


def commodities(yaml_text: str, context: dict) -> str:
    context.update(yaml.safe_load(re.sub("{{.*}}", "", yaml_text)))
    placeholders_list: list[str] = re.findall(r"{{\s*(.*)\s*}}", yaml_text)
    mapping = {}
    for placeholder in placeholders_list:
        parts = placeholder.split(".")
        if len(parts) == 1:
            mapping[placeholder] = context[parts[0]]["items"]
        elif len(parts) == 2:
            mapping[placeholder] = context[parts[0]]["items"][parts[1]]
        else:
            raise ValueError
    for placeholder, value in mapping.items():
        yaml_text = yaml_text.replace("{{" + placeholder + "}}", str(value))
    return yaml_text


def industries(yaml_text: str, context: dict) -> str:
    yaml_text = anchor_handler(yaml_text, context)
    return yaml_text


def occupations(yaml_text: str, context: dict) -> str:
    yaml_text = anchor_handler(yaml_text, context)
    return yaml_text


def anchor_handler(yaml_text: str, context: dict) -> str:
    anchor_list: list[str] = re.findall(r"\$\$([^\$]*)\$\$", yaml_text)
    for anchor in anchor_list:
        anchor_content = context.copy()
        for part in  anchor.split("."):
            anchor_content = anchor_content[part]["items"]
        yaml_text = yaml_text.replace(f"$${anchor}$$", str(anchor_content))
    return yaml_text


INTERPRETERS: dict[str, Callable[[str, dict], str]] = {
    "commodities": commodities,
    "industries": industries,
    "occupations": occupations,
}
