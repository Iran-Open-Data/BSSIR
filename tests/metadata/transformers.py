from copy import deepcopy

from bssir.utils.dicts import (
    flatten_dict,
    unflatten_dict,
    update_dict,
)


def test_flatten_dict_flattens_nested_dictionary():
    dictionary = {
        "a": 1,
        "b": {
            "c": 2,
            "d": {
                "e": 3,
            },
        },
    }

    result = flatten_dict(dictionary)

    assert result == {
        ("a",): 1,
        ("b", "c"): 2,
        ("b", "d", "e"): 3,
    }


def test_flatten_dict_keeps_non_dict_values():
    dictionary = {
        "name": "household",
        "columns": [
            "id",
            "year",
        ],
        "active": True,
    }

    result = flatten_dict(dictionary)

    assert result == {
        ("name",): "household",
        ("columns",): [
            "id",
            "year",
        ],
        ("active",): True,
    }


def test_unflatten_dict_unflattens_tuple_keys():
    dictionary = {
        ("a",): 1,
        ("b", "c"): 2,
        ("b", "d", "e"): 3,
    }

    result = unflatten_dict(dictionary)

    assert result == {
        "a": 1,
        "b": {
            "c": 2,
            "d": {
                "e": 3,
            },
        },
    }


def test_flatten_and_unflatten_are_inverse_operations():
    dictionary = {
        "table": {
            "name": "food",
            "columns": {
                "amount": {
                    "type": "float",
                },
                "code": {
                    "type": "string",
                },
            },
        },
    }

    result = unflatten_dict(flatten_dict(dictionary))

    assert result == dictionary


def test_update_dict_merges_nested_dictionaries():
    base = {
        "table": {
            "description": "old description",
            "unit": "kg",
            "columns": {
                "amount": {
                    "type": "float",
                },
            },
        },
    }

    updates = {
        "table": {
            "description": "new description",
            "columns": {
                "amount": {
                    "type": "integer",
                },
            },
        },
    }

    result = update_dict(base, updates)

    assert result == {
        "table": {
            "description": "new description",
            "unit": "kg",
            "columns": {
                "amount": {
                    "type": "integer",
                },
            },
        },
    }


def test_update_dict_does_not_modify_inputs():
    base = {
        "a": {
            "b": 1,
        },
    }

    updates = {
        "a": {
            "c": 2,
        },
    }

    original_base = deepcopy(base)
    original_updates = deepcopy(updates)

    update_dict(base, updates)

    assert base == original_base
    assert updates == original_updates


def test_update_dict_adds_new_metadata_sections():
    base = {
        "table": {
            "name": "food",
        },
    }

    updates = {
        "columns": {
            "amount": {
                "type": "float",
            },
        },
    }

    result = update_dict(base, updates)

    assert result == {
        "table": {
            "name": "food",
        },
        "columns": {
            "amount": {
                "type": "float",
            },
        },
    }
