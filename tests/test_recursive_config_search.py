"""Configuration searches preserve order, references and matched-branch pruning."""

import pytest

from premise.external import get_recursively as search_external
from premise.external_data_validation import get_recursively as search_validation


@pytest.mark.parametrize("search", [search_external, search_validation])
def test_recursive_search_keeps_depth_first_order_and_matched_values(search):
    matched = {"name": "do not descend into this match"}
    payload = ["one", "two"]
    configuration = {
        "first": {"name": "first"},
        "name": matched,
        "entries": [
            {"name": None},
            {"nested": {"name": payload}},
            [{"name": "nested lists are not traversed"}],
            "plain value",
        ],
    }

    result = search(configuration, "name")

    assert result == ["first", matched, None, payload]
    assert result[1] is matched
    assert result[3] is payload
    assert configuration["entries"][1]["nested"]["name"] is payload


@pytest.mark.parametrize("search", [search_external, search_validation])
def test_recursive_search_returns_an_empty_list_for_absent_fields(search):
    assert search({}, "name") == []
    assert search({"unrelated": [{"value": 1}]}, "name") == []
