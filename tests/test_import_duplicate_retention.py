"""Duplicate inventory removal retains first records and list identity."""

import pytest
from premise.inventory_imports import check_for_duplicate_datasets


@pytest.mark.parametrize("order", ["aaa", "aaaa", "ababa", "abcabc", "abc"])
def test_all_duplicates_removed_with_first_record_and_order_retained(order, capsys):
    data = [
        {"name": name, "reference product": "product", "location": "GLO", "ordinal": i}
        for i, name in enumerate(order)
    ]
    first = {}
    for record in data:
        first.setdefault(record["name"], record)

    result = check_for_duplicate_datasets(data)

    assert result is data
    assert result == list(first.values())
    assert all(actual is expected for actual, expected in zip(result, first.values()))
    assert ("Duplicate datasets found" in capsys.readouterr().out) is (
        len(set(order)) != len(order)
    )


def test_equal_duplicate_records_retain_first_object():
    first = {"name": "same", "reference product": "product", "location": "GLO"}
    data = [first, dict(first), dict(first)]

    assert check_for_duplicate_datasets(data) == [first]
    assert data[0] is first
