"""Matrix shape depends on unique activity and production counts."""

from copy import deepcopy
from types import SimpleNamespace

import pytest

from premise.validation import BaseDatasetValidator


@pytest.mark.parametrize(
    "activity_names,product_names,counts",
    [
        ([], [], None),
        (["a", "a"], ["a", "a"], None),
        (["a", "b"], ["x", "y"], None),
        (["a", "b"], ["a", "a"], (2, 1)),
        (["a"], ["a", "b"], (1, 2)),
    ],
)
def test_matrix_shape_uses_unique_counts(activity_names, product_names, counts):
    database = [
        {
            "name": name,
            "reference product": "service",
            "unit": "unit",
            "location": "GLO",
            "exchanges": [],
        }
        for name in activity_names
    ]
    if database:
        database[0]["exchanges"] = [
            {
                "name": name,
                "product": "service",
                "unit": "unit",
                "location": "GLO",
                "type": "production",
            }
            for name in product_names
        ]
    before = deepcopy(database)
    issues = []
    validator = SimpleNamespace(
        database=database,
        log_issue=lambda *args, **kwargs: issues.append((args, kwargs)),
    )

    assert BaseDatasetValidator.check_matrix_squareness(validator) is None
    assert database == before
    assert issues == (
        []
        if counts is None
        else [
            (
                (
                    {},
                    "non-square matrix",
                    f"Matrix is not square: {counts[0]} activities, {counts[1]} products.",
                ),
                {"issue_type": "major"},
            )
        ]
    )
