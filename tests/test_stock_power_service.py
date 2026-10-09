"""Prevent silent identity conflation in stock-to-service data joins."""

import pytest

from dev.stock_vintage.audit_power_service import match_generation


def test_numeric_padding_matches_with_audited_alias_and_retains_zero_output():
    stock = [
        {"plant_code": 10, "generator_id": "0001"},
        {"plant_code": 10, "generator_id": "0002"},
        {"plant_code": 20, "generator_id": "001"},
    ]
    matched, aliases, missing = match_generation(stock, {(10, "1"): 100, (10, "2"): 0})
    assert matched == {(10, "0001"): 100, (10, "0002"): 0}
    assert len(aliases) == 2
    assert missing == [(20, "001")]


@pytest.mark.parametrize(
    "stock,generation",
    [
        ([{"plant_code": 10, "generator_id": "001"}], {(10, "1"): 5, (10, "01"): 7}),
        (
            [
                {"plant_code": 10, "generator_id": "001"},
                {"plant_code": 10, "generator_id": "1"},
            ],
            {(10, "1"): 5},
        ),
    ],
)
def test_alias_cannot_reuse_or_merge_distinct_generator_identities(stock, generation):
    with pytest.raises(ValueError, match="Ambiguous"):
        match_generation(stock, generation)
