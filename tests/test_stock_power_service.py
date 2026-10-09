"""Prevent silent identity conflation in stock-to-service data joins."""

import pytest

from dev.stock_vintage.audit_power_service import (
    complete_block_membership,
    match_generation,
)


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


def test_complete_block_audit_rejects_hidden_retired_member_and_partial_year():
    block = {
        "plant_code": 10,
        "unit_code": "CC1",
        "cohort_year": 2000,
        "generator_ids": ["CT1", "CA1"],
    }
    rows = [
        {
            "Generator ID": g,
            "sheet": "Operable",
            "Status": "OP",
            "Technology": "Natural Gas Fired Combined Cycle",
            "Energy Source 1": "NG",
            "Associated with Combined Heat and Power System": "N",
            "Operating Year": 2000,
            "Prime Mover": pm,
        }
        for g, pm in [("CT1", "CT"), ("CA1", "CA")]
    ]
    assert complete_block_membership([block], {(10, "CC1"): rows}, 2022) == (
        [block],
        [],
    )
    hidden = {
        **rows[0],
        "Generator ID": "CT0",
        "sheet": "Retired and Canceled",
        "Status": "RE",
    }
    accepted, rejected = complete_block_membership(
        [block], {(10, "CC1"): rows + [hidden]}, 2022
    )
    assert not accepted
    assert (
        "unexpected_or_duplicate_unit_members" in rejected[0]["membership_exclusions"]
    )
    assert "incompatible_unit_member" in rejected[0]["membership_exclusions"]
    accepted, rejected = complete_block_membership([block], {(10, "CC1"): rows}, 2000)
    assert not accepted
    assert "partial_initial_year_output" in rejected[0]["membership_exclusions"]
