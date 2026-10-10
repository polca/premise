"""Prevent silent identity conflation in stock-to-service data joins."""

import pytest

from dev.stock_vintage.audit_power_service import (
    complete_block_membership,
    complete_pv_membership,
    match_generation,
)


def test_complete_pv_preserves_zero_output_and_independent_AC_DC_units():
    plant = {
        "plant_code": 10,
        "selected_capacity_mw_ac": 2,
        "net_pv_generation_mwh": 0,
        "all_operating_pv_matches_selected_technology": True,
        "commissioning_years": [2010],
    }
    selected = [{"plant_code": 10, "generator_id": "A", "dc_capacity_mw": 2.5}]
    member = {
        "Generator ID": "A",
        "sheet": "Operable",
        "Status": "OP",
        "Technology": "Solar Photovoltaic",
        "Prime Mover": "PV",
        "Operating Year": 2010,
    }
    accepted, rejected = complete_pv_membership([plant], selected, {10: [member]}, 2022)
    assert not rejected and len(accepted) == 1
    assert accepted[0]["capacity_mw_dc"] == 2.5
    assert accepted[0]["selected_capacity_mw_ac"] == 2
    assert accepted[0]["net_pv_generation_mwh"] == 0
    cases = [
        ({**plant, "commissioning_years": [2010, 2020]}, {10: [member]}, selected),
        (
            {**plant, "commissioning_years": [2022]},
            {10: [{**member, "Operating Year": 2022}]},
            selected,
        ),
        (
            plant,
            {
                10: [
                    member,
                    {**member, "Generator ID": "B", "sheet": "Retired and Canceled"},
                ]
            },
            selected,
        ),
        (
            {**plant, "all_operating_pv_matches_selected_technology": False},
            {10: [member]},
            selected,
        ),
        ({**plant, "net_pv_generation_mwh": 18000}, {10: [member]}, selected),
        (plant, {10: [member]}, [{**selected[0], "dc_capacity_mw": None}]),
    ]
    for candidate, members, generators in cases:
        accepted, rejected = complete_pv_membership(
            [candidate], generators, members, 2022
        )
        assert not accepted and rejected[0]["membership_exclusions"]


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
