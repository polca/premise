"""Preserve processing/yield when IAM shares replace a native metal basket."""

from copy import deepcopy

import pytest

from premise.inventory.activity_maps import InventorySet, STEEL_TECHS, get_mapping
from premise.materials.steel import Steel
from premise.validation.validation import SteelValidation
from test_steel_market_grades import activity, model

ROLE = "steel processing market - low-alloyed"
SPEC = get_mapping(STEEL_TECHS, "uvek_aliases")[ROLE]
NAME = SPEC["fltr"]["name"]
PRIMARY, SECONDARY = (s["name"] for s in SPEC["replace_suppliers"])


def source(basis=1, rolling=-0.7):
    result = activity(NAME, NAME, "RER")
    result["exchanges"] += [
        {
            "name": name,
            "product": name,
            "unit": "kilogram",
            "location": "RER",
            "amount": amount,
            "type": "technosphere",
        }
        for name, amount in [(PRIMARY, basis * 0.6), (SECONDARY, basis * 0.4)]
    ]
    result["exchanges"].append(
        {
            "name": "Hot rolling, steel",
            "product": "Hot rolling, steel",
            "unit": "kilogram",
            "location": "RER",
            "amount": rolling,
            "type": "technosphere",
            "input": ("native", "rolling"),
            "uncertainty type": 3,
            "loc": rolling,
            "scale": 0.05,
            "comment": "Retain processing; the kg unit does not make this raw steel.",
        }
    )
    return result


@pytest.mark.parametrize("basis", [1, 1.04])
@pytest.mark.parametrize("rolling", [-0.7, 0, 0.7])
def test_real_market_builder_retains_signed_processing_and_original_input_basis(
    basis, rolling
):
    obj = model("3.10", "cutoff")
    original = source(basis, rolling)
    before = deepcopy(original)
    primary = [activity(PRIMARY, PRIMARY, r) for r in ["R1", "R2"]]
    secondary = [activity(SECONDARY, SECONDARY, r) for r in ["R1", "R2"]]
    obj.database = [original, *primary, *secondary]
    obj.steel_map = {
        "steel - primary - BF/BOF": primary,
        "steel - secondary": secondary,
    }
    obj.inv = InventorySet(obj.database, "2026", "remind", family="uvek")
    obj.processing_market_definitions = {ROLE: SPEC}
    obj.processing_market_bases = {}
    with pytest.warns(UserWarning, match="Updating processing market"):
        obj.create_steel_markets()
    assert original == before
    markets = [a for a in obj.database if a.get("regionalized")]
    assert len(markets) == 3  # Two IAM regions and their World mix.
    for market in markets:
        tech = [e for e in market["exchanges"] if e["type"] == "technosphere"]
        if market["location"] == "World":
            assert sum(e["amount"] for e in tech) == pytest.approx(1)
            assert all(e["name"] == NAME for e in tech)
            continue
        processing = [e for e in tech if e["name"] == "Hot rolling, steel"]
        assert len(processing) == 1
        expected = {k: v for k, v in before["exchanges"][-1].items() if k != "input"}
        assert processing[0] == expected
        assert sum(
            e["amount"] for e in tech if e["name"] in {PRIMARY, SECONDARY}
        ) == pytest.approx(basis)
    assert obj.emptied == [original]
    # Copying another regional market must not share the processing dictionaries.
    regional = [a for a in markets if a["location"] != "World"]
    regional[0]["exchanges"][-1]["amount"] = 99
    assert regional[1]["exchanges"][-1]["amount"] == rolling
    obj.iam_data.steel_technology_mix = obj.iam_data.production_volumes
    validator = SteelValidation(
        "remind",
        "test",
        2050,
        ["R1", "R2"],
        [regional[1]],
        obj.iam_data,
        "cutoff",
        technology_map=obj.steel_map,
        processing_market_bases=obj.processing_market_bases,
    )
    issues = []
    validator.log_issue = lambda *args, **kw: issues.append((args, kw))
    validator.check_steel_markets()
    assert not issues  # Hot rolling is kg, but is outside the steel supply basket.
    for e in regional[1]["exchanges"]:
        if e["name"] == SECONDARY:
            e["amount"] = 0
    validator.check_steel_markets()
    assert issues  # Retained processing cannot conceal missing secondary steel.


@pytest.mark.parametrize(
    "change", ["reference", "negative", "missing", "product", "unit", "nonfinite"]
)
def test_processing_stage_refuses_unreviewed_reference_or_basket(change):
    obj = Steel.__new__(Steel)
    obj.processing_market_bases = {}
    data = source()
    if change == "reference":
        data["exchanges"][0]["amount"] = 2
    elif change == "negative":
        data["exchanges"][1]["amount"] = -0.1
    elif change == "missing":
        data["exchanges"].pop(1)
    elif change == "product":
        data["exchanges"][1]["product"] = "alloy steel"
    elif change == "unit":
        data["exchanges"][1]["unit"] = "megajoule"
    else:
        data["exchanges"][1]["amount"] = float("nan")
    with pytest.raises(ValueError, match="reviewed reference or supplier basket"):
        obj._processing_market_options(data, SPEC, {"steel - secondary": []})
    assert not obj.processing_market_bases
