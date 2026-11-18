import copy

import pytest
from wurst.errors import NoResults

import premise.materials.metals as module
from premise.metals import Metals
from premise.metals_rules import MetalsConfigError

NAME = "Indium, at regional storage"


def supply(location="RER", amount=1.0):
    return {
        "name": NAME,
        "reference product": NAME,
        "unit": "kilogram",
        "location": location,
        "exchanges": [
            {
                "name": NAME,
                "product": NAME,
                "type": "production",
                "unit": "kilogram",
                "amount": amount,
            }
        ],
    }


def owner(records, version="2026"):
    obj = object.__new__(Metals)
    obj.version = version
    obj.database = records
    obj._provider_semantic_index = {
        (a["name"], a["reference product"], a["location"]) for a in records
    }
    obj.build_db_indexes()
    return obj


def test_reviewed_region_is_used_without_a_global_wrapper():
    record = supply()
    with pytest.warns(UserWarning, match="reviewed regional material supplier"):
        assert owner([record]).get_metal_market_dataset(NAME, NAME) is record


def test_unqualified_region_is_still_refused(monkeypatch):
    policy = copy.deepcopy(module._RESOURCE_POLICY)
    for alias in policy["provider_aliases"]:
        alias.pop("locations", None)
    monkeypatch.setattr(module, "_RESOURCE_POLICY", policy)
    with pytest.raises(NoResults):
        owner([supply()]).get_metal_market_dataset(NAME, NAME)


def test_original_global_preference_is_retained():
    regional, global_supply = supply(), supply("GLO")
    assert (
        owner([regional, global_supply]).get_metal_market_dataset(NAME, NAME)
        is global_supply
    )


def test_declared_region_does_not_admit_other_countries():
    with pytest.raises(NoResults):
        owner([supply("US")]).get_metal_market_dataset(NAME, NAME)


def test_native_alias_does_not_change_ecoinvent_selection():
    with pytest.raises(NoResults):
        owner([supply()], version="3.12").get_metal_market_dataset(NAME, NAME)


def test_ambiguous_regional_supply_still_fails():
    with pytest.raises(ValueError, match="Ambiguous"):
        owner([supply(), supply()]).get_metal_market_dataset(NAME, NAME)


@pytest.mark.parametrize("amount", [-1.0, 0.0, 2.0, None, True])
def test_regional_reference_must_be_positive_one_kg(amount):
    with pytest.raises(MetalsConfigError, match="positive one-kg reference"):
        owner([supply(amount=amount)]).get_metal_market_dataset(NAME, NAME)


def test_regional_product_unit_must_remain_kg():
    record = supply()
    record["unit"] = "megajoule"
    with pytest.raises(MetalsConfigError, match="positive one-kg reference"):
        owner([record]).get_metal_market_dataset(NAME, NAME)


def test_unreviewed_regional_location_cannot_be_enabled(monkeypatch):
    policy = copy.deepcopy(module._RESOURCE_POLICY)
    for alias in policy["provider_aliases"]:
        if alias.get("locations"):
            alias.pop("limitations", None)
    monkeypatch.setattr(module, "_RESOURCE_POLICY", policy)
    with pytest.raises(MetalsConfigError, match="reviewed limitations"):
        owner([supply()]).get_metal_market_dataset(NAME, NAME)


def test_existing_platinum_restriction_is_unchanged():
    record = supply()
    record["name"] = record["reference product"] = "Platinum, at regional storage"
    with pytest.raises(NoResults):
        owner([record]).get_metal_market_dataset(
            record["name"], record["reference product"]
        )
