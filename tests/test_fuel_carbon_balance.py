from copy import deepcopy
from types import SimpleNamespace

import pytest
from stats_arrays import UncertaintyBase, uncertainty_choices

from premise.fuels.base import Fuels
from premise.fuels.carbon import reclassify_fuel_co2
from premise.inventory_imports import get_biosphere_code

FLOW = {("Carbon dioxide, non-fossil", "air", "unspecified", "kilogram"): "bio"}


def emission(name, amount, **kwargs):
    return dict(
        name=name,
        amount=amount,
        unit="kilogram",
        type="biosphere",
        categories=("air",),
        **kwargs,
    )


def amounts(ds):
    return {
        name: sum(e["amount"] for e in ds["exchanges"] if e["name"] == name)
        for name in ("Carbon dioxide, fossil", "Carbon dioxide, non-fossil")
    }


@pytest.mark.parametrize("share", [0, 0.01, 0.45, 1])
@pytest.mark.parametrize("captured", [0, 0.36184272170066833])
def test_capture_balance_and_repeatability(share, captured):
    gross = 0.40204746648669243
    stack = gross - captured
    capture = dict(
        name="carbon dioxide, captured from natural gas",
        type="technosphere",
        amount=captured,
        unit="kilogram",
    )
    ds = {"exchanges": [emission("Carbon dioxide, fossil", stack), capture]}
    for _ in range(2):
        reclassify_fuel_co2(ds, 0.20681454241275787 * 2.12, share, FLOW, "gas")
        result = amounts(ds)
        assert result["Carbon dioxide, fossil"] == pytest.approx(stack * (1 - share))
        assert result["Carbon dioxide, non-fossil"] == pytest.approx(stack * share)
        assert sum(result.values()) + capture["amount"] == pytest.approx(gross)
        assert capture["amount"] == captured
    assert sum(e["name"] == "Carbon dioxide, non-fossil" for e in ds["exchanges"]) <= 1


def test_preserves_other_biogenic_carbon_and_replaces_prior_share():
    bio = emission("Carbon dioxide, non-fossil", 3)
    ds = {"exchanges": [emission("Carbon dioxide, fossil", 10), bio]}
    for share in (0.5, 0.25, 1, 0):
        reclassify_fuel_co2(ds, 2, share, FLOW, "liquid")
        assert amounts(ds)["Carbon dioxide, fossil"] == pytest.approx(10 - 2 * share)
        assert bio["amount"] == pytest.approx(3 + 2 * share)
        assert len(ds["exchanges"]) == 2


def test_two_fuels_do_not_overwrite_each_other():
    ds = {"exchanges": [emission("Carbon dioxide, fossil", 10)]}
    for _ in range(2):
        reclassify_fuel_co2(ds, 2, 0.5, FLOW, "diesel")
        reclassify_fuel_co2(ds, 3, 0.5, FLOW, "gas")
    assert amounts(ds) == pytest.approx(
        {"Carbon dioxide, fossil": 7.5, "Carbon dioxide, non-fossil": 2.5}
    )


def test_compartments_negative_flows_and_uncertainty():
    fossil = emission(
        "Carbon dioxide, fossil",
        0.04,
        **{"uncertainty type": 5, "loc": 0.04, "minimum": 0.02, "maximum": 0.06},
    )
    fossil2 = deepcopy(fossil)
    fossil2["categories"] = ("air", "urban air close to ground")
    uptake = emission("Carbon dioxide, fossil", -0.2)
    ds = {"exchanges": [fossil, fossil2, uptake]}
    flow = {
        **FLOW,
        (
            "Carbon dioxide, non-fossil",
            "air",
            "urban air close to ground",
            "kilogram",
        ): "urban",
    }
    reclassify_fuel_co2(ds, 0.4, 0.45, flow, "gas")
    assert fossil["amount"] == pytest.approx(0.022)
    assert fossil["loc"] == pytest.approx(0.022)
    assert fossil["minimum"] == pytest.approx(0.011)
    assert fossil["maximum"] == pytest.approx(0.033)
    assert uptake["amount"] == -0.2
    assert {
        e["input"][1]
        for e in ds["exchanges"]
        if e["name"] == "Carbon dioxide, non-fossil"
    } == {"bio", "urban"}
    uncertainty_choices[5].validate(
        UncertaintyBase.from_dicts(
            dict(
                uncertainty_type=5,
                loc=fossil["loc"],
                minimum=fossil["minimum"],
                maximum=fossil["maximum"],
            )
        )
    )


def test_zero_endpoint_removes_invalid_lognormal_parameters():
    fossil = emission(
        "Carbon dioxide, fossil",
        0.04,
        **{"uncertainty type": 2, "loc": -3.2, "scale": 0.2},
    )
    ds = {"exchanges": [fossil]}
    reclassify_fuel_co2(ds, 0.4, 1, FLOW, "gas")
    assert fossil["amount"] == fossil["loc"] == 0
    assert fossil["uncertainty type"] == 0
    assert "scale" not in fossil
    reclassify_fuel_co2(ds, 0.4, 0, FLOW, "gas")
    assert fossil["amount"] == pytest.approx(0.04)


def gas_fixture(share=0.45):
    ds = dict(
        name="CCS plant",
        location="R1",
        exchanges=[
            dict(
                name="market group for natural gas, high pressure",
                product="natural gas, high pressure",
                location="GLO",
                unit="cubic meter",
                amount=0.20681454241275787,
                type="technosphere",
            ),
            emission("Carbon dioxide, fossil", 0.040204744786024094),
        ],
    )
    fuels = object.__new__(Fuels)
    fuels.database = [ds]
    fuels.fuel_map = {"natural gas": [], "methane, from biomass": []}
    fuels.iam_data = SimpleNamespace(production_volumes=None)
    fuels.regions = ["R1"]
    fuels.ecoinvent_to_iam_loc = {}
    fuels.biosphere_flows = FLOW
    fuels.is_in_index = lambda exc, loc: True
    fuels.get_technology_and_regional_production_shares = lambda **kwargs: (
        None,
        {("natural gas", "R1"): 1 - share, ("methane, from biomass", "R1"): share},
        {"R1": 1},
    )
    return fuels, ds


@pytest.mark.parametrize("share", [0.01, 0.45])
def test_gas_mixin_reproduces_corrected_2020_2050(share):
    fuels, ds = gas_fixture(share)
    fuels.update_carbon_dioxide_emissions()
    fuels.update_carbon_dioxide_emissions()
    assert amounts(ds)["Carbon dioxide, non-fossil"] == pytest.approx(
        0.040204744786024094 * share
    )
    assert sum(amounts(ds).values()) == pytest.approx(0.040204744786024094)


def test_coal_methane_is_not_biogenic_and_small_shares_are_not_rounded():
    fuels, ds = gas_fixture()
    fuels.fuel_map["methane, from coal"] = []
    fuels.get_technology_and_regional_production_shares = lambda **kwargs: (
        None,
        {
            ("natural gas", "R1"): 0.5,
            ("methane, from coal", "R1"): 0.496,
            ("methane, from biomass", "R1"): 0.004,
        },
        {"R1": 1},
    )
    fuels.update_carbon_dioxide_emissions()
    assert amounts(ds)["Carbon dioxide, non-fossil"] == pytest.approx(
        0.040204744786024094 * 0.004
    )


def test_liquid_ccs_uses_residual_emissions():
    fuels, ds = gas_fixture()
    ds["exchanges"][0].update(name="market for diesel", unit="kilogram", amount=0.1)
    fuels.fuel_map = {"diesel": [], "biodiesel": []}
    fuels.get_technology_and_regional_production_shares = lambda **kwargs: (
        None,
        {("diesel", "R1"): 0.55, ("biodiesel", "R1"): 0.45},
        {"R1": 1},
    )
    for _ in range(2):
        fuels.update_fuel_carbon_dioxide_emissions(
            ["diesel", "biodiesel"], ["market for diesel"], 3.15, ["diesel"]
        )
    assert amounts(ds)["Carbon dioxide, non-fossil"] == pytest.approx(
        0.040204744786024094 * 0.45
    )


def test_compact_exchanges_do_not_duplicate_biogenic_flow():
    from premise.inventory_store import compact_exchange_payload

    fossil = compact_exchange_payload(emission("Carbon dioxide, fossil", 0.04))
    bio = compact_exchange_payload(emission("Carbon dioxide, non-fossil", 0.01))
    ds = {"exchanges": [fossil, bio]}
    for _ in range(2):
        reclassify_fuel_co2(ds, 0.4, 0.45, FLOW, "gas")
    assert len(ds["exchanges"]) == 2
    assert amounts(ds) == pytest.approx(
        {"Carbon dioxide, fossil": 0.022, "Carbon dioxide, non-fossil": 0.028}
    )


def test_gas_share_uses_remapped_supplier_region():
    fuels, ds = gas_fixture()
    # Consumer location is an ecoinvent alias; the actual linked supplier is R1.
    ds["location"] = "CH"
    fuels.ecoinvent_to_iam_loc = {"CH": "R1"}
    fuels.update_carbon_dioxide_emissions()
    assert ds["exchanges"][0]["location"] == "R1"
    assert amounts(ds)["Carbon dioxide, non-fossil"] == pytest.approx(
        0.040204744786024094 * 0.45
    )


def test_zero_world_weights_do_not_divide_by_zero():
    fuels, ds = gas_fixture()
    fuels.get_technology_and_regional_production_shares = lambda **kwargs: (
        None,
        {("natural gas", "R1"): 0.55, ("methane, from biomass", "R1"): 0.45},
        {},
    )
    fuels.update_carbon_dioxide_emissions()
    assert sum(amounts(ds).values()) == pytest.approx(0.040204744786024094)


@pytest.mark.parametrize("version", ["3.7", "3.8", "3.9", "3.10", "3.11", "3.12"])
def test_aircraft_carbon_uses_supported_biosphere_compartment(version):
    """The high-altitude biogenic flow migrated to unspecified air in 3.10."""
    altitude = ("air", "lower stratosphere + upper troposphere")
    fossil = emission("Carbon dioxide, fossil", 0.8558549744600895)
    fossil["categories"] = altitude
    ds = {"name": "short-haul freight aircraft", "exchanges": [fossil]}
    flows = get_biosphere_code(version)
    expected_categories = (
        altitude if version in {"3.7", "3.8", "3.9"} else ("air", "unspecified")
    )
    expected_code = (
        "4e1f0bb0-2703-4303-bf86-972d810612cf"
        if version in {"3.7", "3.8", "3.9"}
        else "eba59fd6-f37e-41dc-9ca3-c7ea22d602c7"
    )
    total = fossil["amount"]

    for share in (0.5, 0.5, 1, 0.25, 0):
        reclassify_fuel_co2(ds, total, share, flows, "kerosene")
        assert fossil["categories"] == altitude
        assert fossil["amount"] == pytest.approx(total * (1 - share))
        assert len(ds["exchanges"]) == 2
        bio = ds["exchanges"][1]
        assert bio["categories"] == expected_categories
        assert bio["input"] == ("biosphere3", expected_code)
        assert bio["amount"] == pytest.approx(total * share)
        assert sum(amounts(ds).values()) == pytest.approx(total)


@pytest.mark.parametrize("compact", [False, True])
@pytest.mark.parametrize("existing_bio", [False, True])
def test_fallbacks_and_unspecified_aliases_share_one_carbon_pool(compact, existing_bio):
    from premise.inventory_store import compact_exchange_payload

    fossil = [emission("Carbon dioxide, fossil", 2) for _ in range(3)]
    fossil[0]["categories"] = ("air", "lower stratosphere + upper troposphere")
    fossil[1]["categories"] = ["air", "unspecified"]
    exchanges = fossil[:]
    if existing_bio:
        exchanges.append(emission("Carbon dioxide, non-fossil", 3))
    if compact:
        exchanges = [compact_exchange_payload(exc) for exc in exchanges]
    ds = {"exchanges": exchanges}
    original_bio = 3 if existing_bio else 0

    for share in (0.5, 1, 0.25, 0, 0.5):
        reclassify_fuel_co2(ds, 6, share, FLOW, "kerosene")
        assert len(ds["exchanges"]) == 4
        assert amounts(ds) == pytest.approx(
            {
                "Carbon dioxide, fossil": 6 * (1 - share),
                "Carbon dioxide, non-fossil": original_bio + 6 * share,
            }
        )
        assert all(exc["amount"] >= 0 for exc in ds["exchanges"])


def test_existing_exact_flow_is_preferred_to_unspecified_fallback():
    altitude = ("air", "lower stratosphere + upper troposphere")
    fossil = emission("Carbon dioxide, fossil", 2)
    fossil["categories"] = altitude
    bio = emission("Carbon dioxide, non-fossil", 3, input=("custom-biosphere", "high"))
    bio["categories"] = altitude
    unspecified = emission("Carbon dioxide, non-fossil", 4)
    ds = {"exchanges": [fossil, bio, unspecified]}

    reclassify_fuel_co2(ds, 2, 0.5, FLOW, "kerosene")

    assert fossil["amount"] == 1
    assert bio["amount"] == 4
    assert bio["input"] == ("custom-biosphere", "high")
    assert unspecified["amount"] == 4
    assert len(ds["exchanges"]) == 3


def test_missing_target_leaves_all_compartments_unchanged():
    urban = emission("Carbon dioxide, fossil", 2)
    urban["categories"] = ("air", "urban air close to ground")
    aircraft = emission("Carbon dioxide, fossil", 3)
    aircraft["categories"] = ("air", "lower stratosphere + upper troposphere")
    ds = {"name": "aircraft consumer", "exchanges": [urban, aircraft]}
    original = deepcopy(ds)
    flows = {
        (
            "Carbon dioxide, non-fossil",
            "air",
            "urban air close to ground",
            "kilogram",
        ): "urban"
    }

    with pytest.raises(ValueError, match="aircraft consumer.*lower stratosphere"):
        reclassify_fuel_co2(ds, 5, 0.5, flows, "kerosene")

    assert ds == original


@pytest.mark.parametrize("categories", [None, (), [], ("air",), ["air", "unspecified"]])
def test_empty_or_equivalent_air_categories_reuse_existing_flow(categories):
    fossil = emission("Carbon dioxide, fossil", 2)
    fossil["categories"] = categories
    bio = emission("Carbon dioxide, non-fossil", 3)
    bio["categories"] = ("air", "unspecified")
    ds = {"exchanges": [fossil, bio]}

    reclassify_fuel_co2(ds, 2, 0.5, FLOW, "gas")

    assert len(ds["exchanges"]) == 2
    assert fossil["amount"] == 1
    assert bio["amount"] == 4


@pytest.mark.parametrize(
    "name", ["Carbon dioxide, fossil", "Carbon dioxide, non-fossil"]
)
@pytest.mark.parametrize("invalid", [float("nan"), float("inf"), -float("inf")])
def test_nonfinite_emissions_are_rejected_before_modifying_inventory(name, invalid):
    fossil = emission("Carbon dioxide, fossil", 2)
    broken = emission(name, invalid)
    ds = {"name": "invalid consumer", "exchanges": [fossil, broken]}
    original_fossil = deepcopy(fossil)

    with pytest.raises(ValueError, match="finite"):
        reclassify_fuel_co2(ds, 2, 0.5, FLOW, "gas")

    assert fossil == original_fossil
    assert len(ds["exchanges"]) == 2
