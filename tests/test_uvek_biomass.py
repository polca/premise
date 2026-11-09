"""Native wood-chip stage keeps dry mass, provider ownership and IAM shares."""

import copy
from types import SimpleNamespace

import numpy as np
import pytest
import xarray as xr

import premise.core.transformation as transformation
from premise.energy.uvek_biomass import MARKET_NAME, NATIVE_MIX, UVEKBiomass
from premise.inventory.clic_requirements import required_roots

FOREST = [
    "Wood chips, hardwood, wet, sustainable forest management, measured as dry mass, at forest road",
    "Wood chips, softwood, wet, sustainable forest management, measured as dry mass, at forest road",
]
SAWMILL = [
    "Wood chips, hardwood, wet, measured as dry mass, at sawmill",
    "Wood chips, softwood, wet, measured as dry mass, at sawmill",
]


def activity(name, location="CH", unit="kilogram"):
    a = {
        "name": name,
        "reference product": name,
        "location": location,
        "unit": unit,
        "database": "native",
        "code": name + location,
        "exchanges": [],
    }
    a["exchanges"].append(exchange(a, 1, kind="production"))
    return a


def exchange(a, amount, kind="technosphere"):
    return {
        "name": a["name"],
        "product": a["reference product"],
        "location": a["location"],
        "unit": a["unit"],
        "type": kind,
        "amount": amount,
        "input": (a["database"], a["code"]),
    }


@pytest.fixture
def native(monkeypatch):
    class Geo:
        def __init__(self, model):
            pass

        def ecoinvent_to_iam_location(self, location):
            return {"CH": "EUR", "RER": "EUR", "US": "USA"}.get(location, "World")

    monkeypatch.setattr(transformation, "Geomap", Geo)
    suppliers = [activity(n, "CH" if n in FOREST else "RER") for n in FOREST + SAWMILL]
    for i, supplier in enumerate(suppliers):
        supplier["exchanges"].append(
            {
                "name": "Retained upstream flow",
                "type": "biosphere",
                "amount": i + 1,
                "unit": "kilogram",
                "categories": ("air",),
                "input": ("bio310", "flow"),
            }
        )
    mix = activity(NATIVE_MIX, "RER")
    mix["exchanges"] += [
        exchange(a, v) for a, v in zip(suppliers, [0.1, 0.3, 0.3, 0.3], strict=True)
    ]
    heat = activity("Heat, wood", unit="megajoule")
    heat["exchanges"].append(exchange(mix, 2))
    material = activity("Particle board, at plant")
    material["exchanges"].append(exchange(mix, -3))
    values = np.asarray(
        [[[0.2, 0.2], [0.8, 0.8]], [[0.7, 0.7], [0.3, 0.3]], [[0.4, 0.4], [0.6, 0.6]]]
    )
    iam = SimpleNamespace(
        regions=["EUR", "USA", "World"],
        biomass_mix=xr.DataArray(
            values,
            dims=("region", "variables", "year"),
            coords={
                "region": ["EUR", "USA", "World"],
                "variables": ["biomass crops - purpose grown", "biomass - residual"],
                "year": [2020, 2060],
            },
        ),
    )
    return [*suppliers, mix, heat, material], iam


def adapter(native, **kwargs):
    data, iam = native
    return UVEKBiomass(data, iam, "remind", "test", 2050, **kwargs)


@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_dry_mass_native_inventory_and_material_byproducts_are_retained(
    native, amount, uncertainty
):
    data, _ = native
    heat, material = data[-2:]
    fuel = heat["exchanges"][-1]
    fuel.update(
        amount=amount,
        formula="fuel_dose",
        **{
            "uncertainty type": uncertainty,
            "loc": np.log(abs(amount) or 1) if uncertainty == 2 else amount,
            "scale": 0.3,
        },
    )
    if uncertainty == 2:
        fuel["negative"] = amount < 0
    else:
        fuel.update(minimum=amount - 1, maximum=amount + 1)
    before = copy.deepcopy(data)
    biomass = adapter(native)
    plans = biomass.plan()
    assert data == before
    biomass.apply(plans)
    biomass.validate(plans)
    assert data[:5] == before[:5] and material == before[-1]
    assert fuel["name"] == MARKET_NAME and fuel["location"] == "EUR"
    assert all(
        fuel[k] == v
        for k, v in before[-2]["exchanges"][-1].items()
        if k not in {"name", "product", "location", "input"}
    )
    market = plans["EUR"]["market"]
    actual = {
        e["name"]: e["amount"]
        for e in market["exchanges"]
        if e["type"] == "technosphere"
    }
    assert actual == pytest.approx(
        {FOREST[0]: 0.05, FOREST[1]: 0.15, SAWMILL[0]: 0.4, SAWMILL[1]: 0.4}
    )
    assert len(data) == len(before) + 3
    assert any(v["geographic_proxy"] for v in plans["USA"]["vector"])
    assert market["premise biomass"]["reference_basis"].startswith("one kilogram dry")


def test_foreground_owned_link_is_not_replaced(native):
    data, _ = native
    heat = data[-2]
    heat["exchanges"][-1]["input"] = ("foreground", "owned")
    before = copy.deepcopy(heat)
    b = adapter(native)
    plans = b.plan()
    b.apply(plans)
    b.validate(plans)
    assert heat == before


@pytest.mark.parametrize(
    "change",
    ["source_reference", "negative_weight", "unknown_iam", "missing_global_mix"],
)
def test_unsupported_basis_fails_before_mutation(native, change):
    data, iam = native
    if change == "source_reference":
        data[0]["exchanges"][0]["amount"] = -1
    elif change == "negative_weight":
        data[4]["exchanges"][1]["amount"] = -0.1
    elif change == "unknown_iam":
        iam.biomass_mix = iam.biomass_mix.assign_coords(
            variables=["unreviewed supply", "biomass - residual"]
        )
    else:
        iam.biomass_mix = iam.biomass_mix.sel(region=["EUR", "USA"])
        data[-2]["location"] = "GLO"
    before = copy.deepcopy(data)
    with pytest.raises(ValueError):
        adapter(native).plan()
    assert data == before


def test_validator_detects_supplier_share_and_quantity_drift(native):
    fuel = native[0][-2]["exchanges"][-1]
    b = adapter(native)
    plans = b.plan()
    b.apply(plans)
    fuel["amount"] += 1
    with pytest.raises(ValueError, match="quantity"):
        b.validate(plans)
    fuel["amount"] -= 1
    plans["EUR"]["market"]["exchanges"][1]["amount"] += 0.1
    with pytest.raises(ValueError, match="supplier vector"):
        b.validate(plans)


def test_other_system_models_and_biospheres_are_refused(native):
    before = copy.deepcopy(native[0])
    for kwargs in ({"system_model": "consequential"}, {"biosphere_version": "2026"}):
        with pytest.raises(ValueError, match="requires"):
            adapter(native, **kwargs)
    assert native[0] == before


def test_biomass_native_roles_do_not_import_legacy_forestry_inventories():
    assert required_roots(["biomass"], family="uvek") == ()


def test_public_sector_gate_stays_closed_during_woodchip_stage():
    from premise.database.background import validate_uvek_scope

    with pytest.raises(ValueError, match="supports electricity only"):
        validate_uvek_scope("biomass")


def test_repeated_application_keeps_owned_market_codes_and_does_not_duplicate(native):
    first = adapter(native)
    first.apply(first.plan())
    before = copy.deepcopy(native[0])
    second = adapter(native)
    plans = second.plan()
    second.apply(plans)
    second.validate(plans)
    assert native[0] == before


def test_unrelated_legacy_material_region_is_not_mapped(native, monkeypatch):
    data, _ = native
    data[-1]["location"] = "CPA"
    original = transformation.Geomap.ecoinvent_to_iam_location

    def strict(self, location):
        if location == "CPA":
            raise KeyError("Unreviewed geography")
        return original(self, location)

    monkeypatch.setattr(transformation.Geomap, "ecoinvent_to_iam_location", strict)
    before = copy.deepcopy(data[-1])
    b = adapter(native)
    plans = b.plan()
    b.apply(plans)
    b.validate(plans)
    assert data[6] == before


def test_nonfinite_caller_quantity_is_rejected_before_mutation(native):
    data, _ = native
    data[-2]["exchanges"][-1]["amount"] = float("inf")
    before = copy.deepcopy(data)
    with pytest.raises(ValueError, match="finite"):
        adapter(native).plan()
    assert data == before


def test_missing_iam_data_is_explicit_and_cannot_be_a_passing_empty_update(native):
    data, iam = native
    iam.biomass_mix = None
    before = copy.deepcopy(data)
    b = adapter(native)
    with pytest.raises(ValueError, match="not applicable"):
        b.plan()
    with pytest.raises(ValueError, match="nonempty"):
        b.validate({})
    assert data == before
