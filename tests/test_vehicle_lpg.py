"""LPG must retain its own fuel identity, explicit scenario coverage and carbon basis."""

import json
from copy import deepcopy
from types import SimpleNamespace

import pytest

from premise.fuels.carbon import update_vehicle_fuel_co2, vehicle_fuel_carbon
from premise.fuels.properties import PROPERTIES_KEY, activity_key
from premise.inventory_store import compact_exchange_payload
from premise.validation import CarValidation
from premise.vehicle_lpg import (
    COMMENT_KEY,
    LPG,
    SPEC_KEY,
    STATE_KEY,
    describe_lpg_supply,
    record_lpg_supply,
    services_and_providers,
)


def example(kind="car", *, model="remind", production=1, compact=False):
    unit = "kilometer" if kind == "car" else "ton kilometer"
    name = (
        f"transport, passenger car, {LPG}, Medium, EURO-6"
        if kind == "car"
        else f"transport, freight, lorry, {LPG}, 40 metric ton"
    )
    supplier = {
        "name": f"market for {LPG}",
        "reference product": LPG,
        "location": "Europe without Switzerland",
        "unit": "kilogram",
    }
    spec = {
        "version": 1,
        "baseline year": 2020,
        "fuel": LPG,
        "reference unit": unit,
        "fuel kg per service": 0.1,
        "energy MJ per service": 4.55,
        "lhv MJ per kg": 45.5,
        "fossil CO2 kg per kg": 45.5 * 0.0631,
        "IAM technology": (
            f"passenger car, {LPG}, medium"
            if kind == "car"
            else f"truck, {LPG}, 40 metric ton"
        ),
        "proxy boundary": "CNG manufacture including tank and non-CO2 emissions",
        "CNG pump-to-tank methane leakage": "excluded",
    }
    dataset = {
        "name": name,
        "reference product": "transport",
        "unit": unit,
        "location": "RER",
        SPEC_KEY: json.dumps(spec),
        "comment": "original model",
        "exchanges": [
            dict(
                name=name,
                product="transport",
                location="RER",
                unit=unit,
                type="production",
                amount=production,
            ),
            dict(
                name=supplier["name"],
                product=LPG,
                location=supplier["location"],
                unit="kilogram",
                type="technosphere",
                amount=0.1 * production,
            ),
            dict(
                name="Carbon dioxide, fossil",
                type="biosphere",
                unit="kilogram",
                categories=("air",),
                amount=0.1 * production * 45.5 * 0.0631,
            ),
        ],
    }
    if compact:
        dataset["exchanges"] = [
            compact_exchange_payload(e) for e in dataset["exchanges"]
        ]
    context = SimpleNamespace(
        database=[supplier, dataset],
        model=model,
        scenario="SSP2-NPi",
        year=2050,
        vehicle_type=kind,
    )
    return context, dataset, supplier


@pytest.mark.parametrize("kind", ["car", "truck"])
@pytest.mark.parametrize("compact", [False, True])
@pytest.mark.parametrize("production", [1, 2])
def test_fallback_is_fossil_and_records_actual_geography_without_changing_exchanges(
    kind, compact, production
):
    context, ds, _ = example(kind, compact=compact, production=production)
    before = deepcopy(ds["exchanges"])
    record_lpg_supply(context)
    state = ds[STATE_KEY]
    assert state["status"] == "resolved"
    assert state["supply mode"] == "fossil fallback"
    assert state["LPG efficiency mapping"] == "unavailable"
    assert state["suppliers"][0]["location"] == "Europe without Switzerland"
    assert state["energy MJ per service"] == pytest.approx(4.55)
    assert state["fossil CO2 kg per service"] == pytest.approx(0.287105)
    assert state["non-fossil CO2 kg per service"] == 0
    assert ds["exchanges"] == before
    once = deepcopy(ds)
    record_lpg_supply(context)
    assert ds == once
    assert ds["comment"].count("LPG supply (") == 1


@pytest.mark.parametrize("kind", ["car", "truck"])
def test_explicit_tiam_lpg_efficiency_and_linked_blend_are_preserved(kind):
    context, ds, supplier = example(kind, model="tiam-ucl")
    supplier[PROPERTIES_KEY] = {
        "reference unit": "kilogram",
        "energy MJ per unit": 45.5,
        "fossil CO2 kg per unit": 1.5,
        "non-fossil CO2 kg per unit": 1.5,
        "unresolved CO2 kg per unit": 0,
    }
    ds["exchanges"][1]["amount"] *= 0.8
    ds["log parameters"] = {"efficiency change": 0.8}
    update_vehicle_fuel_co2(context.database, "3.12")
    record_lpg_supply(context)
    state = ds[STATE_KEY]
    assert state["status"] == "resolved"
    assert state["supply mode"] == "linked supplier properties"
    assert state["LPG efficiency mapping"] == "declared"
    assert state["applied energy factor"] == 0.8
    assert state["energy MJ per service"] == pytest.approx(3.64)
    assert state["fossil CO2 kg per service"] == pytest.approx(0.12)
    assert state["non-fossil CO2 kg per service"] == pytest.approx(0.12)
    assert "No scenario LPG blend" not in ds["comment"]


@pytest.mark.parametrize(
    "mutation,message",
    [
        ("cng", "must purchase LPG"),
        ("unit", "no direct fuel"),
        ("supplier", "Missing final LPG supplier"),
        ("quantity", "fuel energy changed"),
        ("efficiency", "Unmapped LPG"),
        ("source", "Unmapped LPG"),
        ("biogenic", "non-fossil"),
        ("leakage", "methane leakage"),
        ("unknown carbon", "Unresolved LPG"),
    ],
)
def test_validation_recomputes_current_inputs_and_catches_regressions(
    mutation, message
):
    context, ds, supplier = example()
    record_lpg_supply(context)
    assert ds[STATE_KEY]["status"] == "resolved"
    if mutation == "cng":
        ds["exchanges"][1].update(
            name="market for natural gas, vehicle grade",
            product="natural gas, vehicle grade",
        )
    elif mutation == "unit":
        ds["exchanges"][1]["unit"] = "cubic meter"
    elif mutation == "supplier":
        ds["exchanges"][1]["location"] = "unknown"
    elif mutation == "quantity":
        ds["exchanges"][1]["amount"] *= 0.8
    elif mutation == "efficiency":
        ds["log parameters"] = {"efficiency change": 0.8}
    elif mutation == "source":
        ds["log parameters"] = {"IAM efficiency source": {"kind": "proxy intensity"}}
    elif mutation in {"biogenic", "leakage"}:
        ds["exchanges"].append(
            dict(
                name=(
                    "Carbon dioxide, non-fossil"
                    if mutation == "biogenic"
                    else "Methane, fossil"
                ),
                type="biosphere",
                unit="kilogram",
                amount=0.001,
                categories=("air", "unspecified"),
            )
        )
    else:
        supplier[PROPERTIES_KEY] = {
            "reference unit": "kilogram",
            "energy MJ per unit": 45.5,
            "fossil CO2 kg per unit": 0,
            "non-fossil CO2 kg per unit": 0,
            "unresolved CO2 kg per unit": 1,
        }
    validator = object.__new__(CarValidation)
    validator.__dict__.update(context.__dict__)
    issues = []
    validator.log_issue = lambda *args, **kwargs: issues.append((args, kwargs))
    validator.check_lpg_supply("car")
    assert len(issues) == 1 and message in issues[0][0][2]
    assert issues[0][1]["issue_type"] == "major"
    record_lpg_supply(context)
    assert ds[STATE_KEY]["status"] == "invalid"


def test_cng_combustion_methane_is_distinct_from_removed_leakage():
    context, ds, _ = example()
    ds["exchanges"].append(
        dict(
            name="Methane, fossil",
            type="biosphere",
            unit="kilogram",
            amount=0.001,
            categories=("air", "urban air close to ground"),
        )
    )
    record_lpg_supply(context)
    assert ds[STATE_KEY]["status"] == "resolved"


def test_forwarder_loses_stale_supply_metadata():
    context, ds, _ = example()
    record_lpg_supply(context)
    ds["exchanges"] = [
        ds["exchanges"][0],
        dict(ds["exchanges"][0], type="technosphere", location="EUR"),
    ]
    record_lpg_supply(context)
    assert STATE_KEY not in ds and COMMENT_KEY not in ds
    assert ds["comment"] == "original model"


def test_missing_supplier_properties_cannot_label_a_cng_product_as_fossil_lpg():
    context, ds, supplier = example()
    ds["exchanges"][1]["product"] = supplier["reference product"] = "natural gas"
    result = vehicle_fuel_carbon(ds, {activity_key(supplier): supplier})
    assert result["status"] == "unresolved"


def test_installed_lpg_templates_have_the_approved_energy_and_proxy_contract():
    from bw2io import ExcelImporter
    from premise.filesystem_constants import INVENTORY_DIR

    for filename, count in (("lci-pass_cars.xlsx", 1), ("lci-trucks.xlsx", 4)):
        data = ExcelImporter(str(INVENTORY_DIR / filename)).data
        services = [d for d in data if SPEC_KEY in d]
        assert len(services) == count
        for ds in services:
            spec = json.loads(ds[SPEC_KEY])
            fuel = [
                e
                for e in ds["exchanges"]
                if e["type"] == "technosphere"
                and e.get("reference product", e.get("product")) == LPG
            ]
            assert len(fuel) == 1 and fuel[0]["unit"] == "kilogram"
            # JSON metadata and Excel numeric cells round-trip independently.
            assert spec["fuel kg per service"] == pytest.approx(
                fuel[0]["amount"], rel=1e-14
            )
            assert spec["energy MJ per service"] == pytest.approx(
                fuel[0]["amount"] * 45.5
            )
            assert spec["proxy powertrain"] == "ICEV-g"
            assert spec["CNG pump-to-tank methane leakage"] == "excluded"
            assert (
                "Operating fuel blends and electricity supplied by Premise regional scenario markets"
                not in ds["comment"]
            )
