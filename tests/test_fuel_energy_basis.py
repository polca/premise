"""Independent delivered-energy and carbon-origin regression expectations."""

from copy import deepcopy
from types import SimpleNamespace

import pytest
import xarray as xr

from premise.car_energy import fuel_spec
from premise.fuels.base import Fuels
from premise.fuels.properties import (
    PROPERTIES_KEY,
    CarbonOriginResolver,
    energy_per_unit,
    supplied_energy,
    summarize_components,
)
from premise.transformation import BaseTransformation
from premise.utils import get_fuel_properties
from premise.validation import FuelsValidation


def activity(name, product="fuel", unit="kilogram", inputs=(), **metadata):
    return dict(
        name=name,
        **{"reference product": product},
        location="EUR",
        unit=unit,
        exchanges=[
            dict(
                name=name,
                product=product,
                location="EUR",
                unit=unit,
                type="production",
                amount=1,
            ),
            *deepcopy(list(inputs)),
        ],
        **metadata,
    )


def purchase(supplier, amount=1):
    return {
        "name": supplier["name"],
        "product": supplier["reference product"],
        "unit": supplier["unit"],
        "location": supplier["location"],
        "type": "technosphere",
        "amount": amount,
    }


def build_market(
    technologies,
    shares,
    reference,
    unit="kilogram",
    blacklist=None,
    system_model="cutoff",
    ancillary=(),
):
    mapping = {
        technology: [
            activity(
                technology,
                unit=("cubic meter" if technology == "natural gas" else "kilogram"),
            )
        ]
        for technology in technologies
    }
    fuels = object.__new__(Fuels)
    BaseTransformation.__init__(
        fuels,
        database=[d for items in mapping.values() for d in items],
        iam_data=SimpleNamespace(regions=["EUR", "World"]),
        model="remind",
        pathway="SSP2-NPi",
        year=2050,
        version="3.12",
        system_model=system_model,
    )
    fuels.write_log = lambda *args, **kwargs: None
    fuels.extract_market_ancillary_exchanges = lambda **kwargs: {"GLO": list(ancillary)}
    volumes = xr.DataArray(
        [[[10 * share]] for share in shares],
        dims=("variables", "region", "year"),
        coords={"variables": technologies, "region": ["EUR"], "year": [2050]},
    )
    fuels.process_and_add_fuel_markets(
        reference_fuel=reference,
        name="market for test fuel",
        reference_product="test fuel",
        unit=unit,
        mapping=mapping,
        production_volumes=volumes,
        system_model=system_model,
        blacklist=blacklist,
        flip_treatment_supplier_sign=True,
    )
    fuels.clear_validation_provenance()
    return [ds for ds in fuels.database if PROPERTIES_KEY in ds]


@pytest.mark.parametrize(
    "technologies,shares,reference,unit,expected",
    [
        (["gasoline"], [1], "gasoline", "kilogram", [1]),
        (
            ["gasoline", "bioethanol, from sugar"],
            [0.7, 0.3],
            "gasoline",
            "kilogram",
            [0.7, 0.3 * 42.6 / 26.5],
        ),
        (
            ["diesel", "biodiesel, from used cooking oil, with CCS"],
            [0.5, 0.5],
            "diesel",
            "kilogram",
            [0.5, 0.5 * 43 / 38],
        ),
        (
            ["natural gas", "methane, from biomass"],
            [0.25, 0.75],
            "natural gas",
            "cubic meter",
            [0.25, 0.75 * 36 / 47.5],
        ),
        (["methane, from biomass"], [1], "natural gas", "cubic meter", [36 / 47.5]),
    ],
)
def test_market_preserves_reference_energy_and_world_output(
    technologies, shares, reference, unit, expected
):
    markets = build_market(technologies, shares, reference, unit)
    regional = next(d for d in markets if d["location"] == "EUR")
    inputs = [e for e in regional["exchanges"] if e["type"] == "technosphere"]
    assert [e["amount"] for e in inputs] == pytest.approx(expected)
    for market in markets:
        properties = market[PROPERTIES_KEY]
        assert supplied_energy(market) == pytest.approx(
            properties["reference energy MJ per unit"]
        )
        assert properties["unresolved CO2 kg per unit"] == 0
        assert all("premise market technology" not in e for e in market["exchanges"])


def test_blacklist_renormalizes_energy_before_quantity_conversion():
    market = build_market(
        ["natural gas", "methane, from biomass"],
        [0.2, 0.8],
        "natural gas",
        "cubic meter",
        {"consequential": ["methane, from biomass"]},
        "consequential",
    )[0]
    assert supplied_energy(market) == pytest.approx(36)
    inputs = [e for e in market["exchanges"] if e["type"] == "technosphere"]
    assert len(inputs) == 1 and inputs[0]["amount"] == 1


def test_fuel_market_prefers_direct_supplier_over_recursive_source_market():
    technology = "liquefied petroleum gas"
    recursive = activity("market for liquefied petroleum gas", technology)
    direct = activity("liquefied petroleum gas production", technology)
    mapping = {technology: [recursive, direct]}
    fuels = object.__new__(Fuels)
    BaseTransformation.__init__(
        fuels,
        database=[recursive, direct],
        iam_data=SimpleNamespace(regions=["EUR", "World"]),
        model="remind",
        pathway="SSP2-NPi",
        year=2050,
        version="3.12",
        system_model="cutoff",
    )
    fuels.write_log = lambda *args, **kwargs: None
    volumes = xr.DataArray(
        [[[1.0]]],
        dims=("variables", "region", "year"),
        coords={"variables": [technology], "region": ["EUR"], "year": [2050]},
    )

    fuels.process_and_add_fuel_markets(
        reference_fuel=technology,
        name=recursive["name"],
        reference_product=technology,
        unit="kilogram",
        mapping=mapping,
        production_volumes=volumes,
        system_model="cutoff",
    )

    regional = next(
        dataset
        for dataset in fuels.database
        if dataset.get("regionalized") and dataset["location"] == "EUR"
    )
    suppliers = [
        exchange
        for exchange in regional["exchanges"]
        if exchange["type"] == "technosphere"
    ]
    assert [(exchange["name"], exchange["amount"]) for exchange in suppliers] == [
        (direct["name"], pytest.approx(1.0))
    ]
    assert supplied_energy(regional) == pytest.approx(45.5)


def test_process_energy_is_additional_and_carbon_is_carbon_weighted():
    electricity = purchase(activity("electricity", unit="kilowatt hour"), 0.7)
    market = build_market(
        ["gasoline", "bioethanol, from sugar"],
        [0.5, 0.5],
        "gasoline",
        ancillary=[electricity],
    )[0]
    props = market[PROPERTIES_KEY]
    assert supplied_energy(market) == pytest.approx(42.6)
    assert props["fossil CO2 kg per unit"] == pytest.approx(21.3 * 0.0737)
    assert props["non-fossil CO2 kg per unit"] == pytest.approx(21.3 * 0.0739)
    assert electricity in market["exchanges"]
    # Catch a regressed quantity normalization even if the ledger was not updated.
    for e in market["exchanges"]:
        if e.get("name") == "bioethanol, from sugar":
            e["amount"] = 0.5
    validator = object.__new__(FuelsValidation)
    validator.database, validator.regions = [market], ["EUR"]
    issues = []
    validator.log_issue = lambda *args, **kwargs: issues.append(args)
    validator.check_fuel_market_composition()
    assert len(issues) == 1
    assert issues[0][1] == "fuel market energy does not match reference"


@pytest.mark.parametrize(
    "name,share,metadata",
    [
        ("carbon dioxide, direct air capture", 1, {}),
        (
            "carbon dioxide, captured at cement production",
            0.06,
            {"share biogenic co2": 0.06},
        ),
        (
            "carbon dioxide, captured at municipal waste incinerator",
            0.613636363636,
            {"share biogenic co2": 0.613636363636},
        ),
        ("carbon dioxide, unknown capture", None, {}),
    ],
)
def test_synthetic_fuel_traces_carbon_source_not_hydrogen(name, share, metadata):
    carbon = activity(name, "carbon dioxide", **metadata)
    hydrogen = activity("hydrogen from electrolysis", "hydrogen")
    fuel = activity(
        "synthetic methane",
        "methane",
        inputs=[purchase(carbon, 2.75), purchase(hydrogen, 0.5)],
    )
    resolver = CarbonOriginResolver([carbon, hydrogen, fuel])
    fraction, _ = resolver.fuel_origin(
        "methane, synthetic", fuel, get_fuel_properties()
    )
    assert fraction == pytest.approx(share) if share is not None else fraction is None


def test_coal_hydrogen_methane_contains_fossil_carbon():
    intermediate = activity(
        "methane, from coal-based hydrogen",
        "methane",
        inputs=[
            {"name": "Carbon dioxide, fossil", "amount": -2.768, "type": "biosphere"}
        ],
    )
    fuel = activity(
        "methane at fuelling station", "methane", inputs=[purchase(intermediate, 1.02)]
    )
    assert (
        CarbonOriginResolver([intermediate, fuel]).fuel_origin(
            "methane, synthetic", fuel, get_fuel_properties()
        )[0]
        == 0
    )


def test_electrolysis_without_carbon_source_is_unresolved():
    fuel = activity("synthetic diesel from electrolysis", "diesel")
    assert (
        CarbonOriginResolver([fuel]).fuel_origin(
            "diesel, synthetic, from electrolysis", fuel, get_fuel_properties()
        )[0]
        is None
    )


def test_mixed_capture_carbon_and_unknown_component_are_not_lost():
    fossil = activity(
        "fossil captured carbon", "carbon dioxide", **{"share biogenic co2": 0}
    )
    air = activity("direct air capture", "carbon dioxide")
    fuel = activity(
        "synthetic methane", "methane", inputs=[purchase(fossil, 1), purchase(air, 3)]
    )
    assert CarbonOriginResolver([fossil, air, fuel]).trace(fuel)[0] == 0.75
    component = dict(
        name="unknown methane",
        amount=1,
        energy_MJ_per_unit=47.5,
        CO2_kg_per_MJ=0.06,
        non_fossil_share=None,
    )
    props = summarize_components([component], 47.5, "kilogram")
    assert props["unresolved CO2 kg per unit"] == pytest.approx(2.85)
    assert props["fossil CO2 kg per unit"] == props["non-fossil CO2 kg per unit"] == 0
    component["name"], component["amount"] = "treatment of waste fuel", -1
    assert (
        summarize_components([component], 47.5, "kilogram")["energy MJ per unit"]
        == 47.5
    )
    component["name"] = "ordinary fuel"
    with pytest.raises(ValueError, match="waste-treatment"):
        summarize_components([component], 47.5, "kilogram")


def test_explicit_energy_units_and_delivered_cng_basis():
    properties = get_fuel_properties()
    assert energy_per_unit("natural gas", "cubic meter", properties) == 36
    with pytest.raises(ValueError, match="requires"):
        energy_per_unit("natural gas", "kilogram", properties)
    with pytest.raises(ValueError, match="Missing"):
        energy_per_unit("unknown", "kilogram", properties)
    assert (
        fuel_spec("market for natural gas, medium pressure, vehicle grade", "kilogram")[
            0
        ]
        == 46.5
    )
    for technology in (
        "methane, from coal",
        "kerosene, synthetic, from natural gas, energy allocation",
        "kerosene, synthetic, from natural gas, energy allocation, with CCS",
    ):
        assert properties[technology]["biogenic_share"] == 0


def test_consequential_validator_compares_energy_shares(monkeypatch):
    import premise.validation as validation

    markets = build_market(
        ["gasoline", "bioethanol, from sugar"], [0.2, 0.8], "gasoline"
    )
    market = next(ds for ds in markets if ds["location"] == "EUR")
    market["name"] = "market for petrol"
    blend = xr.DataArray(
        [[0.2, 0.8]],
        dims=("region", "variables"),
        coords={"region": ["EUR"], "variables": ["gasoline", "bioethanol, from sugar"]},
    )
    monkeypatch.setattr(
        validation,
        "independent_consequential_mix",
        lambda iam, sector, year: blend if sector == "petrol" else None,
    )
    validator = object.__new__(FuelsValidation)
    validator.system_model, validator.year = "consequential", 2050
    validator.database, validator.regions, validator.iam_data = [market], ["EUR"], None
    validator.technology_map = {k: [activity(k)] for k in blend.variables.values}
    issues = []
    validator.log_issue = lambda *args, **kwargs: issues.append(args)
    validator.check_consequential_fuel_supplier_vectors()
    assert issues == []
    for e in market["exchanges"]:
        if e["name"] == "bioethanol, from sugar":
            e["amount"] = 0.8
    validator.check_consequential_fuel_supplier_vectors()
    assert any(i[1] == "incorrect consequential fuel supplier vector" for i in issues)


@pytest.mark.parametrize("stem,count", [("lci-pass_cars", 8), ("lci-trucks", 7)])
def test_installed_cng_vehicle_purchase_preserves_recorded_model_energy(stem, count):
    import csv
    from pathlib import Path
    import openpyxl

    root = Path(__file__).resolve().parents[1] / "premise/data/additional_inventories"
    with (root / f"{stem}.parameters.csv").open() as stream:
        parameters = {row["name"]: row for row in csv.DictReader(stream)}
    workbook = openpyxl.load_workbook(
        root / f"{stem}.xlsx", read_only=True, data_only=True
    )
    matched = 0
    vehicle = None
    fields = None
    try:
        for row in workbook.active.iter_rows(values_only=True):
            if row[0] == "Activity":
                vehicle, fields = row[1], None
            elif row[0] == "name":
                fields = row
            elif fields and vehicle.startswith("transport,"):
                exchange = dict(zip(fields, row))
                if (
                    exchange.get("name")
                    == "market for natural gas, medium pressure, vehicle grade"
                ):
                    key = (
                        "fuel_energy_MJ_per_vkm"
                        if stem == "lci-pass_cars"
                        else "fuel_energy_MJ_per_tkm"
                    )
                    assert float(exchange["amount"]) * 46.5 == pytest.approx(
                        float(parameters[vehicle][key]), rel=1e-10
                    )
                    assert "46.5 MJ/kg" in exchange["comment"]
                    matched += 1
    finally:
        workbook.close()
    assert matched == count


def test_cng_records_delivered_and_upstream_energy_as_distinct_boundaries():
    from premise.fuels.properties import annotate_cng_properties

    gas = activity(
        "market for natural gas, high pressure",
        "natural gas, high pressure",
        "cubic meter",
    )
    gas[PROPERTIES_KEY] = {
        "energy MJ per unit": 36,
        "fossil CO2 kg per unit": 36 * 0.06 * 0.7,
        "non-fossil CO2 kg per unit": 36 * 0.06 * 0.3,
        "unresolved CO2 kg per unit": 0,
    }
    product = "natural gas, medium pressure, vehicle grade"
    compression = activity(
        "natural gas production, medium pressure, vehicle grade",
        product,
        inputs=[
            purchase(gas, 1.3605442176870748),
            purchase(activity("electricity", unit="kilowatt hour"), 0.361),
        ],
    )
    market = activity(
        "market for natural gas, medium pressure, vehicle grade",
        product,
        inputs=[purchase(compression)],
    )
    before = deepcopy(compression["exchanges"])
    database = [gas, compression, market]
    annotate_cng_properties(database)
    properties = compression[PROPERTIES_KEY]
    assert properties["energy MJ per unit"] == 46.5
    assert properties["upstream fuel energy MJ per unit"] == pytest.approx(
        48.97959183673469
    )
    assert properties["upstream minus delivered energy MJ per unit"] == pytest.approx(
        2.47959183673469
    )
    assert supplied_energy(compression) == pytest.approx(48.97959183673469)
    assert compression["exchanges"] == before
    assert market[PROPERTIES_KEY]["fossil CO2 kg per unit"] == pytest.approx(2.79 * 0.7)
    assert market[PROPERTIES_KEY]["non-fossil CO2 kg per unit"] == pytest.approx(
        2.79 * 0.3
    )
    assert supplied_energy(market) == 46.5
    updated = deepcopy(database)
    annotate_cng_properties(database)
    assert database == updated


def test_carbon_trace_excludes_infrastructure_and_handles_atmospheric_not_biogenic_carbon():
    carbon = activity(
        "carbon dioxide, direct air capture",
        "carbon dioxide",
        **{"share biogenic co2": 0},
    )
    plant = activity(
        "methanol production facility, construction",
        "methanol production facility",
        "unit",
    )
    methanol = activity(
        "methanol production",
        "methanol",
        inputs=[purchase(carbon, 1.37), purchase(plant, 1e-8)],
    )
    fuel = activity("diesel from methanol", "diesel", inputs=[purchase(methanol, 2.24)])
    assert CarbonOriginResolver([carbon, plant, methanol, fuel]).trace(fuel)[0] == 1


def test_cng_properties_are_built_after_final_fuel_relinking(monkeypatch):
    import premise.fuels.base as module

    events = []
    fake = SimpleNamespace(database=[], fuel_map={}, cache={}, index={})
    for name in (
        "generate_hydrogen_activities",
        "generate_synthetic_fuel_activities",
        "generate_biogas_activities",
        "clear_validation_provenance",
        "relink_datasets",
    ):
        setattr(fake, name, lambda n=name: events.append(n))
    monkeypatch.setattr(module, "Fuels", lambda **kwargs: fake)
    monkeypatch.setattr(
        module, "annotate_cng_properties", lambda data: events.append("annotate")
    )
    monkeypatch.setattr(module, "get_scenario_inventory", lambda scenario: [])
    monkeypatch.setattr(module, "replace_scenario_inventory", lambda *args: None)
    monkeypatch.setattr(module, "record_validation_phase", lambda *args: None)
    monkeypatch.setattr(
        module,
        "FuelsValidation",
        lambda **kwargs: SimpleNamespace(run_fuel_checks=lambda **kwargs: None),
    )
    scenario = {
        "iam data": SimpleNamespace(
            petrol_blend=1,
            diesel_blend=None,
            natural_gas_blend=None,
            hydrogen_blend=None,
            regions=["EUR"],
        ),
        "model": "remind",
        "pathway": "SSP2-NPi",
        "year": 2050,
    }
    module._update_fuels(scenario, "3.12", "cutoff")
    assert events[-2:] == ["relink_datasets", "annotate"]
