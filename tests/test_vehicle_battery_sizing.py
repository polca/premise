"""Constant range, lifetime capacity and regional supplier regressions."""

import json
from copy import deepcopy
from types import SimpleNamespace

import pytest

from premise.inventory_store import compact_exchange_payload
from premise.transport import Transport
from premise.vehicle_battery import (
    SPEC_KEY,
    STATE_KEY,
    battery_state,
    cell_density,
    mass_factors,
    projected_cell_density,
    resize_regional_batteries,
    specification,
)


@pytest.fixture
def spec():
    return {
        "version": 1,
        "baseline year": 2020,
        "family": "car",
        "powertrain": "BEV",
        "chemistry": "NMC-622",
        "capacity kWh": 84.0,
        "pack mass kg": 500.0,
        "cell density kWh/kg": 0.24,
        "cell mass share": 0.7,
        "curb mass kg": 2000.0,
        "driving mass kg": 2200.0,
        "electric range km": 400.0,
        "replacements": 0.25,
        "grid kWh per service": 0.21,
        "depth of discharge": 0.8,
        "charge efficiency": 0.8,
        "charger efficiency": 1.0,
        "wear terms": {
            "road": [[0.8, 2200, 2 / 3], [0.2, 2200, 10 / 11]],
            "brake": [[1.0, 2200, 2 / 3]],
            "tyre": [[1.0, 2200, 10 / 23]],
        },
    }


@pytest.mark.parametrize("factor", [0.8, 1, 1.1])
@pytest.mark.parametrize("density", [0.24, 0.25, 0.26])
def test_range_mass_and_lifetime_energy_balance(spec, factor, density):
    result = battery_state(spec, factor, density, 2050)
    stored_per_km = 0.21 * factor * 0.8
    assert result["capacity kWh"] * 0.8 / stored_per_km == pytest.approx(400)
    assert result["pack mass kg"] * 0.7 * density == pytest.approx(
        result["capacity kWh"]
    )
    assert result["curb mass kg"] - result["pack mass kg"] == pytest.approx(1500)
    assert result["driving mass kg"] - result["curb mass kg"] == pytest.approx(200)
    assert result["lifetime capacity kWh"] == pytest.approx(84 * factor * 1.25)
    # Same range, lifetime distance and electric share imply the same cycle demand.
    assert 200000 * stored_per_km / result["capacity kWh"] == pytest.approx(400)


def test_phev_pack_tracks_electricity_not_total_fuel_energy(spec):
    spec["powertrain"] = "PHEV-p"
    spec["electric share"] = 0.4
    spec["grid kWh per service"] *= 0.4
    future = battery_state(spec, 0.8, 0.26, 2050)
    per_electric_km = spec["grid kWh per service"] * 0.8 / 0.4
    assert future["capacity kWh"] * 0.8 / (per_electric_km * 0.8) == pytest.approx(400)


def test_density_projection_is_the_existing_original_chemistry_series():
    assert cell_density("NMC-622", 2020) == 0.24
    assert cell_density("NMC-622", 2030) == pytest.approx(0.24666666666666667)
    assert cell_density("NMC-622", 2050) == 0.26
    assert cell_density("NMC-622", 2100) == 0.26


@pytest.mark.parametrize(
    "year,density_ratio", [(2020, 1), (2030, 37 / 36), (2050, 13 / 12)]
)
def test_calibrated_phev_retains_pack_density_offset_and_range(
    spec, year, density_ratio
):
    spec.update(
        {
            "powertrain": "PHEV-p",
            "capacity kWh": 12,
            "pack mass kg": 118.5,
            "cell density kWh/kg": 12 / (118.5 * 0.7),
            "density projection basis": "relative chemistry trend",
        }
    )
    density = projected_cell_density(spec, year)
    factor = 1 if year == 2020 else 0.8
    result = battery_state(spec, factor, density, year)
    assert result["capacity kWh"] == pytest.approx(12 * factor)
    assert result["pack mass kg"] == pytest.approx(118.5 * factor / density_ratio)
    assert result["cell mass kg"] * density == pytest.approx(12 * factor)
    assert result["electric range km"] == spec["electric range km"]


def test_legacy_density_projection_and_unknown_basis(spec):
    assert projected_cell_density(spec, 2020) == spec["cell density kWh/kg"]
    assert projected_cell_density(spec, 2050) == 0.26
    spec["density projection basis"] = "misspelled trend"
    with pytest.raises(ValueError, match="Unknown battery density"):
        projected_cell_density(spec, 2050)


@pytest.mark.parametrize("factor", [0, -1, float("nan"), float("inf")])
def test_invalid_energy_target_rejected(spec, factor):
    with pytest.raises(ValueError, match="positive"):
        battery_state(spec, factor, 0.26, 2050)


def test_truck_payload_cannot_be_silently_clipped(spec):
    spec["gross mass kg"] = 2250
    spec["cargo mass kg"] = 125
    spec["powertrain"] = "PHEV-d"
    with pytest.raises(ValueError, match="gross mass"):
        battery_state(spec, 2, 0.24, 2050)


def test_smaller_pack_frees_payload_but_retains_the_actual_load(spec):
    spec["family"] = "truck"
    spec["powertrain"] = "PHEV-d"
    spec["gross mass kg"] = 2250
    spec["cargo mass kg"] = 125
    result = battery_state(spec, 0.8, 0.24, 2050)
    assert result["cargo mass kg"] == 125
    assert result["available payload kg"] == pytest.approx(275)
    assert result["capacity utilization"] == pytest.approx(125 / 275)
    assert result["cell mass kg"] + result["balance of pack mass kg"] == pytest.approx(
        result["pack mass kg"]
    )


@pytest.fixture
def truck_spec(spec):
    spec.update(
        {
            "family": "truck",
            "gross mass kg": 4075,
            "cargo mass kg": 1000,
            "driving mass kg": 3075,
        }
    )
    for terms in spec["wear terms"].values():
        for term in terms:
            term[1] = 3075
    return spec


@pytest.mark.parametrize(
    "factor,cargo,available,driving",
    [(0.8, 1050, 2100, 3025), (1, 1000, 2000, 3075), (1.2, 950, 1900, 3125)],
)
def test_bev_truck_keeps_utilization_when_pack_changes(
    truck_spec, factor, cargo, available, driving
):
    result = battery_state(truck_spec, factor, 0.24, 2050)
    assert result["cargo mass kg"] == pytest.approx(cargo)
    assert result["available payload kg"] == pytest.approx(available)
    assert result["driving mass kg"] == pytest.approx(driving)
    assert result["capacity utilization"] == 0.5
    assert result["payload normalization factor"] == pytest.approx(1000 / cargo)
    assert result["driving mass kg"] - result["curb mass kg"] - cargo == pytest.approx(
        75
    )
    assert mass_factors(truck_spec, result)["brake"] == pytest.approx(
        (driving / 3075) ** (2 / 3)
    )


def test_full_bev_truck_retains_gross_driving_mass(truck_spec):
    truck_spec["cargo mass kg"] = 2000
    truck_spec["driving mass kg"] = 4075
    result = battery_state(truck_spec, 0.8, 0.24, 2050)
    assert result["cargo mass kg"] == 2100
    assert result["capacity utilization"] == 1
    assert result["driving mass kg"] == 4075
    assert mass_factors(truck_spec, result)["brake"] == 1


@pytest.mark.parametrize("cargo", [0, -1, 3100, float("nan"), float("inf")])
def test_invalid_bev_truck_payload_is_rejected(truck_spec, cargo):
    truck_spec["cargo mass kg"] = cargo
    with pytest.raises(ValueError, match="baseline truck payload"):
        battery_state(truck_spec, 0.8, 0.26, 2050)


def test_bev_truck_cannot_sacrifice_all_payload_for_battery(truck_spec):
    with pytest.raises(ValueError, match="no payload"):
        battery_state(truck_spec, 5, 0.24, 2050)


@pytest.mark.parametrize("grid", [0, -1, float("nan"), float("inf")])
def test_invalid_reference_electricity_is_not_a_neutral_signal(spec, grid):
    spec["grid kWh per service"] = grid
    with pytest.raises(ValueError, match="positive reference electricity"):
        specification({SPEC_KEY: spec})


@pytest.mark.parametrize("powertrain", ["HEV-d", "FCEV", "ICEV-d"])
def test_power_buffers_cannot_use_the_range_sizing_rule(spec, powertrain):
    spec["powertrain"] = powertrain
    with pytest.raises(ValueError, match="energy pack"):
        specification({SPEC_KEY: json.dumps(spec)})


def test_precomputed_wear_uses_mode_mass_change(spec):
    spec["wear terms"]["road"] = [[0.4, 2100, 2 / 3], [0.6, 2300, 10 / 11]]
    result = battery_state(spec, 0.8, 0.24, 2050)
    assert mass_factors(spec, result)["road"] == pytest.approx(
        0.4 * (2000 / 2100) ** (2 / 3) + 0.6 * (2200 / 2300) ** (10 / 11)
    )


def exchange(name, product, unit, amount, *, location="RER", type="technosphere"):
    return dict(
        name=name,
        product=product,
        unit=unit,
        amount=amount,
        location=location,
        type=type,
    )


def transformer(spec, *, compact=False, year=2050, factor=0.8, production=1):
    source = {
        "name": "passenger car, battery electric, Medium",
        "reference product": "passenger car",
        "location": "RER",
        "unit": "unit",
        "code": "source",
        "exchanges": [
            exchange("car production", "passenger car", "unit", 1, type="production"),
            exchange(
                "MIX capacity",
                "electricity storage capacity",
                "kilowatt hour",
                105,
                location="GLO",
            ),
            exchange("motor", "electric motor", "kilogram", 25),
        ],
    }
    service = {
        "name": "transport, passenger car, battery electric, Medium",
        "reference product": "transport, passenger car",
        "unit": "kilometer",
        "location": "EUR",
        "regionalized": True,
        SPEC_KEY: json.dumps(spec),
        "exchanges": [
            exchange(
                "service",
                "transport, passenger car",
                "kilometer",
                production,
                type="production",
            ),
            exchange(source["name"], "passenger car", "unit", production / 200000),
            exchange(
                "grid",
                "electricity, low voltage",
                "kilowatt hour",
                0.21 * factor * production,
            ),
            exchange("road", "road", "meter-year", 0.001),
            exchange(
                "wear treatment",
                "road wear emissions, passenger car",
                "kilogram",
                -1e-5,
            ),
            exchange("maintenance", "passenger car maintenance", "unit", 1e-5),
            exchange("NOx", "", "kilogram", 1e-6, type="biosphere"),
        ],
    }
    if spec.get("family") == "truck":
        source.update(
            name="lorry production, battery electric, 3.5t",
            **{"reference product": "lorry"},
        )
        source["exchanges"][0].update(name=source["name"], product="lorry")
        source["exchanges"].append(
            exchange("assembly", "assembly operation, for lorry", "kilogram", 2000)
        )
        service.update(
            name="transport, freight, lorry, battery electric 3.5t gross weight",
            unit="ton kilometer",
            **{"reference product": "transport, freight, lorry"},
        )
        service["exchanges"][0].update(
            product=service["reference product"], unit=service["unit"]
        )
        service["exchanges"][1].update(name=source["name"], product="lorry")
        service["exchanges"][4]["product"] = "road wear emissions, lorry"
        service["exchanges"][5]["product"] = "maintenance, lorry"
    if compact:
        for ds in (source, service):
            ds["exchanges"] = [compact_exchange_payload(e) for e in ds["exchanges"]]
    added = []
    tr = SimpleNamespace(
        year=year,
        vehicle_type=spec.get("family", "car"),
        database=[source, service],
        vehicle_map={"BEV": [service]},
        add_to_index=lambda data: added.extend(data),
    )
    return tr, source, service, added


@pytest.mark.parametrize("compact", [False, True])
@pytest.mark.parametrize("production", [1, 2])
def test_bev_payload_normalizes_all_service_burdens_once(
    truck_spec, compact, production
):
    tr, source, service, added = transformer(
        truck_spec, compact=compact, production=production
    )
    original = deepcopy(source)
    old = [e["amount"] for e in service["exchanges"]]
    service["exchanges"][1].update(
        {"uncertainty type": 4, "minimum": old[1] * 0.9, "maximum": old[1] * 1.1}
    )
    resize_regional_batteries(tr)
    assert source == original
    # Reconstruct pack, driver and load independently from the capacity and density.
    pack = 67.2 / (0.7 * 0.26)
    curb = 1500 + pack
    cargo = (4075 - curb - 75) / 2
    load_ratio = 1000 / cargo
    driving_ratio = (curb + 75 + cargo) / 3075
    state = service[STATE_KEY]
    assert state["cargo mass kg"] == pytest.approx(cargo)
    assert state["capacity utilization"] == 0.5
    assert state["energy factor"] == 0.8
    assert service["exchanges"][0]["amount"] == production
    for index in (1, 2, 5, 6):
        assert service["exchanges"][index]["amount"] == pytest.approx(
            old[index] * load_ratio
        )
    purchase = service["exchanges"][1]
    assert purchase["amount"] / production == pytest.approx(1 / (200000 * cargo / 1000))
    assert purchase["minimum"] == pytest.approx(old[1] * 0.9 * load_ratio)
    assert purchase["maximum"] == pytest.approx(old[1] * 1.1 * load_ratio)
    assert service["exchanges"][3]["amount"] == pytest.approx(
        old[3] * driving_ratio * load_ratio
    )
    wear = 0.8 * driving_ratio ** (2 / 3) + 0.2 * driving_ratio ** (10 / 11)
    assert service["exchanges"][4]["amount"] == pytest.approx(
        old[4] * wear * load_ratio
    )
    per_km = service["exchanges"][2]["amount"] / production * cargo / 1000
    assert per_km == pytest.approx(0.21 * 0.8)
    assert state["capacity kWh"] * 0.8 / (per_km * 0.8) == pytest.approx(400)
    # Per-vehicle manufacture is resized but never divided by freight load.
    assert added[0]["exchanges"][1]["amount"] == pytest.approx(84)
    assert added[0]["exchanges"][2]["amount"] == 25
    assert added[0]["exchanges"][3]["amount"] == pytest.approx(curb)
    once = deepcopy(tr.database)
    resize_regional_batteries(tr)
    assert tr.database == once
    assert len(added) == 1


def test_bev_payload_requires_tkm_service(truck_spec):
    tr, _, service, _ = transformer(truck_spec)
    service["unit"] = "kilometer"
    with pytest.raises(ValueError, match="tonne-kilometres"):
        resize_regional_batteries(tr)


def test_2020_bev_truck_keeps_original_load_and_every_exchange(truck_spec):
    tr, _, service, added = transformer(truck_spec, year=2020, factor=1)
    before = deepcopy([d["exchanges"] for d in tr.database])
    resize_regional_batteries(tr)
    assert [d["exchanges"] for d in tr.database] == before
    assert not added
    assert service[STATE_KEY]["cargo mass kg"] == 1000
    assert service[STATE_KEY]["payload normalization factor"] == 1


@pytest.mark.parametrize("compact", [False, True])
@pytest.mark.parametrize("production", [1, 2])
def test_regional_clone_and_idempotent_mass_updates(spec, compact, production):
    tr, source, service, added = transformer(
        spec, compact=compact, production=production
    )
    original = deepcopy(source)
    resize_regional_batteries(tr)
    assert source == original
    assert len(added) == 1
    target = added[0]
    assert target["location"] == service["exchanges"][1]["location"] == "EUR"
    assert target["exchanges"][1]["location"] == "GLO"
    assert target["exchanges"][1]["amount"] == pytest.approx(84)
    assert target["exchanges"][2]["amount"] == 25
    assert service["exchanges"][1]["amount"] == production / 200000
    assert service["exchanges"][2]["amount"] == 0.21 * 0.8 * production
    assert service["exchanges"][6]["amount"] == 1e-6
    driving_ratio = (1700 + 84 * 0.8 / 0.7 / 0.26) / 2200
    assert service["exchanges"][3]["amount"] == pytest.approx(0.001 * driving_ratio)
    expected_wear = 0.8 * driving_ratio ** (2 / 3) + 0.2 * driving_ratio ** (10 / 11)
    assert service["exchanges"][4]["amount"] == pytest.approx(-1e-5 * expected_wear)
    once = deepcopy(tr.database)
    resize_regional_batteries(tr)
    assert tr.database == once
    assert len(added) == 1


def test_two_regions_have_independent_purchases(spec):
    tr, source, eur, added = transformer(spec)
    usa = deepcopy(eur)
    usa["location"] = "USA"
    usa["exchanges"][2]["amount"] = 0.21 * 0.9
    tr.database.append(usa)
    tr.vehicle_map["BEV"].append(usa)
    resize_regional_batteries(tr)
    assert len(added) == 2
    assert {
        d["location"]: d[STATE_KEY]["capacity kWh"] for d in added
    } == pytest.approx({"EUR": 67.2, "USA": 75.6})


def test_2020_retains_every_exchange_and_provider(spec):
    tr, source, service, added = transformer(spec, year=2020, factor=1)
    before = deepcopy([d["exchanges"] for d in tr.database])
    resize_regional_batteries(tr)
    assert not added
    assert [d["exchanges"] for d in tr.database] == before
    assert service[STATE_KEY]["capacity kWh"] == 84


def test_unmarked_legacy_and_unregionalized_templates_are_untouched(spec):
    tr, source, service, added = transformer(spec)
    service.pop(SPEC_KEY)
    before = deepcopy(tr.database)
    resize_regional_batteries(tr)
    assert tr.database == before
    service[SPEC_KEY] = json.dumps(spec)
    service["regionalized"] = False
    before = deepcopy(tr.database)
    resize_regional_batteries(tr)
    assert tr.database == before


def test_legacy_truck_capacity_override_cannot_overwrite_the_new_contract(spec):
    ds = {SPEC_KEY: json.dumps(spec)}
    tr = object.__new__(Transport)
    tr.adjust_battery_size(ds)
    assert ds == {SPEC_KEY: json.dumps(spec)}


@pytest.mark.parametrize("family,expected", [("pass_cars", 25), ("trucks", 14)])
def test_installed_metadata_survives_excel_import_and_matches_purchases(
    family, expected
):
    from bw2io import ExcelImporter
    from premise.filesystem_constants import INVENTORY_DIR

    datasets = ExcelImporter(str(INVENTORY_DIR / f"lci-{family}.xlsx")).data
    services = [
        d for d in datasets if d["name"].startswith("transport,") and SPEC_KEY in d
    ]
    assert len(services) == expected
    by_name = {d["name"]: d for d in datasets}
    for service in services:
        baseline = specification(service)
        assert baseline["baseline year"] == 2020
        assert baseline["capacity kWh"] == pytest.approx(
            baseline["pack mass kg"]
            * baseline["cell mass share"]
            * baseline["cell density kWh/kg"],
            rel=2e-6,
        )
        stored_per_km = baseline["stored kWh per electric km"]
        assert baseline["capacity kWh"] * baseline[
            "depth of discharge"
        ] / stored_per_km == pytest.approx(baseline["electric range km"])
        for terms in baseline["wear terms"].values():
            assert sum(t[0] for t in terms) == pytest.approx(1)
            assert all(min(t) > 0 for t in terms)
        vehicles = [
            e
            for e in service["exchanges"]
            if e["type"] == "technosphere"
            and e["unit"] == "unit"
            and e["name"].lower().startswith(("passenger car,", "lorry production,"))
        ]
        assert len(vehicles) == 1
        provider = by_name[vehicles[0]["name"]]
        battery = [
            e
            for e in provider["exchanges"]
            if e["name"] == "market for battery capacity (MIX scenario)"
        ]
        assert len(battery) == 1
        assert battery[0]["amount"] == pytest.approx(
            baseline["capacity kWh"] * (1 + baseline["replacements"]), rel=2e-6
        )


def test_inconsistent_production_capacity_fails(spec):
    tr, source, _, _ = transformer(spec)
    source["exchanges"][1]["amount"] = 50
    with pytest.raises(ValueError, match="baseline metadata"):
        resize_regional_batteries(tr)


def test_different_designs_cannot_share_one_regional_producer(spec):
    tr, _, first, _ = transformer(spec)
    other = deepcopy(first)
    other["name"] += ", other duty"
    other["exchanges"][2]["amount"] *= 0.9
    tr.vehicle_map["BEV"].append(other)
    with pytest.raises(ValueError, match="Conflicting regional"):
        resize_regional_batteries(tr)
