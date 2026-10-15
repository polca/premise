"""Physical invariants, independent of the workbook-regeneration implementation."""

from copy import deepcopy
import pytest

from premise import vehicle_energy_use as ve


def exc(name, amount, unit, product=None, kind="technosphere"):
    return dict(
        name=name,
        amount=amount,
        unit=unit,
        product=product or name,
        location="RER",
        type=kind,
    )


def model(unit, service_per_km):
    vehicle = dict(
        name="Passenger car, battery electric, Medium",
        **{"reference product": "car"},
        location="RER",
        unit="unit",
        exchanges=[
            exc(
                "market for battery capacity (MIX scenario)",
                60,
                "kilowatt hour",
                "electricity storage capacity",
            ),
            exc("market for electric motor", 90, "kilogram"),
            exc("market for glider", 1000, "kilogram"),
            exc("market for used powertrain", -100, "kilogram"),
        ],
    )
    source = dict(
        name="transport",
        **{"reference product": "transport"},
        location="RER",
        unit=unit,
        exchanges=[
            exc("transport", 1, unit, kind="production"),
            exc(
                "market for electricity",
                0.2 / service_per_km,
                "kilowatt hour",
                "electricity, low voltage",
            ),
            exc(vehicle["name"], 1 / (200000 * service_per_km), "unit", "car"),
            exc("market for road", 0.001 / service_per_km, "meter-year"),
        ],
    )
    wrapper = dict(
        name="electricity, used in passenger car",
        **{"reference product": "electricity, low voltage"},
        unit="megajoule",
        location="RER",
        exchanges=[exc("old", 1, "megajoule", kind="production")],
    )
    return wrapper, source, vehicle


@pytest.mark.parametrize(
    "unit,service", [("kilometer", 1), ("person kilometer", 10), ("ton kilometer", 20)]
)
def test_service_units_cancel_and_boundary_is_preserved(unit, service):
    wrapper, source, vehicle = model(unit, service)
    original = deepcopy(source)
    record = ve.normalize(wrapper, source, [source, vehicle])
    values = {e["name"]: e["amount"] for e in wrapper["exchanges"]}
    assert values["market for electricity"] == pytest.approx(1 / 3.6)
    assert values["market for battery capacity (MIX scenario)"] == pytest.approx(
        60 / (200000 * 0.72)
    )
    assert values["market for electric motor"] == pytest.approx(90 / (200000 * 0.72))
    assert values["market for used powertrain"] == pytest.approx(-100 / (200000 * 0.72))
    assert "market for road" not in values and "market for glider" not in values
    assert source == original
    assert record["purchased MJ per source service"] == pytest.approx(0.72 / service)


def test_hydrogen_compression_is_not_in_traction_denominator():
    wrapper, source, vehicle = model("kilometer", 1)
    wrapper["name"] = "hydrogen, used in passenger car"
    source["exchanges"].append(
        exc("market for hydrogen", 0.01, "kilogram", "hydrogen, gaseous, low pressure")
    )
    record = ve.normalize(wrapper, source, [source, vehicle])
    assert record["purchased MJ per source service"] == pytest.approx(1.2)
    assert next(
        e["amount"]
        for e in wrapper["exchanges"]
        if e["type"] == "technosphere" and e["product"] == "electricity, low voltage"
    ) == pytest.approx(0.2 / 1.2)


def test_missing_energy_and_ambiguous_supplier_fail():
    wrapper, source, vehicle = model("kilometer", 1)
    source["exchanges"][1]["amount"] = 0
    with pytest.raises(ValueError, match="positive"):
        ve.normalize(wrapper, source, [source, vehicle])
    with pytest.raises(ValueError, match="Ambiguous"):
        ve.select_source([source, source], source["name"], "RER")


def test_repeated_normalization_does_not_double_equipment():
    wrapper, source, vehicle = model("kilometer", 1)
    ve.normalize(wrapper, source, [source, vehicle])
    first = deepcopy(wrapper)
    ve.normalize(wrapper, source, [source, vehicle])
    assert wrapper == first


@pytest.mark.parametrize(
    "stored,expected_factor", [(8.100000381, 2), (16.200000762, 1)]
)
def test_motorcycle_replacement_tolerates_float32_without_double_counting(
    stored, expected_factor
):
    wrapper, source, vehicle = model("kilometer", 1)
    source["name"] = "transport, Motorbike, battery electric, 11-35kW"
    source["comment"] = (
        "Battery capacity: 8.1 kWh; Battery replacement throughout lifetime: 1"
    )
    vehicle["exchanges"][0]["amount"] = stored
    record = ve.normalize(wrapper, source, [source, vehicle])
    pack = record["retained equipment"][0]
    assert pack["replacement correction factor"] == expected_factor
    assert pack["amount per MJ"] == pytest.approx(16.2 / (200000 * 0.72))


def test_final_emissions_refresh_preserves_scenario_fuel_and_links(monkeypatch):
    import premise.inventory_store as store

    wrapper, source, vehicle = model("kilometer", 1)
    nox = dict(
        name="Nitrogen oxides",
        amount=0.004,
        type="biosphere",
        categories=("air", "urban air close to ground"),
        input=("bio", "nox"),
    )
    source["exchanges"].append(nox)
    ve.normalize(wrapper, source, [source, vehicle])
    co2 = dict(
        name="Carbon dioxide, fossil",
        amount=0.05,
        type="biosphere",
        categories=("air",),
        input=("bio", "co2"),
    )
    wrapper["exchanges"].append(co2)
    wrapper["exchanges"][1]["name"] = "Swiss SPS electricity"
    nox["amount"] = 0.001  # Final source after GAINS, four times lower.
    scenario = {"database": [wrapper, source, vehicle]}
    monkeypatch.setattr(store, "get_scenario_inventory", lambda s: s["database"])
    monkeypatch.setattr(
        store, "replace_scenario_inventory", lambda s, d: s.update(database=d)
    )
    ve.refresh_vehicle_energy_emissions(scenario)
    values = {e["name"]: e for e in wrapper["exchanges"]}
    assert values["Nitrogen oxides"]["amount"] == pytest.approx(0.001 / 0.72)
    assert values["Nitrogen oxides"]["input"] == ("bio", "nox")
    assert values["Carbon dioxide, fossil"] == co2
    assert "Swiss SPS electricity" in values
    first = deepcopy(wrapper)
    ve.refresh_vehicle_energy_emissions(scenario)
    assert wrapper == first


def test_vehicle_inventory_change_invalidates_import_cache_only(tmp_path, monkeypatch):
    import premise.new_database as module

    builder = module.NewDatabase.__new__(module.NewDatabase)
    builder.keep_imports_uncertainty = builder.keep_source_db_uncertainty = False
    path = tmp_path / "vehicle.xlsx"
    path.write_bytes(b"first inventory")
    monkeypatch.setattr(module, "FILEPATH_PASS_CARS", path)
    original = builder._database_cache_path("test", inventories=True)
    source = builder._database_cache_path("test")
    path.write_bytes(b"updated inventory")
    assert builder._database_cache_path("test", inventories=True) != original
    assert builder._database_cache_path("test") == source


@pytest.mark.parametrize(
    "supplier,product,lhv",
    [
        ("ethanol production", "ethanol", 26.5),
        ("biodiesel production, via transesterification", "biodiesel", 38),
        ("biodiesel production, via Fischer-Tropsch", "biodiesel", 43),
        ("methane, from electrochemical methanation", "methane, synthetic", 47.5),
    ],
)
def test_fuel_specific_heating_values(supplier, product, lhv):
    assert ve.energy_per_unit(exc(supplier, 1, "kilogram", product)) == lhv


def test_alternative_ethanol_is_one_MJ_and_idempotent():
    wrapper, source, vehicle = model("kilometer", 1)
    wrapper["name"] = "bioethanol, burned in passenger car"
    wrapper["exchanges"].append(
        exc("ethanol production", 1 / 42.6, "kilogram", "ethanol")
    )
    wrapper["exchanges"].append(
        dict(
            name="Carbon dioxide, non-fossil",
            amount=0.074,
            type="biosphere",
            categories=("air",),
        )
    )
    source["exchanges"][1] = exc(
        "market for petrol", 0.05, "kilogram", "petrol, low-sulfur"
    )
    ve.normalize(wrapper, source, [source, vehicle])
    quantities = {e["name"]: e["amount"] for e in wrapper["exchanges"]}
    assert quantities["ethanol production"] == pytest.approx(1 / 26.5)
    assert quantities["Carbon dioxide, non-fossil"] == pytest.approx(0.0739)
    first = deepcopy(wrapper)
    ve.normalize(wrapper, source, [source, vehicle])
    assert wrapper == first


def test_hydrogen_tank_is_equipment_not_an_energy_purchase():
    tank = exc("fuel tank, compressed hydrogen gas", 1, "kilogram", "hydrogen tank")
    assert ve.carrier(tank) is None
    assert ve.equipment(tank)
