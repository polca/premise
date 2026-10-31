"""Opt-in static pack purchases become lifetime capacity before regional sizing."""

import json
import math
import pickle
from copy import deepcopy
from types import SimpleNamespace

import numpy as np
import pytest
from wurst import searching as ws

import premise.final_energy as final_energy_module
import premise.transport as transport_module
import premise.utils as utils_module
from premise.battery_mass import (
    LEGACY_MASS_PROVIDER,
    MASS_PROVIDER,
    RETIREMENT_PROVIDER,
    SPEC_KEY,
    STATE_KEY,
    convert_battery_mass_inputs,
)
from premise.inventory_normalization import normalize_inventory_numeric_types
from premise.inventory_store import (
    CompactInventoryStore,
    IndexedInventoryList,
    InventoryStore,
    get_scenario_inventory,
)
from premise.transport import Transport
from premise.vehicle_energy_use import SOURCE_KEY, source_name

CAPACITY_PROVIDER = {
    "name": "market for battery capacity, Li-ion, NMC622",
    "reference product": "electricity storage capacity",
    "location": "GLO",
    "unit": "kilowatt hour",
}
MIX_CAPACITY_PROVIDER = {
    **CAPACITY_PROVIDER,
    "name": "market for battery capacity, Li-ion, MIX",
}
HIGH_POWER_PROVIDERS = {
    "car": {
        "name": "market for electricity storage capacity, battery, high power, fuel cell passenger car",
        "reference product": "electricity storage capacity, high power, fuel cell passenger car",
        "location": "GLO",
        "unit": "kilowatt hour",
    },
    "truck": {
        "name": "market for electricity storage capacity, battery, high power, fuel cell truck",
        "reference product": "electricity storage capacity, high power, fuel cell truck",
        "location": "GLO",
        "unit": "kilowatt hour",
    },
}
MASS_KG = 812.5
# Two lifetime pack purchases, rather than just one nominal 76.875 kWh pack.
LIFETIME_CAPACITY_KWH = 153.75


def exchange(provider, amount, **extra):
    result = deepcopy(provider)
    result["product"] = result.pop("reference product")
    result.update(type="technosphere", amount=amount)
    result.update(extra)
    return result


def provider(identity, code):
    return {
        **deepcopy(identity),
        "database": "synthetic",
        "code": code,
        "exchanges": [exchange(identity, 1, type="production")],
    }


def vehicle(name="synthetic battery electric car", family="car"):
    identity = {
        "name": name,
        "reference product": "vehicle",
        "unit": "unit",
        "location": "RER",
    }
    return {
        **identity,
        "database": "additional inventories",
        "code": name,
        SPEC_KEY: {
            "version": 1,
            "vehicle family": family,
            "baseline mass kg": MASS_KG,
            "baseline capacity kWh": LIFETIME_CAPACITY_KWH,
            "retirement amount kg": -MASS_KG,
            "capacity supplier": deepcopy(CAPACITY_PROVIDER),
            "capacity supplier includes end of life": True,
        },
        "exchanges": [
            exchange(identity, 1, type="production"),
            exchange(
                {
                    "name": "market for steel",
                    "reference product": "steel",
                    "location": "RER",
                    "unit": "kilogram",
                },
                910,
            ),
            exchange(
                MASS_PROVIDER,
                MASS_KG,
                input=("static", "old-pack"),
                output=("static", "old-vehicle"),
                database="static",
                code="old-pack",
                comment="Static complete-pack proxy; original source retained.",
                custom={"source": ["synthetic baseline"]},
            ),
            exchange(RETIREMENT_PROVIDER, -MASS_KG),
        ],
    }


@pytest.fixture
def database():
    return [
        vehicle(),
        provider(CAPACITY_PROVIDER, "capacity"),
        provider(MASS_PROVIDER, "mass"),
        provider(RETIREMENT_PROVIDER, "retirement"),
    ]


def capacity_purchase(dataset):
    purchases = [
        e
        for e in dataset["exchanges"]
        if e["type"] == "technosphere"
        and e.get("product") == "electricity storage capacity"
    ]
    assert len(purchases) == 1
    return purchases[0]


def assert_converted(dataset, amount=LIFETIME_CAPACITY_KWH):
    purchase = capacity_purchase(dataset)
    assert purchase["amount"] == pytest.approx(amount, rel=1e-12)
    assert purchase["unit"] == "kilowatt hour"
    assert purchase["name"] == CAPACITY_PROVIDER["name"]
    assert purchase["location"] == CAPACITY_PROVIDER["location"]
    assert not any(
        e["type"] == "technosphere"
        and e.get("name") in (MASS_PROVIDER["name"], RETIREMENT_PROVIDER["name"])
        and e.get("location") == "GLO"
        for e in dataset["exchanges"]
    )
    assert STATE_KEY in dataset
    return purchase


def assert_rejected_atomically(database, family="car"):
    # Byte snapshots also compare NaN-containing inputs without NaN != NaN noise.
    before = pickle.dumps(database)
    with pytest.raises(ValueError):
        convert_battery_mass_inputs(database, family)
    assert pickle.dumps(database) == before


@pytest.mark.parametrize("encoding", ["dict", "json"])
@pytest.mark.parametrize("explicit_suppliers", [False, True])
def test_restores_exact_lifetime_capacity_and_only_matching_retirement(
    database, encoding, explicit_suppliers
):
    dataset = database[0]
    spec = dataset[SPEC_KEY]
    if explicit_suppliers:
        spec["mass supplier"] = deepcopy(MASS_PROVIDER)
        spec["retirement supplier"] = deepcopy(RETIREMENT_PROVIDER)
    untouched = [
        exchange({**RETIREMENT_PROVIDER, "location": "CH"}, -19),
        exchange({**RETIREMENT_PROVIDER, "name": "other battery retirement"}, -7),
        exchange({**MASS_PROVIDER, "location": "CH"}, 23),
        exchange(MASS_PROVIDER, 0.125, type="biosphere"),
    ]
    dataset["exchanges"].extend(deepcopy(untouched))
    original = deepcopy(dataset)
    suppliers_before = deepcopy(database[1:])
    if encoding == "json":
        dataset[SPEC_KEY] = json.dumps(spec)
    contract_before = deepcopy(dataset[SPEC_KEY])

    assert convert_battery_mass_inputs(database, "car") == 1

    actual = capacity_purchase(dataset)
    assert actual["amount"] == pytest.approx(2 * 76.875, rel=1e-12)
    assert actual["amount"] / MASS_KG == pytest.approx(
        LIFETIME_CAPACITY_KWH / MASS_KG, rel=1e-12
    )
    assert actual["unit"] == "kilowatt hour"
    assert actual["product"] == CAPACITY_PROVIDER["reference product"]
    assert actual["name"] == CAPACITY_PROVIDER["name"]
    assert actual["location"] == "GLO"
    assert all(key not in actual for key in ("input", "output", "database", "code"))
    assert actual["custom"] == original["exchanges"][2]["custom"]
    assert original["exchanges"][2]["comment"] in actual["comment"]
    assert len(dataset["exchanges"]) == len(original["exchanges"]) - 1
    assert dataset["exchanges"][:2] == original["exchanges"][:2]
    assert dataset["exchanges"][-len(untouched) :] == untouched
    assert dataset[SPEC_KEY] == contract_before
    assert database[1:] == suppliers_before
    state = dataset[STATE_KEY]
    assert state["baseline mass kg"] == MASS_KG
    assert state["baseline capacity kWh"] == LIFETIME_CAPACITY_KWH
    assert state["original mass exchange"] == original["exchanges"][2]
    assert state["original retirement exchange"] == original["exchanges"][3]
    # Audit snapshots must remain independent of the live replacement exchange.
    actual["custom"]["source"].append("runtime")
    assert state["original mass exchange"]["custom"] == {
        "source": ["synthetic baseline"]
    }


@pytest.mark.parametrize("year", [2020, 2050, 2100])
def test_transport_method_uses_declared_ratio_independent_of_year(database, year):
    transport = object.__new__(Transport)
    transport.database = database
    transport.vehicle_type = "car"
    transport.year = year
    assert transport.convert_battery_mass_inputs() == 1
    assert_converted(database[0])
    before = deepcopy(database)
    assert transport.convert_battery_mass_inputs() == 0
    assert database == before


@pytest.mark.parametrize("uncertainty_type", [2, 3, 4, 5])
def test_uncertainty_retains_relative_spread_in_capacity_units(
    database, uncertainty_type
):
    purchase = database[0]["exchanges"][2]
    purchase.update(
        {
            "uncertainty type": uncertainty_type,
            "loc": math.log(MASS_KG) if uncertainty_type == 2 else MASS_KG,
            "scale": 0.21 if uncertainty_type == 2 else MASS_KG * 0.21,
            "minimum": MASS_KG * 0.7,
            "maximum": MASS_KG * 1.4,
        }
    )
    original = deepcopy(purchase)
    assert convert_battery_mass_inputs(database, "car") == 1
    converted = assert_converted(database[0])
    assert converted["uncertainty type"] == uncertainty_type
    assert converted["minimum"] == pytest.approx(LIFETIME_CAPACITY_KWH * 0.7)
    assert converted["maximum"] == pytest.approx(LIFETIME_CAPACITY_KWH * 1.4)
    if uncertainty_type == 2:
        assert math.exp(converted["loc"]) == pytest.approx(LIFETIME_CAPACITY_KWH)
        assert converted["scale"] == original["scale"]
    else:
        assert converted["loc"] == pytest.approx(LIFETIME_CAPACITY_KWH)
        assert converted["scale"] / converted["amount"] == pytest.approx(0.21)
    assert database[0][STATE_KEY]["original mass exchange"] == original


@pytest.mark.parametrize("encoding", ["dict", "json"])
@pytest.mark.parametrize("uncertainty_type", [2, 3])
def test_restored_amount_is_bit_exact_baseline_after_uncertainty_scaling(
    database, encoding, uncertainty_type
):
    # These literals lose one ULP when restored by mass * (capacity / mass).
    mass = 33.08108108108108
    capacity = 67.2
    assert mass * (capacity / mass) != capacity
    dataset = database[0]
    spec = dataset[SPEC_KEY]
    spec.update(
        {
            "baseline mass kg": mass,
            "baseline capacity kWh": capacity,
            "retirement amount kg": -mass,
        }
    )
    dataset["exchanges"][2].update(
        {
            "amount": mass,
            "uncertainty type": uncertainty_type,
            "loc": math.log(mass) if uncertainty_type == 2 else mass,
            "scale": 0.2 if uncertainty_type == 2 else mass * 0.2,
            "minimum": mass * 0.7,
            "maximum": mass * 1.4,
        }
    )
    dataset["exchanges"][3]["amount"] = -mass
    if encoding == "json":
        dataset[SPEC_KEY] = json.dumps(spec)
    assert convert_battery_mass_inputs(database, "car") == 1
    purchase = capacity_purchase(dataset)
    assert purchase["amount"].hex() == capacity.hex()
    assert dataset[STATE_KEY]["baseline capacity kWh"].hex() == capacity.hex()
    assert purchase["minimum"] == pytest.approx(capacity * 0.7)
    assert purchase["maximum"] == pytest.approx(capacity * 1.4)
    if uncertainty_type == 2:
        assert math.exp(purchase["loc"]) == pytest.approx(capacity)
        assert purchase["scale"] == 0.2
    else:
        assert purchase["loc"] == pytest.approx(capacity)
        assert purchase["scale"] / purchase["amount"] == pytest.approx(0.2)
    before = deepcopy(database)
    assert convert_battery_mass_inputs(database, "car") == 0
    assert database == before


def high_power_database(database, family):
    identity = deepcopy(HIGH_POWER_PROVIDERS[family])
    dataset = database[0]
    dataset[SPEC_KEY]["vehicle family"] = family
    dataset[SPEC_KEY]["capacity supplier"] = identity
    database.extend(
        [provider(identity, "high-power"), provider(MIX_CAPACITY_PROVIDER, "mix")]
    )
    return identity


@pytest.mark.parametrize("family", ["car", "truck"])
@pytest.mark.parametrize("encoding", ["dict", "json"])
@pytest.mark.parametrize("compact", [False, True])
def test_dedicated_high_power_contract_preserves_supplier_instead_of_mix(
    database, family, encoding, compact, tmp_path
):
    identity = high_power_database(database, family)
    if encoding == "json":
        database[0][SPEC_KEY] = json.dumps(database[0][SPEC_KEY])
    original = deepcopy(database)
    if compact:
        checkpoint = CompactInventoryStore(database).checkpoint(
            tmp_path / "high-power.inventory-store"
        )
        database = get_scenario_inventory({"_inventory_checkpoint": checkpoint})
    assert convert_battery_mass_inputs(database, family) == 1
    purchases = [e for e in database[0]["exchanges"] if e["unit"] == "kilowatt hour"]
    assert len(purchases) == 1
    purchase = purchases[0]
    assert purchase["name"] == identity["name"]
    assert purchase["product"] == identity["reference product"]
    assert purchase["location"] == identity["location"]
    assert purchase["amount"] == LIFETIME_CAPACITY_KWH
    assert len(database[0]["exchanges"]) == 3
    assert (
        database[0][STATE_KEY]["original retirement exchange"]
        == original[0]["exchanges"][3]
    )
    assert list(database[1:]) == original[1:]
    regional = deepcopy(database[0])
    regional.update(location="NEU", code="high-power-regional", regionalized=True)
    regional["exchanges"][0]["location"] = "NEU"
    regional["exchanges"][2]["amount"] *= 0.8
    database.append(regional)
    if compact:
        checkpoint = CompactInventoryStore(database).checkpoint(
            tmp_path / "converted-high-power.inventory-store"
        )
        database = get_scenario_inventory({"_inventory_checkpoint": checkpoint})
    before = deepcopy(database)
    assert convert_battery_mass_inputs(database, family) == 0
    assert database == before
    assert database[-1]["exchanges"][2]["name"] == identity["name"]
    assert database[-1]["exchanges"][2]["product"] == identity["reference product"]
    assert database[-1]["exchanges"][2]["amount"] == LIFETIME_CAPACITY_KWH * 0.8


@pytest.mark.parametrize("family", ["car", "truck"])
@pytest.mark.parametrize(
    "problem",
    ["missing", "duplicate", "generic name", "generic product", "crossed name"],
)
def test_high_power_supplier_is_exact_and_never_falls_back_to_mix(
    database, family, problem
):
    identity = high_power_database(database, family)
    if problem == "missing":
        database.pop(-2)
    elif problem == "duplicate":
        database.append(provider(identity, "duplicate-high-power"))
    else:
        supplier = database[0][SPEC_KEY]["capacity supplier"]
        if problem == "generic name":
            supplier["name"] = MIX_CAPACITY_PROVIDER["name"]
        elif problem == "generic product":
            supplier["reference product"] = CAPACITY_PROVIDER["reference product"]
        else:
            other_family = "truck" if family == "car" else "car"
            supplier["name"] = HIGH_POWER_PROVIDERS[other_family]["name"]
        database.append(provider(supplier, "unsupported-high-power-alias"))
    assert_rejected_atomically(database, family)


@pytest.mark.parametrize("family", ["car", "truck"])
def test_preexisting_dedicated_high_power_capacity_rejects_double_purchase(
    database, family
):
    identity = high_power_database(database, family)
    database[0]["exchanges"].append(exchange(identity, LIFETIME_CAPACITY_KWH))
    assert_rejected_atomically(database, family)


@pytest.mark.parametrize(
    "family", ["car", "truck", "bus", "train", "two-wheeler", "ship"]
)
def test_vehicle_family_selects_only_its_marked_inputs(database, family):
    marked = vehicle(f"marked {family}", family)
    other_family = "bus" if family != "bus" else "truck"
    other = vehicle("other family", other_family)
    unmarked = vehicle("unmarked import")
    unmarked.pop(SPEC_KEY)
    native = deepcopy(unmarked)
    native.update(name="native ecoinvent vehicle", database="ecoinvent-3.12-cutoff")
    legacy = deepcopy(unmarked)
    legacy["name"] = "legacy capacity vehicle"
    legacy["exchanges"] = legacy["exchanges"][:2] + [exchange(CAPACITY_PROVIDER, 42)]
    database[:] = [marked, other, unmarked, native, legacy, *database[1:]]
    before = deepcopy(database[1:])
    assert convert_battery_mass_inputs(database, family) == 1
    assert_converted(marked)
    assert database[1:] == before
    assert convert_battery_mass_inputs(database, other_family) == 1
    assert_converted(other)
    assert database[2:] == before[1:]


def test_empty_and_unmarked_inventories_are_unchanged(database):
    assert convert_battery_mass_inputs([], "car") == 0
    database[0].pop(SPEC_KEY)
    before = deepcopy(database)
    assert convert_battery_mass_inputs(database, "car") == 0
    assert database == before


@pytest.mark.parametrize(
    "field",
    [
        "version",
        "vehicle family",
        "baseline mass kg",
        "baseline capacity kWh",
        "retirement amount kg",
        "capacity supplier",
        "capacity supplier includes end of life",
    ],
)
def test_missing_contract_field_rejects_entire_batch(database, field):
    invalid = vehicle("invalid second inventory")
    del invalid[SPEC_KEY][field]
    database.append(invalid)
    assert_rejected_atomically(database)


@pytest.mark.parametrize("metadata", [None, 17, [], "{invalid", "null", "[]", '"text"'])
def test_malformed_contract_rejects_entire_batch(database, metadata):
    invalid = vehicle("invalid second inventory")
    invalid[SPEC_KEY] = metadata
    database.append(invalid)
    assert_rejected_atomically(database)


@pytest.mark.parametrize(
    "field,value",
    [
        ("version", 2),
        ("version", True),
        ("version", "1"),
        ("vehicle family", "passenger cars"),
        ("vehicle family", None),
        ("retirement amount kg", -MASS_KG / 2),
        ("capacity supplier includes end of life", False),
        ("capacity supplier includes end of life", "true"),
        ("capacity supplier includes end of life", 1),
        ("capacity supplier", {}),
        ("capacity supplier", "market for battery capacity"),
        ("mass supplier", {**MASS_PROVIDER, "unit": "kilowatt hour"}),
        ("mass supplier", {**MASS_PROVIDER, "name": "market for battery, LFP"}),
        ("retirement supplier", {**RETIREMENT_PROVIDER, "location": "RER"}),
    ],
)
def test_invalid_contract_rejects_entire_batch(database, field, value):
    invalid = vehicle("invalid second inventory")
    invalid[SPEC_KEY][field] = value
    database.append(invalid)
    assert_rejected_atomically(database)


@pytest.mark.parametrize(
    "field,value",
    [
        ("name", "market for battery, Li-ion, NMC622, rechargeable"),
        ("reference product", "battery"),
        ("location", ""),
        ("location", None),
        ("unit", "kilogram"),
        ("unit", "kWh"),
        ("product", "conflicting product alias"),
    ],
)
def test_wrong_capacity_supplier_identity_rejects_atomically(database, field, value):
    invalid = vehicle("invalid second inventory")
    invalid[SPEC_KEY]["capacity supplier"][field] = value
    database.append(invalid)
    assert_rejected_atomically(database)


@pytest.mark.parametrize(
    "target",
    [
        "baseline mass kg",
        "baseline capacity kWh",
        "retirement amount kg",
        "mass exchange",
        "retirement exchange",
    ],
)
@pytest.mark.parametrize(
    "value", [0, -1, float("nan"), float("inf"), -float("inf"), True, "812.5", None]
)
def test_nonpositive_nonfinite_or_nonnumeric_values_reject_atomically(
    database, target, value
):
    invalid = vehicle("invalid second inventory")
    # Retirement is required to be negative, so +1 is the invalid sign there.
    if "retirement" in target and value == -1:
        value = 1
    if target == "mass exchange":
        invalid["exchanges"][2]["amount"] = value
    elif target == "retirement exchange":
        invalid["exchanges"][3]["amount"] = value
    else:
        invalid[SPEC_KEY][target] = value
    database.append(invalid)
    assert_rejected_atomically(database)


@pytest.mark.parametrize("target", ["provider", "mass", "retirement"])
@pytest.mark.parametrize("problem", ["missing", "duplicate"])
def test_missing_or_duplicate_target_rejects_atomically(database, target, problem):
    invalid = vehicle("invalid second inventory")
    database.append(invalid)
    if target == "provider":
        if problem == "missing":
            database.pop(1)
        else:
            duplicate = deepcopy(database[1])
            duplicate["code"] = "different-code-same-supplier-identity"
            database.append(duplicate)
    else:
        index = 2 if target == "mass" else 3
        if problem == "missing":
            invalid["exchanges"].pop(index)
        else:
            invalid["exchanges"].append(deepcopy(invalid["exchanges"][index]))
    assert_rejected_atomically(database)


@pytest.mark.parametrize("target", ["mass", "retirement"])
@pytest.mark.parametrize(
    "field,value",
    [
        ("name", "wrong supplier"),
        ("product", "wrong reference product"),
        ("reference product", "conflicting product alias"),
        ("location", "RER"),
        ("unit", "kilowatt hour"),
        ("type", "biosphere"),
        ("formula", "battery_mass / density"),
        ("amount", 7),
    ],
)
def test_wrong_input_identity_units_amount_or_formula_rejects_atomically(
    database, target, field, value
):
    invalid = vehicle("invalid second inventory")
    index = 2 if target == "mass" else 3
    invalid["exchanges"][index][field] = value
    database.append(invalid)
    assert_rejected_atomically(database)


def test_preexisting_capacity_on_marked_mass_inventory_rejects_atomically(database):
    invalid = vehicle("invalid second inventory")
    invalid["exchanges"].append(exchange(CAPACITY_PROVIDER, 7))
    database.append(invalid)
    assert_rejected_atomically(database)


def test_independent_retirement_uncertainty_rejects_atomically(database):
    invalid = vehicle("invalid second inventory")
    invalid["exchanges"][3].update({"uncertainty type": 3, "scale": 10})
    database.append(invalid)
    assert_rejected_atomically(database)


@pytest.mark.parametrize("field", ["name", "reference product", "location", "unit"])
def test_target_provider_must_match_every_declared_identity_field(database, field):
    database[1][field] = "different provider field"
    assert_rejected_atomically(database)


@pytest.mark.parametrize("keep_product", [False, True])
def test_imported_reference_product_alias_is_converted_consistently(
    database, keep_product
):
    purchase = database[0]["exchanges"][2]
    purchase["reference product"] = purchase["product"]
    if not keep_product:
        del purchase["product"]
    assert convert_battery_mass_inputs(database, "car") == 1
    converted = assert_converted(database[0])
    assert converted["reference product"] == converted["product"]


def test_repeated_conversion_preserves_resized_regional_copy(database):
    assert convert_battery_mass_inputs(database, "car") == 1
    regional = deepcopy(database[0])
    regional.update(location="NEU", code="regional-vehicle", regionalized=True)
    regional["exchanges"][0]["location"] = "NEU"
    capacity_purchase(regional)["amount"] *= 0.73
    database.append(regional)
    before = deepcopy(database)
    for _ in range(2):
        assert convert_battery_mass_inputs(database, "car") == 0
        assert database == before
        assert_converted(regional, LIFETIME_CAPACITY_KWH * 0.73)


@pytest.mark.parametrize(
    "damage", ["contract", "state", "mass", "retirement", "capacity", "amount"]
)
def test_inconsistent_conversion_provenance_rejects_before_new_writes(database, damage):
    assert convert_battery_mass_inputs(database, "car") == 1
    converted = database[0]
    if damage == "contract":
        converted[SPEC_KEY]["baseline capacity kWh"] *= 2
    elif damage == "state":
        converted[STATE_KEY]["version"] = 999
    elif damage == "mass":
        converted["exchanges"].append(exchange(MASS_PROVIDER, MASS_KG))
    elif damage == "retirement":
        converted["exchanges"].append(exchange(RETIREMENT_PROVIDER, -MASS_KG))
    elif damage == "capacity":
        converted["exchanges"].append(exchange(CAPACITY_PROVIDER, 1))
    else:
        capacity_purchase(converted)["amount"] = float("nan")
    database.insert(0, vehicle("new inventory before damaged converted inventory"))
    assert_rejected_atomically(database)


def test_transport_hook_converts_before_regionalization_and_resizing(
    database, monkeypatch
):
    transport = object.__new__(Transport)
    transport.database = database
    transport.vehicle_type = "car"
    transport.year = 2050
    transport.vehicle_map = {"BEV": [database[0]]}
    events = []

    def regionalize(*, mapping, efficiency_adjustment_fn):
        assert mapping is transport.vehicle_map
        assert efficiency_adjustment_fn == transport.adjust_transport_efficiency
        assert_converted(database[0])
        events.append("regionalize")
        regional = deepcopy(database[0])
        regional.update(location="NEU", code="regional-vehicle", regionalized=True)
        database.append(regional)

    def resize(actual_transport):
        assert actual_transport is transport
        assert events == ["regionalize"]
        assert_converted(database[-1])
        capacity_purchase(database[-1])["amount"] *= 0.8
        events.append("resize")

    monkeypatch.setattr(transport, "process_and_add_activities", regionalize)
    monkeypatch.setattr(transport_module, "resize_regional_batteries", resize)
    transport.regionalize_transport_datasets()
    assert events == ["regionalize", "resize"]
    assert_converted(database[0])
    assert_converted(database[-1], LIFETIME_CAPACITY_KWH * 0.8)
    before = deepcopy(database)
    assert transport.convert_battery_mass_inputs() == 0
    assert database == before


def test_invalid_mass_contract_stops_regionalization_and_resizing(
    database, monkeypatch
):
    transport = object.__new__(Transport)
    transport.database = database
    transport.vehicle_type = "car"
    database[0]["exchanges"][3]["amount"] = -1
    before = deepcopy(database)

    def unexpected_work(*args, **kwargs):
        pytest.fail("Invalid mass contracts must fail before regionalization or sizing")

    monkeypatch.setattr(transport, "process_and_add_activities", unexpected_work)
    monkeypatch.setattr(transport_module, "resize_regional_batteries", unexpected_work)
    with pytest.raises(ValueError):
        transport.regionalize_transport_datasets()
    assert database == before


@pytest.mark.parametrize("route", ["indexed", "compact", "columnar", "copy on write"])
def test_inventory_collection_and_compact_paths_convert_real_exchanges(
    database, route, tmp_path
):
    original = deepcopy(database)
    template = sibling = None
    if route == "indexed":
        working = IndexedInventoryList(deepcopy(database))
    elif route == "compact":
        working = get_scenario_inventory(
            {"_inventory_store": CompactInventoryStore(database)}
        )
    else:
        checkpoint = CompactInventoryStore(database).checkpoint(
            tmp_path / "source.inventory-store"
        )
        template = InventoryStore.open(checkpoint)
        if route == "copy on write":
            working = get_scenario_inventory(
                {"_inventory_store": template.fresh_columnar_view("first")}
            )
            sibling = get_scenario_inventory(
                {"_inventory_store": template.fresh_columnar_view("second")}
            )
            assert working[0]._storage is sibling[0]._storage
        else:
            working = get_scenario_inventory({"_inventory_store": template})
    assert isinstance(working, IndexedInventoryList)
    if route in {"columnar", "copy on write"}:
        assert type(working[0]["exchanges"][2]).__name__ == "_ColumnarExchangeMapping"
    assert list(ws.get_many(working, ws.equals("unit", "kilowatt hour")))
    assert convert_battery_mass_inputs(working, "car") == 1
    actual = assert_converted(working[0])
    assert len(working[0]["exchanges"]) == len(original[0]["exchanges"]) - 1
    assert actual["custom"] == original[0]["exchanges"][2]["custom"]
    assert (
        working[0][STATE_KEY]["original mass exchange"] == original[0]["exchanges"][2]
    )
    assert convert_battery_mass_inputs(working, "car") == 0
    if sibling is not None:
        assert template.materialize() == original
        assert list(sibling) == original
        assert convert_battery_mass_inputs(sibling, "car") == 1
        assert_converted(sibling[0])
        assert template.materialize() == original
    assert database == original


def test_fresh_columnar_exchange_views_are_replaced_and_retirement_removed(
    database, tmp_path
):
    checkpoint = CompactInventoryStore(database).checkpoint(
        tmp_path / "fresh-views.inventory-store"
    )
    store = InventoryStore.open(checkpoint)
    table = store._state.exchanges
    exchange_ids = tuple(store._state.activity_exchanges[0])

    class ColumnarExchangeViews:
        """Expose real compact rows without retaining their Python wrappers."""

        def __iter__(self):
            return (table[exchange_id] for exchange_id in exchange_ids)

    working = get_scenario_inventory({"_inventory_store": store})
    working[0]["exchanges"] = ColumnarExchangeViews()
    assert list(working[0]["exchanges"])[2] is not list(working[0]["exchanges"])[2]
    assert convert_battery_mass_inputs(working, "car") == 1
    assert_converted(working[0])
    assert len(working[0]["exchanges"]) == 3
    assert (
        working[0][STATE_KEY]["original mass exchange"] == database[0]["exchanges"][2]
    )
    assert (
        working[0][STATE_KEY]["original retirement exchange"]
        == database[0]["exchanges"][3]
    )
    assert convert_battery_mass_inputs(working, "car") == 0


def test_columnar_batch_failure_preserves_source_sibling_and_valid_first_vehicle(
    database, tmp_path
):
    invalid = vehicle("invalid second inventory")
    invalid["exchanges"].append(deepcopy(invalid["exchanges"][3]))
    database.append(invalid)
    checkpoint = CompactInventoryStore(database).checkpoint(
        tmp_path / "atomic.inventory-store"
    )
    template = InventoryStore.open(checkpoint)
    working = get_scenario_inventory(
        {"_inventory_store": template.fresh_columnar_view("invalid scenario")}
    )
    sibling = get_scenario_inventory(
        {"_inventory_store": template.fresh_columnar_view("independent scenario")}
    )
    before = deepcopy(working)
    with pytest.raises(ValueError):
        convert_battery_mass_inputs(working, "car")
    assert working == before
    assert STATE_KEY not in working[0]
    assert list(sibling) == database
    assert template.materialize() == database


def test_compact_checkpoint_preserves_conversion_state_and_resized_copy(
    database, tmp_path
):
    assert convert_battery_mass_inputs(database, "car") == 1
    regional = deepcopy(database[0])
    regional.update(location="NEU", code="resized-regional-vehicle", regionalized=True)
    regional["exchanges"][0]["location"] = "NEU"
    capacity_purchase(regional)["amount"] *= 0.64
    database.append(regional)
    checkpoint = CompactInventoryStore(database).checkpoint(
        tmp_path / "converted.inventory-store"
    )
    working = get_scenario_inventory({"_inventory_checkpoint": checkpoint})
    assert working[0][STATE_KEY] == database[0][STATE_KEY]
    assert working[-1][STATE_KEY] == regional[STATE_KEY]
    assert_converted(working[0])
    assert_converted(working[-1], LIFETIME_CAPACITY_KWH * 0.64)
    before = deepcopy(working)
    assert convert_battery_mass_inputs(working, "car") == 0
    assert working == before
    assert_converted(working[-1], LIFETIME_CAPACITY_KWH * 0.64)


def set_cache_baseline(dataset):
    # Neither literal is exactly representable as float32 in source caches.
    mass, capacity = 123.456789, 67.2
    dataset[SPEC_KEY].update(
        {
            "baseline mass kg": mass,
            "baseline capacity kWh": capacity,
            "retirement amount kg": -mass,
        }
    )
    dataset["exchanges"][2]["amount"] = mass
    dataset["exchanges"][3]["amount"] = -mass
    return mass, capacity


def source_cache_round_trip(database, tmp_path, monkeypatch, stem="source"):
    monkeypatch.setattr(utils_module, "DIR_CACHED_DB", tmp_path)
    path = tmp_path / f"{stem}.pickle"
    utils_module.create_cache(deepcopy(database), path)
    return utils_module.load_cached_database(path)


@pytest.mark.parametrize(
    "family", [None, "car", "truck"], ids=["MIX", "FCEV car", "FCEV truck"]
)
@pytest.mark.parametrize("encoding", ["dict", "json"])
def test_real_source_cache_preserves_contract_conversion_and_provenance(
    database, family, encoding, tmp_path, monkeypatch
):
    identity = (
        high_power_database(database, family)
        if family
        else deepcopy(MIX_CAPACITY_PROVIDER)
    )
    if family is None:
        database[0][SPEC_KEY]["capacity supplier"] = identity
        database.append(provider(identity, "mix"))
    mass, capacity = set_cache_baseline(database[0])
    if encoding == "json":
        database[0][SPEC_KEY] = json.dumps(database[0][SPEC_KEY])
    original_contract = deepcopy(database[0][SPEC_KEY])
    cached = source_cache_round_trip(database, tmp_path, monkeypatch)
    assert cached[0][SPEC_KEY] == original_contract
    stored = cached[0]["exchanges"][2]["amount"]
    assert isinstance(stored, np.float32)
    assert stored == np.float32(mass)
    assert float(stored) != mass
    assert convert_battery_mass_inputs(cached, family) == 1
    purchase = cached[0]["exchanges"][2]
    assert purchase["name"] == identity["name"]
    assert purchase["product"] == identity["reference product"]
    assert purchase["amount"].hex() == capacity.hex()
    assert len(cached[0]["exchanges"]) == 3
    state = deepcopy(cached[0][STATE_KEY])
    assert state["original mass exchange"]["amount"] == np.float32(mass)
    assert state["original retirement exchange"]["amount"] == np.float32(-mass)
    # Source-cache trimming must retain runtime provenance as well as opt-in.
    recached = source_cache_round_trip(cached, tmp_path, monkeypatch, "converted")
    assert recached[0][SPEC_KEY] == original_contract
    assert recached[0][STATE_KEY] == state
    before = deepcopy(recached)
    assert convert_battery_mass_inputs(recached, family) == 0
    assert recached == before


@pytest.mark.parametrize("target", ["mass", "retirement"])
@pytest.mark.parametrize(
    "direction", [-np.inf, np.inf], ids=["one ULP lower", "one ULP higher"]
)
def test_source_cache_one_float32_unit_error_rejects_atomically(
    database, target, direction, tmp_path, monkeypatch
):
    set_cache_baseline(database[0])
    invalid = vehicle("invalid cached second inventory")
    set_cache_baseline(invalid)
    database.append(invalid)
    cached = source_cache_round_trip(database, tmp_path, monkeypatch)
    exchange_index = 2 if target == "mass" else 3
    selected = cached[-1]["exchanges"][exchange_index]
    expected = selected["amount"]
    selected["amount"] = np.nextafter(expected, np.float32(direction))
    assert isinstance(selected["amount"], np.float32)
    assert selected["amount"] != expected
    # A generic 1e-6 comparison would incorrectly accept this neighboring value.
    assert math.isclose(float(selected["amount"]), float(expected), rel_tol=1e-6)
    assert_rejected_atomically(cached)


@pytest.mark.parametrize("target", ["mass", "retirement"])
@pytest.mark.parametrize("scalar_type", [float, np.float64])
def test_float32_representation_exception_does_not_relax_float64_validation(
    database, target, scalar_type
):
    mass, _ = set_cache_baseline(database[0])
    index = 2 if target == "mass" else 3
    expected = mass if target == "mass" else -mass
    observed = scalar_type(np.float32(expected))
    assert observed != expected
    assert math.isclose(observed, expected, rel_tol=1e-6)
    database[0]["exchanges"][index]["amount"] = observed
    assert_rejected_atomically(database)


@pytest.mark.parametrize("scalar_type", [np.float32, np.float64])
def test_numpy_floating_exchange_amounts_are_valid(database, scalar_type):
    mass, capacity = set_cache_baseline(database[0])
    database[0]["exchanges"][2]["amount"] = scalar_type(mass)
    database[0]["exchanges"][3]["amount"] = scalar_type(-mass)
    assert convert_battery_mass_inputs(database, "car") == 1
    assert capacity_purchase(database[0])["amount"].hex() == capacity.hex()


@pytest.mark.parametrize("legacy", [False, True])
def test_normalized_source_cache_checkpoint_retains_battery_precision(
    database, legacy, tmp_path, monkeypatch
):
    mass, capacity = set_cache_baseline(database[0])
    if legacy:
        purchase = database[0]["exchanges"][2]
        purchase.update(exchange(LEGACY_MASS_PROVIDER, mass))
    cached = source_cache_round_trip(database, tmp_path, monkeypatch)
    normalize_inventory_numeric_types(cached)
    assert isinstance(cached[0]["exchanges"][2]["amount"], np.float32)
    assert isinstance(cached[0]["exchanges"][3]["amount"], np.float32)
    checkpoint = CompactInventoryStore(cached).checkpoint(tmp_path / "normalized.store")
    working = get_scenario_inventory({"_inventory_checkpoint": checkpoint})
    normalize_inventory_numeric_types(working)
    assert convert_battery_mass_inputs(working, "car") == 1
    assert capacity_purchase(working[0])["amount"] == capacity
    assert working[0][STATE_KEY]["original mass exchange"]["amount"] == np.float32(mass)


@pytest.mark.parametrize("index", [2, 3])
def test_normalization_does_not_hide_one_ulp_battery_error(
    database, index, tmp_path, monkeypatch
):
    set_cache_baseline(database[0])
    cached = source_cache_round_trip(database, tmp_path, monkeypatch)
    purchase = cached[0]["exchanges"][index]
    purchase["amount"] = np.nextafter(purchase["amount"], np.float32(np.inf))
    normalize_inventory_numeric_types(cached)
    assert isinstance(purchase["amount"], np.float32)
    assert_rejected_atomically(cached)


@pytest.mark.parametrize(
    "target",
    [
        "baseline mass kg",
        "baseline capacity kWh",
        "retirement amount kg",
        "mass exchange",
        "retirement exchange",
    ],
)
@pytest.mark.parametrize("value", [np.bool_(False), np.bool_(True)])
def test_numpy_boolean_is_not_a_numeric_battery_quantity(database, target, value):
    if target == "mass exchange":
        database[0]["exchanges"][2]["amount"] = value
    elif target == "retirement exchange":
        database[0]["exchanges"][3]["amount"] = value
    else:
        database[0][SPEC_KEY][target] = value
    assert_rejected_atomically(database)


@pytest.mark.parametrize("uncertainty_type", [2, 3])
def test_cached_uncertainty_uses_actual_stored_mass_for_scaling(
    database, uncertainty_type, tmp_path, monkeypatch
):
    mass, capacity = set_cache_baseline(database[0])
    database[0]["exchanges"][2].update(
        {
            "uncertainty type": uncertainty_type,
            "loc": math.log(mass) if uncertainty_type == 2 else mass,
            "scale": 0.2 if uncertainty_type == 2 else mass * 0.2,
            "minimum": mass * 0.7,
            "maximum": mass * 1.4,
        }
    )
    cached = source_cache_round_trip(database, tmp_path, monkeypatch)
    original = deepcopy(cached[0]["exchanges"][2])
    factor = capacity / float(original["amount"])
    assert float(original["amount"]) != mass
    assert convert_battery_mass_inputs(cached, "car") == 1
    purchase = capacity_purchase(cached[0])
    assert purchase["amount"].hex() == capacity.hex()
    for bound in ("minimum", "maximum"):
        assert float(purchase[bound]) == pytest.approx(
            float(original[bound]) * factor, rel=1e-12
        )
    if uncertainty_type == 2:
        assert purchase["loc"] == pytest.approx(
            float(original["loc"]) + math.log(factor), rel=1e-12
        )
        assert purchase["scale"] == original["scale"]
    else:
        assert purchase["loc"] == pytest.approx(capacity, rel=1e-12)
        assert purchase["scale"] / purchase["amount"] == pytest.approx(
            float(original["scale"]) / float(original["amount"]), rel=1e-12
        )


def test_omitting_family_converts_all_marked_families_once(database):
    families = ["car", "truck", "bus", "train", "two-wheeler", "ship"]
    vehicles = [vehicle(f"marked {family}", family) for family in families]
    database[:] = [*vehicles, *database[1:]]
    unmarked = vehicle("unmarked source")
    unmarked.pop(SPEC_KEY)
    database.append(unmarked)
    original_unmarked = deepcopy(unmarked)
    assert convert_battery_mass_inputs(database) == len(vehicles)
    for marked in vehicles:
        assert_converted(marked)
    assert unmarked == original_unmarked
    before = deepcopy(database)
    assert convert_battery_mass_inputs(database, vehicle_type=None) == 0
    assert database == before


def test_all_family_conversion_is_atomic_across_families(database):
    invalid = vehicle("invalid truck after valid car", "truck")
    invalid["exchanges"][3]["amount"] = -1
    database.append(invalid)
    assert_rejected_atomically(database, family=None)


@pytest.mark.parametrize("compact", [False, True])
def test_final_energy_only_update_restores_cached_car_and_dedicated_fcev_capacity(
    database, compact, tmp_path, monkeypatch
):
    car = database[0]
    car["name"] = car["exchanges"][0]["name"] = (
        "passenger car, synthetic battery electric"
    )
    car_mass, car_capacity = set_cache_baseline(car)
    truck = vehicle("lorry production, synthetic fuel cell electric", "truck")
    truck_identity = deepcopy(HIGH_POWER_PROVIDERS["truck"])
    truck[SPEC_KEY]["capacity supplier"] = truck_identity
    _, truck_capacity = set_cache_baseline(truck)
    database.extend([truck, provider(truck_identity, "fcev capacity")])
    database.append(provider(MIX_CAPACITY_PROVIDER, "competing mix"))
    wrapper_names = [
        "electricity, used in passenger car",
        "hydrogen, used in heavy-duty vehicle",
    ]
    for production, wrapper_name in zip((car, truck), wrapper_names):
        production_identity = {
            key: production[key]
            for key in ("name", "reference product", "location", "unit")
        }
        wrapper_identity = {
            "name": wrapper_name,
            "reference product": wrapper_name,
            "location": "RER",
            "unit": "megajoule",
        }
        wrapper = {
            **wrapper_identity,
            SOURCE_KEY: "{}",
            "exchanges": [exchange(wrapper_identity, 1, type="production")],
        }
        service_identity = {
            "name": source_name(wrapper_name),
            "reference product": "transport service",
            "location": "RER",
            "unit": "kilometer",
        }
        fuel_identity = (
            {
                "name": "market for electricity, low voltage",
                "reference product": "electricity, low voltage",
                "location": "RER",
                "unit": "kilowatt hour",
            }
            if wrapper_name.startswith("electricity")
            else {
                "name": "market for hydrogen",
                "reference product": "hydrogen",
                "location": "RER",
                "unit": "kilogram",
            }
        )
        database.extend(
            [
                {
                    **service_identity,
                    "exchanges": [
                        exchange(service_identity, 1, type="production"),
                        exchange(production_identity, 1 / 200000),
                        exchange(
                            fuel_identity,
                            0.2 if wrapper_name.startswith("electricity") else 0.01,
                        ),
                    ],
                },
                wrapper,
            ]
        )
    cached = source_cache_round_trip(database, tmp_path, monkeypatch)
    if compact:
        checkpoint = CompactInventoryStore(cached).checkpoint(
            tmp_path / "final-energy.inventory-store"
        )
        cached = get_scenario_inventory({"_inventory_checkpoint": checkpoint})
    by_name = {d["name"]: d for d in cached}
    source_car = by_name[car["name"]]
    assert isinstance(source_car["exchanges"][2]["amount"], np.float32)
    assert source_car["exchanges"][2]["amount"] == np.float32(car_mass)
    events = []

    def regionalize():
        assert STATE_KEY in by_name[car["name"]]
        assert STATE_KEY in by_name[truck["name"]]
        events.append("regionalize")

    def relink():
        assert events == ["regionalize"]
        for wrapper_name in wrapper_names:
            assert any(
                e.get("product", "").startswith("electricity storage capacity")
                for e in by_name[wrapper_name]["exchanges"]
            )
        events.append("relink")

    transform = SimpleNamespace(
        database=cached,
        index={},
        cache={},
        final_energy_map={},
        regionalize_heating_datasets=regionalize,
        relink_datasets=relink,
    )
    monkeypatch.setattr(final_energy_module, "FinalEnergy", lambda **kwargs: transform)
    scenario = {
        "database": cached,
        "model": "remind",
        "pathway": "test",
        "year": 2050,
        "iam data": SimpleNamespace(final_energy_use=True),
    }
    final_energy_module._update_final_energy(scenario, "3.12", "cutoff")
    assert events == ["regionalize", "relink"]
    for production, wrapper_name, expected_capacity in zip(
        (car, truck), wrapper_names, (car_capacity, truck_capacity)
    ):
        source = by_name[source_name(wrapper_name)]
        allocation = next(
            e["amount"]
            for e in source["exchanges"]
            if e.get("name") == production["name"]
        )
        fuel = next(
            e
            for e in source["exchanges"]
            if e["type"] == "technosphere" and e["unit"] != "unit"
        )
        energy = float(fuel["amount"]) * (
            3.6 if wrapper_name.startswith("electricity") else 120
        )
        wrapper = by_name[wrapper_name]
        purchases = [
            e
            for e in wrapper["exchanges"]
            if e.get("product", "").startswith("electricity storage capacity")
        ]
        assert len(purchases) == 1
        purchase = purchases[0]
        assert purchase["unit"] == "kilowatt hour"
        assert purchase["amount"] == pytest.approx(
            expected_capacity * float(allocation) / energy, rel=1e-12
        )
        if production is truck:
            assert purchase["name"] == truck_identity["name"]
            assert purchase["product"] == truck_identity["reference product"]
        assert not any(
            e.get("name") in (MASS_PROVIDER["name"], RETIREMENT_PROVIDER["name"])
            for e in wrapper["exchanges"]
        )
    before = deepcopy(cached)
    assert convert_battery_mass_inputs(cached) == 0
    assert cached == before
