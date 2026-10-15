"""Installed electric buses retain lifetime battery capacity per MJ as efficiency changes."""

from copy import deepcopy
from types import SimpleNamespace

import pytest
from bw2io import ExcelImporter

from premise.filesystem_constants import INVENTORY_DIR
from premise.inventory_store import CompactInventoryStore
from premise.vehicle_battery import (
    SPEC_KEY,
    STATE_KEY,
    electricity_per_service,
    resize_regional_batteries,
    specification,
)
from premise.vehicle_energy_use import normalize, source_name


@pytest.fixture(scope="module")
def catalogue():
    data = ExcelImporter(INVENTORY_DIR / "lci-buses.xlsx").data
    for dataset in data:
        for exchange in dataset["exchanges"]:
            if "reference product" in exchange:
                exchange["product"] = exchange.pop("reference product")
    return data


@pytest.mark.parametrize("compact", [False, True])
@pytest.mark.parametrize("year,factor", [(2020, 1), (2050, 0.5), (2050, 1.2)])
def test_all_electric_buses_keep_capacity_per_MJ(catalogue, compact, year, factor):
    data = deepcopy(catalogue)
    if compact:
        data = CompactInventoryStore(data).materialize()
    originals = {d["name"]: deepcopy(d) for d in data}
    services = [d for d in data if d["name"].startswith("transport,") and SPEC_KEY in d]
    assert len(services) == 10
    for service in services:
        service["location"] = "NEU"
        service["regionalized"] = True
        for exchange in service["exchanges"]:
            if exchange["type"] == "production":
                exchange["location"] = "NEU"
            elif exchange.get("product") == "electricity, low voltage":
                exchange["amount"] *= factor
    added = []
    tr = SimpleNamespace(
        year=year,
        vehicle_type="bus",
        database=data,
        vehicle_map={"BEV": services},
        add_to_index=lambda values: added.extend(values),
    )
    resize_regional_batteries(tr)
    assert len(added) == (0 if factor == 1 else 10)
    for service in services:
        spec = specification(service)
        original = originals[service["name"]]
        state = service[STATE_KEY]
        assert state["capacity kWh"] == pytest.approx(spec["capacity kWh"] * factor)
        # Fixed passenger occupancy makes the per-pkm efficiency ratio also
        # the per-vkm ratio. Capacity / grid energy (and hence range) is fixed.
        assert state["capacity kWh"] / electricity_per_service(
            service
        ) == pytest.approx(
            spec["capacity kWh"] / electricity_per_service(original), rel=2e-6
        )
        link = next(
            e for e in service["exchanges"] if e["name"].startswith("Passenger bus,")
        )
        producer = next(
            d
            for d in data
            if d["name"] == link["name"] and d["location"] == link["location"]
        )
        purchase = next(
            e
            for e in producer["exchanges"]
            if e.get("product") == "electricity storage capacity"
        )
        original_producer = originals[link["name"]]
        original_purchase = next(
            e
            for e in original_producer["exchanges"]
            if e.get("product") == "electricity storage capacity"
        )
        assert purchase["amount"] == pytest.approx(original_purchase["amount"] * factor)
        assert purchase["name"] == original_purchase["name"]
        assert purchase["location"] == original_purchase["location"]
        assert link["amount"] == pytest.approx(
            1 / (spec["lifetime km"] * spec["passengers"]), rel=2e-6
        )
        actual = (
            purchase["amount"]
            * link["amount"]
            / (electricity_per_service(service) * 3.6)
        )
        expected = original_purchase["amount"] / (
            spec["lifetime km"]
            * spec["passengers"]
            * electricity_per_service(original)
            * 3.6
        )
        assert actual == pytest.approx(expected, rel=2e-6)
        # Density is not a second multiplier on capacity. Other production
        # components and the source RER supplier are unchanged.
        for new, old in zip(producer["exchanges"], original_producer["exchanges"]):
            if old.get("product") != "electricity storage capacity":
                assert new["amount"] == old["amount"]
        assert (
            next(
                d for d in data if d["name"] == link["name"] and d["location"] == "RER"
            )
            == original_producer
        )
    before = deepcopy(data)
    resize_regional_batteries(tr)
    assert data == before

    # Rebuild the two actual per-MJ bus wrappers against regional suppliers.
    wrappers = ExcelImporter(INVENTORY_DIR / "lci-final-energy.xlsx").data
    for name, capacity, lifetime, passengers in (
        ("electricity, used in single deck coach or urban bus", 399, 700000, 10),
        ("electricity, used in single deck trolleybus", 13, 1000000, 10),
    ):
        wrapper = next(d for d in wrappers if d["name"] == name)
        for e in wrapper["exchanges"]:
            if "reference product" in e:
                e["product"] = e.pop("reference product")
        service = next(d for d in services if d["name"] == source_name(name))
        normalize(wrapper, service, data)
        amount = sum(
            e["amount"]
            for e in wrapper["exchanges"]
            if e["type"] == "technosphere"
            and e.get("product") == "electricity storage capacity"
        )
        expected = capacity / (
            lifetime
            * passengers
            * electricity_per_service(originals[service["name"]])
            * 3.6
        )
        assert amount == pytest.approx(expected, rel=2e-6)


@pytest.mark.parametrize("factor", [0, -1, float("nan"), float("inf")])
def test_bus_invalid_energy_cannot_create_a_pack(catalogue, factor):
    data = deepcopy(catalogue)
    service = next(
        d for d in data if d["name"].startswith("transport,") and SPEC_KEY in d
    )
    service["location"] = "NEU"
    service["regionalized"] = True
    for e in service["exchanges"]:
        if e.get("product") == "electricity, low voltage":
            e["amount"] *= factor
    tr = SimpleNamespace(
        year=2050, vehicle_type="bus", database=data, vehicle_map={"BEV": [service]}
    )
    with pytest.raises(ValueError, match="positive design|finite and nonnegative"):
        resize_regional_batteries(tr)
