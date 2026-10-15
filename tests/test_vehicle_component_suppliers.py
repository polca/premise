"""Shared vehicle components resolve to one canonical bundled supplier."""

from bw2io import ExcelImporter

from premise.filesystem_constants import INVENTORY_DIR


def _load(stem):
    return ExcelImporter(str(INVENTORY_DIR / f"{stem}.xlsx")).data


def test_refreshed_truck_components_use_canonical_names():
    trucks = _load("lci-trucks")
    names = {dataset["name"] for dataset in trucks}
    canonical = {
        "assembly operation, for lorry",
        "exhaust system, for lorry",
        "frame, blanks and saddle, for lorry",
        "fuel tank assembly, compressed natural gas, 200 bar",
        "gearbox, for lorry",
        "internal combustion engine, for lorry",
        "lead acid battery, for lorry",
        "other components, for hybrid electric lorry",
        "power electronics, for lorry",
        "retarder, for lorry",
        "suspension, for lorry",
        "tires and wheels, for lorry",
        "transmission, for lorry",
        "fuel tank, for diesel vehicle",
        "EV charger, level 3, plugin, 200 kW",
        "storage battery",
        "charger",
        "charging stand",
        "fuel tank, compressed hydrogen gas, 700bar, with HDPE liner",
        "other components, for electric lorry",
    }
    suffix = ", carculator regional truck 2020"

    assert canonical <= names
    assert not {f"{name}{suffix}" for name in canonical} & names


def test_shared_component_suppliers_are_unique_and_consumers_use_them():
    inventories = {
        stem: _load(stem)
        for stem in ("lci-trucks", "lci-buses", "lci-final-energy", "lci-ships")
    }
    suppliers = {
        "EV charger, level 3, plugin, 200 kW": "lci-trucks",
        "Electrical equipment": "lci-buses",
        "Transformers": "lci-buses",
    }

    for name, expected_workbook in suppliers.items():
        producers = [
            stem
            for stem, datasets in inventories.items()
            if any(dataset["name"] == name for dataset in datasets)
        ]
        assert producers == [expected_workbook]

    old_tank = "fuel tank assembly, compressed hydrogen gas, 700bar"
    for datasets in inventories.values():
        assert not any(
            exchange.get("name") == old_tank
            for dataset in datasets
            for exchange in dataset.get("exchanges", [])
        )

    buses = inventories["lci-buses"]
    (catenary,) = [dataset for dataset in buses if dataset["name"] == "Catenary system"]
    catenary_suppliers = {
        exchange["name"]
        for exchange in catenary["exchanges"]
        if exchange["type"] == "technosphere"
    }
    assert {"Electrical equipment", "Transformers"} <= catenary_suppliers


def test_all_bundled_hydrogen_tank_consumers_resolve():
    """Check cross-workbook closure, including rail, without an ecoinvent licence."""
    from collections import Counter

    datasets = [
        dataset
        for stem in (
            "lci-trucks",
            "lci-buses",
            "lci-pass_cars",
            "lci-rail-freight",
            "lci-final-energy",
            "lci-ships",
        )
        for dataset in _load(stem)
    ]

    def identity(record):
        return (
            record["name"].lower(),
            record["reference product"].lower(),
            record["location"],
            record["unit"],
        )

    suppliers = Counter(identity(dataset) for dataset in datasets)
    checked = []
    for dataset in datasets:
        for exchange in dataset["exchanges"]:
            if exchange["type"] != "technosphere":
                continue
            if exchange.get("reference product", "").lower() != "hydrogen tank":
                continue
            assert suppliers[identity(exchange)] == 1, (
                dataset["name"],
                identity(exchange),
                suppliers[identity(exchange)],
            )
            checked.append(dataset["name"])
    assert "production of locomotive, fuel cell, hydrogen" in checked
    assert "hydrogen, used in a fuel cell in a passenger train" in checked
