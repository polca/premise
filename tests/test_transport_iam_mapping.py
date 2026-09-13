from pathlib import Path
from types import SimpleNamespace

import yaml

import premise.transport as transport_module
from premise.transport import Transport

MAPPING = (
    Path(__file__).parents[1]
    / "premise"
    / "iam_variables_mapping"
    / "transport_road_freight.yaml"
)


def test_image_bev_truck_aliases_use_preprocessed_residual_variables():
    mapping = yaml.safe_load(MAPPING.read_text(encoding="utf-8"))

    expected = {
        "truck, battery electric, 18 metric ton": (
            "Energy Service|Transportation|Freight|Medium Truck|Battery Electric",
            "Final Energy|Transportation|Freight|Medium Truck|Battery Electric|Electricity",
        ),
        "truck, battery electric, 40 metric ton": (
            "Energy Service|Transportation|Freight|Heavy Truck|Battery Electric",
            "Final Energy|Transportation|Freight|Heavy Truck|Battery Electric|Electricity",
        ),
    }

    for technology, (service, energy) in expected.items():
        assert mapping[technology]["iam_aliases"]["image"] == service
        assert mapping[technology]["energy_use_aliases"]["image"] == energy


def test_image_aggregate_truck_electricity_is_not_mapped_as_bev_service():
    mapping = yaml.safe_load(MAPPING.read_text(encoding="utf-8"))
    bev_aliases = {
        mapping[technology]["energy_use_aliases"]["image"]
        for technology in (
            "truck, battery electric, 18 metric ton",
            "truck, battery electric, 40 metric ton",
        )
    }

    assert (
        "Final Energy|Transportation|Freight|Medium Truck|Electricity"
        not in bev_aliases
    )
    assert (
        "Final Energy|Transportation|Freight|Heavy Truck|Electricity" not in bev_aliases
    )


def test_transport_does_not_retain_provider_index_as_relinking_cache(monkeypatch):
    captured = {}

    def fake_base_init(instance, *args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs

    class FakeInventorySet:
        def __init__(self, **kwargs):
            pass

        @staticmethod
        def generate_transport_map(transport_type):
            return {}

        @staticmethod
        def generate_vehicle_fuel_map(transport_type):
            return {}

    monkeypatch.setattr(transport_module.BaseTransformation, "__init__", fake_base_init)
    monkeypatch.setattr(transport_module, "InventorySet", FakeInventorySet)
    monkeypatch.setattr(transport_module, "get_vehicles_mapping", lambda: {})
    monkeypatch.setattr(transport_module, "get_battery_size", lambda: {})

    provider_index = {("market", "service"): [{"location": "GLO"}]}
    iam_data = SimpleNamespace()
    Transport(
        database=[],
        iam_data=iam_data,
        model="image",
        pathway="SSP2-M",
        year=2050,
        version="3.12",
        system_model="cutoff",
        relink=False,
        vehicle_type="car",
        has_fleet=True,
        index=provider_index,
    )

    assert captured["args"] == (
        [],
        iam_data,
        "image",
        "SSP2-M",
        2050,
        "3.12",
        "cutoff",
    )
    assert captured["kwargs"] == {"cache": None, "index": None}

    Transport(
        database=[],
        iam_data=iam_data,
        model="image",
        pathway="SSP2-M",
        year=2050,
        version="3.12",
        system_model="cutoff",
        relink=False,
        vehicle_type="car",
        has_fleet=True,
        index=provider_index,
        reuse_index=True,
    )

    assert captured["kwargs"] == {"cache": None, "index": provider_index}


def test_transport_relink_scans_legacy_market_names_by_membership():
    matching = {
        "name": "old freight market",
        "product": "freight",
        "location": "GLO",
        "unit": "ton kilometer",
        "type": "technosphere",
        "amount": 2.0,
    }
    skipped_by_dataset_unit = matching.copy()
    transport = object.__new__(Transport)
    transport.vehicle_type = "truck"
    transport.model = "image"
    transport.mapping = {
        "truck": {
            "name": "transport, freight, lorry",
            "old": {"old freight market": {"image": "new freight market"}},
        }
    }
    transport.database = [
        {
            "name": "consumer",
            "location": "CH",
            "unit": "kilogram",
            "exchanges": [matching, {**matching, "unit": "kilogram"}],
        },
        {
            "name": "transport service",
            "location": "CH",
            "unit": "ton kilometer",
            "exchanges": [skipped_by_dataset_unit],
        },
    ]
    transport._find_available_transport_market = lambda **kwargs: (
        "new freight market",
        "EUR",
    )

    transport.relink_transport_datasets()

    assert matching["name"] == "new freight market"
    assert matching["product"] == "transport, freight, lorry"
    assert matching["location"] == "EUR"
    assert skipped_by_dataset_unit["name"] == "old freight market"
