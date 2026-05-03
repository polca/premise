import sys
import types
from collections import defaultdict
from types import SimpleNamespace

try:
    import xarray  # noqa: F401
except ModuleNotFoundError:
    xarray_stub = types.ModuleType("xarray")
    xarray_stub.DataArray = object
    xarray_stub.Dataset = object
    sys.modules["xarray"] = xarray_stub

try:
    import schema  # noqa: F401
except ModuleNotFoundError:
    schema_stub = types.ModuleType("schema")

    class DummySchema:
        def __init__(self, *args, **kwargs):
            pass

        def validate(self, value):
            return value

    schema_stub.And = lambda *args, **kwargs: ("And", args, kwargs)
    schema_stub.Optional = lambda *args, **kwargs: ("Optional", args, kwargs)
    schema_stub.Schema = DummySchema
    schema_stub.Use = lambda *args, **kwargs: ("Use", args, kwargs)
    sys.modules["schema"] = schema_stub

from premise.transport import Transport


def make_transport(index_entries):
    transport = object.__new__(Transport)
    transport.vehicle_type = "train"
    transport.model = "gcam"
    transport.mapping = {
        "train": {
            "name": "transport, freight, train",
            "old": {
                "market group for transport, freight train": {
                    "gcam": "market for transport, freight, train"
                }
            },
        }
    }
    transport.geo = SimpleNamespace(ecoinvent_to_iam_location=lambda loc: "Middle East")
    transport.index = defaultdict(list, index_entries)
    transport.database = [
        {
            "name": "market for oxygen, liquid",
            "reference product": "oxygen, liquid",
            "location": "Middle East",
            "unit": "kilogram",
            "exchanges": [
                {
                    "name": "oxygen production",
                    "product": "oxygen, liquid",
                    "location": "Middle East",
                    "unit": "kilogram",
                    "type": "production",
                    "amount": 1,
                },
                {
                    "name": "market group for transport, freight train",
                    "product": "transport, freight train",
                    "location": "RoW",
                    "unit": "ton kilometer",
                    "type": "technosphere",
                    "amount": 1,
                },
            ],
        }
    ]
    return transport


def get_transport_exchange(transport):
    return transport.database[0]["exchanges"][1]


def test_relink_transport_datasets_falls_back_to_world_market():
    transport = make_transport(
        {
            ("market for transport, freight, train", "transport, freight, train"): [
                {"location": "World"}
            ]
        }
    )

    transport.relink_transport_datasets()

    exchange = get_transport_exchange(transport)
    assert exchange["name"] == "market for transport, freight, train"
    assert exchange["product"] == "transport, freight, train"
    assert exchange["location"] == "World"


def test_relink_transport_datasets_prefers_regional_market():
    transport = make_transport(
        {
            ("market for transport, freight, train", "transport, freight, train"): [
                {"location": "Middle East"},
                {"location": "World"},
            ]
        }
    )

    transport.relink_transport_datasets()

    exchange = get_transport_exchange(transport)
    assert exchange["name"] == "market for transport, freight, train"
    assert exchange["product"] == "transport, freight, train"
    assert exchange["location"] == "Middle East"


def test_relink_transport_datasets_keeps_original_link_if_no_market_exists():
    transport = make_transport({})

    transport.relink_transport_datasets()

    exchange = get_transport_exchange(transport)
    assert exchange["name"] == "market group for transport, freight train"
    assert exchange["product"] == "transport, freight train"
    assert exchange["location"] == "RoW"
