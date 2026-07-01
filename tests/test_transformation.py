from collections import defaultdict

import pytest
import xarray as xr

from premise.activity_maps import InventorySet
from premise.transformation import BaseTransformation, find_fuel_efficiency


def test_find_fuel_efficiency_uses_default_fuels_when_filter_is_none(capsys):
    dataset = {
        "name": "electricity production, biomass",
        "location": "GLO",
        "exchanges": [
            {
                "name": "market for wood chips, green, measured as dry mass",
                "amount": 1.0,
                "unit": "kilogram",
                "type": "technosphere",
            },
        ],
    }

    efficiency = find_fuel_efficiency(
        dataset=dataset,
        energy_out=3.6,
        fuel_specs={"wood chips": {"lhv": {"value": 18.0}}},
        fuel_map_reverse={
            "market for wood chips, green, measured as dry mass": "wood chips",
        },
        fuel_filters=None,
    )

    assert efficiency == pytest.approx(0.2)
    assert capsys.readouterr().out == ""


def test_find_fuel_efficiency_rejects_empty_filter():
    dataset = {
        "name": "electricity production, biomass",
        "location": "GLO",
        "exchanges": [],
    }

    with pytest.raises(ValueError, match="No fuel filters configured"):
        find_fuel_efficiency(
            dataset=dataset,
            energy_out=3.6,
            fuel_specs={},
            fuel_map_reverse={},
            fuel_filters=[],
        )


def test_find_fuel_efficiency_rejects_missing_fuel_input():
    dataset = {
        "name": "electricity production, biomass",
        "location": "GLO",
        "exchanges": [
            {
                "name": "market for steel, low-alloyed",
                "amount": 1.0,
                "unit": "kilogram",
                "type": "technosphere",
            },
        ],
    }

    with pytest.raises(ValueError, match="No fuel input found"):
        find_fuel_efficiency(
            dataset=dataset,
            energy_out=3.6,
            fuel_specs={"wood chips": {"lhv": {"value": 18.0}}},
            fuel_map_reverse={
                "market for wood chips, green, measured as dry mass": "wood chips",
            },
            fuel_filters=["market for wood chips, green, measured as dry mass"],
        )


def test_biomass_fuel_map_includes_green_wood_chips():
    fuel_dataset = {
        "name": "market for wood chips, green, measured as dry mass",
        "reference product": "wood chips, green, measured as dry mass",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [],
    }

    fuel_map = InventorySet(
        database=[fuel_dataset],
        version="3.12",
        model="image",
    ).generate_powerplant_fuels_map()

    assert fuel_dataset in fuel_map["Biomass CHP (existing)"]
    assert fuel_dataset in fuel_map["Biomass IGCC CCS"]


def test_process_and_add_activities_indexes_proxies_before_emptying(monkeypatch):
    original = {
        "name": "clinker production, test",
        "reference product": "clinker",
        "location": "Europe without Switzerland",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": "clinker production, test",
                "product": "clinker",
                "location": "Europe without Switzerland",
                "amount": 1.0,
                "unit": "kilogram",
                "type": "production",
            }
        ],
    }
    regionalized = {
        "name": "clinker production, test",
        "reference product": "clinker",
        "location": "WEU",
        "unit": "kilogram",
        "regionalized": True,
        "exchanges": [
            {
                "name": "clinker production, test",
                "product": "clinker",
                "location": "WEU",
                "amount": 1.0,
                "unit": "kilogram",
                "type": "production",
            }
        ],
    }

    transformation = object.__new__(BaseTransformation)
    transformation.regions = ["WEU"]
    transformation.database = [original]
    transformation.index = defaultdict(list)
    transformation.add_to_index(original)
    transformation.geo = type(
        "FakeGeo",
        (),
        {
            "ecoinvent_to_iam_location": staticmethod(
                lambda location: {"Europe without Switzerland": "WEU"}[location]
            )
        },
    )()

    calls = []
    real_add_to_index = BaseTransformation.add_to_index

    def record_add_to_index(self, datasets):
        calls.append("add_to_index")
        return real_add_to_index(self, datasets)

    def fake_fetch_proxies(self, **kwargs):
        return {"WEU": regionalized}

    def fake_add_geo_definition_metadata(self, dataset):
        return dataset

    def fake_empty_original_datasets(
        self, datasets, production_shares, loc_map, regions
    ):
        calls.append("empty_original_datasets")
        assert self.is_in_index(original, "WEU")
        assert datasets == [original]
        assert loc_map == {"Europe without Switzerland": "WEU"}
        assert production_shares == {"WEU": 1.0}

    def fake_write_log(self, dataset, status="created"):
        calls.append(f"write_log:{status}")

    monkeypatch.setattr(BaseTransformation, "add_to_index", record_add_to_index)
    monkeypatch.setattr(BaseTransformation, "fetch_proxies", fake_fetch_proxies)
    monkeypatch.setattr(
        BaseTransformation,
        "add_geo_definition_metadata",
        fake_add_geo_definition_metadata,
    )
    monkeypatch.setattr(
        BaseTransformation,
        "empty_original_datasets",
        fake_empty_original_datasets,
    )
    monkeypatch.setattr(BaseTransformation, "write_log", fake_write_log)

    transformation.process_and_add_activities(
        mapping={"cement, dry feed rotary kiln, efficient": [original]},
        regions=["WEU"],
    )

    assert calls == [
        "add_to_index",
        "empty_original_datasets",
        "write_log:created",
    ]
    assert regionalized in transformation.database


def test_process_and_add_activities_reuses_shared_regionalized_dataset(monkeypatch):
    original = {
        "name": "carbon dioxide, captured and stored, at wood burning power plant",
        "reference product": "carbon dioxide, captured",
        "location": "RER",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": "carbon dioxide, captured and stored, at wood burning power plant",
                "product": "carbon dioxide, captured",
                "location": "RER",
                "amount": 1.0,
                "unit": "kilogram",
                "type": "production",
            }
        ],
    }
    regionalized = {
        **original,
        "location": "WEU",
        "regionalized": True,
        "exchanges": [
            {
                "name": original["name"],
                "product": original["reference product"],
                "location": "WEU",
                "amount": 1.0,
                "unit": "kilogram",
                "type": "production",
            }
        ],
    }

    transformation = object.__new__(BaseTransformation)
    transformation.regions = ["WEU"]
    transformation.database = [original]
    transformation.index = defaultdict(list)
    transformation.add_to_index(original)
    transformation.geo = type(
        "FakeGeo",
        (),
        {
            "ecoinvent_to_iam_location": staticmethod(
                lambda location: {"RER": "WEU"}[location]
            )
        },
    )()

    fetch_calls = []

    def fake_fetch_proxies(self, **kwargs):
        fetch_calls.append(kwargs)
        return {"WEU": dict(regionalized)}

    monkeypatch.setattr(BaseTransformation, "fetch_proxies", fake_fetch_proxies)
    monkeypatch.setattr(
        BaseTransformation, "add_geo_definition_metadata", lambda self, dataset: dataset
    )
    monkeypatch.setattr(
        BaseTransformation, "empty_original_datasets", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(BaseTransformation, "write_log", lambda *args, **kwargs: None)

    mapping = {
        "biomass power generation, with CCS": [original],
        "biomass heat generation, with CCS": [dict(original)],
    }

    transformation.process_and_add_activities(mapping=mapping, regions=["WEU"])

    assert len(fetch_calls) == 1
    assert any(ds.get("regionalized") for ds in mapping["biomass power generation, with CCS"])
    assert any(ds.get("regionalized") for ds in mapping["biomass heat generation, with CCS"])
    assert len(
        [
            ds
            for ds in transformation.database
            if ds["name"] == original["name"]
            and ds.get("location") == "WEU"
            and ds.get("regionalized")
        ]
    ) == 1


def test_process_and_add_activities_deduplicates_shared_mapping_lists(monkeypatch):
    original_a = {
        "name": "amine-based silica production, test",
        "reference product": "amine-based silica",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": "amine-based silica production, test",
                "product": "amine-based silica",
                "location": "GLO",
                "amount": 1.0,
                "unit": "kilogram",
                "type": "production",
            }
        ],
    }
    original_b = {
        "name": "polyethyleneimine production, test",
        "reference product": "polyethyleneimine",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": "polyethyleneimine production, test",
                "product": "polyethyleneimine",
                "location": "GLO",
                "amount": 1.0,
                "unit": "kilogram",
                "type": "production",
            }
        ],
    }

    regionalized = {
        original_a["name"]: {**original_a, "location": "WEU", "regionalized": True},
        original_b["name"]: {**original_b, "location": "WEU", "regionalized": True},
    }

    transformation = object.__new__(BaseTransformation)
    transformation.regions = ["WEU"]
    transformation.database = [original_a, original_b]
    transformation.index = defaultdict(list)
    transformation.add_to_index([original_a, original_b])
    transformation.geo = type(
        "FakeGeo",
        (),
        {
            "ecoinvent_to_iam_location": staticmethod(
                lambda location: {"GLO": "WEU"}[location]
            )
        },
    )()

    fetch_calls = []

    def fake_fetch_proxies(self, **kwargs):
        fetch_calls.append(tuple(ds["name"] for ds in kwargs["datasets"]))
        dataset = regionalized[kwargs["datasets"][0]["name"]]
        return {"WEU": dict(dataset)}

    monkeypatch.setattr(BaseTransformation, "fetch_proxies", fake_fetch_proxies)
    monkeypatch.setattr(
        BaseTransformation, "add_geo_definition_metadata", lambda self, dataset: dataset
    )
    monkeypatch.setattr(
        BaseTransformation, "empty_original_datasets", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(BaseTransformation, "write_log", lambda *args, **kwargs: None)

    shared_activities = [original_a, original_b]
    mapping = {
        "support route a": shared_activities,
        "support route b": shared_activities,
    }

    transformation.process_and_add_activities(mapping=mapping, regions=["WEU"])

    assert mapping["support route a"] is not mapping["support route b"]
    assert fetch_calls == [
        (original_a["name"],),
        (original_b["name"],),
    ]

    for activities in mapping.values():
        identities = [
            (ds["name"], ds["reference product"], ds["location"]) for ds in activities
        ]
        assert len(identities) == len(set(identities))

    for original in (original_a, original_b):
        assert (
            len(
                [
                    ds
                    for ds in transformation.database
                    if ds["name"] == original["name"]
                    and ds.get("location") == "WEU"
                    and ds.get("regionalized")
                ]
            )
            == 1
        )


def test_process_and_add_markets_deduplicates_shared_supplier_exchanges():
    transformation = object.__new__(BaseTransformation)
    transformation.regions = ["WEU"]
    transformation.year = 2050
    transformation.database = []
    transformation.iam_to_ecoinvent_loc = {"WEU": ["WEU"]}

    transformation.extract_market_ancillary_exchanges = lambda **kwargs: {}
    transformation.add_geo_definition_metadata = lambda dataset: dataset
    transformation.add_to_index = lambda dataset: None
    transformation.write_log = lambda dataset, status="created": None
    transformation.empty_original_datasets = lambda **kwargs: None
    transformation.is_in_index = lambda candidate, region: False

    shared_supplier = {
        "name": "carbon dioxide, captured and stored, at wood burning power plant",
        "reference product": "carbon dioxide, captured",
        "location": "WEU",
        "unit": "kilogram",
        "production volume": 10,
    }
    other_supplier = {
        "name": "carbon dioxide, captured and stored, by olivine spreading",
        "reference product": "carbon dioxide, captured",
        "location": "WEU",
        "unit": "kilogram",
        "production volume": 10,
    }
    production_volumes = xr.DataArray(
        [[[60.0]], [[40.0]]],
        dims=("variables", "region", "year"),
        coords={
            "variables": ["biomass electricity", "biomass heat"],
            "region": ["WEU"],
            "year": [2050],
        },
    )

    transformation.process_and_add_markets(
        name="market for carbon dioxide removal",
        reference_product="carbon dioxide, captured and stored",
        unit="kilogram",
        mapping={
            "biomass electricity": [
                shared_supplier,
                dict(shared_supplier),
                other_supplier,
            ],
            "biomass heat": [shared_supplier],
        },
        production_volumes=production_volumes,
    )

    market = next(
        dataset
        for dataset in transformation.database
        if dataset["name"] == "market for carbon dioxide removal"
        and dataset["location"] == "WEU"
    )
    technosphere = [
        exchange
        for exchange in market["exchanges"]
        if exchange["type"] == "technosphere"
    ]

    assert len(technosphere) == 2
    amounts = {
        exchange["name"]: exchange["amount"]
        for exchange in technosphere
    }
    assert amounts[
        "carbon dioxide, captured and stored, at wood burning power plant"
    ] == pytest.approx(0.7)
    assert amounts[
        "carbon dioxide, captured and stored, by olivine spreading"
    ] == pytest.approx(0.3)
