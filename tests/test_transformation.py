from collections import defaultdict

import pytest
import xarray as xr

from premise.activity_maps import InventorySet
from premise.marginal_mixes import get_list_contrained_suppliers
from premise.transformation import BaseTransformation, find_fuel_efficiency


def make_market_transformation(monkeypatch, technology_shares):
    transformation = object.__new__(BaseTransformation)
    transformation.regions = ["WEU"]
    transformation.database = []
    transformation.year = 2050
    transformation.iam_to_ecoinvent_loc = {"WEU": ["RER"]}

    production_volumes = xr.DataArray(
        [[1.0] for _ in technology_shares],
        dims=("variables", "region"),
        coords={"variables": list(technology_shares), "region": ["WEU"]},
    )

    monkeypatch.setattr(
        transformation,
        "get_technology_and_regional_production_shares",
        lambda **kwargs: (
            production_volumes,
            {
                (technology, "WEU"): share
                for technology, share in technology_shares.items()
            },
            {"WEU": 1.0},
        ),
    )
    monkeypatch.setattr(
        transformation, "extract_market_ancillary_exchanges", lambda **kwargs: {}
    )
    monkeypatch.setattr(
        transformation, "add_geo_definition_metadata", lambda dataset: dataset
    )
    monkeypatch.setattr(transformation, "add_to_index", lambda dataset: None)
    monkeypatch.setattr(transformation, "write_log", lambda *args: None)
    monkeypatch.setattr(transformation, "is_in_index", lambda dataset, location: True)

    return transformation, production_volumes


def make_supplier(name, product="fuel"):
    return {
        "name": name,
        "reference product": product,
        "location": "WEU",
        "unit": "kilogram",
        "exchanges": [],
    }


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


def test_used_cooking_oil_biodiesel_is_constrained_in_marginal_mixes():
    assert (
        "biodiesel, from used cooking oil, with CCS"
        in get_list_contrained_suppliers()
    )


def test_cutoff_fuel_market_flips_treatment_supplier_sign_after_normalization(
    monkeypatch,
):
    transformation, production_volumes = make_market_transformation(
        monkeypatch,
        {"waste biodiesel": 0.8, "fossil diesel": 0.2},
    )
    mapping = {
        "waste biodiesel": [
            make_supplier(
                "treatment of used vegetable cooking oil",
                product="used vegetable cooking oil",
            )
        ],
        "fossil diesel": [make_supplier("diesel production, petroleum refinery")],
    }

    transformation.process_and_add_markets(
        name="market for diesel",
        reference_product="diesel",
        unit="kilogram",
        mapping=mapping,
        production_volumes=production_volumes,
        system_model="cutoff",
        flip_treatment_supplier_sign=True,
    )

    regional_market = next(
        dataset
        for dataset in transformation.database
        if dataset["location"] == "WEU"
    )
    suppliers = {
        exchange["name"]: exchange["amount"]
        for exchange in regional_market["exchanges"]
        if exchange["type"] == "technosphere"
    }
    assert suppliers["treatment of used vegetable cooking oil"] == pytest.approx(-0.8)
    assert suppliers["diesel production, petroleum refinery"] == pytest.approx(0.2)
