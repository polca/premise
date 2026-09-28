from collections import defaultdict
from collections.abc import Mapping
from contextlib import contextmanager
from types import SimpleNamespace

import numpy as np
import pytest
import xarray as xr

import premise.transformation as transformation_module
from premise.activity_maps import InventorySet
from premise.marginal_mixes import get_list_contrained_suppliers
from premise.transformation import (
    BaseTransformation,
    _exclude_exchange_objects,
    clone_inventory_dataset,
    find_fuel_efficiency,
    prepare_fuel_filters,
)


def test_exclude_exchange_objects_does_not_compare_mapping_contents():
    class EqualityRaises(dict):
        def __eq__(self, other):
            raise AssertionError("exchange equality must not be evaluated")

    selected = EqualityRaises(amount=1)
    equal_but_distinct = EqualityRaises(amount=1)

    assert _exclude_exchange_objects([selected, equal_but_distinct], [selected]) == [
        equal_but_distinct
    ]


def test_transient_validation_cleanup_only_touches_registered_activities():
    transformation = object.__new__(BaseTransformation)
    transformation._validation_provenance_targets = {}
    target = {
        "exchanges": [
            {"premise market technology": "diesel"},
            {"type": "production"},
        ]
    }
    unrelated = {"exchanges": [{"premise market technology": "must remain untouched"}]}

    transformation.track_validation_provenance(target, "premise market technology")
    transformation.clear_validation_provenance_field("premise market technology")

    assert "premise market technology" not in target["exchanges"][0]
    assert unrelated["exchanges"][0]["premise market technology"] == (
        "must remain untouched"
    )


def test_summarize_exchanges_uses_compact_payloads():
    transformation = object.__new__(BaseTransformation)
    exchanges = [
        {
            "name": "market for fuel",
            "product": "fuel",
            "location": "GLO",
            "unit": "kilogram",
            "amount": 1.5,
        },
        {
            "name": "market for fuel",
            "product": "fuel",
            "location": "GLO",
            "unit": "kilogram",
            "amount": 2.5,
        },
    ]

    summarized = transformation.summarize_exchanges(exchanges)

    assert len(summarized) == 1
    assert summarized[0] == {
        "name": "market for fuel",
        "product": "fuel",
        "location": "GLO",
        "unit": "kilogram",
        "type": "technosphere",
        "amount": 4.0,
    }
    assert bool(getattr(summarized[0], "_premise_compact_exchange", False)) is True


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


def test_production_share_extraction_preserves_coordinate_order_and_scalars():
    transformation = object.__new__(BaseTransformation)
    transformation.regions = ["R1", "R2", "World"]
    transformation.year = 2050
    production_volumes = xr.DataArray(
        [
            [[3.0], [1.0]],
            [[1.0], [3.0]],
        ],
        dims=("variables", "region", "year"),
        coords={
            "variables": ["tech-a", "tech-b"],
            "region": ["R1", "R2"],
            "year": [2050],
        },
    )
    mapping = {"tech-b": {}, "missing": {}, "tech-a": {}}

    selected, technology_shares, regional_shares = (
        transformation.get_technology_and_regional_production_shares(
            production_volumes, mapping
        )
    )

    assert selected.variables.values.tolist() == ["tech-b", "tech-a"]
    assert list(technology_shares) == [
        ("tech-b", "R1"),
        ("tech-b", "R2"),
        ("tech-a", "R1"),
        ("tech-a", "R2"),
    ]
    assert technology_shares == {
        ("tech-b", "R1"): 0.25,
        ("tech-b", "R2"): 0.75,
        ("tech-a", "R1"): 0.75,
        ("tech-a", "R2"): 0.25,
    }
    assert regional_shares == {"R1": 0.5, "R2": 0.5}
    assert all(type(value) is float for value in technology_shares.values())
    assert all(type(value) is float for value in regional_shares.values())


def test_inventory_dataset_clone_is_lossless_and_isolated():
    class CustomMetadataKey(str):
        pass

    shared_metadata = {
        "values": [np.float64(2.5), ("CPC", "171")],
    }
    custom_key = CustomMetadataKey("custom key")
    dataset = {
        "name": "proxy template",
        "reference product": "service",
        "location": "GLO",
        "custom": shared_metadata,
        "same custom": shared_metadata,
        custom_key: "custom value",
        "array": np.array([1.0, 2.0]),
        "exchanges": [
            {
                "name": "flow",
                "amount": np.float64(1.0),
                "type": "biosphere",
                "metadata": shared_metadata,
            }
        ],
    }

    cloned = clone_inventory_dataset(dataset)

    assert cloned["name"] == dataset["name"]
    assert cloned["custom"] == dataset["custom"]
    assert cloned["custom"] is cloned["same custom"]
    assert cloned["custom"] is cloned["exchanges"][0]["metadata"]
    assert cloned["custom"] is not shared_metadata
    assert custom_key in cloned
    assert type(next(key for key in cloned if key == custom_key)) is CustomMetadataKey
    assert np.array_equal(cloned["array"], dataset["array"])
    assert cloned["array"] is not dataset["array"]
    assert type(cloned["exchanges"][0]["amount"]) is np.float64
    assert cloned["exchanges"][0]["amount"] is dataset["exchanges"][0]["amount"]

    cloned["custom"]["values"].append("changed")
    cloned["array"][0] = 99
    cloned["exchanges"][0]["amount"] = 3.0

    assert dataset["custom"]["values"] == [np.float64(2.5), ("CPC", "171")]
    assert dataset["array"].tolist() == [1.0, 2.0]
    assert dataset["exchanges"][0]["amount"] == np.float64(1.0)


def test_relink_allocation_uses_minimal_internal_candidates_only():
    exchange = {
        "name": "market for fuel",
        "product": "fuel",
        "location": "GLO",
        "unit": "kilogram",
        "type": "technosphere",
        "amount": 8.0,
        "uncertainty type": 2,
        "loc": np.log(8.0),
        "custom metadata": {"kept": True},
    }
    providers = [
        {
            "name": "market for fuel",
            "location": "CH",
            "production volume": 3.0,
        },
        {
            "name": "market for fuel",
            "location": "DE",
            "production volume": 1.0,
        },
    ]

    default, default_shares = transformation_module.allocate_inputs(
        exchange.copy(), providers
    )
    minimal, minimal_shares = transformation_module.allocate_inputs(
        exchange.copy(),
        providers,
        exchange_factory=transformation_module._relinked_exchange,
    )

    assert default[0]["custom metadata"] == {"kept": True}
    assert default_shares == minimal_shares == [0.75, 0.25]
    assert minimal == [
        {
            "name": "market for fuel",
            "product": "fuel",
            "location": "CH",
            "unit": "kilogram",
            "type": "technosphere",
            "amount": 6.0,
        },
        {
            "name": "market for fuel",
            "product": "fuel",
            "location": "DE",
            "unit": "kilogram",
            "type": "technosphere",
            "amount": 2.0,
        },
    ]


def test_relink_prepass_preserves_order_totals_and_uncertainty(monkeypatch):
    transformation = object.__new__(BaseTransformation)
    production = {
        "name": "fuel production",
        "product": "fuel",
        "location": "GLO",
        "unit": "kilogram",
        "type": "production",
        "amount": 1.0,
    }
    biosphere = {
        "name": "emission",
        "unit": "kilogram",
        "type": "biosphere",
        "amount": 0.2,
    }
    zero = {
        "name": "market for fuel",
        "product": "fuel",
        "location": "GLO",
        "unit": "kilogram",
        "type": "technosphere",
        "amount": 0.0,
    }
    uncertain = {
        **zero,
        "amount": 2.0,
        "uncertainty type": 2,
        "loc": np.log(2.0),
        "scale": 0.2,
    }
    regular = {**zero, "amount": 3.0}
    dataset = {
        "name": "consumer",
        "location": "CH",
        "exchanges": [production, zero, biosphere, uncertain, regular],
    }
    selected = []

    def find_candidates(dataset, *, technosphere_exchanges, **kwargs):
        selected.extend(technosphere_exchanges)
        return [
            transformation_module._relinked_exchange(exchange, "GLO")
            for exchange in technosphere_exchanges
        ]

    monkeypatch.setattr(transformation, "find_candidates", find_candidates)

    result = transformation.relink_technosphere_exchanges(dataset)

    assert selected == [uncertain, regular]
    assert result["exchanges"][:2] == [production, biosphere]
    assert result["exchanges"][2] == {
        "name": "market for fuel",
        "product": "fuel",
        "location": "GLO",
        "unit": "kilogram",
        "type": "technosphere",
        "amount": 5.0,
        "uncertainty type": 2,
        "loc": np.log(5.0),
        "scale": 0.2,
    }
    assert sum(exchange["amount"] for exchange in result["exchanges"]) == 6.2


def test_provider_groups_preserve_order_and_invalidate_after_index_mutation():
    transformation = object.__new__(BaseTransformation)
    key = ("market for fuel", "fuel")
    first_provider = {
        "name": key[0],
        "reference product": key[1],
        "location": "GLO",
        "unit": "kilogram",
        "production volume": 2.0,
    }
    second_provider = {
        "name": key[0],
        "reference product": key[1],
        "location": "RoW",
        "unit": "kilogram",
        "production volume": 1.0,
    }
    transformation.index = defaultdict(list, {key: [first_provider, second_provider]})
    transformation._provider_index_generation = 0
    transformation._provider_group_cache = {}
    transformation._provider_location_cache = {}
    transformation._provider_semantic_index = None
    exchange = {"name": key[0], "product": key[1]}

    first = transformation._get_provider_groups(exchange)
    repeated = transformation._get_provider_groups(exchange)
    first_semantics = transformation._get_provider_semantic_index()

    assert repeated is first
    assert transformation._get_provider_semantic_index() is first_semantics
    assert first[2] == ["GLO", "RoW"]
    assert first[4]["GLO"] == [first_provider]
    assert transformation.is_in_index({**exchange, "location": "RoW"})
    first_locations = transformation._get_provider_locations(key)

    added = {
        "name": key[0],
        "reference product": key[1],
        "location": "CH",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": key[0],
                "product": key[1],
                "location": "CH",
                "unit": "kilogram",
                "type": "production",
                "amount": 1.0,
                "production volume": 3.0,
            }
        ],
    }
    transformation.add_to_index(added)
    after_addition = transformation._get_provider_groups(exchange)

    assert after_addition is not first
    assert after_addition[2] == ["GLO", "RoW", "CH"]
    assert transformation.is_in_index({**exchange, "location": "CH"})
    assert transformation._get_provider_semantic_index() is first_semantics
    assert first_semantics[(key[0], key[1], "CH")] == 1
    assert transformation._get_provider_locations(key) is not first_locations

    transformation.add_to_index(added)
    assert first_semantics[(key[0], key[1], "CH")] == 2

    transformation._provider_semantic_index = None
    transformation.remove_from_index(added)
    added_semantics = transformation._get_provider_semantic_index()
    after_duplicate_removal = transformation._get_provider_groups(exchange)

    assert after_duplicate_removal[2] == ["GLO", "RoW", "CH"]
    assert transformation._get_provider_semantic_index() is added_semantics
    assert added_semantics[(key[0], key[1], "CH")] == 1
    assert transformation.is_in_index({**exchange, "location": "CH"})

    transformation.remove_from_index(added)
    after_removal = transformation._get_provider_groups(exchange)

    assert after_removal is not after_duplicate_removal
    assert after_removal[2] == ["GLO", "RoW"]
    assert transformation._get_provider_semantic_index() is added_semantics
    assert (key[0], key[1], "CH") not in added_semantics
    assert not transformation.is_in_index({**exchange, "location": "CH"})


def test_provider_index_records_are_compact_immutable_mappings():
    production_volume = np.float64(2.5)
    dataset = {
        "name": "market for fuel",
        "reference product": "fuel",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": "market for fuel",
                "product": "fuel",
                "location": "GLO",
                "unit": "kilogram",
                "type": "production",
                "amount": 1.0,
                "production volume": production_volume,
            }
        ],
    }
    transformation = object.__new__(BaseTransformation)
    transformation.database = [dataset]

    record = transformation.create_index()[("market for fuel", "fuel")][0]

    assert isinstance(record, Mapping)
    assert not isinstance(record, dict)
    assert dict(record) == {
        "name": "market for fuel",
        "reference product": "fuel",
        "location": "GLO",
        "unit": "kilogram",
        "production volume": production_volume,
    }
    assert record.get("location") == "GLO"
    assert record.get("missing") is None
    assert record["production volume"] is production_volume
    with pytest.raises((AttributeError, TypeError)):
        record.location = "CH"


def test_provider_record_stops_after_first_production_exchange():
    class UnreachableExchange:
        def __getitem__(self, key):
            raise AssertionError("provider lookup scanned past first production")

    dataset = {
        "name": "market for fuel",
        "reference product": "fuel",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [
            {
                "type": "production",
                "production volume": 2.0,
            },
            UnreachableExchange(),
        ],
    }

    record = transformation_module._provider_record(dataset)

    assert record["production volume"] == 2.0


def test_provider_record_preserves_missing_production_error():
    dataset = {
        "name": "market for fuel",
        "reference product": "fuel",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [{"type": "technosphere"}],
    }

    with pytest.raises(IndexError, match="list index out of range"):
        transformation_module._provider_record(dataset)


def test_fetch_proxies_extracts_regional_production_volumes_once():
    transformation = object.__new__(BaseTransformation)
    transformation.database = []
    transformation.is_in_index = lambda dataset, location: False
    transformation.relink_technosphere_exchanges = lambda dataset: dataset
    dataset = {
        "name": "fuel production",
        "reference product": "fuel",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": "fuel production",
                "product": "fuel",
                "location": "GLO",
                "unit": "kilogram",
                "type": "production",
                "amount": 1.0,
            }
        ],
    }
    production_volumes = xr.DataArray(
        [[2.0, 3.0]],
        dims=("year", "region"),
        coords={"year": [2050], "region": ["R1", "R2"]},
    )

    proxies = transformation.fetch_proxies(
        datasets=[dataset],
        production_volumes=production_volumes,
        geo_mapping={"R2": dataset, "World": dataset},
        unlist=False,
    )

    regional_production = next(transformation_module.ws.production(proxies["R2"]))
    world_production = next(transformation_module.ws.production(proxies["World"]))
    assert regional_production["location"] == "R2"
    assert regional_production["production volume"] == 3.0
    assert world_production["location"] == "World"
    assert world_production["production volume"] == 5.0


def test_find_new_exchange_entries_accepts_preaggregated_amount(monkeypatch):
    transformation = object.__new__(BaseTransformation)
    transformation.cache = {}
    transformation.model = "image"
    transformation.find_alternative_locations = lambda act, exc, alt_names: [
        (
            exc["name"],
            exc["product"],
            "World",
            exc["unit"],
            1.0,
        )
    ]
    exchange = {
        "name": "market for fuel",
        "product": "fuel",
        "location": "GLO",
        "unit": "kilogram",
    }

    monkeypatch.setattr(
        transformation_module.ws,
        "technosphere",
        lambda dataset: (_ for _ in ()).throw(
            AssertionError("preaggregated amounts must not rescan exchanges")
        ),
    )

    entries, amount = transformation.find_new_exchange_entries(
        {"location": "CH"},
        exchange,
        [],
        amount=np.float64(3.25),
    )

    assert entries == [("market for fuel", "fuel", "World", "kilogram", 1.0)]
    assert type(amount) is np.float64
    assert amount == np.float64(3.25)


def test_gis_resolution_cache_is_shared_between_sector_instances(monkeypatch):
    match_calls = []
    row_calls = []

    class FakeMatcher:
        def __init__(self):
            self.locations = {"CH", "DE", "FR"}
            self.rows = {}

        def __contains__(self, key):
            return key in self.locations or key in self.rows

        def __getitem__(self, key):
            return self.rows[key]

        def __setitem__(self, key, value):
            self.rows[key] = value

        def __delitem__(self, key):
            del self.rows[key]

        @staticmethod
        def intersects(*args, **kwargs):
            match_calls.append((args, kwargs))
            return ("DE",)

        contained = intersects

    class FakeGeo:
        iam_regions = []

        def __init__(self, model):
            self.model = model
            self.geo = FakeMatcher()

        @staticmethod
        def ecoinvent_to_iam_location(location):
            return location

    @contextmanager
    def fake_resolved_row(possible_locations, geomatcher):
        row_calls.append(tuple(possible_locations))
        geomatcher["RoW"] = {"unused-face"}
        try:
            yield geomatcher
        finally:
            del geomatcher["RoW"]

    monkeypatch.setattr(transformation_module, "Geomap", FakeGeo)
    monkeypatch.setattr(transformation_module, "resolved_row", fake_resolved_row)
    monkeypatch.setattr(transformation_module, "get_fuel_properties", lambda: {})

    shared_cache = {}
    iam_data = SimpleNamespace(regions=[])
    instances = [
        BaseTransformation(
            database=[],
            iam_data=iam_data,
            model="image",
            pathway="SSP2-M",
            year=2050,
            version="3.12",
            system_model="cutoff",
            cache=shared_cache,
            index=defaultdict(list),
        )
        for _ in range(2)
    ]

    first = instances[0].get_gis_match("CH", ("DE",), False, True, False)
    repeated = instances[1].get_gis_match("CH", ("DE",), False, True, False)
    another_location = instances[0].get_gis_match("FR", ("DE",), False, True, False)

    assert first == repeated == another_location == ("DE",)
    assert instances[0]._gis_match_cache is instances[1]._gis_match_cache
    assert (
        instances[0]._resolved_row_faces_cache is instances[1]._resolved_row_faces_cache
    )
    assert len(match_calls) == 2
    assert len(row_calls) == 1


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


def test_find_fuel_efficiency_rejects_empty_prepared_filter():
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
            fuel_filters=prepare_fuel_filters([]),
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


def test_prepared_fuel_filters_preserve_sanitized_prefix_matching():
    filters = prepare_fuel_filters(
        [
            "market for hard coal, at mine",
            "market group for natural gas, high pressure",
            "market for wood chips, used as fuel",
        ]
    )

    assert filters.matches("market group for hard coal")
    assert filters.matches("market for natural gas, low pressure")
    assert filters.matches("market for wood chips")
    assert not filters.matches("market for steel, low-alloyed")


def test_find_fuel_efficiency_accepts_prepared_fuel_filters():
    dataset = {
        "name": "electricity production, hard coal",
        "location": "GLO",
        "exchanges": [
            {
                "name": "market group for hard coal",
                "amount": 0.2,
                "unit": "kilogram",
                "type": "technosphere",
            },
        ],
    }

    efficiency = find_fuel_efficiency(
        dataset=dataset,
        energy_out=3.6,
        fuel_specs={"hard coal": {"lhv": {"value": 18.0}}},
        fuel_map_reverse={"hard coal": "hard coal"},
        fuel_filters=prepare_fuel_filters(["market for hard coal, at mine"]),
    )

    assert efficiency == pytest.approx(1.0)


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
    suppliers = get_list_contrained_suppliers()

    assert isinstance(suppliers, list)
    assert "biodiesel, from used cooking oil, with CCS" in suppliers
    assert "liquefied petroleum gas, synthetic, from coal" in suppliers
    assert "diesel" not in suppliers
    assert "liquefied petroleum gas" not in suppliers


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
        dataset for dataset in transformation.database if dataset["location"] == "WEU"
    )
    suppliers = {
        exchange["name"]: exchange["amount"]
        for exchange in regional_market["exchanges"]
        if exchange["type"] == "technosphere"
    }
    assert suppliers["treatment of used vegetable cooking oil"] == pytest.approx(-0.8)
    assert suppliers["diesel production, petroleum refinery"] == pytest.approx(0.2)


def test_market_conversion_factors_can_be_region_specific(monkeypatch):
    transformation, production_volumes = make_market_transformation(
        monkeypatch, {"technology one": 0.5, "technology two": 0.5}
    )
    transformation.process_and_add_markets(
        name="market for test product",
        reference_product="test product",
        unit="kilogram",
        mapping={
            "technology one": [make_supplier("supplier one")],
            "technology two": [make_supplier("supplier two")],
        },
        production_volumes=production_volumes,
        conversion_factor={
            ("technology one", "WEU"): 0.5,
            ("technology two", "WEU"): 1.0,
        },
    )
    market = next(ds for ds in transformation.database if ds["location"] == "WEU")
    amounts = {
        exc["name"]: exc["amount"]
        for exc in market["exchanges"]
        if exc["type"] == "technosphere"
    }
    assert amounts == {
        "supplier one": pytest.approx(1 / 3),
        "supplier two": pytest.approx(2 / 3),
    }


def test_market_does_not_require_supplier_for_zero_share(monkeypatch):
    transformation, production_volumes = make_market_transformation(
        monkeypatch, {"available": 1.0, "unused": 0.0}
    )
    transformation.process_and_add_markets(
        name="market for test product",
        reference_product="test product",
        unit="kilogram",
        mapping={"available": [make_supplier("supplier")], "unused": []},
        production_volumes=production_volumes,
    )
    market = next(ds for ds in transformation.database if ds["location"] == "WEU")
    assert [
        exc["name"] for exc in market["exchanges"] if exc["type"] == "technosphere"
    ] == ["supplier"]


def test_market_uses_explicit_technology_shares_and_keeps_absolute_volume(
    monkeypatch,
):
    transformation, _ = make_market_transformation(
        monkeypatch, {"diesel": 0.9, "synthetic diesel": 0.1}
    )
    production_volumes = xr.DataArray(
        [[[9.0]], [[1.0]]],
        dims=("variables", "region", "year"),
        coords={
            "variables": ["diesel", "synthetic diesel"],
            "region": ["WEU"],
            "year": [2050],
        },
    )
    marginal_mix = xr.DataArray(
        [[[1.0]], [[0.0]]],
        dims=("variables", "region", "year"),
        coords={
            "variables": ["diesel", "synthetic diesel"],
            "region": ["WEU"],
            "year": [2050],
        },
    )
    monkeypatch.setattr(
        transformation,
        "get_technology_and_regional_production_shares",
        lambda **kwargs: BaseTransformation.get_technology_and_regional_production_shares(
            transformation,
            production_volumes=kwargs["production_volumes"],
            mapping=kwargs["mapping"],
        ),
    )

    transformation.process_and_add_markets(
        name="market for diesel",
        reference_product="diesel",
        unit="kilogram",
        mapping={
            "diesel": [make_supplier("diesel production")],
            "synthetic diesel": [make_supplier("synthetic diesel production")],
        },
        production_volumes=production_volumes,
        technology_shares=marginal_mix,
        retain_validation_technology=True,
    )

    market = next(ds for ds in transformation.database if ds["location"] == "WEU")
    production = next(exc for exc in market["exchanges"] if exc["type"] == "production")
    suppliers = {
        exc["name"]: exc["amount"]
        for exc in market["exchanges"]
        if exc["type"] == "technosphere"
    }

    assert production["production volume"] == 10.0
    assert suppliers == {"diesel production": 1.0}
    assert (
        next(exc for exc in market["exchanges"] if exc["type"] == "technosphere")[
            "premise market technology"
        ]
        == "diesel"
    )
