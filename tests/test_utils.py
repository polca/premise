import json
import pickle
from copy import deepcopy
from pathlib import Path
from unittest.mock import call, patch

import numpy as np
import pytest
from wurst import rescale_exchange as wurst_rescale_exchange

from premise import __version__
from premise.export import exc_codes, fetch_exchange_code
from premise.geomap import Geomap
from premise.inventory_store import CompactInventoryStore, InventoryStore
from premise.utils import *
from premise.fuels.utils import get_crops_properties
import premise.utils as utils_module


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, False),
        ("", False),
        ("None", False),
        ("nan", False),
        ("metadata", True),
        ([], True),
        ({}, True),
        ((), True),
        (set(), True),
        (False, True),
        (0, True),
        (float("nan"), False),
        (np.float32("nan"), False),
        (np.float64(1.0), True),
        (np.int64(0), True),
        (np.bool_(False), True),
        (np.array([np.nan, np.nan]), True),
    ],
)
def test_has_cache_value_preserves_legacy_presence_semantics(value, expected):
    assert utils_module._has_cache_value(value) is expected


def test_scenario_exchange_presence_checks_metadata_truth_only_once(monkeypatch):
    calls = 0
    original = utils_module._has_cache_value

    def counted(value):
        nonlocal calls
        calls += 1
        return original(value)

    monkeypatch.setattr(utils_module, "_has_cache_value", counted)

    assert utils_module._scenario_cache_exchange_field_is_restored("amount", 0)
    assert not utils_module._scenario_cache_exchange_field_is_restored("comment", "")
    assert calls == 2


def test_end_of_process_can_preserve_applied_functions_for_repeated_exports():
    scenario = {
        "database": [{"name": "temporary export payload"}],
        "applied functions": ["update_electricity"],
        "cache": {"temporary": True},
        "index": {"temporary": True},
    }

    result = end_of_process(scenario, preserve_applied_functions=True)

    assert result == {
        "applied functions": ["update_electricity"],
        "cache": {},
        "index": {},
    }


@pytest.mark.parametrize("remove_uncertainty", [False, True])
def test_rescale_exchanges_accepts_compact_exchange_mappings(
    tmp_path, remove_uncertainty
):
    exchange = {
        "name": "input",
        "product": "product",
        "location": "GLO",
        "unit": "kilogram",
        "type": "technosphere",
        "amount": 2.0,
        "uncertainty type": 3,
        "loc": 2.0,
        "scale": 0.4,
        "minimum": 1.0,
        "maximum": 3.0,
    }
    data = [
        {
            "name": "consumer",
            "reference product": "service",
            "location": "GLO",
            "unit": "unit",
            "exchanges": [deepcopy(exchange)],
        }
    ]
    checkpoint = CompactInventoryStore(data).checkpoint(
        tmp_path / "rescale.inventory-store"
    )
    activity = InventoryStore.open(checkpoint)._checkout_materialized()[0]
    compact_exchange = activity["exchanges"][0]
    expected = deepcopy(exchange)
    wurst_rescale_exchange(expected, 2.5, remove_uncertainty)

    rescale_exchanges(activity, 2.5, remove_uncertainty=remove_uncertainty)

    assert compact_exchange.copy() == expected


def _write_cache_manifest(cache_ref, *shard_files):
    manifest_path = get_cache_manifest_path(cache_ref)

    with open(manifest_path, "w", encoding="utf-8") as file:
        json.dump(
            {
                "cache_format": 1,
                "storage": "pickle-shards",
                "files": [Path(shard_file).name for shard_file in shard_files],
            },
            file,
        )

    return manifest_path


def test_ei_db_label():
    model = "remind"
    pathway = "SSP2-Base"
    year = 2012
    version = "3.9"
    system_model = "cutoff"
    scenario = {
        "model": model,
        "pathway": pathway,
        "year": year,
        "external scenarios": [{"scenario": "test"}],
    }
    assert (
        eidb_label(scenario, version, system_model)
        == f"ei_{system_model}_{version}_{model}_{pathway}_{year}_test {datetime.now().strftime('%Y-%m-%d')}"
    )


def test_crops_properties():
    crop_props = get_crops_properties()
    assert type(crop_props) == dict
    assert crop_props["sugar"]["crop_type"]["image"]["temperate"] == "sugarbeet"


def test_fuels_properties():
    fuels_props = get_fuel_properties()
    assert type(fuels_props) == dict
    assert fuels_props["bioethanol, from wood, with CCS"]["lhv"]["value"] == 26.5


def test_eff_solar_PV():
    eff_PV = get_efficiency_solar_photovoltaics()
    assert type(eff_PV) == xr.DataArray
    assert eff_PV.sel(technology="multi-Si", year=2010, efficiency_type="mean") == 0.14
    assert "moni-Si" not in eff_PV.technology.values


def test_default_location():
    dummy_db = [
        {
            "name": "fake activity",
            "reference product": "fake product",
            "location": None,
            "unit": "kilogram",
            "exchanges": [
                {
                    "name": "fake activity",
                    "product": "fake product",
                    "amount": 1,
                    "type": "production",
                    "unit": "kilogram",
                    "input": ("dummy_db", "6543541"),
                },
                {
                    "name": "1,4-Butanediol",
                    "categories": ("air", "urban air close to ground"),
                    "amount": 1,
                    "type": "biosphere",
                    "unit": "kilogram",
                    "input": ("dummy_bio", "123"),
                },
            ],
        },
        {
            "name": "fake activity 2",
            "reference product": "fake product",
            "location": "FR",
            "unit": "kilogram",
            "exchanges": [
                {
                    "name": "fake activity 2",
                    "product": "fake product 2",
                    "amount": 1,
                    "type": "production",
                    "unit": "kilogram",
                    "input": ("dummy_db", "6543541"),
                },
                {
                    "name": "1,4-Butanediol",
                    "categories": ("air", "urban air close to ground"),
                    "amount": 1,
                    "type": "biosphere",
                    "unit": "kilogram",
                    "input": ("dummy_bio", "123"),
                },
            ],
        },
    ]

    corrected_db = default_global_location(dummy_db)

    assert not any(ds["location"] is None for ds in corrected_db)


@patch("builtins.print")
def test_print_version(mocked_print):
    print_version()
    assert mocked_print.mock_calls == [call(f"premise v.{__version__}")]


def test_clear_runtime_caches():
    geomap = Geomap("image")
    geomap.iam_to_ecoinvent_location("WEU")
    geomap.ecoinvent_to_iam_location("FR")
    fetch_exchange_code("fake activity", "fake product", "FR", "kilogram")

    assert Geomap.iam_to_ecoinvent_location.cache_info().currsize > 0
    assert Geomap.ecoinvent_to_iam_location.cache_info().currsize > 0
    assert fetch_exchange_code.cache_info().currsize > 0
    assert exc_codes

    clear_runtime_caches()

    assert Geomap.iam_to_ecoinvent_location.cache_info().currsize == 0
    assert Geomap.ecoinvent_to_iam_location.cache_info().currsize == 0
    assert fetch_exchange_code.cache_info().currsize == 0
    assert exc_codes == {}


def test_load_cached_database_supports_manifest_shards(tmp_path):
    cache_ref = tmp_path / "db-cache.pickle"
    shard_a = tmp_path / "db-cache.part-a.pickle"
    shard_b = tmp_path / "db-cache.part-b.pickle"

    with open(shard_a, "wb") as file:
        pickle.dump([{"name": "dataset-a"}], file)

    with open(shard_b, "wb") as file:
        pickle.dump([{"name": "dataset-b"}], file)

    manifest_path = _write_cache_manifest(cache_ref, shard_a, shard_b)

    assert cache_ref_exists(cache_ref) is True
    assert resolve_cache_ref(cache_ref) == manifest_path
    assert load_cached_database(cache_ref) == [
        {"name": "dataset-a"},
        {"name": "dataset-b"},
    ]


def test_iter_cached_metadata_supports_manifest_shards(tmp_path):
    metadata_ref = tmp_path / "db-cache (metadata).pickle"
    shard_a = tmp_path / "db-cache.metadata.part-a.pickle"
    shard_b = tmp_path / "db-cache.metadata.part-b.pickle"
    metadata_a = {("dataset-a", "product", "GLO"): {"comment": "first"}}
    metadata_b = {("dataset-b", "product", "GLO"): {"categories": ["test"]}}

    with open(shard_a, "wb") as file:
        pickle.dump(metadata_a, file)

    with open(shard_b, "wb") as file:
        pickle.dump(metadata_b, file)

    _write_cache_manifest(metadata_ref, shard_a, shard_b)

    assert list(iter_cached_metadata(metadata_ref)) == [metadata_a, metadata_b]


def test_scenario_cache_preserves_regionalized_without_metadata_reload(tmp_path):
    cache_ref = tmp_path / "scenario-cache.pickle"
    database = [
        {
            "database": "test-db",
            "code": "market-code",
            "name": "market group for electricity, low voltage",
            "reference product": "electricity, low voltage",
            "location": "BRA",
            "unit": "kilowatt hour",
            "regionalized": True,
            "classifications": [("CPC", "17100")],
            "exchanges": [
                {
                    "name": "market group for electricity, low voltage",
                    "product": "electricity, low voltage",
                    "amount": 1.0,
                    "type": "production",
                    "unit": "kilowatt hour",
                    "location": "BRA",
                }
            ],
        }
    ]

    database_ref, metadata_ref = create_scenario_cache(database, cache_ref)
    scenario = {
        "database filepath": database_ref,
        "database metadata filepath": metadata_ref,
    }

    load_database(scenario, original_database=[], delete=False, load_metadata=False)

    assert scenario["database"][0]["regionalized"] is True
    assert scenario["database"][0]["classifications"] == [("CPC", "17100")]


def test_scenario_compatible_store_matches_legacy_cache_roundtrip(tmp_path):
    database = [
        {
            "database": "test-db",
            "code": "activity-code",
            "name": "activity",
            "reference product": "service",
            "location": "GLO",
            "unit": "unit",
            "regionalized": False,
            "comment": "None",
            "has_downstream_consumer": False,
            "empty metadata": [],
            "meaningful metadata": {"kept": True},
            "exchanges": [
                {
                    "name": "activity",
                    "product": "service",
                    "location": "None",
                    "unit": "unit",
                    "type": "production",
                    "amount": 0,
                    "uncertainty type": 0,
                    "production volume": float("nan"),
                    "comment": 0,
                    "categories": [],
                    "custom false": False,
                    "custom value": "kept",
                }
            ],
        }
    ]
    legacy_database = deepcopy(database)
    database_ref, metadata_ref = create_scenario_cache(
        legacy_database,
        tmp_path / "scenario-cache.pickle",
    )
    scenario = {
        "database filepath": database_ref,
        "database metadata filepath": metadata_ref,
    }
    load_database(
        scenario,
        original_database=[],
        delete=False,
        load_metadata=True,
    )

    compact = CompactInventoryStore(
        deepcopy(database),
        scenario_cache_compatibility=True,
    ).materialize()

    assert compact == scenario["database"]


def test_scenario_cache_compacts_each_exchange_only_once(tmp_path, monkeypatch):
    calls = 0
    original = utils_module._trim_scenario_exchange

    def counted(exchange):
        nonlocal calls
        calls += 1
        return original(exchange)

    monkeypatch.setattr(utils_module, "_trim_scenario_exchange", counted)
    database = [
        {
            "name": "market for test",
            "reference product": "test product",
            "location": "GLO",
            "unit": "kilogram",
            "exchanges": [
                {
                    "name": "market for test",
                    "product": "test product",
                    "location": "GLO",
                    "unit": "kilogram",
                    "amount": 1.0,
                    "type": "production",
                    "comment": "metadata",
                },
                {
                    "name": "input",
                    "product": "input",
                    "location": "GLO",
                    "unit": "kilogram",
                    "amount": 2.0,
                    "type": "technosphere",
                },
            ],
        }
    ]

    create_scenario_cache(database, tmp_path / "scenario-cache.pickle")

    assert calls == 2


def test_create_cache_writes_legacy_database_and_manifest_metadata(tmp_path):
    cache_ref = tmp_path / "db-cache.pickle"
    database = [
        {
            "name": "market for test",
            "reference product": "test product",
            "location": "GLO",
            "unit": "kilogram",
            "comment": "hello",
            "foo": "bar",
            "classifications": [("CPC", "12345")],
            "exchanges": [
                {
                    "name": "market for test",
                    "product": "test product",
                    "amount": 1.0,
                    "type": "production",
                    "unit": "kilogram",
                    "location": "GLO",
                    "input": ("db", "code"),
                }
            ],
        }
    ]

    trimmed, metadata_ref = create_cache(database, cache_ref)

    assert cache_ref.exists() is True
    assert get_cache_manifest_path(cache_ref).exists() is False
    assert metadata_ref == get_cache_manifest_path(
        Path(str(cache_ref).replace(".pickle", " (metadata).pickle"))
    )
    assert load_cached_database(cache_ref) == trimmed
    assert trimmed[0]["comment"] == "hello"
    assert trimmed[0]["classifications"] == [("CPC", "12345")]
    assert list(iter_cached_metadata(metadata_ref))[0] == {
        ("market for test", "test product", "GLO"): {"foo": "bar"}
    }


def test_load_database_restores_classifications_from_legacy_metadata(tmp_path):
    scenario_db = tmp_path / "scenario-db.pickle"
    metadata_ref = tmp_path / "scenario-db (metadata).pickle"
    metadata_shard = tmp_path / "scenario-db.metadata.part-a.pickle"
    dataset = {
        "name": "market for test",
        "reference product": "test product",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [],
    }

    with open(scenario_db, "wb") as file:
        pickle.dump([dataset], file)

    with open(metadata_shard, "wb") as file:
        pickle.dump(
            {
                ("market for test", "test product", "GLO"): {
                    "classifications": [("CPC", "12345")],
                    "comment": "kept in metadata only",
                }
            },
            file,
        )

    metadata_manifest = _write_cache_manifest(metadata_ref, metadata_shard)
    scenario = {
        "database filepath": scenario_db,
        "database metadata filepath": metadata_manifest,
    }

    loaded = load_database(
        scenario=scenario,
        original_database=[],
        delete=False,
        load_metadata=False,
    )

    assert loaded["database"][0]["classifications"] == [("CPC", "12345")]
    assert "comment" not in loaded["database"][0]


def test_load_database_rehydrates_metadata_from_manifest_shards(tmp_path):
    scenario_db = tmp_path / "scenario-db.pickle"
    database_metadata_ref = tmp_path / "database (metadata).pickle"
    inventories_metadata_ref = tmp_path / "inventories (metadata).pickle"
    database_shard = tmp_path / "database.metadata.part-a.pickle"
    inventories_shard = tmp_path / "inventories.metadata.part-a.pickle"
    dataset = {
        "name": "market for test",
        "reference product": "test product",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [],
    }

    with open(scenario_db, "wb") as file:
        pickle.dump([dataset], file)

    with open(database_shard, "wb") as file:
        pickle.dump(
            {
                ("market for test", "test product", "GLO"): {
                    "comment": "database metadata",
                }
            },
            file,
        )

    with open(inventories_shard, "wb") as file:
        pickle.dump(
            {
                ("market for test", "test product", "GLO"): {
                    "classifications": {"foo": "bar"},
                }
            },
            file,
        )

    _write_cache_manifest(database_metadata_ref, database_shard)
    _write_cache_manifest(inventories_metadata_ref, inventories_shard)

    scenario = {
        "database filepath": scenario_db,
        "database metadata cache filepath": database_metadata_ref,
        "inventories metadata cache filepath": inventories_metadata_ref,
    }

    loaded = load_database(scenario=scenario, original_database=[], delete=False)

    assert loaded["database"][0]["comment"] == "database metadata"
    assert loaded["database"][0]["classifications"] == {"foo": "bar"}
