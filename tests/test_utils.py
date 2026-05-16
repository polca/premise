import json
import pickle
from pathlib import Path
from unittest.mock import call, patch

from premise import __version__
from premise.export import exc_codes, fetch_exchange_code
from premise.geomap import Geomap
from premise.utils import *
from premise.fuels.utils import get_crops_properties


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
    assert list(iter_cached_metadata(metadata_ref))[0] == {
        ("market for test", "test product", "GLO"): {"foo": "bar"}
    }


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
