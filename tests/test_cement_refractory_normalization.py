"""Canadian source normalization and cache guards, without a licensed database."""

from copy import deepcopy
from math import log

import numpy as np
import pytest

from premise.clean_datasets import DatabaseCleaner

ORIGINAL = 1492307607.08133
RATE = 0.00130904176059766
PLANT = "integrated cement plant construction"


def plant(amount=ORIGINAL):
    return {
        "name": PLANT,
        "reference product": "integrated cement plant",
        "location": "CA",
        "unit": "unit",
        "comment": "Original plant description.",
        "exchanges": [
            {
                "name": PLANT,
                "product": "integrated cement plant",
                "location": "CA",
                "unit": "unit",
                "type": "production",
                "amount": 1,
            },
            {
                "name": "market for refractory, basic, packed",
                "product": "refractory, basic, packed",
                "location": "GLO",
                "unit": "kilogram",
                "type": "technosphere",
                "amount": amount,
                "uncertainty type": 0,
                "loc": amount,
                "comment": "Original refractory source description.",
            },
            {
                "name": "market for steel, low-alloyed",
                "product": "steel, low-alloyed",
                "location": "GLO",
                "unit": "kilogram",
                "type": "technosphere",
                "amount": 123,
            },
        ],
    }


def cleaner(dataset, version="3.12"):
    obj = object.__new__(DatabaseCleaner)
    obj.version = version
    obj.database = [dataset]
    return obj


@pytest.mark.parametrize("keep_uncertainty", [False, True])
def test_cleaning_pipeline_restores_documented_refractory_rate(keep_uncertainty):
    obj = cleaner(plant())
    data = obj.prepare_datasets(keep_uncertainty)
    refractory = data[0]["exchanges"][1]
    # One average plant is allocated over its share of the 13-plant output.
    plant_per_kg_clinker = 13 / (11.4e9 * 100)
    assert refractory["amount"] * plant_per_kg_clinker == pytest.approx(RATE)
    assert refractory["loc"] == refractory["amount"]
    assert "refractory normalization v1" in refractory["comment"]


@pytest.mark.parametrize("version", ["3.11", "3.12"])
@pytest.mark.parametrize("dtype", [float, np.float32])
def test_correction_is_targeted_and_idempotent(version, dtype):
    dataset = plant(dtype(ORIGINAL))
    before = deepcopy(dataset)
    obj = cleaner(dataset, version)
    unrelated = {**plant(), "name": "unrelated plant"}
    original_unrelated = deepcopy(unrelated)
    obj.database.append(unrelated)
    assert obj.correct_canadian_cement_refractory() == 1
    assert dataset["exchanges"][1]["amount"] == pytest.approx(ORIGINAL / 13, rel=1e-7)
    assert dataset["exchanges"][1]["loc"] == dataset["exchanges"][1]["amount"]
    assert dataset["exchanges"][0] == before["exchanges"][0]
    assert dataset["exchanges"][2] == before["exchanges"][2]
    assert unrelated == original_unrelated
    assert dataset["comment"].startswith(before["comment"])
    assert dataset["exchanges"][1]["comment"].startswith(
        before["exchanges"][1]["comment"]
    )
    once = deepcopy(obj.database)
    assert obj.correct_canadian_cement_refractory() == 0
    assert obj.database == once


@pytest.mark.parametrize(
    "scope,field,value",
    [
        ("version", None, "3.10"),
        ("version", None, "3.13"),
        ("dataset", "name", "different cement plant"),
        ("dataset", "reference product", "different infrastructure"),
        ("dataset", "location", "CA-ON"),
        ("dataset", "unit", "kilogram"),
        ("dataset", "activity", "different-native-activity"),
        ("production", "amount", 2),
        ("exchange", "name", "refractory production, basic, packed"),
        ("exchange", "product", "other refractory"),
        ("exchange", "location", "CA"),
        ("exchange", "unit", "ton"),
        ("exchange", "type", "biosphere"),
        ("exchange", "amount", ORIGINAL * 1.01),
        ("exchange", "amount", ORIGINAL / 13),
        ("exchange", "formula", "custom_parameter * 100"),
        ("exchange", "uncertainty type", 7),
    ],
)
def test_guards_preserve_unrecognized_recipes(scope, field, value):
    dataset = plant()
    version = value if scope == "version" else "3.12"
    if scope != "version":
        target = {
            "dataset": dataset,
            "production": dataset["exchanges"][0],
            "exchange": dataset["exchanges"][1],
        }[scope]
        target[field] = value
    before = deepcopy(dataset)
    assert cleaner(dataset, version).correct_canadian_cement_refractory() == 0
    assert dataset == before


def test_ambiguous_refractory_inputs_are_preserved():
    dataset = plant()
    dataset["exchanges"].append(deepcopy(dataset["exchanges"][1]))
    before = deepcopy(dataset)
    assert cleaner(dataset).correct_canadian_cement_refractory() == 0
    assert dataset == before


@pytest.mark.parametrize("distribution", [2, 3, 5])
def test_relative_source_uncertainty_is_preserved(distribution):
    dataset = plant()
    exc = dataset["exchanges"][1]
    exc.update(
        {
            "uncertainty type": distribution,
            "loc": log(ORIGINAL) if distribution == 2 else ORIGINAL,
            "minimum": ORIGINAL * 0.8,
            "maximum": ORIGINAL * 1.2,
            "pedigree": {"reliability": 2},
        }
    )
    if distribution in (2, 3):
        exc["scale"] = 0.2 if distribution == 2 else ORIGINAL * 0.2
    assert cleaner(dataset).correct_canadian_cement_refractory() == 1
    assert exc["loc"] == pytest.approx(
        log(ORIGINAL / 13) if distribution == 2 else ORIGINAL / 13
    )
    assert exc["minimum"] / exc["amount"] == pytest.approx(0.8)
    assert exc["maximum"] / exc["amount"] == pytest.approx(1.2)
    assert exc["pedigree"] == {"reliability": 2}
    if distribution in (2, 3):
        assert exc["scale"] == pytest.approx(
            0.2 if distribution == 2 else ORIGINAL * 0.2 / 13
        )


@pytest.mark.parametrize("version", ["3.10", "3.11", "3.12"])
def test_source_cache_revision_is_limited_to_affected_versions(version, monkeypatch):
    import premise.new_database as module

    obj = object.__new__(module.NewDatabase)
    obj.version = version
    obj.keep_source_db_uncertainty = obj.keep_imports_uncertainty = False
    source = obj._database_cache_path("test")
    inventories = obj._database_cache_path("test", inventories=True)
    with monkeypatch.context() as old:
        old.setattr(module, "CA_CEMENT_REFRACTORY_VERSIONS", frozenset())
        legacy_source = obj._database_cache_path("test")
        legacy_inventories = obj._database_cache_path("test", inventories=True)
    assert (source != legacy_source) == (version in ("3.11", "3.12"))
    assert inventories == legacy_inventories


def test_uncorrected_compact_checkpoint_is_not_reused(tmp_path, monkeypatch):
    import premise.new_database as module
    from premise.inventory_store import CompactInventoryStore

    obj = object.__new__(module.NewDatabase)
    obj.version = "3.12"
    obj.source_type = "brightway"
    obj.keep_source_db_uncertainty = obj.keep_imports_uncertainty = False
    monkeypatch.setattr(module, "DIR_CACHED_DB", tmp_path)
    with monkeypatch.context() as old:
        old.setattr(module, "CA_CEMENT_REFRACTORY_VERSIONS", frozenset())
        source = obj._database_cache_path("test")
        imports = obj._database_cache_path("test", inventories=True)
        for p in (
            source,
            obj._metadata_cache_path(source),
            imports,
            obj._metadata_cache_path(imports),
        ):
            p.write_bytes(b"legacy cache reference")
        obj._write_compact_source_checkpoint("test", CompactInventoryStore([plant()]))
        assert obj._find_compact_source_checkpoint("test") is not None
    assert obj._find_compact_source_checkpoint("test") is None


def test_source_cache_preserves_provenance_and_does_not_repeat_correction(tmp_path):
    from premise.utils import create_cache, load_cached_database

    obj = cleaner(plant())
    assert obj.correct_canadian_cement_refractory() == 1
    path = tmp_path / "corrected-source.pickle"
    create_cache(obj.database, path)
    restored = load_cached_database(path)
    assert "refractory normalization v1" in restored[0]["comment"]
    exc = restored[0]["exchanges"][1]
    assert "refractory normalization v1" in exc["comment"]
    assert exc["amount"] == pytest.approx(ORIGINAL / 13, rel=1e-7)
    assert cleaner(restored[0]).correct_canadian_cement_refractory() == 0
