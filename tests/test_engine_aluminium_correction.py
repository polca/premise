"""Public source arithmetic, atomic native guards and cache invalidation."""

from copy import deepcopy
from math import log

import numpy as np
import pytest

from premise.clean_datasets import DatabaseCleaner

ENGINE = "internal combustion engine, passenger car"
USED = f"used {ENGINE}"
PRODUCTION = "internal combustion engine production, passenger car"
MARKET = f"market for {USED}"
TREATMENT = f"treatment of {USED}, shredding"
FACTOR = 61.875 / 170.07


def flow(name, product, amount, kind="technosphere"):
    return dict(
        name=name,
        product=product,
        amount=amount,
        loc=amount,
        type=kind,
        unit="kilogram",
        location="GLO",
        **{"uncertainty type": 0, "comment": "Original exchange."},
    )


def recipe(name, product, output, *inputs):
    return {
        "name": name,
        "reference product": product,
        "unit": "kilogram",
        "location": "GLO",
        "comment": "Original dataset.",
        "exchanges": [flow(name, product, output, "production"), *inputs],
    }


def chain(dtype=float):
    return [
        recipe(
            PRODUCTION,
            ENGINE,
            1,
            flow(
                "market for aluminium, cast alloy",
                "aluminium, cast alloy",
                dtype(170.07 / 275),
            ),
            flow(
                "aluminium scrap, new, Recycled Content cut-off",
                "aluminium scrap, new",
                dtype(-34.014 / 275),
            ),
            flow(MARKET, USED, -1),
            flow(
                "market for steel, low-alloyed, hot rolled",
                "steel, low-alloyed, hot rolled",
                108.195 / 275,
            ),
        ),
        recipe(MARKET, USED, -1, flow(TREATMENT, USED, -1)),
        recipe(
            TREATMENT,
            USED,
            -1,
            flow(
                "aluminium scrap, post-consumer, Recycled Content cut-off",
                "aluminium scrap, post-consumer",
                dtype(-0.408771),
            ),
            flow(
                "iron scrap, unsorted, Recycled Content cut-off",
                "iron scrap, unsorted",
                -0.298965,
            ),
        ),
    ]


def cleaner(data, version="3.12"):
    obj = object.__new__(DatabaseCleaner)
    obj.database = data
    obj.version = version
    return obj


@pytest.mark.parametrize("version", ["3.10", "3.11", "3.12"])
@pytest.mark.parametrize("dtype", [float, np.float32])
def test_joint_source_arithmetic_scope_and_idempotency(version, dtype):
    data = chain(dtype)
    unrelated = recipe("other engine", ENGINE, 1, deepcopy(data[0]["exchanges"][1]))
    data.append(unrelated)
    before = deepcopy(data)
    obj = cleaner(data, version)
    assert obj.correct_passenger_car_engine_aluminium() == 1
    gross, scrap = data[0]["exchanges"][1:3]
    recovered = data[2]["exchanges"][1]
    assert gross["amount"] * 275 == pytest.approx(61.875, rel=1e-7)
    assert scrap["amount"] * 275 == pytest.approx(-12.375, rel=1e-7)
    retained = gross["amount"] + scrap["amount"]
    assert retained * 275 == pytest.approx(49.5, rel=1e-7)
    assert 0 < -recovered["amount"] < retained
    assert -recovered["amount"] / retained == pytest.approx(
        0.408771 / ((170.07 - 34.014) / 275), rel=1e-7
    )
    for current, original in zip(data, before):
        for exc, old in zip(current["exchanges"], original["exchanges"]):
            if exc == old:
                continue
            assert exc["loc"] == exc["amount"]
            assert exc["comment"].startswith(old["comment"])
            for key in set(exc) - {"amount", "loc", "comment"}:
                assert exc[key] == old[key]
    assert data[0]["exchanges"][0] == before[0]["exchanges"][0]
    assert data[0]["exchanges"][3:] == before[0]["exchanges"][3:]
    assert data[1] == before[1]
    assert data[2]["exchanges"][0] == before[2]["exchanges"][0]
    assert data[2]["exchanges"][2:] == before[2]["exchanges"][2:]
    assert unrelated == before[3]
    once = deepcopy(data)
    assert obj.correct_passenger_car_engine_aluminium() == 0
    assert data == once


@pytest.mark.parametrize("keep_uncertainty", [False, True])
def test_pipeline_and_signed_disposal_solve(keep_uncertainty):
    data = cleaner(chain()).prepare_datasets(keep_uncertainty)
    # A = outputs minus inputs; both waste reference products are negative.
    names = {d["name"]: i for i, d in enumerate(data)}
    matrix = np.zeros((3, 3))
    for col, ds in enumerate(data):
        for exc in ds["exchanges"]:
            if exc["type"] == "production":
                matrix[col, col] += exc["amount"]
            elif exc["name"] in names:
                matrix[names[exc["name"]], col] -= exc["amount"]
    activity = np.linalg.solve(matrix, [1, 0, 0])
    assert activity == pytest.approx([1, 1, 1])
    assert data[0]["exchanges"][1]["amount"] * activity[0] == pytest.approx(0.225)
    assert data[2]["exchanges"][1]["amount"] * activity[2] == pytest.approx(
        -0.14871938393014641
    )


@pytest.mark.parametrize("index", [0, 1, 2])
@pytest.mark.parametrize(
    "field,value",
    [
        ("name", "different activity"),
        ("reference product", "different product"),
        ("unit", "unit"),
        ("location", "RER"),
    ],
)
def test_dataset_guards_are_atomic(index, field, value):
    data = chain()
    data[index][field] = value
    before = deepcopy(data)
    assert cleaner(data).correct_passenger_car_engine_aluminium() == 0
    assert data == before


@pytest.mark.parametrize("index,exc_index", [(0, 1), (0, 2), (0, 3), (1, 1), (2, 1)])
@pytest.mark.parametrize(
    "field,value",
    [
        ("amount", 123),
        ("product", "different product"),
        ("name", "different supplier"),
        ("unit", "tonne"),
        ("location", "RER"),
        ("type", "biosphere"),
        ("formula", "custom_parameter"),
        ("uncertainty type", 7),
    ],
)
def test_exchange_guards_are_atomic(index, exc_index, field, value):
    data = chain()
    data[index]["exchanges"][exc_index][field] = value
    before = deepcopy(data)
    assert cleaner(data).correct_passenger_car_engine_aluminium() == 0
    assert data == before


@pytest.mark.parametrize("index", [0, 1, 2])
@pytest.mark.parametrize(
    "change",
    [
        "duplicate dataset",
        "duplicate production",
        "output amount",
        "output formula",
        "missing dataset",
    ],
)
def test_missing_ambiguous_or_changed_chain_is_unchanged(index, change):
    data = chain()
    if change == "duplicate dataset":
        data.append(deepcopy(data[index]))
    elif change == "duplicate production":
        data[index]["exchanges"].append(deepcopy(data[index]["exchanges"][0]))
    elif change == "output amount":
        data[index]["exchanges"][0]["amount"] *= 2
    elif change == "output formula":
        data[index]["exchanges"][0]["formula"] = "custom"
    else:
        data.pop(index)
    before = deepcopy(data)
    assert cleaner(data).correct_passenger_car_engine_aluminium() == 0
    assert data == before


def test_duplicate_target_exchange_is_unchanged():
    data = chain()
    data[2]["exchanges"].append(deepcopy(data[2]["exchanges"][1]))
    before = deepcopy(data)
    assert cleaner(data).correct_passenger_car_engine_aluminium() == 0
    assert data == before


@pytest.mark.parametrize("version", ["3.9", "3.13"])
def test_unverified_versions_are_unchanged(version):
    data = chain()
    before = deepcopy(data)
    assert cleaner(data, version).correct_passenger_car_engine_aluminium() == 0
    assert data == before


def test_unknown_native_activity_is_unchanged():
    data = chain()
    data[0]["activity"] = "custom-activity"
    before = deepcopy(data)
    assert cleaner(data).correct_passenger_car_engine_aluminium() == 0
    assert data == before


@pytest.mark.parametrize("distribution", [0, 1, 2, 3, 4, 5])
def test_relative_uncertainty_and_negative_scrap_are_preserved(distribution):
    data = chain()
    targets = [
        data[0]["exchanges"][1],
        data[0]["exchanges"][2],
        data[2]["exchanges"][1],
    ]
    for exc in targets:
        amount = exc["amount"]
        exc.update(
            {
                "uncertainty type": distribution,
                "loc": log(abs(amount)) if distribution == 2 else amount,
                "scale": 0.2 if distribution == 2 else abs(amount) * 0.2,
                "minimum": min(amount * 0.8, amount * 1.2),
                "maximum": max(amount * 0.8, amount * 1.2),
                "negative": amount < 0,
                "pedigree": {"reliability": 2},
            }
        )
    before = deepcopy(targets)
    assert cleaner(data).correct_passenger_car_engine_aluminium() == 1
    for exc, old in zip(targets, before):
        assert exc["loc"] == pytest.approx(
            old["loc"] + log(FACTOR) if distribution == 2 else old["loc"] * FACTOR
        )
        if distribution:
            assert exc["minimum"] == pytest.approx(old["minimum"] * FACTOR)
            assert exc["maximum"] == pytest.approx(old["maximum"] * FACTOR)
            assert exc["scale"] == pytest.approx(
                old["scale"] if distribution == 2 else old["scale"] * FACTOR
            )
        assert exc["negative"] == old["negative"]
        assert exc["pedigree"] == old["pedigree"]


@pytest.mark.parametrize("version", ["3.9", "3.10", "3.11", "3.12", "3.13"])
def test_cache_revision_scope(version, monkeypatch):
    import premise.new_database as module

    obj = object.__new__(module.NewDatabase)
    obj.version = version
    obj.keep_source_db_uncertainty = obj.keep_imports_uncertainty = False
    source = obj._database_cache_path("test")
    imports = obj._database_cache_path("test", inventories=True)
    with monkeypatch.context() as old:
        old.setattr(module, "ICE_ALUMINIUM_VERSIONS", frozenset())
        legacy = obj._database_cache_path("test")
        assert imports == obj._database_cache_path("test", inventories=True)
    assert (source != legacy) == (version in {"3.10", "3.11", "3.12"})


def test_legacy_compact_checkpoint_is_not_reused(tmp_path, monkeypatch):
    import premise.new_database as module
    from premise.inventory_store import CompactInventoryStore

    obj = object.__new__(module.NewDatabase)
    obj.version = "3.12"
    obj.source_type = "brightway"
    obj.keep_source_db_uncertainty = obj.keep_imports_uncertainty = False
    monkeypatch.setattr(module, "DIR_CACHED_DB", tmp_path)
    with monkeypatch.context() as old:
        old.setattr(module, "ICE_ALUMINIUM_VERSIONS", frozenset())
        source = obj._database_cache_path("test")
        imports = obj._database_cache_path("test", inventories=True)
        for path in (
            source,
            imports,
            obj._metadata_cache_path(source),
            obj._metadata_cache_path(imports),
        ):
            path.write_bytes(b"legacy cache reference")
        obj._write_compact_source_checkpoint("test", CompactInventoryStore(chain()))
        assert obj._find_compact_source_checkpoint("test") is not None
    assert obj._find_compact_source_checkpoint("test") is None


def test_cache_retains_comments_and_correction_is_not_repeated(tmp_path):
    from premise.utils import create_cache, load_cached_database

    obj = cleaner(chain())
    assert obj.correct_passenger_car_engine_aluminium() == 1
    path = tmp_path / "source.pickle"
    create_cache(obj.database, path)
    data = load_cached_database(path)
    for i, j in [(0, 1), (0, 2), (2, 1)]:
        assert "ICE aluminium correction v1" in data[i]["comment"]
        assert "ICE aluminium correction v1" in data[i]["exchanges"][j]["comment"]
    assert cleaner(data).correct_passenger_car_engine_aluminium() == 0
