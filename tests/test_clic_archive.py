"""Scientific graph and reproducibility contracts for native CLIC backups."""

import copy
import hashlib
import json
import zipfile

import pytest

from premise.inventory.clic_archive import (
    ClicArchive,
    ClicArchiveError,
    ClicRecipe,
    ClicResolutionError,
)


def native(
    did,
    vid,
    name,
    semantic="1.0.0",
    *,
    unit="kilogram",
    exchanges=(),
    status="published",
):
    return {
        "id": vid,
        "dataset_id": did,
        "semantic_version": semantic,
        "status": status,
        "activity_code": f"clic-{did}-{vid}",
        "name": name,
        "reference_product": name,
        "location": "GLO",
        "unit": unit,
        "background_family": "ecoinvent",
        "background_version": "3.12",
        "background_system_model": "cutoff",
        "biosphere_family": "ecoinvent",
        "biosphere_version": "3.12",
        "selected_license": "CC-BY-4.0",
        "inventory_snapshot": {
            "name": name,
            "reference product": name,
            "location": "GLO",
            "unit": unit,
            "database": "original",
            "code": f"old-{did}",
            "exchanges": [
                {
                    "name": name,
                    "reference product": name,
                    "location": "GLO",
                    "unit": unit,
                    "amount": 1,
                    "type": "production",
                },
                *copy.deepcopy(list(exchanges)),
            ],
        },
    }


def purchase(name, *, unit="kilogram", amount=2, **extra):
    return {
        "name": name,
        "reference product": name,
        "location": "GLO",
        "unit": unit,
        "amount": amount,
        "type": "technosphere",
        **extra,
    }


def catalog(*versions, dependencies=()):
    ids = sorted({v["dataset_id"] for v in versions})
    return ClicArchive(
        {"format": "clic-inventory-archive-v4", "fingerprint": "snapshot"},
        [{"id": did, "slug": f"dataset-{did}"} for did in ids],
        list(versions),
        dependencies,
    )


@pytest.fixture
def archive():
    return catalog(
        native(
            1, 1, "root", exchanges=[purchase("component", input=["original", "old-2"])]
        ),
        native(2, 2, "component", status="superseded"),
        native(2, 3, "component", "1.0.2", exchanges=[purchase("native background")]),
        native(3, 4, "unrelated"),
    )


def test_only_requested_chain_is_loaded_and_source_is_unchanged(archive):
    original = copy.deepcopy(archive.versions)
    result = archive.resolve(["dataset-1"])
    assert {row["slug"] for row in result.versions} == {"dataset-1", "dataset-2"}
    assert (
        next(row for row in result.versions if row["slug"] == "dataset-2")[
            "semantic_version"
        ]
        == "1.0.2"
    )
    assert [row["role"] for row in result.versions] == ["root", "upstream"]
    assert result.activities[0]["exchanges"][1]["input"] == ("__clic__", "clic-2-3")
    assert "input" not in result.activities[1]["exchanges"][1]
    result.activities[0]["exchanges"][1]["amount"] = 999
    assert archive.versions == original


def test_recipe_pins_upstream_without_adding_unrelated_inventory(archive):
    result = archive.resolve(
        ["dataset-1"], recipe={"versions": {"dataset-2": "1.0.0", "dataset-3": "1.0.0"}}
    )
    assert {row["version_id"] for row in result.versions} == {1, 2}
    assert result.activities[0]["exchanges"][1]["input"] == ("__clic__", "clic-2-2")
    assert result.versions[1]["pinned"] is True


def test_latest_uses_numeric_semantic_versions():
    source = catalog(native(1, 2, "root", "1.0.2"), native(1, 10, "root", "1.0.10"))
    assert source.resolve(["dataset-1"]).versions[0]["version_id"] == 10


def test_locked_graph_survives_a_new_release_and_supersession(archive):
    first = archive.resolve(["dataset-1"])
    changed = copy.deepcopy(list(archive.versions.values()))
    changed[2].update(status="superseded", updated_at="tomorrow")
    later = catalog(*changed, native(2, 5, "component", "1.0.3"))
    replay = later.resolve(recipe=first.lockfile())
    assert replay.fingerprint == first.fingerprint
    assert {r["version_id"] for r in replay.versions} == {1, 3}
    assert later.resolve(["dataset-1"]).fingerprint != first.fingerprint


def test_lockfile_does_not_promote_a_new_foreground_for_old_background(archive):
    first = archive.resolve(["dataset-1"])
    later = catalog(*archive.versions.values(), native(4, 5, "native background"))
    assert len(later.resolve(["dataset-1"]).activities) == 3
    assert len(later.resolve(recipe=first.lockfile()).activities) == 2


def test_lockfile_rejects_silently_changed_science(archive):
    locked = archive.resolve(["dataset-1"]).lockfile()
    changed = copy.deepcopy(list(archive.versions.values()))
    changed[2]["inventory_snapshot"]["exchanges"][1]["amount"] = 100
    with pytest.raises(ClicResolutionError, match="content changed"):
        catalog(*changed).resolve(recipe=locked)


def test_cycles_and_shared_suppliers_are_traversed_once():
    source = catalog(
        native(1, 1, "one", exchanges=[purchase("two"), purchase("three")]),
        native(2, 2, "two", exchanges=[purchase("one"), purchase("three")]),
        native(3, 3, "three"),
    )
    result = source.resolve(["dataset-1"])
    assert len(result.activities) == 3
    assert len({a["code"] for a in result.activities}) == 3


def test_renamed_reference_keeps_native_market_purchase_instead_of_historical_self_loop():
    old = native(1, 1, "native market", status="superseded")
    current = native(
        1,
        2,
        "supply reference",
        semantic="1.0.1",
        exchanges=[
            purchase(
                "native market", input=["ecoinvent-3.12-cutoff", "actual-native-code"]
            )
        ],
    )
    current["reference_product"] = "native market"
    current["inventory_snapshot"]["reference product"] = "native market"
    source = catalog(old, current)
    resolved = source.resolve(["dataset-1"])
    edge = resolved.activities[0]["exchanges"][1]
    assert edge["name"] == "native market"
    assert edge["amount"] == 2
    assert "input" not in edge


def test_current_unit_helper_overrides_stale_original_input_code():
    source = catalog(
        native(
            1,
            1,
            "battery",
            exchanges=[
                purchase("control unit", unit="unit", input=["original", "old-2"])
            ],
        ),
        native(2, 2, "old BMS", unit="kilogram"),
        native(3, 3, "control unit", unit="unit"),
    )
    result = source.resolve(["dataset-1"])
    assert {r["slug"] for r in result.versions} == {"dataset-1", "dataset-3"}
    assert result.activities[0]["exchanges"][1]["amount"] == 2


def test_version_change_cannot_silently_convert_units():
    source = catalog(
        native(
            1,
            1,
            "root",
            exchanges=[
                purchase("old component", unit="unit", input=["original", "old-2"])
            ],
        ),
        native(2, 2, "old component", unit="unit", status="superseded"),
        native(2, 3, "new component", "1.0.1", unit="kilogram"),
    )
    with pytest.raises(ClicResolutionError, match="unit changed"):
        source.resolve(["dataset-1"])


def test_ambiguous_identity_requires_a_unique_original_key():
    source = catalog(
        native(1, 1, "root", exchanges=[purchase("same")]),
        native(2, 2, "same"),
        native(3, 3, "same"),
    )
    with pytest.raises(ClicResolutionError, match="Ambiguous"):
        source.resolve(["dataset-1"])
    source.versions[1]["inventory_snapshot"]["exchanges"][1]["input"] = [
        "original",
        "old-2",
    ]
    assert {r["slug"] for r in source.resolve(["dataset-1"]).versions} == {
        "dataset-1",
        "dataset-2",
    }


def test_declared_missing_foreground_is_not_treated_as_background():
    source = catalog(
        native(1, 1, "root", exchanges=[purchase("missing public supplier")]),
        dependencies=[{"version_id": 1, "exchange_index": 1}],
    )
    with pytest.raises(ClicResolutionError, match="Known missing foreground"):
        source.resolve(["dataset-1"])


@pytest.mark.parametrize(
    "recipe",
    [
        {"versions": {"dataset-1": 1.0}},
        {"versions": {"dataset-1": "not a version"}},
        {"versions": {"dataset-1": {"version": "1.0.0", "version_id": True}}},
        {"schema_version": 99},
    ],
)
def test_invalid_recipes_fail_before_inventory_use(recipe):
    with pytest.raises(ClicResolutionError):
        ClicRecipe.read(recipe)


def write_archive(path, *, corrupt=False):
    versions = [native(1, 1, "root")]
    payloads = {
        "datasets.jsonl": json.dumps({"id": 1, "slug": "dataset-1"}).encode() + b"\n",
        "versions.jsonl": json.dumps(versions[0]).encode() + b"\n",
    }
    manifest = {
        "format": "clic-inventory-archive-v4",
        "dataset_count": 1,
        "version_count": 1,
        "payloads": {k: hashlib.sha256(v).hexdigest() for k, v in payloads.items()},
    }
    if corrupt:
        payloads["versions.jsonl"] = payloads["versions.jsonl"].replace(
            b'"amount": 1', b'"amount": 9'
        )
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("manifest.json", json.dumps(manifest))
        for name, payload in payloads.items():
            archive.writestr(name, payload)
    return path


def test_archive_checksums_protect_inventory_quantities(tmp_path):
    good = write_archive(tmp_path / "good.zip")
    assert (
        ClicArchive.read(good)
        .resolve(["dataset-1"])
        .activities[0]["exchanges"][0]["amount"]
        == 1
    )
    bad = write_archive(tmp_path / "corrupt.zip", corrupt=True)
    with pytest.raises(ClicArchiveError, match="checksum mismatch"):
        ClicArchive.read(bad)
