"""A mixed provider must retain private recipes and never invent replacements."""

import copy
import hashlib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from premise.inventory import clic_hybrid as module
from premise.inventory.clic_archive import ClicInventoryError


def activity(name, **kwargs):
    return {
        "name": name,
        "reference product": name,
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [],
        **kwargs,
    }


def test_prepared_cache_restores_owned_hashable_brightway_references(
    monkeypatch, tmp_path
):
    record = activity(
        "private",
        input=("foreground", "private"),
        exchanges=[
            {"type": "production", "input": ("foreground", "private"), "amount": 1},
            {
                "type": "technosphere",
                "input": ("background", "supplier"),
                "amount": 0.5,
            },
            {"type": "biosphere", "input": ("biosphere3", "flow"), "amount": 2},
        ],
    )
    prepare = Mock(side_effect=lambda *a, **k: ([copy.deepcopy(record)], [], []))
    monkeypatch.setattr(module, "registry", lambda: {"hybrid_sources": {"files": {}}})
    monkeypatch.setattr(module, "_import_retained_inventories", prepare)
    plan = module.HybridInventoryPlan((), frozenset(), (), frozenset(), "fixture")
    kwargs = dict(
        version="3.12",
        system_model="cutoff",
        cache_dir=tmp_path,
        cache_signature={"source": "source-fingerprint"},
    )
    first, _, _ = module.import_retained_inventories(plan, [], [], **kwargs)
    first[0]["exchanges"][2]["amount"] = 99
    cached, _, _ = module.import_retained_inventories(plan, [], [], **kwargs)
    assert prepare.call_count == 1
    assert cached == [record]
    assert {edge["input"] for edge in cached[0]["exchanges"]} == {
        ("foreground", "private"),
        ("background", "supplier"),
        ("biosphere3", "flow"),
    }


def test_dependency_walk_stops_at_public_replacement_and_retains_licensed_suppliers(
    monkeypatch,
):
    nodes = [
        {
            "id": 0,
            "file": "licensed.xlsx",
            "identity": ["root", "root", "GLO", "kilogram"],
            "clic_slug": None,
            "science_hash": "0",
            "inputs": [{"nodes": [1]}, {"nodes": [2]}],
        },
        {
            "id": 1,
            "file": "open.xlsx",
            "identity": ["public", "public", "GLO", "kilogram"],
            "clic_slug": "public",
            "science_hash": "1",
            "inputs": [{"nodes": [3]}],
        },
        {
            "id": 2,
            "file": "licensed.xlsx",
            "identity": ["private", "private", "GLO", "kilogram"],
            "clic_slug": None,
            "science_hash": "2",
            "inputs": [],
        },
        {
            "id": 3,
            "file": "unused.xlsx",
            "identity": ["unused", "unused", "GLO", "kilogram"],
            "clic_slug": None,
            "science_hash": "3",
            "inputs": [],
        },
    ]
    files = {
        name: {"sha256": name} for name in ("licensed.xlsx", "open.xlsx", "unused.xlsx")
    }
    data = {
        "sector_mappings": {"steel": ["steel.yaml"]},
        "hybrid_sources": {
            "nodes": nodes,
            "files": files,
            "roles": {"steel.yaml": {"Steel": [0]}},
        },
    }
    monkeypatch.setattr(module, "registry", lambda: data)
    monkeypatch.setattr(module, "required_roots", lambda *args, **kwargs: ())
    import premise.inventory.activity_maps as maps

    monkeypatch.setattr(maps, "get_mapping", lambda *args, **kwargs: {"Steel": {}})
    import premise.inventory.profiles as profiles

    monkeypatch.setattr(
        profiles,
        "profiles",
        lambda *args: [
            {
                "id": "public-companion-projection",
                "phase": "projection",
                "requires": [{"source": "licensed.xlsx"}, {"source": "open.xlsx"}],
                "omit": [{"source": "open.xlsx", "identities": [nodes[1]["identity"]]}],
            }
        ],
    )
    plan = module.plan_hybrid_inventories(
        ["steel"], [], [(Path(name), "3.12") for name in files]
    )
    assert plan.clic_roots == ("public",)
    assert plan.selected_nodes == frozenset({0, 2})
    assert [path.name for path, _ in plan.sources] == ["licensed.xlsx"]


def test_generated_helper_without_slug_cannot_replace_a_retained_activity(
    monkeypatch, tmp_path
):
    path = tmp_path / "licensed.xlsx"
    path.write_bytes(b"licensed workbook fixture")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    private = activity("private")
    buyer = activity(
        "buyer",
        exchanges=[{**activity("private"), "type": "technosphere", "amount": 3}],
    )
    helper = activity(
        "conversion helper",
        code="helper",
        database="clic",
        **{"clic provenance": {"role": "generated helper"}},
    )
    nodes = [
        {
            "id": i,
            "file": path.name,
            "identity": list(module.activity_identity(record)),
            "clic_slug": None,
        }
        for i, record in enumerate([buyer, private])
    ]
    monkeypatch.setattr(
        module,
        "registry",
        lambda: {
            "hybrid_sources": {"nodes": nodes, "files": {path.name: {"sha256": digest}}}
        },
    )
    import bw2io

    monkeypatch.setattr(
        bw2io,
        "ExcelImporter",
        lambda *args: SimpleNamespace(data=copy.deepcopy([buyer, private])),
    )
    import premise.inventory.profiles as profiles

    monkeypatch.setattr(profiles, "select_companion_profiles", lambda *a, **k: None)
    import premise.inventory.inventory_imports as imports

    class Importer:
        def __init__(self, **kwargs):
            self.records = [
                r
                for r in kwargs["preloaded_importer"].data
                if kwargs["record_filter"](r)
            ]
            kwargs["before_linking"](self.records)
            self.list_unlinked = []

        def merge_inventory(self):
            return self.records

    monkeypatch.setattr(imports, "DefaultInventory", Importer)
    plan = module.HybridInventoryPlan(
        (), frozenset({0, 1}), ((path, "3.12"),), frozenset(), "plan"
    )
    records, metadata, _ = module.import_retained_inventories(
        plan, [], [helper], version="3.12", system_model="cutoff"
    )
    assert records[0]["exchanges"][0]["name"] == "private"
    assert records[0]["exchanges"][0]["amount"] == 3
    assert all(module.managed_inventory_key(r)[0] == "workbook" for r in records)
    assert metadata[0]["sha256"] == digest
    with pytest.raises(ClicInventoryError, match="checksum"):
        module.import_retained_inventories(
            plan, [], [helper], version="3.12", system_model="cutoff", workbook_pins={}
        )


@pytest.mark.parametrize("backend", ["legacy", "compact"])
def test_source_signature_changes_after_exchange_mutation(backend):
    from premise.inventory.inventory_store import create_inventory_store

    data = activity(
        "root",
        exchanges=[
            {
                "type": "production",
                "name": "root",
                "product": "root",
                "location": "GLO",
                "unit": "kilogram",
                "amount": 1,
            }
        ],
    )
    store = create_inventory_store([data], backend=backend, compute_fingerprints=True)
    first = module.source_store_signature(store)
    activity_id = next(iter(store.iter_activity_ids()))
    edge_id = next(iter(store.activity(activity_id).exchange_ids))
    with store.transaction("changed quantity") as transaction:
        transaction.patch_exchange(edge_id, {"amount": 2})
    assert module.source_store_signature(store) != first
    assert module.source_store_signature(store) == module.source_store_signature(store)
