import copy
import json
import pickle
import random

import numpy as np
import pytest
from wurst import searching as ws

import premise.new_database as new_database_module
import premise.inventory_store as inventory_store_module
import premise.utils as utils_module
from premise.brightway25 import _prepare_fast_exchange_payload
from premise.inventory_store import (
    ActivityKey,
    ActivityQuery,
    CompactInventoryStore,
    FilterExpression,
    InventoryStore,
    InventoryStoreBuilder,
    InventoryStoreCorruptionError,
    InventoryStoreError,
    InventoryStoreReadOnlyError,
    InventoryStoreVersionError,
    IndexedInventoryList,
    LegacyInventoryStore,
    ProviderKey,
    ReadOnlyInventoryStore,
    _compact_scenario_mapping,
    _hydrate_scenario_mapping,
    compact_exchange_payload,
    filter_biosphere_category,
    get_scenario_inventory,
    replace_scenario_inventory,
)
from premise.new_database import NewDatabase
from premise.transformation import clone_inventory_dataset


def test_compact_exchange_protocols_preserve_generic_semantics(tmp_path):
    exchange = {
        "name": "provider",
        "product": "product",
        "reference product": "legacy product alias",
        "location": "GLO",
        "unit": "kilogram",
        "type": "technosphere",
        "amount": np.float64(2.5),
        "uncertainty type": 0,
        "input": ("db", "provider-code"),
        "comment": None,
    }
    compact = compact_exchange_payload(exchange)
    expected_fields = (
        exchange["type"],
        exchange["name"],
        exchange["reference product"],
        exchange["location"],
        exchange["unit"],
        exchange["amount"],
    )

    assert compact._premise_relink_fields() == expected_fields
    assert _prepare_fast_exchange_payload(compact) == (
        _prepare_fast_exchange_payload(exchange)
    )

    store = CompactInventoryStore(
        [
            {
                "name": "provider",
                "reference product": "product",
                "location": "GLO",
                "unit": "kilogram",
                "code": "provider-code",
                "database": "db",
                "exchanges": [exchange],
            }
        ]
    )
    checkpoint = store.checkpoint(tmp_path / "protocol.inventory-store")
    reopened = InventoryStore.open(checkpoint)
    columnar = reopened._checkout_materialized(discard_shared_state=True)[0][
        "exchanges"
    ][0]

    assert columnar._premise_relink_fields() == expected_fields
    assert _prepare_fast_exchange_payload(columnar) == (
        _prepare_fast_exchange_payload(exchange)
    )


def test_export_load_consumes_private_handoff_without_reopening_checkpoint(
    inventory, tmp_path, monkeypatch
):
    store = CompactInventoryStore(copy.deepcopy(inventory))
    store._scenario_cache_compatibility = True
    checkpoint = store.checkpoint(tmp_path / "handoff.inventory-store")
    handoff = InventoryStore.open(checkpoint)
    expected = InventoryStore.open(checkpoint).materialize(restore_metadata=True)
    scenario = {
        "model": "image",
        "pathway": "SSP2-Base",
        "year": 2050,
        "_inventory_checkpoint": checkpoint,
        "_inventory_export_handoff": handoff,
    }
    monkeypatch.setattr(
        InventoryStore,
        "open",
        classmethod(
            lambda cls, path: (_ for _ in ()).throw(
                AssertionError("bounded handoff should avoid reopening the checkpoint")
            )
        ),
    )

    loaded = utils_module.load_database(
        scenario,
        original_database=[],
        load_metadata=True,
        consume_compact=True,
    )

    assert loaded["database"] == expected
    assert len(handoff) == 0


@pytest.fixture
def inventory():
    return [
        {
            "name": "market for electricity, low voltage",
            "reference product": "electricity, low voltage",
            "location": "CH",
            "unit": "kilowatt hour",
            "database": "ecoinvent",
            "code": "ch-grid",
            "classifications": [("CPC", "171")],
            "custom": {"tuple": (1, 2), "list": [np.float64(3.5)]},
            "exchanges": [
                {
                    "name": "market for electricity, low voltage",
                    "product": "electricity, low voltage",
                    "location": "CH",
                    "unit": "kilowatt hour",
                    "type": "production",
                    "amount": 1.0,
                    "output": ("ecoinvent", "ch-grid"),
                }
            ],
        },
        {
            "name": "market for electricity, low voltage",
            "reference product": "electricity, low voltage",
            "location": "CH",
            "unit": "kilowatt hour",
            "database": "ecoinvent",
            "code": "ch-grid-duplicate",
            "exchanges": [],
        },
        {
            "name": "electricity use",
            "reference product": "service",
            "location": "CH",
            "unit": "unit",
            "database": "ecoinvent",
            "code": "consumer",
            "exchanges": [
                {
                    "name": "market for electricity, low voltage",
                    "product": "electricity, low voltage",
                    "location": "CH",
                    "unit": "kilowatt hour",
                    "type": "technosphere",
                    "amount": np.float64(2.0),
                    "input": ("ecoinvent", "ch-grid"),
                }
            ],
        },
        {
            "name": "market for electricity, low voltage",
            "reference product": "electricity, low voltage",
            "location": "GLO",
            "unit": "kilowatt hour",
            "database": "ecoinvent",
            "code": "glo-grid",
            "exchanges": [],
        },
    ]


def test_compact_added_exchange_preserves_mutable_mapping_semantics():
    payload = {
        "name": "market for fuel",
        "product": "fuel",
        "amount": np.float32(2.5),
        "type": "technosphere",
        "unit": "kilogram",
        "location": "GLO",
        "uncertainty type": 2,
        "custom": {"values": [1, 2]},
    }

    exchange = compact_exchange_payload(payload)

    assert dict(exchange) == payload
    assert exchange._extra is not None
    assert exchange.get("amount") == np.float32(2.5)
    assert exchange.get("missing", "fallback") == "fallback"
    exchange["amount"] = 4.0
    exchange["minimum"] = 1.0
    del exchange["uncertainty type"]
    assert exchange["amount"] == 4.0
    assert exchange["minimum"] == 1.0
    assert "uncertainty type" not in exchange

    cloned = copy.deepcopy(exchange)
    cloned["custom"]["values"].append(3)
    assert exchange["custom"] == {"values": [1, 2]}
    assert dict(pickle.loads(pickle.dumps(exchange))) == dict(exchange)


def test_columnar_exchange_get_preserves_sidecar_and_overlay_semantics(
    inventory, tmp_path
):
    inventory[0]["exchanges"][0]["location"] = None
    checkpoint = CompactInventoryStore(inventory).checkpoint(
        tmp_path / "source.inventory-store"
    )
    exchange = InventoryStore.open(checkpoint)._checkout_materialized()[0]["exchanges"][
        0
    ]
    marker = object()

    assert exchange.get("location", marker) is None
    assert exchange.get("missing", marker) is marker
    exchange["location"] = "DE"
    assert exchange.get("location", marker) == "DE"
    del exchange["location"]
    assert exchange.get("location", marker) is marker


def test_biosphere_category_filter_preserves_order_sidecars_and_overlays(
    inventory, tmp_path
):
    categories = ("natural resource", "in ground")
    exchanges = [
        {
            "name": "Copper",
            "unit": "kilogram",
            "type": "biosphere",
            "categories": categories,
            "amount": 1.0,
        },
        {
            "name": "Gold",
            "unit": "kilogram",
            "type": "biosphere",
            "categories": list(categories),
            "amount": 2.0,
        },
        {
            "name": "market for copper",
            "product": "copper",
            "unit": "kilogram",
            "type": "technosphere",
            "amount": 1.0,
        },
    ]
    inventory[0]["exchanges"].extend(exchanges)
    checkpoint = CompactInventoryStore(inventory).checkpoint(
        tmp_path / "source.inventory-store"
    )
    source_exchanges = InventoryStore.open(checkpoint)._checkout_materialized()[0][
        "exchanges"
    ]
    copper, gold, technosphere = source_exchanges[1:4]
    storage = copper._storage

    first_masks = storage.biosphere_category_masks(categories, "kilogram")
    repeated_masks = storage.biosphere_category_masks(categories, "kilogram")
    match_mask, decisive_mask = first_masks

    assert repeated_masks is first_masks
    assert not match_mask.flags.writeable
    assert not decisive_mask.flags.writeable
    assert match_mask[copper._row]
    assert decisive_mask[copper._row]
    assert not decisive_mask[gold._row]
    assert decisive_mask[technosphere._row]

    assert filter_biosphere_category(source_exchanges, categories, "kilogram") == [
        copper,
        gold,
    ]
    copper["categories"] = ("air", "urban air close to ground")
    assert filter_biosphere_category(source_exchanges, categories, "kilogram") == [gold]
    compact = compact_exchange_payload(exchanges[0])
    assert filter_biosphere_category([compact], categories, "kilogram") == [compact]


def test_ordered_queries_duplicates_masks_and_immutable_records(inventory):
    store = CompactInventoryStore(inventory)
    query = ActivityQuery(
        filters=(
            FilterExpression("name", "market for", "contains"),
            FilterExpression("name", "market", "startswith"),
        ),
        masks=(FilterExpression("location", "GLO"),),
    )

    records = store.find(query)

    assert [record["code"] for record in records] == ["ch-grid", "ch-grid-duplicate"]
    assert store.contains(
        ActivityKey(
            "market for electricity, low voltage",
            "electricity, low voltage",
            "CH",
        )
    )
    with pytest.raises(TypeError):
        records[0]["custom"]["new"] = "forbidden"
    with pytest.raises(AttributeError):
        records[0]["custom"]["list"].append(4)


def test_compact_indexes_are_lazy_and_invalidated_atomically(inventory):
    store = CompactInventoryStore(inventory)
    assert store._state.indexes_ready is False
    assert store._state.exchange_owner == {}
    assert type(store._state.exchanges).__name__ == "_DenseExchangeTable"
    assert isinstance(store._state.activity_exchanges[0], range)

    assert store.find_one({"code": "consumer"})["name"] == "electricity use"
    assert store._state.indexes_ready is True

    with store.transaction("move-consumer") as tx:
        tx.patch_activity(2, {"location": "DE"})
        assert store._state.indexes_ready is False

    assert store.find_one({"location": "DE"})["code"] == "consumer"
    assert store._state.indexes_ready is True

    exchange = store.exchange(0)
    assert exchange.activity_id == 0
    assert len(store._state.exchange_owner) == len(store._state.exchanges)

    with store.transaction("append-exchange") as tx:
        tx.add_exchange(0, {"name": "new flow", "type": "biosphere", "amount": 1})
    assert isinstance(store._state.activity_exchanges[0], list)


def test_find_one_provider_order_and_reverse_consumers(inventory):
    store = CompactInventoryStore(inventory)
    provider_key = ProviderKey(
        "market for electricity, low voltage",
        "electricity, low voltage",
        "kilowatt hour",
    )

    assert [record["location"] for record in store.providers(provider_key, "CH")] == [
        "CH",
        "CH",
        "GLO",
    ]
    assert store.consumers(0) == (2,)
    with pytest.raises(ValueError, match="found 2"):
        store.find_one({"location": "CH", "unit": "kilowatt hour"})
    assert store.find_one({"code": "consumer"}).id == 2


def test_transaction_commands_commit_and_rollback(inventory):
    store = CompactInventoryStore(inventory)
    initial = store.materialize()

    with pytest.raises(RuntimeError):
        with store.transaction("rollback") as tx:
            tx.patch_activity(0, {"location": "DE"})
            tx.remove_exchange(0)
            tx.add_activity(
                {
                    "name": "temporary",
                    "reference product": "temporary",
                    "location": "GLO",
                    "unit": "unit",
                    "exchanges": [],
                }
            )
            raise RuntimeError("abort")

    assert store.materialize() == initial
    assert store.generation == 0

    with store.transaction("complete") as tx:
        cloned = tx.clone_activity(
            0,
            {"name": "cloned grid", "code": "clone"},
            {0: {"amount": 4.0}},
        )
        tx.patch_activity(cloned, {"comment": "added"}, delete_fields=("custom",))
        exchange_id = tx.add_exchange(
            cloned,
            {"name": "flow", "type": "biosphere", "amount": 1.0},
        )
        tx.patch_exchange(exchange_id, {"amount": 2.0})
        tx.remove_exchange(exchange_id)
        tx.replace_exchanges(
            1,
            [{"name": "replacement", "type": "production", "amount": 1.0}],
        )
        tx.remove_activity(3)

    assert store.generation == 1
    assert store.find_one({"code": "clone"})["exchanges"][0]["amount"] == 4.0
    assert (
        store.find_one({"code": "ch-grid-duplicate"})["exchanges"][0]["name"]
        == "replacement"
    )
    assert not store.contains(
        ActivityKey(
            "market for electricity, low voltage",
            "electricity, low voltage",
            "GLO",
        )
    )


def test_compact_transaction_snapshot_copies_structure_not_untouched_payloads(
    inventory,
):
    store = CompactInventoryStore(inventory)
    untouched_activity = store._state.activities[1]
    untouched_exchange = store._state.exchanges[1]

    with pytest.raises(RuntimeError):
        with store.transaction("shallow-rollback") as tx:
            assert store._state.activities[1] is untouched_activity
            assert store._state.exchanges[1] is untouched_exchange
            tx.patch_activity(0, {"location": "DE"})
            tx.patch_exchange(1, {"amount": 3.0})
            assert store._state.activities[0] is not tx._snapshot.activities[0]
            assert store._state.exchanges[1] is not tx._snapshot.exchanges[1]
            raise RuntimeError("abort")

    assert store._state.activities[1] is untouched_activity
    assert store._state.exchanges[1] is untouched_exchange
    assert store.activity(0)["location"] == "CH"
    assert store.exchange(1)["amount"] == np.float64(2.0)


def test_nested_and_out_of_context_transactions_are_rejected(inventory):
    store = CompactInventoryStore(inventory)
    transaction = store.transaction("outside")
    with pytest.raises(InventoryStoreError, match="with block"):
        transaction.add_activity({})

    with store.transaction("outer"):
        with pytest.raises(InventoryStoreError, match="Nested"):
            with store.transaction("inner"):
                pass


def test_compact_forks_are_copy_on_write_and_isolated(inventory):
    source = CompactInventoryStore(inventory)
    forks = [source.fork(identity) for identity in ("a", "b", "c")]
    source_activity = source._state.activities[0]
    untouched_activity = source._state.activities[1]
    untouched_exchange = source._state.exchanges[1]

    with forks[0].transaction("scenario:a") as tx:
        tx.patch_activity(0, {"location": "DE"})
    assert forks[0]._state.activities[0] is not source_activity
    assert forks[0]._state.activities[1] is untouched_activity
    assert forks[0]._state.exchanges[1] is untouched_exchange
    with forks[1].transaction("scenario:b") as tx:
        tx.remove_activity(0)
    with forks[2].transaction("scenario:c") as tx:
        tx.add_activity(
            {
                "name": "scenario c",
                "reference product": "service",
                "location": "GLO",
                "unit": "unit",
                "exchanges": [],
            }
        )

    assert source.activity(0)["location"] == "CH"
    assert forks[0].activity(0)["location"] == "DE"
    assert len(forks[1]) == len(source) - 1
    assert len(forks[2]) == len(source) + 1


def test_compact_checkout_requires_exclusive_or_discarded_shared_state(inventory):
    source = CompactInventoryStore(inventory)
    scenario = source.fork("scenario")

    with pytest.raises(InventoryStoreError, match="shared compact state"):
        scenario._checkout_materialized()

    checked_out = scenario._checkout_materialized(discard_shared_state=True)

    assert checked_out == inventory
    assert len(scenario) == 0
    checked_out[0]["location"] = "DE"
    # A forced checkout transfers ownership away from every shared reference;
    # NewDatabase drops the source reference as part of the same operation.
    assert source.activity(0)["location"] == "DE"


def test_checkpoint_roundtrip_preserves_arbitrary_metadata(inventory, tmp_path):
    inventory[2]["exchanges"][0].update(
        {
            "uncertainty type": 2,
            "loc": np.float32(-0.25),
            "production volume": 42.5,
            "categories": ("air", "urban air close to ground"),
            "comment": "kept in the lossless sidecar",
            "maximum": None,
        }
    )
    store = CompactInventoryStore(inventory, scenario_identity=("image", 2050))
    with store.transaction("metadata") as tx:
        tx.patch_activity(0, {"empty": None, "categories": ("air", "urban")})

    checkpoint = store.checkpoint(tmp_path / "scenario.inventory-store")
    reopened = InventoryStore.open(checkpoint)

    materialized = reopened.materialize()
    exchange = materialized[2]["exchanges"][0]

    assert reopened.backend_name == store.backend_name
    assert materialized == store.materialize()
    assert type(exchange["amount"]) is np.float64
    assert type(exchange["uncertainty type"]) is int
    assert type(exchange["loc"]) is np.float32
    assert type(exchange["production volume"]) is float
    assert exchange["categories"] == ("air", "urban air close to ground")
    assert exchange["comment"] == "kept in the lossless sidecar"
    assert exchange["maximum"] is None
    assert reopened._state.activity_fingerprints == store._state.activity_fingerprints
    assert set(path.name for path in checkpoint.iterdir()) == {
        "manifest.json",
        "strings.arrow",
        "activities.arrow",
        "exchanges.arrow",
        "metadata.bin",
        "metadata_offsets.arrow",
        "activity-fingerprints.pkl",
        "checksums.json",
    }


def test_activity_fingerprint_is_invalidated_and_recomputed_on_exchange_patch(
    inventory,
):
    store = CompactInventoryStore(inventory)
    activity_id = 2
    exchange_id = next(iter(store._state.activity_exchanges[activity_id]))
    before = store._activity_fingerprint(activity_id)

    with store.transaction("patch exchange") as transaction:
        transaction.patch_exchange(
            exchange_id,
            {"amount": 4.0},
            activity_id=activity_id,
        )

    assert activity_id not in store._state.activity_fingerprints
    assert store._activity_fingerprint(activity_id) != before


def test_activity_fingerprints_are_invariant_to_materialized_roundtrip(inventory):
    store = CompactInventoryStore(copy.deepcopy(inventory))
    before = dict(store._ensure_activity_fingerprints())

    reopened = CompactInventoryStore(store.materialize(restore_metadata=True))

    assert reopened._ensure_activity_fingerprints() == before


def test_columnar_checkpoint_checkout_and_proxy_clone_are_copy_on_write(
    inventory, tmp_path
):
    checkpoint = CompactInventoryStore(inventory).checkpoint(
        tmp_path / "source.inventory-store"
    )
    reopened = InventoryStore.open(checkpoint)

    assert type(reopened._state.exchanges).__name__ == "_ColumnarExchangeTable"
    working_copy = reopened._checkout_materialized()
    source_exchange = working_copy[2]["exchanges"][0]
    cloned = clone_inventory_dataset(working_copy[2])
    cloned_exchange = cloned["exchanges"][0]

    assert type(cloned).__name__ == "_ColumnarActivityMapping"
    assert cloned._storage is working_copy[2]._storage
    assert type(source_exchange).__name__ == "_ColumnarExchangeMapping"
    assert cloned_exchange is not source_exchange
    assert cloned_exchange._storage is source_exchange._storage

    cloned_exchange["amount"] = np.float64(7)
    del cloned_exchange["input"]

    assert source_exchange["amount"] == np.float64(2)
    assert source_exchange["input"] == ("ecoinvent", "ch-grid")
    assert cloned_exchange["amount"] == np.float64(7)
    assert "input" not in cloned_exchange

    expected = copy.deepcopy(cloned)
    restored_store = CompactInventoryStore(
        [cloned], take_ownership=True, scenario_identity="clone"
    )
    assert restored_store._state.exchanges[0] is cloned_exchange
    assert restored_store.materialize()[0] == expected


def test_columnar_metadata_row_index_preserves_activity_ordinals(inventory, tmp_path):
    inventory[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.5,
        }
    )
    checkpoint = CompactInventoryStore(inventory).checkpoint(
        tmp_path / "source.inventory-store"
    )
    storage = InventoryStore.open(checkpoint)._state.exchanges._storage

    assert storage._activity_and_ordinal(0) == (0, 0)
    assert storage._activity_and_ordinal(1) == (0, 1)
    assert storage._activity_and_ordinal(2) == (2, 0)
    with pytest.raises(KeyError):
        storage._activity_and_ordinal(-1)
    with pytest.raises(KeyError):
        storage._activity_and_ordinal(storage.row_count)


def test_columnar_activity_deepcopy_keeps_sidecar_lazy_and_isolated(
    inventory, tmp_path
):
    checkpoint = CompactInventoryStore(inventory).checkpoint(
        tmp_path / "source.inventory-store"
    )
    source = InventoryStore.open(checkpoint)._checkout_materialized()[0]
    storage = source._storage

    cloned = copy.deepcopy(source)

    assert type(cloned).__name__ == "_ColumnarActivityMapping"
    assert cloned._storage is storage
    assert type(cloned["exchanges"][0]).__name__ == "_ColumnarExchangeMapping"
    assert len(storage._activity_cache) == 0

    cloned["location"] = "DE"
    cloned["custom"]["list"].append("changed")
    assert source["location"] == "CH"
    assert source["custom"]["list"] == [np.float64(3.5)]


def test_fresh_columnar_views_share_storage_but_isolate_mutations(inventory, tmp_path):
    checkpoint = CompactInventoryStore(inventory).checkpoint(
        tmp_path / "source.inventory-store"
    )
    source = InventoryStore.open(checkpoint)

    first = source.fresh_columnar_view(("image", 2030))
    second = source.fresh_columnar_view(("image", 2050))
    first_database = first._checkout_materialized()
    second_database = second._checkout_materialized()

    assert first_database[0]._storage is second_database[0]._storage
    assert first_database[0]._storage is source._state.exchanges._storage

    first_database[0]["location"] = "DE"
    first_database[0]["exchanges"][0]["amount"] = 7.0

    assert second_database[0]["location"] == "CH"
    assert second_database[0]["exchanges"][0]["amount"] == 1.0
    assert source.materialize() == inventory


def test_columnar_activity_hot_fields_remain_mapping_compatible(inventory, tmp_path):
    checkpoint = CompactInventoryStore(inventory).checkpoint(
        tmp_path / "source.inventory-store"
    )
    activity = InventoryStore.open(checkpoint)._checkout_materialized()[0]

    activity["database"] = "scenario-db"
    activity["code"] = "scenario-code"
    activity["type"] = "process"

    assert activity["database"] == "scenario-db"
    assert activity["code"] == "scenario-code"
    assert activity["type"] == "process"
    assert list(activity).count("code") == 1
    assert dict(activity)["database"] == "scenario-db"
    assert activity.copy()["type"] == "process"

    del activity["type"]
    assert "type" not in activity
    activity["type"] = "processwithreferenceproduct"
    assert activity.pop("type") == "processwithreferenceproduct"
    assert "type" not in activity


def test_columnar_exchange_fast_checkpoint_roundtrip_is_lossless(inventory, tmp_path):
    inventory[2]["exchanges"][0].update(
        {
            "uncertainty type": 3,
            "loc": 2.0,
            "scale": 0.4,
            "minimum": 1.0,
            "maximum": 3.0,
            "categories": ("technosphere", "test"),
            "comment": "source metadata",
            "custom": {"values": [1, 2]},
        }
    )
    source = CompactInventoryStore(inventory).checkpoint(
        tmp_path / "source.inventory-store"
    )
    activity = InventoryStore.open(source)._checkout_materialized()[2]
    exchange = activity["exchanges"][0]

    exchange["name"] = "updated input"
    exchange["amount"] = np.float32(3.25)
    exchange["categories"] = ("water", "ocean")
    exchange["comment"] = "updated metadata"
    exchange["custom"]["values"].append(3)
    del exchange["input"]
    exchange["new metadata"] = ("kept", None)
    expected = copy.deepcopy(activity)

    updated_store = CompactInventoryStore([activity], take_ownership=True)
    updated = updated_store.checkpoint(tmp_path / "updated.inventory-store")
    restored = InventoryStore.open(updated).materialize()[0]

    assert restored == expected
    assert type(restored["exchanges"][0]["amount"]) is np.float32
    assert "input" not in restored["exchanges"][0]


def test_columnar_scenario_checkpoint_reuses_compatibility_scan(inventory, tmp_path):
    inventory[2]["exchanges"][0].update(
        {
            "location": "None",
            "amount": 0,
            "uncertainty type": 0,
            "loc": np.float32("nan"),
            "production volume": np.nan,
            "comment": 0,
            "custom false": False,
        }
    )
    source = CompactInventoryStore(inventory).checkpoint(
        tmp_path / "source.inventory-store"
    )
    store = InventoryStore.open(source)
    store._scenario_cache_compatibility = True

    first = store.checkpoint(tmp_path / "first.inventory-store")
    storage = store._state.exchanges._storage
    compatible_rows = storage.scenario_cache_compatible_rows()
    scanned_activities = frozenset(storage._scenario_cache_scanned_metadata_activities)
    invalid_row = int(storage.exchange_starts[2])

    assert storage.scenario_cache_compatible_rows() is compatible_rows
    assert not compatible_rows.flags.writeable
    assert not compatible_rows[invalid_row]
    second = store.checkpoint(tmp_path / "second.inventory-store")

    assert scanned_activities == frozenset(storage.exchange_metadata_offsets)
    assert scanned_activities == frozenset(
        storage._scenario_cache_scanned_metadata_activities
    )
    assert (
        InventoryStore.open(first).materialize()
        == InventoryStore.open(second).materialize()
    )
    exchange = InventoryStore.open(second).materialize()[2]["exchanges"][0]
    assert exchange["amount"] == 0
    assert exchange["uncertainty type"] == 0
    assert "location" not in exchange
    assert "loc" not in exchange
    assert "production volume" not in exchange
    assert "comment" not in exchange
    assert "custom false" not in exchange


def test_scenario_delta_checkpoint_roundtrip_preserves_order_and_compatibility(
    inventory, tmp_path
):
    source = CompactInventoryStore(inventory).checkpoint(
        tmp_path / "source.inventory-store"
    )
    database = InventoryStore.open(source)._checkout_materialized()
    database[0]["comment"] = None
    database[0]["has_downstream_consumer"] = False
    database[0]["custom"]["list"].append("scenario overlay")
    database[0]["exchanges"][0].update(
        {
            "location": "None",
            "amount": 0,
            "production volume": np.nan,
            "comment": 0,
        }
    )
    database[2]["exchanges"][0]["amount"] = np.float32(3.25)
    database.append(
        {
            "name": "added activity",
            "reference product": "service",
            "location": "GLO",
            "unit": "unit",
            "code": "added",
            "exchanges": [
                {
                    "name": "added activity",
                    "product": "service",
                    "location": "GLO",
                    "unit": "unit",
                    "type": "production",
                    "amount": 1,
                }
            ],
        }
    )

    expected = copy.deepcopy(database)
    for dataset in expected:
        utils_module._normalize_scenario_cache_activity(dataset)
        for exchange in dataset["exchanges"]:
            utils_module._normalize_scenario_cache_exchange(exchange)

    scenario_store = CompactInventoryStore(database, take_ownership=True)
    scenario_store._scenario_cache_compatibility = True
    checkpoint = scenario_store.checkpoint(tmp_path / "scenario.inventory-store")
    manifest = json.loads((checkpoint / "manifest.json").read_text())
    restored_store = InventoryStore.open(checkpoint)
    restored = restored_store.materialize()
    checkout = restored_store._checkout_materialized()
    checkout[2]["exchanges"][0]["input"] = ("export-db", "provider")

    assert manifest["columnar_format"] == "scenario-delta-v2"
    assert set(path.name for path in checkpoint.iterdir()) == {
        "manifest.json",
        "scenario-activities.arrow",
        "scenario-segments.arrow",
        "activity-fingerprints.pkl",
        "checksums.json",
    }
    assert restored == expected
    assert checkout[2]["exchanges"][0]["input"] == ("export-db", "provider")
    assert type(restored[2]["exchanges"][0]["amount"]) is np.float32
    loaded = utils_module.load_database(
        {"_inventory_checkpoint": checkpoint},
        original_database=[],
        consume_compact=True,
    )
    assert loaded["database"] == expected

    # The independent V1 dispatcher remains readable after V2 becomes the
    # default writer.
    v1_checkpoint = inventory_store_module._write_scenario_delta_checkpoint_v1(
        scenario_store,
        tmp_path / "scenario-v1.inventory-store",
        inventory_store_module._scenario_delta_base_storage(scenario_store),
    )
    assert InventoryStore.open(v1_checkpoint).materialize() == expected

    source.rename(tmp_path / "moved-source.inventory-store")
    with pytest.raises(InventoryStoreCorruptionError, match="base checkpoint"):
        InventoryStore.open(checkpoint)


def test_load_database_transfers_compact_store_without_exchange_materialization(
    inventory, tmp_path
):
    checkpoint = CompactInventoryStore(inventory).checkpoint(
        tmp_path / "scenario.inventory-store"
    )
    store = InventoryStore.open(checkpoint)
    scenario = {"_inventory_store": store}

    loaded = utils_module.load_database(
        scenario,
        original_database=[],
        consume_compact=True,
    )

    assert len(store) == 0
    assert len(loaded["database"]) == len(inventory)
    assert type(loaded["database"][2]).__name__ == "_ColumnarActivityMapping"
    assert (
        type(loaded["database"][2]["exchanges"][0]).__name__
        == "_ColumnarExchangeMapping"
    )


def test_load_database_keeps_compact_store_for_non_consuming_callers(inventory):
    store = CompactInventoryStore(inventory)
    scenario = {"_inventory_store": store}

    loaded = utils_module.load_database(scenario, original_database=[])

    assert len(store) == len(inventory)
    assert loaded["database"] == inventory
    assert type(loaded["database"][0]) is dict


def test_checkpoint_backed_scenario_can_be_consumed_repeatedly(inventory, tmp_path):
    checkpoint = CompactInventoryStore(inventory).checkpoint(
        tmp_path / "scenario.inventory-store"
    )
    scenario = {"_inventory_checkpoint": checkpoint}
    manifest = (checkpoint / "manifest.json").read_bytes()

    first = utils_module.load_database(
        scenario,
        original_database=[],
        consume_compact=True,
    )
    second = utils_module.load_database(
        scenario,
        original_database=[],
        consume_compact=True,
    )

    assert first["database"] == inventory
    assert second["database"] == inventory
    assert scenario == {"_inventory_checkpoint": checkpoint}
    assert (checkpoint / "manifest.json").read_bytes() == manifest


@pytest.mark.parametrize("consume_compact", [False, True])
def test_load_database_assigns_missing_codes_for_compact_store(
    inventory, consume_compact
):
    inventory_without_code = copy.deepcopy(inventory)
    inventory_without_code[0].pop("code")
    store = CompactInventoryStore(inventory_without_code)

    loaded = utils_module.load_database(
        {"_inventory_store": store},
        original_database=[],
        consume_compact=consume_compact,
    )

    assert isinstance(loaded["database"][0]["code"], str)
    assert loaded["database"][0]["code"]


def test_consuming_loader_assigns_code_without_reading_activity_sidecar(
    inventory, tmp_path
):
    inventory_without_code = copy.deepcopy(inventory)
    inventory_without_code[0].pop("code")
    checkpoint = CompactInventoryStore(inventory_without_code).checkpoint(
        tmp_path / "source.inventory-store"
    )
    store = InventoryStore.open(checkpoint)
    storage = store._state.activities[0]._storage

    loaded = utils_module.load_database(
        {"_inventory_store": store},
        original_database=[],
        consume_compact=True,
    )

    assert loaded["database"][0]["code"]
    assert len(storage._activity_cache) == 0


def test_compact_scenario_mapping_keeps_metadata_lazy_and_lossless(inventory, tmp_path):
    mapped_activity = inventory[0]
    detached_activity = {"name": "detached", "reference product": "service"}
    mapping = {
        "electricity": {
            "grid": [mapped_activity, mapped_activity, detached_activity],
        }
    }
    store = CompactInventoryStore(inventory, take_ownership=True)
    checkpoint = store.checkpoint(tmp_path / "scenario.inventory-store")

    compacted = _compact_scenario_mapping(mapping, store, checkpoint)
    first, repeated, detached = compacted["electricity"]["grid"]

    assert first is repeated
    assert first is not mapped_activity
    assert detached is detached_activity
    assert first._resolver._store is None
    assert first["name"] == "market for electricity, low voltage"
    assert first.get("location") == "CH"
    assert "lhv" not in first
    assert first._resolver._store is None
    assert "classifications" in first
    assert first._resolver._store is not None
    assert dict(first) == store._state.activities[0]
    assert "_activity_id" not in first
    assert "exchanges" not in first

    custom = first["custom"]
    custom["list"].append("changed")
    assert first["custom"]["list"] == [np.float64(3.5)]

    copied = copy.deepcopy(first)
    restored = pickle.loads(pickle.dumps(first))
    assert copied["name"] == restored["name"] == first["name"]
    assert copied._resolver._store is None
    assert restored._resolver._store is None

    activity_ids = tuple(store.iter_activity_ids())
    working_copy = store._checkout_materialized()
    hydrated = _hydrate_scenario_mapping(
        compacted,
        dict(zip(activity_ids, working_copy)),
    )
    hydrated_first, hydrated_repeated, hydrated_detached = hydrated["electricity"][
        "grid"
    ]
    assert hydrated_first is hydrated_repeated
    assert hydrated_first is working_copy[0]
    assert hydrated_detached is detached_activity


def test_checkpoint_corruption_and_schema_are_rejected(inventory, tmp_path):
    checkpoint = CompactInventoryStore(inventory).checkpoint(
        tmp_path / "scenario.inventory-store"
    )
    (checkpoint / "metadata.bin").write_bytes(b"corrupt")
    with pytest.raises(InventoryStoreCorruptionError, match="Checksum"):
        InventoryStore.open(checkpoint)

    checkpoint = CompactInventoryStore(inventory).checkpoint(checkpoint)
    manifest_path = checkpoint / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["schema_version"] = 999
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(InventoryStoreVersionError, match="Unsupported"):
        InventoryStore.open(checkpoint)


def test_legacy_pickle_cleanup_preserves_inventory_checkpoints(
    inventory, monkeypatch, tmp_path
):
    checkpoint = CompactInventoryStore(inventory).checkpoint(
        tmp_path / "scenario.inventory-store"
    )
    legacy_pickle = tmp_path / "scenario.pickle"
    legacy_pickle.write_bytes(b"legacy")
    monkeypatch.setattr(utils_module, "DIR_CACHED_FILES", tmp_path)

    utils_module.delete_all_pickles()

    assert checkpoint.is_dir()
    assert not legacy_pickle.exists()


def test_builder_and_read_only_facade(inventory):
    builder = InventoryStoreBuilder()
    builder.extend(inventory[:2])
    store = builder.seal("scenario")
    view = ReadOnlyInventoryStore(store)

    assert len(view) == 2
    with pytest.raises(InventoryStoreReadOnlyError):
        view.transaction("forbidden")
    with pytest.raises(InventoryStoreError, match="only be called once"):
        builder.seal()


def test_internal_ids_never_appear_in_materialized_payloads(inventory):
    store = CompactInventoryStore(inventory)

    assert all("activity_id" not in dataset for dataset in store.materialize())
    assert all(
        "exchange_id" not in exchange
        for dataset in store.materialize()
        for exchange in dataset["exchanges"]
    )


def test_new_database_public_store_api_and_removed_database_attribute(inventory):
    obj = object.__new__(NewDatabase)
    obj._inventory_api_active = True
    obj._source_inventory_store = CompactInventoryStore(inventory)
    obj.scenarios = [{"model": "image", "pathway": "SSP2-Base", "year": 2050}]

    with pytest.raises(AttributeError, match="materialize_inventory"):
        _ = obj.database
    with pytest.raises(AttributeError, match="get_inventory_store"):
        obj.database = []

    view = obj.get_inventory_store()
    assert isinstance(view, ReadOnlyInventoryStore)
    assert obj.materialize_inventory() == inventory
    assert "database" not in obj.scenarios[0]

    writable = obj.get_inventory_store(writable=True)
    with writable.transaction("test") as tx:
        tx.patch_activity(0, {"location": "DE"})
    assert obj.materialize_inventory()[0]["location"] == "DE"
    assert obj._source_inventory_store.materialize()[0]["location"] == "CH"


def test_inventory_backend_selector_remains_publicly_compatible():
    import inspect

    parameter = inspect.signature(NewDatabase).parameters["inventory_backend"]
    assert parameter.default == "legacy"
    assert LegacyInventoryStore([]).backend_name == "legacy"


def test_new_database_runtime_materialization_is_not_attached_to_scenario(inventory):
    obj = object.__new__(NewDatabase)
    obj._inventory_api_active = True
    obj._source_inventory_store = CompactInventoryStore(inventory)
    obj.scenarios = [{"model": "image", "pathway": "SSP2-Base", "year": 2050}]

    runtime = obj._load_scenario_database_for_update(obj.scenarios[0], 0)

    assert runtime["_inventory_working_copy"] == inventory
    assert "database" not in obj.scenarios[0]
    runtime["_inventory_working_copy"][0]["location"] = "DE"
    obj._store_updated_scenario(obj.scenarios[0], runtime, persist=False)
    assert "database" not in obj.scenarios[0]
    assert obj.materialize_inventory()[0]["location"] == "DE"


def test_new_database_compact_scenario_checks_out_reloadable_source(inventory):
    obj = object.__new__(NewDatabase)
    obj._inventory_api_active = True
    obj._source_inventory_store = CompactInventoryStore(inventory)
    obj.scenarios = [{"model": "image", "pathway": "SSP2-Base", "year": 2050}]
    obj.database_cache_filepath = "source-cache"
    obj.inventories_cache_filepath = "inventory-cache"
    obj.additional_inventories = None

    runtime = obj._load_scenario_database_for_update(obj.scenarios[0], 0)

    assert runtime["_inventory_working_copy"] == inventory
    assert obj._source_inventory_store is None
    assert "_inventory_store" not in obj.scenarios[0]


def test_compact_scenarios_reload_source_instead_of_deep_copying_it(inventory):
    obj = object.__new__(NewDatabase)
    obj._inventory_api_active = True
    obj._source_inventory_store = CompactInventoryStore(inventory)
    obj.scenarios = [
        {"model": "image", "pathway": "SSP2-Base", "year": year}
        for year in (2030, 2050)
    ]
    obj.database_cache_filepath = "source-cache"
    obj.inventories_cache_filepath = "inventory-cache"
    obj.additional_inventories = None
    obj._load_original_database = lambda: copy.deepcopy(inventory)

    first = obj._load_scenario_database_for_update(obj.scenarios[0], 0)
    first["_inventory_working_copy"][0]["location"] = "DE"
    second = obj._load_scenario_database_for_update(obj.scenarios[1], 1)

    assert first["_inventory_working_copy"][0]["location"] == "DE"
    assert second["_inventory_working_copy"][0]["location"] == "CH"
    assert obj._source_inventory_store is None


def test_compact_scenarios_reuse_pristine_columnar_source_storage(inventory, tmp_path):
    checkpoint = CompactInventoryStore(inventory).checkpoint(
        tmp_path / "source.inventory-store"
    )
    obj = object.__new__(NewDatabase)
    obj._inventory_api_active = True
    obj._source_inventory_store = InventoryStore.open(checkpoint)
    obj._compact_source_checkpoint = checkpoint
    obj.scenarios = [
        {"model": "image", "pathway": "SSP2-Base", "year": year}
        for year in (2030, 2050)
    ]
    obj.database_cache_filepath = "source-cache"
    obj.inventories_cache_filepath = "inventory-cache"
    obj.additional_inventories = None

    first = obj._load_scenario_database_for_update(obj.scenarios[0], 0)
    first_database = first["_inventory_working_copy"]
    first_database[0]["location"] = "DE"
    second = obj._load_scenario_database_for_update(obj.scenarios[1], 1)
    second_database = second["_inventory_working_copy"]

    assert obj._source_inventory_store is not None
    assert first_database[0]._storage is second_database[0]._storage
    assert (
        first_database[0]._storage
        is obj._source_inventory_store._state.exchanges._storage
    )
    assert second_database[0]["location"] == "CH"


@pytest.mark.parametrize("persist", [False, True])
def test_new_database_update_keeps_only_private_inventory_state(
    inventory, monkeypatch, tmp_path, persist
):
    obj = object.__new__(NewDatabase)
    obj._inventory_api_active = True
    obj._source_inventory_store = CompactInventoryStore(inventory)
    obj.scenarios = [{"model": "image", "pathway": "SSP2-Base", "year": 2050}]
    obj.version = "3.12"
    obj.system_model = "cutoff"
    obj.use_absolute_efficiency = False
    obj.gains_scenario = "CLE"
    obj.database_cache_filepath = None
    obj.inventories_cache_filepath = None
    obj.additional_inventories = None

    def fake_update(scenario, version, system_model):
        assert version == "3.12"
        assert system_model == "cutoff"
        assert "database" not in scenario
        database = get_scenario_inventory(scenario)
        database.append(
            {
                "name": "updated activity",
                "reference product": "service",
                "location": "GLO",
                "unit": "unit",
                "code": "updated",
                "comment": None,
                "has_downstream_consumer": False,
                "regionalized": False,
                "exchanges": [
                    {
                        "name": "updated activity",
                        "product": "service",
                        "location": "None",
                        "unit": "unit",
                        "type": "production",
                        "amount": 0,
                        "uncertainty type": 0,
                        "production volume": np.nan,
                        "comment": 0,
                        "categories": [],
                    }
                ],
            }
        )
        replace_scenario_inventory(scenario, database)
        return scenario

    monkeypatch.setattr(new_database_module, "_update_biomass", fake_update)
    monkeypatch.setattr(new_database_module, "DIR_CACHED_FILES", tmp_path)

    obj.update("biomass", persist=persist)

    scenario = obj.scenarios[0]
    assert "database" not in scenario
    assert "_inventory_working_copy" not in scenario
    assert ("_inventory_checkpoint" in scenario) is persist
    assert ("_inventory_store" in scenario) is (not persist)
    assert obj.get_inventory_store().find_one({"code": "updated"})["name"] == (
        "updated activity"
    )
    updated = obj.materialize_inventory()[-1]
    assert updated["regionalized"] is False
    assert "comment" not in updated
    assert "has_downstream_consumer" not in updated
    assert updated["exchanges"] == [
        {
            "name": "updated activity",
            "product": "service",
            "unit": "unit",
            "type": "production",
            "amount": 0,
            "uncertainty type": 0,
            "categories": [],
        }
    ]


def test_new_database_promotes_compact_emissions_to_store_native_path(
    inventory, monkeypatch
):
    obj = object.__new__(NewDatabase)
    obj._inventory_api_active = True
    obj._source_inventory_store = CompactInventoryStore(inventory)
    obj.scenarios = [{"model": "image", "pathway": "SSP2-Base", "year": 2050}]
    obj.version = "3.12"
    obj.system_model = "cutoff"
    obj.use_absolute_efficiency = False
    obj.gains_scenario = "CLE"
    obj.database_cache_filepath = None
    obj.inventories_cache_filepath = None
    obj.additional_inventories = None

    def fake_update(scenario, version, system_model, gains_scenario):
        assert (version, system_model, gains_scenario) == ("3.12", "cutoff", "CLE")
        assert "_inventory_working_copy" not in scenario
        store = scenario["_inventory_store"]
        assert isinstance(store, CompactInventoryStore)
        with store.transaction("sector:emissions") as transaction:
            transaction.patch_activity(
                2,
                {"store native": True, "has_downstream_consumer": False},
            )
            transaction.patch_exchange(
                1,
                {"comment": 0, "production volume": np.nan},
            )
        return scenario

    monkeypatch.setattr(new_database_module, "_update_emissions", fake_update)

    obj.update("emissions", persist=False)

    scenario = obj.scenarios[0]
    assert "_inventory_working_copy" not in scenario
    assert isinstance(scenario["_inventory_store"], CompactInventoryStore)
    assert obj.get_inventory_store().activity(2)["store native"] is True
    assert "has_downstream_consumer" not in obj.get_inventory_store().activity(2)
    assert "comment" not in obj.get_inventory_store().exchange(1)
    assert "production volume" not in obj.get_inventory_store().exchange(1)


def test_seeded_randomized_compact_mutations_remain_self_consistent(inventory):
    rng = random.Random(42)
    rng_values = [rng.random() for _ in range(40)]
    store = CompactInventoryStore(inventory)

    for step in range(40):
        activity_ids = list(store.iter_activity_ids())
        operation = rng.choice(("patch", "clone", "add_exchange", "replace"))
        target = rng.choice(activity_ids)
        with store.transaction(f"random:{step}:{operation}") as tx:
            if operation == "patch":
                tx.patch_activity(target, {"random value": rng_values[step]})
            elif operation == "clone":
                tx.clone_activity(
                    target,
                    {"name": f"clone {step}", "code": f"random-clone-{step}"},
                )
            elif operation == "add_exchange":
                tx.add_exchange(
                    target,
                    {
                        "name": f"flow {step}",
                        "type": "biosphere",
                        "amount": rng_values[step],
                    },
                )
            else:
                tx.replace_exchanges(
                    target,
                    [
                        {
                            "name": f"replacement {step}",
                            "type": "production",
                            "amount": rng_values[step],
                        }
                    ],
                )
        assert len(tuple(store.iter_activity_ids())) == len(store.materialize())


def test_indexed_wurst_bridge_preserves_order_and_invalidates(inventory):
    database = IndexedInventoryList(inventory)
    filters = (
        ws.contains("name", "market for"),
        ws.equals("unit", "kilowatt hour"),
        ws.exclude(ws.equals("location", "GLO")),
    )

    assert [dataset["code"] for dataset in ws.get_many(database, *filters)] == [
        "ch-grid",
        "ch-grid-duplicate",
    ]

    database.append(
        {
            "name": "market for added product",
            "reference product": "added product",
            "location": "CH",
            "unit": "kilowatt hour",
            "code": "added",
            "exchanges": [],
        }
    )
    assert [dataset["code"] for dataset in ws.get_many(database, *filters)] == [
        "ch-grid",
        "ch-grid-duplicate",
        "added",
    ]


def test_indexed_wurst_bridge_intersection_does_not_mutate_exact_indexes(inventory):
    database = IndexedInventoryList(inventory)
    name_filter = ws.equals("name", "market for electricity, low voltage")

    assert [
        dataset["code"]
        for dataset in ws.get_many(
            database,
            name_filter,
            ws.equals("location", "GLO"),
        )
    ] == ["glo-grid"]
    assert [dataset["code"] for dataset in ws.get_many(database, name_filter)] == [
        "ch-grid",
        "ch-grid-duplicate",
        "glo-grid",
    ]


def test_indexed_wurst_bridge_builds_fields_lazily_and_updates_them(inventory):
    database = IndexedInventoryList(inventory)

    assert list(ws.get_many(database, ws.contains("name", "market for")))
    assert set(database._query_indexes[0]) == {"name"}
    assert set(database._query_indexes[1]) == {"name"}

    database.append(
        {
            "name": "market for added product",
            "reference product": "added product",
            "location": "CH",
            "unit": "kilogram",
            "code": "added",
            "exchanges": [],
        }
    )
    assert (
        len(inventory) in database._query_indexes[0]["name"]["market for added product"]
    )
    assert "unit" not in database._query_indexes[0]

    assert [
        dataset["code"]
        for dataset in ws.get_many(database, ws.equals("unit", "kilogram"))
    ] == ["added"]
    assert set(database._query_indexes[0]) == {"name", "unit"}


def test_indexed_wurst_bridge_uses_ordered_fallback_for_dynamic_predicates(inventory):
    database = IndexedInventoryList(inventory)
    dynamic = lambda dataset: dataset.get("code", "").endswith("grid")

    assert [dataset["code"] for dataset in ws.get_many(database, dynamic)] == [
        "ch-grid",
        "glo-grid",
    ]


def test_indexed_wurst_bridge_preserves_equals_none_semantics(inventory):
    database = IndexedInventoryList(inventory)

    assert [
        dataset["code"] for dataset in ws.get_many(database, ws.equals("type", None))
    ] == [
        "ch-grid",
        "ch-grid-duplicate",
        "consumer",
        "glo-grid",
    ]
