import pytest

from premise.database.new_database import NewDatabase
from premise.export.sparse_inventory import export_sparse_inventory
from premise.inventory.inventory_store import (
    CompactInventoryStore,
    LegacyInventoryStore,
)


def inventory():
    return [
        {
            "name": "producer",
            "reference product": "product",
            "location": "GLO",
            "unit": "kilogram",
            "database": "db",
            "code": "producer",
            "exchanges": [
                {
                    "name": "producer",
                    "product": "product",
                    "location": "GLO",
                    "unit": "kilogram",
                    "type": "production",
                    "amount": 1.0,
                },
                {
                    "name": "carbon dioxide",
                    "categories": ("air", "urban air close to ground"),
                    "unit": "kilogram",
                    "type": "biosphere",
                    "amount": 2.5,
                    "input": ("biosphere", "co2"),
                },
            ],
        }
    ]


def test_sparse_inventory_streams_compact_rows_without_materializing(monkeypatch):
    store = CompactInventoryStore(inventory(), scenario_identity=("remind", 2050))
    monkeypatch.setattr(
        store,
        "materialize",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("sparse export must not materialize the inventory")
        ),
    )

    exported = export_sparse_inventory(
        store,
        factorization_metadata={"schema_version": 17, "identities": {}},
    )

    activities = list(exported.iter_activities())
    exchanges = list(exported.iter_exchanges())
    assert exported.activity_count == 1
    assert exported.exchange_count == 2
    assert exported.scenario_identity == ("remind", 2050)
    assert exported.factorization_metadata["schema_version"] == 17
    assert activities[0].storage_id == 0
    assert activities[0].payload is store._state.activities[0]
    assert [row.output_activity_id for row in exchanges] == [0, 0]
    assert [row.ordinal for row in exchanges] == [0, 1]
    assert [row.payload["amount"] for row in exchanges] == [1.0, 2.5]
    assert [row.payload for row in exported.iter_exchanges()] == [
        row.payload for row in exchanges
    ]


def test_sparse_inventory_rejects_legacy_store():
    with pytest.raises(TypeError, match="CompactInventoryStore"):
        export_sparse_inventory(LegacyInventoryStore(inventory()))


def test_new_database_sparse_export_includes_factorization_sidecar():
    database = object.__new__(NewDatabase)
    database.inventory_backend = "compact"
    database.factorization_mode = True
    database._source_inventory_store = CompactInventoryStore(inventory())
    database.scenarios = [
        {
            "model": "remind",
            "pathway": "SSP2-NPi",
            "year": 2050,
            "_inventory_store": database._source_inventory_store,
            "_factorization_identities": {
                "schema_version": 17,
                "identities": {"producer": "stable-producer"},
            },
        }
    ]
    database.factorization_metadata_for_activities = lambda datasets, scenario=0: (
        database.scenarios[scenario]["_factorization_identities"]
    )

    exported = database.export_sparse_inventory()

    assert exported.factorization_metadata["identities"] == {
        "producer": "stable-producer"
    }
    assert [row.payload["code"] for row in exported.iter_activities()] == ["producer"]
