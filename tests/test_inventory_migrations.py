import copy
import json

import premise.inventory_imports as inventory_imports
from premise.inventory_imports import (
    apply_aggregation,
    apply_backward_replace,
    apply_biosphere_migration,
    apply_disaggregation,
    discover_available_migrations,
)


def activity_with(*exchanges):
    return [{"name": "consumer", "exchanges": list(exchanges)}]


def test_compiled_migration_rules_preserve_order_and_ignored_units():
    rules = [
        {
            "source": {
                "name": "old provider",
                "reference product": "fuel",
                "location": "GLO",
                "unit": "ignored unit",
            },
            "targets": [
                {
                    "name": "first target",
                    "reference product": "fuel a",
                    "location": "GLO",
                    "allocation": 0.25,
                },
                {
                    "name": "second target",
                    "reference product": "fuel b",
                    "location": "GLO",
                    "allocation": 0.75,
                },
            ],
        },
        {
            "source": {
                "name": "old provider",
                "reference product": "fuel",
                "location": "GLO",
            },
            "targets": [{"name": "must not win", "allocation": 1.0}],
        },
    ]
    original = {
        "name": "old provider",
        "reference product": "fuel",
        "product": "fuel",
        "location": "GLO",
        "unit": "kilogram",
        "type": "technosphere",
        "amount": 8.0,
        "input": ("source", "code"),
    }
    database = activity_with(copy.deepcopy(original))

    apply_disaggregation(database, rules)

    assert [exchange["name"] for exchange in database[0]["exchanges"]] == [
        "first target",
        "second target",
    ]
    assert [exchange["amount"] for exchange in database[0]["exchanges"]] == [
        2.0,
        6.0,
    ]
    assert all("input" not in exchange for exchange in database[0]["exchanges"])

    apply_aggregation(database, rules)

    expected = {**original, "amount": 8.0}
    expected.pop("input")
    assert database == activity_with(expected)


def test_compiled_backward_replacement_and_biosphere_precedence():
    replacement_rules = [
        {
            "source": {
                "name": "old provider",
                "reference product": "old product",
                "location": "GLO",
            },
            "target": {
                "name": "new provider",
                "reference product": "new product",
                "location": "GLO",
                "unit": "ignored",
            },
        }
    ]
    database = activity_with(
        {
            "name": "new provider",
            "reference product": "new product",
            "product": "new product",
            "location": "GLO",
            "unit": "kilogram",
            "type": "technosphere",
            "amount": 1.0,
            "input": ("db", "code"),
        }
    )

    apply_backward_replace(database, replacement_rules)

    exchange = database[0]["exchanges"][0]
    assert (exchange["name"], exchange["product"], exchange["location"]) == (
        "old provider",
        "old product",
        "GLO",
    )
    assert "input" not in exchange

    biosphere_database = activity_with(
        {"name": "deleted flow", "unit": "kg", "type": "biosphere"},
        {"name": "changed flow", "unit": "kg", "type": "biosphere"},
        {"name": "changed flow", "unit": "m3", "type": "biosphere"},
    )
    biosphere_rules = {
        "delete": [{"source": {"uuid": "ignored", "name": "deleted flow"}}],
        "replace": [
            {
                "source": {"name": "changed flow", "unit": "kg"},
                "target": {"name": "replacement", "uuid": "ignored"},
            }
        ],
    }

    apply_biosphere_migration(biosphere_database, biosphere_rules)

    assert biosphere_database[0]["exchanges"] == [
        {"name": "replacement", "unit": "kg", "type": "biosphere"},
        {"name": "changed flow", "unit": "m3", "type": "biosphere"},
    ]


def test_migration_descriptor_cache_invalidates_on_resource_signature(
    tmp_path, monkeypatch
):
    migration_root = tmp_path / "migrations"
    cutoff = migration_root / "cutoff"
    cutoff.mkdir(parents=True)
    descriptor = cutoff / "migration.json"
    descriptor.write_text(
        json.dumps(
            {
                "source_id": "ecoinvent-3.10-cutoff",
                "target_id": "ecoinvent-3.11-cutoff",
                "replace": [],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(inventory_imports, "MIGRATIONS_DIR", migration_root)
    inventory_imports._load_migration_descriptors.cache_clear()
    load_calls = 0
    original_load = inventory_imports.json.load

    def counted_load(stream):
        nonlocal load_calls
        load_calls += 1
        return original_load(stream)

    monkeypatch.setattr(inventory_imports.json, "load", counted_load)

    first = discover_available_migrations()
    second = discover_available_migrations()

    assert first == second
    assert load_calls == 1

    descriptor.write_text(
        json.dumps(
            {
                "source_id": "ecoinvent-3.10-cutoff",
                "target_id": "ecoinvent-3.11-cutoff",
                "replace": [],
                "disaggregate": [],
            }
        ),
        encoding="utf-8",
    )

    discover_available_migrations()
    assert load_calls == 2


def test_compiled_migration_rule_caches_are_bounded(monkeypatch):
    monkeypatch.setattr(inventory_imports, "_COMPILED_MIGRATION_CACHE_SIZE", 2)
    inventory_imports._COMPILED_RULE_INDEX_CACHE.clear()
    inventory_imports._COMPILED_BIOSPHERE_RULE_CACHE.clear()

    for index in range(3):
        rules = [
            {
                "source": {"name": f"source {index}"},
                "targets": [{"name": f"target {index}"}],
            }
        ]
        apply_disaggregation(
            activity_with(
                {
                    "name": f"source {index}",
                    "type": "technosphere",
                    "amount": 1.0,
                }
            ),
            rules,
        )
        apply_biosphere_migration(
            activity_with({"name": "flow", "type": "biosphere"}),
            {"delete": [{"source": {"name": f"unused {index}"}}]},
        )

    assert len(inventory_imports._COMPILED_RULE_INDEX_CACHE) == 2
    assert len(inventory_imports._COMPILED_BIOSPHERE_RULE_CACHE) == 2
