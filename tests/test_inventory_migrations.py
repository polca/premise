import copy
import json
import math

import pytest

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


def test_organic_chemical_market_backward_alias_preserves_forward_mapping():
    rules = discover_available_migrations()[("3.11", "3.12")]["replace"]
    products = (
        "chemical, organic, unspecified",
        "chemical, organic, basic precursors and materials",
    )
    database = activity_with(
        *[
            {
                "name": f"market for {product}",
                "reference product": product,
                "product": product,
                "location": "GLO",
                "unit": "kilogram",
                "type": "technosphere",
                "amount": 2.5,
            }
            for product in products
        ]
    )
    apply_backward_replace(database, rules)
    for exchange in database[0]["exchanges"]:
        assert exchange["name"] == "market for chemical, organic"
        assert exchange["product"] == "chemical, organic"
        assert exchange["reference product"] == "chemical, organic"
        assert exchange["amount"] == 2.5
        assert exchange["unit"] == "kilogram"

    inventory_imports.apply_forward_replace(database, rules)
    for exchange in database[0]["exchanges"]:
        assert exchange["name"] == f"market for {products[1]}"
        assert exchange["reference product"] == products[1]
        assert exchange["amount"] == 2.5


def test_tissue_paper_product_migrates_between_310_and_311():
    rules = discover_available_migrations()[("3.10", "3.11")]["replace"]
    old = {
        "name": "tissue paper production",
        "reference product": "tissue paper",
        "product": "tissue paper",
        "location": "RER",
        "unit": "kilogram",
        "type": "technosphere",
        "amount": 2.5,
        "uncertainty type": 2,
        "loc": 0.9162907318741551,
        "scale": 0.1,
    }
    database = activity_with(copy.deepcopy(old))
    inventory_imports.apply_forward_replace(database, rules)
    assert database == activity_with(
        {**old, "name": "tissue paper production, recycled"}
    )
    apply_backward_replace(database, rules)
    assert database == activity_with(old)


def test_pv_39_supplier_mappings_preserve_amounts_and_forward_market_shares():
    rules = discover_available_migrations()[("3.9", "3.10")]
    sodium = "sodium hydroxide, without water, in 50% solution state"
    cases = [
        (
            "1,1-difluoroethane production, hfc-152a",
            "1,1-difluoroethane, hfc-152a",
            "US",
            "1,1-difluoroethane production, HFC-152a",
            "1,1-difluoroethane, HFC-152a",
            "US",
        ),
        (
            "chlor-alkali electrolysis, mercury cell",
            sodium,
            "GLO",
            "chlor-alkali electrolysis, mercury cell",
            sodium,
            "RoW",
        ),
        (
            "chlor-alkali electrolysis, average production",
            sodium,
            "RER",
            f"market for {sodium}",
            sodium,
            "GLO",
        ),
    ]
    for energy in ("electricity", "heat"):
        product = f"{energy}, for reuse in municipal waste incineration only"
        cases.append(
            (
                "treatment of municipal solid waste, municipal incineration FAE",
                product,
                "CH",
                "treatment of municipal solid waste, incineration",
                product,
                "CH",
            )
        )
    for (
        name,
        product,
        location,
        expected_name,
        expected_product,
        expected_location,
    ) in cases:
        original = {
            "name": name,
            "reference product": product,
            "product": product,
            "location": location,
            "type": "technosphere",
            "amount": 2.5,
            "uncertainty type": 2,
            "loc": 0.9162907318741551,
            "scale": 0.1,
        }
        database = activity_with(copy.deepcopy(original))
        apply_backward_replace(database, rules["replace"])
        apply_aggregation(database, rules["disaggregate"])
        assert database == activity_with(
            {
                **original,
                "name": expected_name,
                "reference product": expected_product,
                "product": expected_product,
                "location": expected_location,
            }
        )

    market = {
        "name": f"market for {sodium}",
        "reference product": sodium,
        "product": sodium,
        "location": "GLO",
        "type": "technosphere",
        "amount": 10.0,
    }
    database = activity_with(copy.deepcopy(market))
    inventory_imports.apply_forward_replace(database, rules["replace"])
    apply_disaggregation(database, rules["disaggregate"])
    assert [
        (e["name"], e["location"], e["amount"]) for e in database[0]["exchanges"]
    ] == [
        (market["name"], "RoW", 10.0 * 0.9110384953195285),
        (market["name"], "RER", 10.0 * 0.08896150468047141),
    ]
    hfc = {
        "name": "1,1-difluoroethane production, HFC-152a",
        "reference product": "1,1-difluoroethane, HFC-152a",
        "location": "US",
        "type": "technosphere",
        "amount": 2.5,
    }
    database = activity_with(hfc)
    inventory_imports.apply_forward_replace(database, rules["replace"])
    assert hfc["name"] == "1,1-difluoroethane production"
    assert hfc["reference product"] == "1,1-difluoroethane"


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


def test_pv_38_concrete_proxy_preserves_volume_and_forward_market():
    rules = discover_available_migrations()[("3.8", "3.9")]["replace"]
    original = {
        "name": "lean concrete production, for building construction, with cement ZN/D, with 100% RC-M aggregates",
        "reference product": "lean concrete",
        "product": "lean concrete",
        "unit": "cubic meter",
        "location": "CH",
        "type": "technosphere",
        "amount": 2.5,
        "uncertainty type": 2,
        "loc": 0.9162907318741551,
        "scale": 0.1,
    }
    database = activity_with(copy.deepcopy(original))
    apply_backward_replace(database, rules)
    expected = activity_with({**original, "name": "market for lean concrete"})
    assert database == expected
    inventory_imports.apply_forward_replace(database, rules)
    assert database == expected


@pytest.mark.parametrize(
    "uncertainty, expected",
    [
        ({"uncertainty type": 2, "loc": math.log(8), "scale": 0.3},
         {"loc": math.log(2), "scale": 0.3}),
        ({"uncertainty type": 3, "loc": 8.0, "scale": 2.0},
         {"loc": 2.0, "scale": 0.5}),
        ({"uncertainty type": 4, "minimum": 4.0, "maximum": 12.0},
         {"minimum": 1.0, "maximum": 3.0}),
        ({"uncertainty type": 5, "loc": 8.0, "minimum": 4.0, "maximum": 12.0},
         {"loc": 2.0, "minimum": 1.0, "maximum": 3.0}),
    ],
)
def test_disaggregation_scales_uncertainty_with_supplier_amount(uncertainty, expected):
    original = {"name": "source", "type": "technosphere", "amount": 8.0,
                "input": ("old", "code"), **uncertainty}
    unchanged = copy.deepcopy(original)
    database = activity_with(original)
    rules = [{"source": {"name": "source"}, "targets": [
        {"name": "allocated", "allocation": 0.25},
        {"name": "remainder", "allocation": 0.75},
    ]}]

    apply_disaggregation(database, rules)

    allocated, remainder = database[0]["exchanges"]
    assert allocated["amount"] == 2.0
    assert allocated["amount"] + remainder["amount"] == 8.0
    assert allocated["uncertainty type"] == uncertainty["uncertainty type"]
    for field, value in expected.items():
        assert allocated[field] == pytest.approx(value)
    assert "input" not in allocated and "input" not in remainder
    assert original == unchanged


def test_polyethylene_migration_preserves_triangular_bounds_after_relinking():
    from premise.transformation import redefine_uncertainty_params

    rules = discover_available_migrations()[("3.11", "3.12")]["disaggregate"]
    amount = 0.00010300429184549356
    original = {
        "name": "market for polyethylene, low density, granulate",
        "reference product": "polyethylene, low density, granulate",
        "product": "polyethylene, low density, granulate",
        "location": "GLO", "unit": "kilogram", "type": "technosphere",
        "amount": amount, "uncertainty type": 5, "loc": amount,
        "minimum": 0.8 * amount, "maximum": 1.2 * amount,
    }
    database = activity_with(copy.deepcopy(original))

    apply_disaggregation(database, rules)

    exchanges = database[0]["exchanges"]
    assert len(exchanges) == 5
    assert sum(e["amount"] for e in exchanges) == pytest.approx(amount)
    for exchange in exchanges:
        assert exchange["minimum"] <= exchange["loc"] <= exchange["maximum"]
        assert exchange["loc"] == pytest.approx(exchange["amount"])
        loc, _, minimum, maximum, _ = redefine_uncertainty_params(
            exchange, {"amount": amount}
        )
        assert loc == pytest.approx(amount)
        assert minimum == pytest.approx(original["minimum"])
        assert maximum == pytest.approx(original["maximum"])
