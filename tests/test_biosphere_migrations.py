"""Biosphere migrations must preserve flow identity and exchange amounts."""

from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest

from premise.inventory_imports import (
    BaseInventoryImport,
    apply_biosphere_migration,
    discover_biosphere_migrations,
    get_biosphere_code,
    get_correspondence_bio_flows,
    migrate_import_db,
)


def migrate_and_link(exchanges, version="3.12"):
    importer = object.__new__(BaseInventoryImport)
    importer.path = Path("test.xlsx")
    importer.import_db = SimpleNamespace(data=[{"exchanges": deepcopy(exchanges)}])
    importer.biosphere_dict = get_biosphere_code(version)
    importer.correspondence_bio_flows = get_correspondence_bio_flows()
    migrate_import_db(importer.import_db, "3.9", version)
    importer.add_biosphere_links()
    return importer.import_db.data[0]["exchanges"]


def exchange(name, categories, unit="kilogram", amount=0.09782716612814853):
    return {
        "name": name,
        "categories": categories,
        "unit": unit,
        "type": "biosphere",
        "amount": amount,
        "uncertainty type": 3,
        "loc": amount,
        "scale": 0.01,
    }


@pytest.mark.parametrize("version", ["3.10", "3.11", "3.12"])
@pytest.mark.parametrize("as_string", [False, True])
def test_waste_heat_moves_to_the_official_replacement_compartment(
    version, as_string, capsys
):
    categories = ("air", "lower stratosphere + upper troposphere")
    if as_string:
        categories = "::".join(categories)
    original = exchange("Heat, waste", categories, "megajoule", 0.369051500728296)
    retained = exchange("Heat, waste", ("water", "surface water"), "megajoule")
    migrated = migrate_and_link([original, retained], version)
    target = get_biosphere_code(version)
    assert migrated == [
        {
            **original,
            "categories": ("air", "unspecified"),
            "input": ("biosphere3", "f2d84834-d0b3-42e5-b41a-f04cc80337a4"),
        },
        {
            **retained,
            "input": (
                "biosphere3",
                target[("Heat, waste", "water", "surface water", "megajoule")],
            ),
        },
    ]
    assert "Could not find" not in capsys.readouterr().out


@pytest.mark.parametrize("version", ["3.10", "3.11", "3.12"])
def test_sodium_resource_is_not_renamed_like_emissions(version, capsys):
    originals = [
        exchange("Sodium", ("natural resource", "in ground")),
        exchange("Sodium", ("soil", "industrial")),
        exchange("Sodium", ("water", "ground-")),
        exchange("Sodium I", ("water", "surface water")),
    ]
    migrated = migrate_and_link(originals, version)
    assert len(migrated) == len(originals)
    assert migrated[0] == {
        **originals[0],
        "input": ("biosphere3", "fab932d4-0a58-491c-9d7f-294d07a7953d"),
    }
    for old, new in zip(originals[1:], migrated[1:]):
        expected = {**old, "name": "Sodium I"}
        expected["input"] = (
            "biosphere3",
            get_biosphere_code(version)[("Sodium I", *old["categories"], "kilogram")],
        )
        # Formula metadata is carried by some official name-change rules.
        assert {k: v for k, v in new.items() if k != "formula"} == expected
    assert "Could not find" not in capsys.readouterr().out


def test_uuid_delete_does_not_remove_other_compartments():
    original = exchange("Sodium", ("natural resource", "in ground"))
    removed = exchange("Sodium", ("soil", "industrial"))
    rules = {
        "source_id": "ecoinvent-3.9-biosphere",
        "target_id": "ecoinvent-3.10-biosphere",
        "delete": [
            {
                "source": {
                    "name": "Sodium",
                    "uuid": "cc1c987a-2b6e-4cbc-969a-73f1c96de448",
                }
            }
        ],
    }
    data = [{"exchanges": [deepcopy(original), removed]}]
    apply_biosphere_migration(data, rules)
    assert data == [{"exchanges": [original]}]


def test_uuid_rule_never_falls_back_to_name_only():
    original = exchange("Sodium", ("natural resource", "in ground"))
    rules = {"delete": [{"source": {"name": "Sodium", "uuid": "unknown"}}]}
    data = [
        {"exchanges": [deepcopy(original), {**original, "input": ("bio", "unknown")}]}
    ]
    apply_biosphere_migration(data, rules)
    assert data == [{"exchanges": [original]}]


def test_replacement_clears_old_link_and_preserves_amount_and_uncertainty():
    original = exchange(
        "Heat, waste", ("air", "lower stratosphere + upper troposphere"), "megajoule"
    )
    original["input"] = ("bio", "ef7b0b7e-16e2-4e14-af28-94d9dd3c675d")
    original["uuid"] = original["input"][1]
    data = [{"exchanges": [deepcopy(original)]}]
    rules = discover_biosphere_migrations()[("3.9", "3.10")]
    apply_biosphere_migration(data, rules)
    expected = {
        **original,
        "categories": ("air", "unspecified"),
        "uuid": "f2d84834-d0b3-42e5-b41a-f04cc80337a4",
    }
    expected.pop("input")
    assert data == [{"exchanges": [expected]}]


def test_biosphere_fix_invalidates_inventory_cache_only(monkeypatch):
    import premise.new_database as module

    database = object.__new__(module.NewDatabase)
    database.source_type = "brightway"
    database.keep_source_db_uncertainty = False
    database.keep_imports_uncertainty = True
    old_inventory = database._database_cache_path("source", inventories=True)
    source = database._database_cache_path("source")
    monkeypatch.setattr(module, "BIOSPHERE_MIGRATION_CACHE_VERSION", 999)
    assert database._database_cache_path("source", inventories=True) != old_inventory
    assert database._database_cache_path("source") == source


@pytest.mark.parametrize(
    "name,categories,expected_name,expected_code",
    [
        (
            "Copper",
            ("air", "non-urban air or from high stacks"),
            "Copper ion",
            "e336eee7-148a-4d1c-8027-780cbfafa12b",
        ),
        (
            "Manganese",
            ("water", "surface water"),
            "Manganese II",
            "f532985c-90b7-46fc-aac9-b039b40e22f1",
        ),
        (
            "Carbon dioxide, from soil or biomass stock",
            ("air", "urban air close to ground"),
            "Carbon dioxide, from soil or biomass stock",
            "e8787b5e-d927-446d-81a9-f56977bbfeb4",
        ),
    ],
)
def test_legacy_flow_identity_survives_all_forward_migration_steps(
    name, categories, expected_name, expected_code
):
    original = exchange(name, categories)
    importer = object.__new__(BaseInventoryImport)
    importer.path = Path("test.xlsx")
    importer.import_db = SimpleNamespace(data=[{"exchanges": [deepcopy(original)]}])
    importer.biosphere_dict = get_biosphere_code("3.12")
    importer.correspondence_bio_flows = get_correspondence_bio_flows()
    migrate_import_db(importer.import_db, "3.7", "3.12")
    importer.add_biosphere_links()
    result = importer.import_db.data[0]["exchanges"]
    assert len(result) == 1
    assert {k: v for k, v in result[0].items() if k != "formula"} == {
        **original,
        "name": expected_name,
        "input": ("biosphere3", expected_code),
    }
