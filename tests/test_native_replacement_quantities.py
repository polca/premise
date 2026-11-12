"""Resolve native quantities with the ordinary grouped placeholder filler."""

import copy
from dataclasses import asdict

import pytest
import yaml

from premise.inventory import replacement_quantities as module
from premise.inventory.clic_archive import ClicInventoryError, activity_identity
from premise.inventory.clic_hybrid import _workbook_background
from premise.inventory.clic_target import resolve_inventory_target


def activity(name, unit="kilogram", location="RER"):
    return {
        "name": name,
        "reference product": name,
        "unit": unit,
        "location": location,
        "database": "native",
        "code": name,
        "exchanges": [
            {
                "name": name,
                "product": name,
                "unit": unit,
                "location": location,
                "type": "production",
                "amount": 1,
            }
        ],
    }


@pytest.fixture
def fixture(tmp_path, monkeypatch):
    donor = activity("native donor")
    gas = activity("native gas", "megajoule")
    donor["exchanges"] += [
        {
            "name": gas["name"],
            "product": gas["reference product"],
            "unit": "megajoule",
            "location": "RER",
            "type": "technosphere",
            "amount": 12,
            "input": ("native", gas["code"]),
        },
        {
            "name": "CO2",
            "unit": "kilogram",
            "categories": ("air",),
            "type": "biosphere",
            "amount": -2,
            "input": ("bio", "co2"),
        },
    ]
    record = activity("workbook CCS")
    record["inventory provenance"] = {
        "file": "test.xlsx",
        "provider": "retained workbook",
        "record_id": "fixture",
    }
    parent = {
        "replacement name": "source donor",
        "replacement product": "source donor",
        "replacement location": "GLO",
    }
    record["exchanges"] += [
        {
            "name": "source gas",
            "product": "gas",
            "unit": "cubic meter",
            "type": "technosphere",
            "location": location,
            "amount": 0,
            **parent,
        }
        for location in ("CH", "RoW")
    ] + [
        {
            "name": "CO2",
            "unit": "kilogram",
            "categories": ["air"],
            "type": "biosphere",
            "amount": 0,
            **parent,
        }
    ]
    target = asdict(resolve_inventory_target("2026", family="uvek"))
    recipe = {
        "id": "test",
        "file": "test.xlsx",
        "source_version": "3.9",
        "target": target,
        "activity": list(activity_identity(record)),
        "replacement_source": ["source donor", "source donor", "GLO"],
        "donor": list(activity_identity(donor)),
        "donor_reference_amount": 1,
        "limitations": ["Synthetic native supply proxy."],
        "slots": [
            {
                "source": module._selector(edge),
                "native": [
                    module._selector(
                        donor["exchanges"][1 if edge["type"] == "technosphere" else 2]
                    )
                ],
            }
            for edge in record["exchanges"][1:]
        ],
    }
    path = tmp_path / "review.yaml"
    path.write_text(yaml.safe_dump({"schema_version": 1, "recipes": [recipe]}))
    monkeypatch.setattr(module, "REVIEW_PATH", path)
    return record, [donor, gas], target, path, recipe


def prepare(record, database, target):
    return module.prepare_replacement_quantities(
        [record], database, _workbook_background("3.9"), target
    )


def test_shared_grouping_preserves_native_units_signs_and_explicit_edges(fixture):
    record, database, target, _, _ = fixture
    before = copy.deepcopy(database)
    explicit = copy.deepcopy(record["exchanges"][0])
    with pytest.warns(UserWarning, match="Filled replacement quantities"):
        notes = prepare(record, database, target)
    assert database == before
    assert record["exchanges"][0] == explicit
    assert len(record["exchanges"]) == 2  # Two source gas geographies fill once.
    gas = record["exchanges"][1]
    assert (gas["amount"], gas["unit"], gas["input"]) == (
        12,
        "megajoule",
        ("native", "native gas"),
    )
    assert gas[module.NATIVE_INPUT] is True
    assert record[module.NATIVE_BIOSPHERE][0]["amount"] == -2
    assert notes[0]["omissions"] == []
    assert all("replacement name" not in edge for edge in record["exchanges"])


@pytest.mark.parametrize(
    "change",
    ["amount", "parent", "missing_donor", "reference", "flow", "scope", "profile"],
)
def test_changed_or_unreviewed_queries_fail_without_mutation(fixture, change):
    record, database, target, path, recipe = fixture
    if change == "amount":
        record["exchanges"][1]["amount"] = 0.1
    elif change == "parent":
        record["exchanges"][1]["replacement name"] = "other"
    elif change == "missing_donor":
        database = database[1:]
    elif change == "reference":
        database[0]["exchanges"][0]["amount"] = -1
    elif change == "flow":
        database[0]["exchanges"].pop()
    elif change == "scope":
        record["exchanges"].pop()
    elif change == "profile":
        recipe["source_version"] = "3.10"
        path.write_text(yaml.safe_dump({"schema_version": 1, "recipes": [recipe]}))
    before = copy.deepcopy(record)
    with pytest.raises(ClicInventoryError):
        prepare(record, database, target)
    assert record == before


def test_declared_omission_is_warned_and_audited(fixture):
    record, database, target, path, recipe = fixture
    recipe["slots"][-1].update(
        native=[],
        limitation="Not represented by this native donor; unknown score effect.",
    )
    path.write_text(yaml.safe_dump({"schema_version": 1, "recipes": [recipe]}))
    with pytest.warns(UserWarning, match="1 source slots"):
        notes = prepare(record, database, target)
    assert len(notes[0]["omissions"]) == 1
    assert record[module.NATIVE_BIOSPHERE] == []


def test_standard_harmonizer_keeps_filled_native_inputs_out_of_source_migration(
    fixture, tmp_path
):
    from brightpath.background import PackageCatalogProvider
    from premise.inventory.clic_hybrid import _prepare_family_records

    record, database, _, _, _ = fixture
    record["exchanges"][-1]["database"] = "biosphere3"
    before = copy.deepcopy((record, database))
    with pytest.warns(UserWarning, match="Filled replacement quantities"):
        prepared, diagnostics = _prepare_family_records(
            [([record], _workbook_background("3.9"))],
            database,
            version="2026",
            family="uvek",
            system_model="cutoff",
            biosphere_name="bio",
            cache_dir=tmp_path / "cache",
            catalog_provider=PackageCatalogProvider(),
        )
    assert (record, database) == before
    assert len(prepared) == 1
    technosphere = [e for e in prepared[0]["exchanges"] if e["type"] == "technosphere"]
    biosphere = [e for e in prepared[0]["exchanges"] if e["type"] == "biosphere"]
    assert len(technosphere) == len(biosphere) == 1
    assert technosphere[0]["input"] == ("native", "native gas")
    assert technosphere[0]["amount"] == 12
    assert biosphere[0]["input"] == ("bio", "co2")
    assert "database" not in biosphere[0]
    assert biosphere[0]["amount"] == -2
    assert any(
        d["code"] == "clic.native_replacement_quantities_filled" for d in diagnostics
    )


def test_warm_cache_cannot_bypass_withdrawn_native_recipe(fixture, tmp_path):
    from brightpath.background import PackageCatalogProvider
    from premise.inventory.clic_hybrid import _prepare_family_records

    record, database, _, path, _ = fixture
    options = dict(
        version="2026",
        family="uvek",
        system_model="cutoff",
        biosphere_name="bio",
        cache_dir=tmp_path / "cache",
        catalog_provider=PackageCatalogProvider(),
    )
    for warm in (False, True):
        with pytest.warns(UserWarning, match="Filled replacement quantities"):
            prepared, diagnostics = _prepare_family_records(
                [([record], _workbook_background("3.9"))], database, **options
            )
        assert sum(e["type"] == "biosphere" for e in prepared[0]["exchanges"]) == 1
        manifest = next(
            d["manifest"]
            for d in diagnostics
            if d["code"] == "clic.workbook_harmonized"
        )
        assert all(c["cache_hit"] is warm for c in manifest["cohorts"])
    path.write_text(yaml.safe_dump({"schema_version": 1, "recipes": []}))
    with pytest.raises(ClicInventoryError, match="no unique reviewed native donor"):
        _prepare_family_records(
            [([record], _workbook_background("3.9"))], database, **options
        )


def test_duplicate_source_geographies_keep_each_native_supplier_coefficient_once(
    fixture,
):
    record, database, target, _, _ = fixture
    extra = copy.deepcopy(database[0]["exchanges"][1])
    extra["amount"] = 3
    database[0]["exchanges"].append(extra)
    with pytest.warns(UserWarning):
        prepare(record, database, target)
    actual = [e["amount"] for e in record["exchanges"] if e["type"] == "technosphere"]
    assert actual == [12, 3]


def test_later_unreviewed_record_leaves_the_entire_batch_unchanged(fixture):
    record, database, target, _, _ = fixture
    other = copy.deepcopy(record)
    other["name"] = "unreviewed record"
    batch = [record, other]
    before = copy.deepcopy(batch)
    with pytest.raises(ClicInventoryError, match="no unique reviewed native donor"):
        module.prepare_replacement_quantities(
            batch, database, _workbook_background("3.9"), target
        )
    assert batch == before


def test_only_declared_slot_specific_donor_alias_is_accepted(fixture):
    record, database, target, path, recipe = fixture
    record["exchanges"][1]["replacement product"] = "source donor spelling variant"
    recipe["slots"][0]["replacement_source"] = [
        "source donor",
        "source donor spelling variant",
        "GLO",
    ]
    path.write_text(yaml.safe_dump({"schema_version": 1, "recipes": [recipe]}))
    with pytest.warns(UserWarning):
        prepare(record, database, target)
    assert [
        e["amount"] for e in record["exchanges"] if e["type"] == "technosphere"
    ] == [12]


def test_production_reference_with_lookup_metadata_is_not_a_placeholder(fixture):
    record, database, target, _, _ = fixture
    reference = record["exchanges"][0]
    reference.update(
        {
            "replacement name": "legacy donor",
            "replacement product": "legacy steel",
            "replacement location": "RER",
        }
    )
    before = copy.deepcopy(reference)
    with pytest.warns(UserWarning):
        prepare(record, database, target)
    assert record["exchanges"][0] == before
    assert record["exchanges"][0]["amount"] == 1


def test_scoped_explicit_input_keeps_quantity_uncertainty_before_normal_migration(
    fixture,
):
    record, database, target, path, recipe = fixture
    explicit = record["exchanges"][1]
    explicit.update(amount=0.25, **{"uncertainty type": 3, "loc": 0.25, "scale": 0.02})
    recipe["slots"][0].update(
        preserve_explicit_quantity=True,
        native=[],
        limitation="Documented source override; retain it and discard stale lookup tags.",
    )
    path.write_text(yaml.safe_dump({"schema_version": 1, "recipes": [recipe]}))
    with pytest.warns(UserWarning):
        notes = prepare(record, database, target)
    result = next(e for e in record["exchanges"] if e["name"] == "source gas")
    assert result["amount"] == result["loc"] == 0.25
    assert result["scale"] == 0.02
    assert "replacement name" not in result and module.NATIVE_INPUT not in result
    assert len(notes[0]["preserved explicit exchanges"]) == 1
    assert not notes[0]["omissions"]


@pytest.fixture
def reference_fixture(fixture):
    record, database, target, path, recipe = fixture
    parent = {
        "replacement name": "source donor",
        "replacement product": "source donor",
        "replacement location": "GLO",
    }
    producer = {
        "name": "source product",
        "product": "source product",
        "unit": "kilogram",
        "location": "GLO",
        "type": "technosphere",
        "amount": 0,
        **parent,
    }
    freight = {
        "name": "source delivery",
        "product": "delivery",
        "unit": "ton kilometer",
        "location": "GLO",
        "type": "technosphere",
        "amount": 0,
        **parent,
    }
    producer.update(minimum=0, maximum=0, negative=True)
    record["exchanges"] = [record["exchanges"][0], producer, freight]
    donor = database[0]
    recipe["slots"] = [
        {
            "source": module._selector(producer),
            "native_reference_supply": True,
            "native": [
                {
                    "type": "technosphere",
                    "name": donor["name"],
                    "product": donor["reference product"],
                    "unit": donor["unit"],
                    "location": donor["location"],
                }
            ],
            "limitation": "Complete native product proxy; internal inputs counted once.",
        },
        {
            "source": module._selector(freight),
            "native": [],
            "limitation": "Unknown outbound delivery omitted; native incoming freight is not copied.",
        },
    ]
    path.write_text(yaml.safe_dump({"schema_version": 1, "recipes": [recipe]}))
    return record, database, target, path, recipe


def test_complete_reference_does_not_duplicate_native_processing_or_biosphere(
    reference_fixture,
):
    record, database, target, _, _ = reference_fixture
    before = copy.deepcopy(database)
    with pytest.warns(UserWarning):
        notes = prepare(record, database, target)
    assert database == before
    assert len(record["exchanges"]) == 2
    purchase = record["exchanges"][1]
    assert (purchase["input"], purchase["amount"]) == (("native", "native donor"), 1)
    assert record[module.NATIVE_BIOSPHERE] == []
    assert not any(key in purchase for key in ("minimum", "maximum", "negative"))
    assert len(notes[0]["omissions"]) == len(notes[0]["native reference supplies"]) == 1


@pytest.mark.parametrize(
    "change", ["units", "explicit_input", "component_copy", "reference"]
)
def test_complete_reference_refuses_mixed_or_incompatible_boundaries(
    reference_fixture, change
):
    record, database, target, path, recipe = reference_fixture
    if change == "units":
        record["unit"] = "cubic meter"
        recipe["activity"] = list(activity_identity(record))
    elif change == "explicit_input":
        record["exchanges"].append(
            {
                "name": "extra supply",
                "unit": "kilogram",
                "type": "technosphere",
                "amount": 1,
            }
        )
    elif change == "component_copy":
        recipe["slots"][1]["native"] = [module._selector(database[0]["exchanges"][1])]
    else:
        record["exchanges"][0]["amount"] = 2
    path.write_text(yaml.safe_dump({"schema_version": 1, "recipes": [recipe]}))
    before = copy.deepcopy(record)
    with pytest.raises(ClicInventoryError, match="complete native product proxy"):
        prepare(record, database, target)
    assert record == before


def test_complete_native_reference_uses_existing_harmonizer_boundary(
    reference_fixture, tmp_path
):
    from brightpath.background import PackageCatalogProvider
    from premise.inventory.clic_hybrid import _prepare_family_records

    record, database, _, _, _ = reference_fixture
    with pytest.warns(UserWarning):
        records, _ = _prepare_family_records(
            [([record], _workbook_background("3.9"))],
            database,
            version="2026",
            family="uvek",
            system_model="cutoff",
            biosphere_name="bio",
            cache_dir=tmp_path / "cache",
            catalog_provider=PackageCatalogProvider(),
        )
    assert len(records) == 1 and len(records[0]["exchanges"]) == 2
    assert records[0]["exchanges"][1]["input"] == ("native", "native donor")
