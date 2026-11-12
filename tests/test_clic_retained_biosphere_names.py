"""Retained source labels use existing aliases without changing physical flows."""

import copy
import json

import pytest

from premise.inventory.clic_hybrid import (
    _canonicalize_owned_biosphere_names,
    _prepare_family_records,
)

BACKGROUND = {
    "technosphere": {"family": "ecoinvent", "version": "3.9", "system_model": "cutoff"},
    "biosphere": {"family": "ecoinvent", "version": "3.9"},
}


def activity(name="Chromium", unit="kilogram", categories=("air",)):
    return {
        "name": "synthetic steel",
        "reference product": "steel",
        "unit": "kilogram",
        "location": "CH",
        "exchanges": [
            {
                "type": "production",
                "name": "synthetic steel",
                "product": "steel",
                "unit": "kilogram",
                "location": "CH",
                "amount": 1,
            },
            {
                "type": "biosphere",
                "name": name,
                "unit": unit,
                "categories": categories,
                "amount": -2.0,
                "uncertainty type": 3,
                "loc": -2.0,
                "scale": 0.3,
                "formula": "dose",
                "comment": "source note",
            },
        ],
    }


def provider(identities):
    from brightpath import BiosphereProfile
    from brightpath.background.catalogs import BiosphereCatalog, InMemoryCatalogProvider

    return InMemoryCatalogProvider(
        biosphere=[
            BiosphereCatalog(
                BiosphereProfile("ecoinvent", "3.9"), frozenset(identities)
            )
        ]
    )


def test_all_thirteen_actual_legacy_labels_match_existing_aliases_and_publisher_UUIDs():
    from brightpath.background import PackageCatalogProvider
    from premise.inventory.inventory_imports import get_biosphere_code

    from pathlib import Path

    packet = json.loads(
        (
            Path(__file__).parents[1]
            / "docs/development/uvek-steel-retained-biosphere-frontier.json"
        ).read_text()
    )
    records = [
        activity(i["name"], i["unit"], tuple(i["categories"]))
        for i in packet["identities"]
    ]
    original = copy.deepcopy(records)
    notes = _canonicalize_owned_biosphere_names(
        records, BACKGROUND, PackageCatalogProvider()
    )
    assert len(notes) == len(records) == 13
    codes = get_biosphere_code("3.9")
    for record, before, note in zip(records, original, notes, strict=True):
        edge = record["exchanges"][1]
        name = edge.pop("name")
        assert edge == {k: v for k, v in before["exchanges"][1].items() if k != "name"}
        edge["name"] = name
        categories = edge["categories"]
        assert (
            note["flow_id"]
            == codes[
                (
                    name,
                    categories[0],
                    categories[1] if len(categories) > 1 else "unspecified",
                    edge["unit"],
                )
            ]
        )
    assert (
        _canonicalize_owned_biosphere_names(
            records, BACKGROUND, PackageCatalogProvider()
        )
        == []
    )


@pytest.mark.parametrize(
    "record,catalog",
    [
        (activity(unit="gram"), {("Chromium III", ("air",), "kilogram")}),
        (
            activity(categories=("air", "urban air close to ground")),
            {("Chromium III", ("air",), "kilogram")},
        ),
        (activity(name="Unreviewed metal"), {("Chromium III", ("air",), "kilogram")}),
        (activity(), {("Chromium VI", ("air",), "kilogram")}),
        (
            activity(),
            {
                ("Chromium", ("air",), "kilogram"),
                ("Chromium III", ("air",), "kilogram"),
            },
        ),
    ],
)
def test_unknown_mismatched_and_already_valid_flows_remain_unchanged(record, catalog):
    before = copy.deepcopy(record)
    assert (
        _canonicalize_owned_biosphere_names([record], BACKGROUND, provider(catalog))
        == []
    )
    assert record == before


def test_integration_copies_source_and_retains_normalization_in_cache_identity(
    tmp_path, monkeypatch
):
    from premise.inventory.clic_archive import content_hash

    record = activity()
    record.update(
        database="source",
        code="one",
        **{"inventory provenance": {"provider": "test", "record_id": "one"}},
    )
    before = copy.deepcopy(record)

    class StopBeforeMigration(Exception):
        pass

    class Harmonizer:
        def __init__(self, *args, **kwargs):
            pass

        def harmonize(self, resolved, **kwargs):
            assert resolved.activities[0]["exchanges"][1]["name"] == "Chromium III"
            assert resolved.diagnostics[0]["from"][0] == "Chromium"
            assert resolved.fingerprint != content_hash(resolved.activities)
            raise StopBeforeMigration

    monkeypatch.setattr("premise.inventory.clic_harmonize.ClicHarmonizer", Harmonizer)
    with pytest.raises(StopBeforeMigration):
        _prepare_family_records(
            [([record], BACKGROUND)],
            [],
            version="2026",
            family="uvek",
            system_model="cutoff",
            biosphere_name="ecoinvent-3.10-biosphere",
            catalog_provider=provider({("Chromium III", ("air",), "kilogram")}),
            cache_dir=tmp_path,
        )
    assert record == before


def test_missing_catalog_and_other_biosphere_family_do_not_acquire_aliases():
    from brightpath.background.catalogs import InMemoryCatalogProvider

    record = activity()
    before = copy.deepcopy(record)
    assert (
        _canonicalize_owned_biosphere_names(
            [record], BACKGROUND, InMemoryCatalogProvider()
        )
        == []
    )
    other = copy.deepcopy(BACKGROUND)
    other["biosphere"] = {"family": "uvek", "version": "2026"}
    assert (
        _canonicalize_owned_biosphere_names(
            [record], other, provider({("Chromium III", ("air",), "kilogram")})
        )
        == []
    )
    assert record == before


def test_owned_workbook_alias_survives_strict_migration_and_warm_cache(tmp_path):
    from brightpath import BiosphereProfile
    from brightpath.background import PackageCatalogProvider

    record = activity()
    record.update(
        database="source",
        code="one",
        **{"inventory provenance": {"provider": "test", "record_id": "one"}},
    )
    before = copy.deepcopy(record)
    catalogs = PackageCatalogProvider()
    reference = catalogs.load_biosphere(
        BiosphereProfile("ecoinvent", "3.10")
    ).references[("Chromium III", ("air",), "kilogram")]
    for cached in (False, True):
        prepared, diagnostics = _prepare_family_records(
            [([record], BACKGROUND)],
            [],
            version="2026",
            family="uvek",
            system_model="cutoff",
            biosphere_name="ecoinvent-3.10-biosphere",
            catalog_provider=catalogs,
            cache_dir=tmp_path,
        )
        assert len(prepared) == 1
        edge = next(e for e in prepared[0]["exchanges"] if e["type"] == "biosphere")
        assert edge["input"] == ("ecoinvent-3.10-biosphere", reference)
        assert edge["amount"] == edge["loc"] == -2.0
        assert edge["uncertainty type"] == 3 and edge["scale"] == 0.3
        assert edge["formula"] == "dose"
        assert (
            prepared[0]["inventory provenance"]["migration_cohorts"][0]["cache_hit"]
            is cached
        )
        assert any(
            d["code"] == "clic.retained_biosphere_name_canonicalized"
            for d in diagnostics
        )
    assert record == before


def test_default_catalog_resolution_respects_the_application_provider(monkeypatch):
    selected = provider({("Chromium VI", ("air",), "kilogram")})
    monkeypatch.setattr(
        "brightpath.background.catalog_provider_from_environment", lambda: selected
    )
    record = activity()
    before = copy.deepcopy(record)
    assert _canonicalize_owned_biosphere_names([record], BACKGROUND) == []
    assert record == before
