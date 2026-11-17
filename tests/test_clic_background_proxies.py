"""Reviewed supplier aliases keep strict validation and do not relicense consumers."""

import csv
import json

import pytest
from openpyxl import Workbook

from premise.inventory import clic_replacements
from premise.inventory.clic import ClicInventoryProvider
from premise.inventory.clic_archive import ClicInventoryError
from premise.reporting.clic import write_inventory_sheet, write_inventory_sidecars
from test_clic_archive import catalog, native, purchase
from test_clic_reporting import scenario


@pytest.fixture
def setup(monkeypatch, tmp_path):
    from brightpath import BiosphereProfile, TechnosphereProfile
    from brightpath.background import (
        BiosphereCatalog,
        InMemoryCatalogProvider,
        TechnosphereCatalog,
    )

    source_id = ("modern polymer", "modern polymer", "GLO", "kilogram")
    target_id = ("old polymer", "old polymer", "GLO", "kilogram")
    provider = InMemoryCatalogProvider(
        technosphere=[
            TechnosphereCatalog(
                TechnosphereProfile("ecoinvent", "3.12", "cutoff"),
                frozenset(
                    [
                        source_id,
                        ("unreviewed polymer", "unreviewed polymer", "GLO", "kilogram"),
                    ]
                ),
            ),
            TechnosphereCatalog(
                TechnosphereProfile("ecoinvent", "3.11", "cutoff"),
                frozenset([target_id]),
            ),
        ],
        biosphere=[
            BiosphereCatalog(BiosphereProfile("ecoinvent", v), frozenset())
            for v in ("3.11", "3.12")
        ],
    )
    rule = {
        "id": "polymer-proxy",
        "target_versions": ["3.11"],
        "review_status": "reviewed",
        "confidence": "low",
        "identities": [list(source_id)],
        "suppliers": [
            {
                "name": "old polymer",
                "product": "old polymer",
                "unit": "kilogram",
                "locations": ["GLO"],
            }
        ],
        "conversion": "One kg per requested kg.",
        "limitations": "Test-only technology approximation.",
        "evidence": ["Test metadata"],
    }
    registry = {
        "schema_version": 1,
        "target_versions": ["3.11", "3.12"],
        "system_models": ["cutoff"],
        "rules": [],
        "background_proxies": [rule],
    }
    monkeypatch.setattr(clic_replacements, "replacement_registry", lambda: registry)
    background = [native(99, 99, "old polymer")["inventory_snapshot"]]

    def client(*extra):
        archive = catalog(
            native(
                1,
                1,
                "consumer",
                exchanges=[purchase("modern polymer", amount=2), *extra],
            )
        )
        return ClicInventoryProvider(
            archive=archive,
            catalog_provider=provider,
            options={
                "cache_dir": tmp_path / "cache",
                "offline": True,
                "license_policy": "commercial",
            },
        )

    return client, background, provider


def test_reviewed_supplier_proxy_preserves_amount_and_is_reported_on_cache_hit(
    setup, tmp_path
):
    factory, background, _ = setup
    client = factory()

    def prepare():
        return client.prepare(
            ["dataset-1"],
            background,
            version="3.11",
            biosphere_codes={},
            classifications={},
        )

    first = prepare()
    assert first[0]["exchanges"][1]["name"] == "old polymer"
    assert first[0]["exchanges"][1]["amount"] == 2
    assert client.manifest["policy"]["minimum_coverage"] == 1
    assert client.manifest["license_policy"]["datasets"][0]["license"] == "CC-BY-4.0"
    assert client.manifest["commercial_replacements"]["substitutions"] == []
    assert (
        client.manifest["commercial_background_substitutions"][0]["rule"]
        == "polymer-proxy"
    )
    assert prepare() == first
    assert all(cohort["cache_hit"] for cohort in client.manifest["cohorts"])
    definition = scenario(tmp_path)
    definition["clic inventories"].update(client.manifest)
    definition["clic recipe"] = client.lockfile
    path, _ = write_inventory_sidecars(tmp_path / "report.xlsx", [definition])
    saved = json.loads(path.read_text())["scenarios"][0]
    with (tmp_path / saved["substitution_report_file"]).open() as stream:
        row = next(csv.DictReader(stream))
    assert row["kind"] == "background supplier"
    assert row["original_license"] == "CC-BY-4.0"
    assert json.loads(row["supplier"]) == [
        "old polymer",
        "old polymer",
        "GLO",
        "kilogram",
    ]
    workbook = Workbook()
    write_inventory_sheet(workbook, [definition])
    assert workbook["CLIC Replacements"].cell(2, 4).value == "polymer-proxy"


def test_unreviewed_missing_supplier_still_fails_strict_migration(setup):
    factory, background, _ = setup
    with pytest.raises(ClicInventoryError, match="coverage|catalog"):
        factory(purchase("unreviewed polymer")).prepare(
            ["dataset-1"],
            background,
            version="3.11",
            biosphere_codes={},
            classifications={},
        )


def test_temporary_alias_cannot_bypass_final_target_catalog_validation(
    setup, monkeypatch
):
    factory, background, provider = setup
    from brightpath.background import TechnosphereCatalog

    original = provider.load_technosphere

    def load(profile):
        result = original(profile)
        return (
            TechnosphereCatalog(profile, frozenset())
            if profile.version == "3.11"
            else result
        )

    monkeypatch.setattr(provider, "load_technosphere", load)
    with pytest.raises(ClicInventoryError, match="invalid target links"):
        factory().prepare(
            ["dataset-1"],
            background,
            version="3.11",
            biosphere_codes={},
            classifications={},
        )
