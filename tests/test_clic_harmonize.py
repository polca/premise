"""Cohort caches must preserve source, target, and dependency contracts."""

import copy
from unittest.mock import Mock

import pytest

from premise.inventory.clic_harmonize import ClicHarmonizer
from premise.inventory.clic_archive import ClicInventoryError
from test_clic_archive import catalog, native, purchase


@pytest.fixture
def provider():
    from brightpath import BiosphereProfile, TechnosphereProfile
    from brightpath.background import (
        BiosphereCatalog,
        InMemoryCatalogProvider,
        TechnosphereCatalog,
    )

    return InMemoryCatalogProvider(
        technosphere=[
            TechnosphereCatalog(
                TechnosphereProfile("ecoinvent", version, "cutoff"), frozenset()
            )
            for version in ("3.11", "3.12")
        ],
        biosphere=[
            BiosphereCatalog(BiosphereProfile("ecoinvent", version), frozenset())
            for version in ("3.11", "3.12")
        ],
    )


def graph(amount=2):
    root = native(1, 1, "root", exchanges=[purchase("component", amount=amount)])
    component = native(2, 2, "component")
    component.update(background_version="3.11", biosphere_version="3.11")
    return catalog(root, component).resolve(["dataset-1"])


def test_pragmatic_policy_accepts_reverse_route_and_keeps_validation(
    tmp_path, provider
):
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=provider)
    result = harmonizer.harmonize(graph(), version="3.11")
    assert len(result.activities) == 2
    assert result.manifest["policy"]["mode"] == "pragmatic"
    assert result.manifest["policy"]["validate_source"] is True
    assert result.manifest["policy"]["validate_target"] is True
    assert result.manifest["policy"]["minimum_coverage"] == 1
    assert any(
        row["code"] == "migration.inferred_reverse" for row in result.diagnostics
    )
    root = next(row for row in result.activities if row["name"] == "root")
    assert root["exchanges"][1]["amount"] == 2
    with pytest.raises(ClicInventoryError, match="inferred reverse"):
        ClicHarmonizer(
            tmp_path, catalog_provider=provider, migration_policy="strict"
        ).harmonize(graph(), version="3.11")


def test_policy_modes_do_not_share_cached_conversions(tmp_path, provider):
    pragmatic = ClicHarmonizer(tmp_path, catalog_provider=provider)
    strict = ClicHarmonizer(
        tmp_path, catalog_provider=provider, migration_policy="strict"
    )
    pragmatic.harmonize(graph(), version="3.12")
    first = strict.harmonize(graph(), version="3.12")
    assert all(not cohort["cache_hit"] for cohort in first.manifest["cohorts"])
    repeated = strict.harmonize(graph(), version="3.12")
    assert all(cohort["cache_hit"] for cohort in repeated.manifest["cohorts"])


@pytest.mark.parametrize("missing_version", ["3.11", "3.12"])
def test_missing_source_or_target_catalog_reports_required_context(
    tmp_path, provider, missing_version
):
    wrapped = Mock(wraps=provider)

    def load(profile):
        if profile.version == missing_version:
            raise FileNotFoundError(f"missing-{missing_version}.json")
        return provider.load_technosphere(profile)

    wrapped.load_technosphere.side_effect = load
    source = native(1, 1, "root")
    source.update(background_version="3.11", biosphere_version="3.11")
    with pytest.raises(
        ClicInventoryError, match="reviewed Brightpath reference catalogs"
    ) as caught:
        ClicHarmonizer(tmp_path, catalog_provider=wrapped).harmonize(
            catalog(source).resolve(["dataset-1"]), version="3.12"
        )
    assert missing_version in str(caught.value)
    assert isinstance(caught.value.__cause__, FileNotFoundError)


def test_harmonization_cache_isolated_by_affected_source_cohort(tmp_path, provider):
    source = graph()
    original = copy.deepcopy(source.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=provider)
    first = harmonizer.harmonize(source, version="3.12")
    assert all(not row["cache_hit"] for row in first.manifest["cohorts"])
    second = harmonizer.harmonize(source, version="3.12")
    assert all(row["cache_hit"] for row in second.manifest["cohorts"])
    assert source.activities == original
    changed = harmonizer.harmonize(graph(amount=7), version="3.12")
    hits = {
        row["source"]["technosphere"]["version"]: row["cache_hit"]
        for row in changed.manifest["cohorts"]
    }
    assert hits == {"3.12": False, "3.11": True}
    assert {row["slug"] for row in changed.manifest["datasets"]} == {
        "dataset-1",
        "dataset-2",
    }
    assert first.lockfile["resolution_fingerprint"] == source.fingerprint


def test_damaged_harmonized_payload_is_rebuilt(tmp_path, provider):
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=provider)
    first = harmonizer.harmonize(graph(), version="3.12")
    cohort = first.manifest["cohorts"][0]
    (tmp_path / f"{cohort['fingerprint']}.inventory.json.gz").write_bytes(b"damaged")
    result = harmonizer.harmonize(graph(), version="3.12")
    assert result.manifest["cohorts"][0]["cache_hit"] is False
    assert len(result.activities) == 2


def test_later_pinning_is_reported_even_when_conversion_is_cached(tmp_path, provider):
    source = graph()
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=provider)
    harmonizer.harmonize(source, version="3.12")
    root = native(1, 1, "root", exchanges=[purchase("component")])
    component = native(2, 2, "component")
    component.update(background_version="3.11", biosphere_version="3.11")
    locked = catalog(root, component).resolve(recipe=source.lockfile())
    result = harmonizer.harmonize(locked, version="3.12")
    assert all(row["cache_hit"] for row in result.manifest["cohorts"])
    assert all(row["pinned"] for row in result.manifest["datasets"])
    assert all(a["clic provenance"]["pinned"] for a in result.activities)
