"""Commercial proxies preserve quantities, graph boundaries and reproducibility."""

from copy import deepcopy

import pytest

from premise.inventory import clic_replacements as module
from premise.inventory.clic import ClicInventoryProvider
from premise.inventory.clic_archive import ClicInventoryError, activity_identity
from premise.inventory.clic_licenses import (
    ClicLicenseError,
    enforce_commercial_licenses,
)
from test_clic_archive import catalog, native, purchase
from test_clic_harmonize import provider


@pytest.fixture
def rule():
    return {
        "id": "pack-proxy",
        "review_status": "reviewed",
        "match": {"names": ["restricted pack"], "unit": "kilogram"},
        "suppliers": [
            {
                "name": "native pack",
                "product": "native pack",
                "unit": "kilogram",
                "locations": ["same", "GLO"],
            }
        ],
        "amount": 3,
        "conversion": "Three native kg per requested kg: test-only conversion.",
        "limitations": "Synthetic proxy for checking the graph contract.",
        "evidence": ["Test fixture"],
    }


@pytest.fixture
def registry(rule):
    return {
        "schema_version": 1,
        "target_versions": ["3.12"],
        "system_models": ["cutoff"],
        "rules": [rule],
    }


@pytest.fixture
def background():
    activity = native(99, 99, "native pack")["inventory_snapshot"]
    activity["exchanges"][0]["amount"] = 2
    activity["exchanges"].append(
        {
            **purchase("permitted material", amount=4),
            "uncertainty type": 3,
            "loc": 4,
            "scale": 1,
            "minimum": 2,
            "maximum": 6,
        }
    )
    return [activity]


def graph(*, shared=False):
    pack = native(
        1,
        1,
        "restricted pack",
        exchanges=[purchase("restricted ingredient", amount=999)],
    )
    pack["selected_license"] = "CC-BY-NC-SA-4.0"
    pack["inventory_snapshot"]["restricted process metadata"] = "must never be copied"
    ingredient = native(2, 2, "restricted ingredient")
    ingredient["selected_license"] = "CC-BY-NC-4.0"
    consumer = native(
        3, 3, "user", exchanges=[purchase("restricted pack", amount=0.25)]
    )
    other = native(4, 4, "other user", exchanges=[purchase("restricted ingredient")])
    archive = catalog(pack, ingredient, consumer, other)
    roots = ["dataset-3", "dataset-4"] if shared else ["dataset-3"]
    return archive, archive.resolve(roots)


def test_parent_replacement_cuts_only_its_old_dependency_cone(registry, background):
    archive, raw = graph()
    before, native_before = deepcopy(raw), deepcopy(background)
    resolved, report = module.replace_commercial_inventories(
        raw,
        archive,
        background,
        version="3.12",
        system_model="cutoff",
        registry=registry,
    )
    assert raw == before and background == native_before
    assert report["pruned_slugs"] == ["dataset-2"]
    assert {row["slug"] for row in resolved.versions} == {"dataset-1", "dataset-3"}
    replacement = next(a for a in resolved.activities if a["name"] == "restricted pack")
    assert "restricted process metadata" not in replacement
    assert [e["amount"] for e in replacement["exchanges"]] == [1, 6]
    edge = replacement["exchanges"][1]
    assert (edge["loc"], edge["scale"], edge["minimum"], edge["maximum"]) == (
        6,
        1.5,
        3,
        9,
    )
    assert "input" not in edge
    consumer = next(a for a in resolved.activities if a["name"] == "user")
    assert consumer["exchanges"][1]["amount"] == 0.25
    assert consumer["exchanges"][1]["input"] == ("__clic__", replacement["code"])
    assert activity_identity(replacement) == activity_identity(raw.activities[0])
    accepted = enforce_commercial_licenses(
        resolved, archive, replacements=report["substitutions"]
    )
    assert accepted["status"] == "passed"
    assert accepted["datasets"][0]["license"] == "ecoinvent"
    assert accepted["datasets"][0]["original_license"] == "CC-BY-NC-SA-4.0"
    # Uploaded metadata alone cannot claim that a restricted process was replaced.
    with pytest.raises(ClicLicenseError):
        enforce_commercial_licenses(resolved, archive)


def test_shared_restricted_supplier_remains_blocked(registry, background):
    archive, raw = graph(shared=True)
    resolved, report = module.replace_commercial_inventories(
        raw,
        archive,
        background,
        version="3.12",
        system_model="cutoff",
        registry=registry,
    )
    assert report["pruned_slugs"] == []
    with pytest.raises(ClicLicenseError, match="dataset-4 -> dataset-2"):
        enforce_commercial_licenses(
            resolved, archive, replacements=report["substitutions"]
        )


@pytest.mark.parametrize("target,available", [("3.8", True), ("3.12", False)])
def test_missing_or_unreviewed_native_candidate_cannot_remove_a_requirement(
    registry, background, target, available
):
    archive, raw = graph()
    resolved, report = module.replace_commercial_inventories(
        raw,
        archive,
        background if available else [],
        version=target,
        system_model="cutoff",
        registry=registry,
    )
    assert not report["substitutions"] and not report["pruned_slugs"]
    assert resolved.activities == raw.activities
    with pytest.raises(ClicLicenseError):
        enforce_commercial_licenses(resolved, archive)


def test_ambiguous_supplier_fails_and_registry_overlap_fails(registry, background):
    archive, raw = graph()
    with pytest.raises(ClicInventoryError, match="Ambiguous native supplier"):
        module.replace_commercial_inventories(
            raw,
            archive,
            background * 2,
            version="3.12",
            system_model="cutoff",
            registry=registry,
        )
    registry["rules"].append(deepcopy(registry["rules"][0]))
    with pytest.raises(ClicInventoryError, match="Overlapping"):
        module.replace_commercial_inventories(
            raw,
            archive,
            background,
            version="3.12",
            system_model="cutoff",
            registry=registry,
        )


def test_registry_change_and_supplier_change_affect_fingerprints(registry, background):
    archive, raw = graph()

    def prepare():
        return module.replace_commercial_inventories(
            raw,
            archive,
            background,
            version="3.12",
            system_model="cutoff",
            registry=registry,
        )[1]

    first = prepare()
    registry["rules"][0]["limitations"] += " Updated documentation."
    second = prepare()
    assert first["registry_fingerprint"] != second["registry_fingerprint"]
    assert first["fingerprint"] != second["fingerprint"]
    background[0]["exchanges"][1]["amount"] += 1
    third = prepare()
    assert second["registry_fingerprint"] == third["registry_fingerprint"]
    assert second["fingerprint"] != third["fingerprint"]


def test_excluding_pack_electronics_renormalizes_remaining_mass(registry, background):
    archive, raw = graph()
    rule = registry["rules"][0]
    rule.update(
        amount=1, exclude_inputs=["native electronics"], renormalize_excluded_mass=True
    )
    background[0]["exchanges"][1].update(
        amount=1.6, loc=1.6, scale=0.1, minimum=1.52, maximum=1.68
    )
    background[0]["exchanges"].append(purchase("native electronics", amount=0.4))
    resolved, report = module.replace_commercial_inventories(
        raw,
        archive,
        background,
        version="3.12",
        system_model="cutoff",
        registry=registry,
    )
    pack = next(a for a in resolved.activities if a["name"] == "restricted pack")
    assert len(pack["exchanges"]) == 2
    assert pack["exchanges"][1]["amount"] == pytest.approx(1)
    assert pack["exchanges"][1]["scale"] == pytest.approx(0.0625)
    assert report["substitutions"][0]["accounting"] == {
        "native_reference_output": 2,
        "excluded_mass_kg": 0.4,
        "scaling_factor": 0.625,
    }
    rule["exclude_inputs"] = ["missing electronics"]
    with pytest.raises(
        ClicInventoryError, match="electronics mass cannot be established"
    ):
        module.replace_commercial_inventories(
            raw,
            archive,
            background,
            version="3.12",
            system_model="cutoff",
            registry=registry,
        )


def test_packaged_registry_is_reviewed_and_has_no_overlapping_identities():
    registry = module.replacement_registry()
    seen = set()
    for rule in registry["rules"]:
        assert rule["confidence"] in {"low", "medium", "high"}
        for name in rule["match"]["names"]:
            key = (name.casefold(), rule["match"]["unit"])
            assert key not in seen
            seen.add(key)


def test_native_en15804_links_are_not_migrated_or_adapted_as_cutoff(
    tmp_path, monkeypatch, provider, registry
):
    from premise.iam import system_models
    from unittest.mock import Mock

    registry["system_models"].append("EN15804")
    registry["rules"][0]["amount"] = 1
    monkeypatch.setattr(module, "replacement_registry", lambda: registry)
    adapt = Mock(return_value=0)
    monkeypatch.setattr(system_models, "adapt_en15804_scrap_exchanges", adapt)
    archive = catalog(native(1, 1, "restricted pack"))
    archive.versions[1]["selected_license"] = "CC-BY-NC-4.0"
    source = native(
        99,
        99,
        "native pack",
        exchanges=[purchase("EN15804-specific scrap", amount=-0.1)],
    )["inventory_snapshot"]
    scrap = native(100, 100, "EN15804-specific scrap")["inventory_snapshot"]
    client = ClicInventoryProvider(
        archive=archive,
        catalog_provider=provider,
        options={
            "cache_dir": tmp_path,
            "offline": True,
            "license_policy": "commercial",
        },
    )
    records = client.prepare(
        ["dataset-1"],
        [source, scrap],
        version="3.12",
        system_model="EN15804",
        biosphere_codes={},
        classifications={},
    )
    assert records[0]["exchanges"][1]["amount"] == -0.1
    assert records[0]["exchanges"][1]["name"] == "EN15804-specific scrap"
    assert client.manifest["native_replacement_count"] == 1
    assert (
        client.manifest["datasets"][0]["background"]["technosphere"]["system_model"]
        == "EN15804"
    )
    assert adapt.call_args.args[0] == []


def test_native_migration_bypass_requires_matching_inventory_fingerprint(
    tmp_path, provider, registry, background
):
    from premise.inventory.clic_harmonize import ClicHarmonizer

    archive, raw = graph()
    resolved, _ = module.replace_commercial_inventories(
        raw,
        archive,
        background,
        version="3.12",
        system_model="cutoff",
        registry=registry,
    )
    next(a for a in resolved.activities if a["name"] == "restricted pack")["exchanges"][
        1
    ]["amount"] = 999
    with pytest.raises(ClicInventoryError, match="target or inventory fingerprint"):
        ClicHarmonizer(tmp_path, catalog_provider=provider).harmonize(
            resolved, version="3.12"
        )


def test_commercial_preparation_and_recipe_replay_use_reviewed_native_inventory(
    tmp_path, monkeypatch, provider, registry
):
    # No external dependencies, so the in-memory migration catalogs suffice.
    archive = catalog(native(1, 1, "restricted pack"))
    archive.versions[1]["selected_license"] = "CC-BY-NC-4.0"
    background = [native(99, 99, "native pack")["inventory_snapshot"]]
    monkeypatch.setattr(module, "replacement_registry", lambda: registry)

    def client(recipe=None):
        return ClicInventoryProvider(
            archive=archive,
            recipe=recipe,
            catalog_provider=provider,
            options={
                "cache_dir": tmp_path,
                "offline": True,
                "license_policy": "commercial",
            },
        )

    first = client()
    records = first.prepare(
        ["dataset-1"],
        background,
        version="3.12",
        biosphere_codes={},
        classifications={},
    )
    assert records[0]["name"].startswith("restricted pack")
    assert records[0]["exchanges"][0]["amount"] == 1
    assert first.manifest["license_policy"]["datasets"][0]["license"] == "ecoinvent"
    assert (
        first.lockfile["resolution_fingerprint"]
        == archive.resolve(["dataset-1"]).fingerprint
    )
    replay = client(first.lockfile)
    again = replay.prepare(
        ["dataset-1"],
        background,
        version="3.12",
        biosphere_codes={},
        classifications={},
    )
    assert again[0]["exchanges"] == records[0]["exchanges"]
    assert (
        replay.manifest["commercial_replacements"]
        == first.manifest["commercial_replacements"]
    )
    from test_clic_reporting import scenario
    from premise.reporting.clic import write_inventory_sheet, write_inventory_sidecars
    from openpyxl import Workbook
    import csv
    import json

    definition = scenario(tmp_path)
    definition["clic inventories"].update(first.manifest)
    definition["clic recipe"] = first.lockfile
    path, _ = write_inventory_sidecars(tmp_path / "report.xlsx", [definition])
    saved = json.loads(path.read_text())["scenarios"][0]
    with (tmp_path / saved["substitution_report_file"]).open() as stream:
        exported = next(csv.DictReader(stream))
    assert exported["rule"] == "pack-proxy"
    assert json.loads(exported["supplier"]) == [
        "native pack",
        "native pack",
        "GLO",
        "kilogram",
    ]
    workbook = Workbook()
    write_inventory_sheet(workbook, [definition])
    assert workbook["CLIC Inventories"].cell(2, 6).value == "CC-BY-NC-4.0"
    assert workbook["CLIC Inventories"].cell(2, 17).value == "ecoinvent"
    assert workbook["CLIC Replacements"].cell(2, 4).value == "pack-proxy"
    with pytest.raises(ClicInventoryError, match="differ from the recipe"):
        replay.prepare(
            ["dataset-1"],
            background,
            version="3.11",
            biosphere_codes={},
            classifications={},
        )
    registry["rules"][0]["limitations"] = "Changed rule"
    with pytest.raises(ClicInventoryError, match="registry changed"):
        client(first.lockfile)
