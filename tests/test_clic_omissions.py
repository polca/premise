"""Identity-matched omissions retain original data and visible warnings."""

import copy
import gzip
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from premise.inventory.clic_archive import ClicInventoryError, ResolvedClicInventory
from premise.inventory.clic_harmonize import ClicHarmonizer
from premise.inventory.clic_omissions import REVIEW_PATH, prepare_approved_omissions

DECISION = json.loads(REVIEW_PATH.read_text())["decisions"][0]


def fixture():
    row = {
        key: copy.deepcopy(DECISION[key])
        for key in ("slug", "version_id", "semantic_version", "sha256")
    }
    row.update(
        activity_code="reviewed-csp", background=copy.deepcopy(DECISION["source"])
    )
    production = {
        "name": "CSP decommissioning",
        "reference product": "decommissioning",
        "location": "GLO",
        "unit": "unit",
        "type": "production",
        "amount": 1,
    }
    retained = {
        "name": "plant material",
        "reference product": "plant material",
        "location": "GLO",
        "unit": "kilogram",
        "type": "technosphere",
        "amount": -7,
        "uncertainty type": 0,
    }
    activity = {
        **copy.deepcopy(DECISION["activity"]),
        "unit": "unit",
        "code": row["activity_code"],
        "database": "__clic__",
        "exchanges": [production, copy.deepcopy(DECISION["exchange"]), retained],
    }
    return [activity], [row], copy.deepcopy(DECISION["target"])


def test_exact_omission_keeps_owned_original_exchange_and_other_inventory():
    activities, versions, target = fixture()
    before = copy.deepcopy(activities)
    result, audits = prepare_approved_omissions(activities, versions, target)
    assert activities == before
    assert result[0]["exchanges"] == [
        before[0]["exchanges"][0],
        before[0]["exchanges"][2],
    ]
    assert len(audits) == 1
    assert audits[0]["details"]["source_exchange"] == before[0]["exchanges"][1]
    assert "foregone reuse credit" in audits[0]["message"]
    audits[0]["details"]["source_exchange"]["amount"] = 0
    assert activities == before


@pytest.mark.parametrize("change", ["source", "duplicate"])
def test_changed_approval_context_fails_without_mutation(change):
    activities, versions, target = fixture()
    if change == "source":
        versions[0]["background"]["technosphere"]["version"] = "3.10"
    else:
        activities[0]["exchanges"].append(copy.deepcopy(activities[0]["exchanges"][1]))
    before = copy.deepcopy(activities)
    with pytest.raises(
        ClicInventoryError, match="omission approval does not cover"
    ) as error:
        prepare_approved_omissions(activities, versions, target)
    message = str(error.value)
    assert f"Dataset: {DECISION['activity']['name']}" in message
    assert DECISION["slug"] in message
    assert f"Approval: {DECISION['id']}" in message
    assert f"Exchange: {DECISION['exchange']['name']}" in message
    assert (
        "Source background" if change == "source" else "Matching exchanges"
    ) in message
    assert "approved " in message and "received " in message
    assert f"Approval file: {REVIEW_PATH}" in message
    assert activities == before


def test_matching_uses_identities_instead_of_hashes_or_versions():
    activities, versions, target = fixture()
    versions[0].update(
        sha256="0" * 64, semantic_version="99.0.0", version_id=-1, slug="renamed-slug"
    )
    exchange = activities[0]["exchanges"][1]
    exchange.update(amount=-15, comment="Updated metadata", formula="salt_mass")
    before = copy.deepcopy(activities)
    result, audits = prepare_approved_omissions(activities, versions, target)
    assert len(result[0]["exchanges"]) == 2
    assert activities == before
    details = audits[0]["details"]
    assert details["source_pin"]["version_id"] == -1
    assert details["matching_basis"] == ["name", "reference product", "location"]
    assert details["source_exchange"] == before[0]["exchanges"][1]
    assert details["approved_exchange"] == DECISION["exchange"]
    assert "Actual omitted quantity: -15 kilogram" in audits[0]["message"]


@pytest.mark.parametrize("owner", [True, False])
@pytest.mark.parametrize("field", ["name", "reference product", "location"])
def test_different_identity_is_not_omitted(owner, field):
    activities, versions, target = fixture()
    record = activities[0] if owner else activities[0]["exchanges"][1]
    record[field] = "another identity"
    before = copy.deepcopy(activities)
    result, audits = prepare_approved_omissions(activities, versions, target)
    assert result == activities == before
    assert not audits


@pytest.mark.parametrize("change", ["target", "other owner", "workbook"])
def test_other_contexts_keep_used_salt_exchange(change):
    activities, versions, target = fixture()
    if change == "target":
        target["technosphere"] = {
            "family": "ecoinvent",
            "version": "3.12",
            "system_model": "cutoff",
        }
    elif change == "other owner":
        activities[0]["name"] = "other-csp"
    else:
        versions[0].pop("slug")
    before = copy.deepcopy(activities)
    result, audits = prepare_approved_omissions(activities, versions, target)
    assert result == activities == before
    assert audits == []


def test_harmonization_and_cache_retain_scoped_warning_audit_and_provenance(
    tmp_path, monkeypatch
):
    from brightpath import BiosphereProfile, BrightwayInventory, TechnosphereProfile
    from brightpath.background import (
        BiosphereCatalog,
        InMemoryCatalogProvider,
        TechnosphereCatalog,
    )

    records, versions, target = fixture()
    graph = ResolvedClicInventory(
        records, versions, (DECISION["slug"],), {}, "synthetic", []
    )
    provider = InMemoryCatalogProvider(
        technosphere=[
            TechnosphereCatalog(
                TechnosphereProfile(**context["technosphere"]), frozenset()
            )
            for context in (DECISION["source"], target)
        ],
        biosphere=[
            BiosphereCatalog(BiosphereProfile(**context["biosphere"]), frozenset())
            for context in (DECISION["source"], target)
        ],
    )
    calls = []

    def migrate(inventory, *args, **kwargs):
        calls.append(copy.deepcopy(inventory.data))
        return SimpleNamespace(
            data=copy.deepcopy(inventory.data),
            last_migration_report=SimpleNamespace(
                losses=(), to_json=lambda: json.dumps({"stages": []})
            ),
        )

    monkeypatch.setattr(BrightwayInventory, "migrate_background", migrate)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=provider)
    for hit in (False, True):
        with pytest.warns(UserWarning, match="foregone reuse credit"):
            result = harmonizer.harmonize(graph, family="uvek", version="2026")
        assert result.manifest["cohorts"][0]["cache_hit"] is hit
        audit_path = result.manifest["cohorts"][0]["audit_file"]
        audit = json.loads(gzip.decompress(Path(audit_path).read_bytes()))
        assert (
            audit["approved_clic_omissions"][0]["details"]["source_exchange"]["amount"]
            == -23000000
        )
        assert (
            result.activities[0]["clic provenance"]["approved_omissions"]
            == result.manifest["approved_clic_omissions"]
        )
        assert any(
            d["code"] == "migration.technosphere_omission" for d in result.diagnostics
        )
    assert len(calls) == 1
    assert len(calls[0][0]["exchanges"]) == 2
    assert len(graph.activities[0]["exchanges"]) == 3


def test_multiple_screening_omissions_retain_original_indices(tmp_path, monkeypatch):
    import premise.inventory.clic_omissions as module

    activities, versions, target = fixture()
    second = copy.deepcopy(DECISION)
    second["id"] = "second-screening"
    second["exchange"] = copy.deepcopy(activities[0]["exchanges"][2])
    second["screening_only"] = True
    second["deferred_rule_id"] = "deferred-rule"
    path = tmp_path / "review.json"
    path.write_text(json.dumps({"schema_version": 1, "decisions": [DECISION, second]}))
    monkeypatch.setattr(module, "REVIEW_PATH", path)
    before = copy.deepcopy(activities)
    result, audits = prepare_approved_omissions(activities, versions, target)
    assert activities == before
    assert len(result[0]["exchanges"]) == 1
    assert [a["details"]["original_exchange_index"] for a in audits] == [1, 2]
    assert audits[1]["path"].endswith("exchanges[2]")
    assert audits[1]["details"]["screening_only"]
    assert audits[1]["details"]["source_exchange"] == second["exchange"]
