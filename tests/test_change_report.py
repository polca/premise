from __future__ import annotations

import json
from pathlib import Path

import openpyxl
import pyarrow.parquet as pq
import pytest
import premise

from premise.change_report import (
    DETAIL_SCHEMA,
    REPORT_SCHEMA_VERSION,
    ReportScenario,
    generate_structured_change_report,
)
from premise.inventory_store import CompactInventoryStore
from premise.new_database import NewDatabase
from premise.provenance import ProvenanceCollector, record_change_event
from premise.validation_framework import (
    PremiseValidationError,
    ValidationIssue,
    ValidationPhaseResult,
    ValidationReport,
    ValidationRuleResult,
    inventory_store_fingerprint,
)


def _activity(code, name, location, exchanges, **extra):
    return {
        "database": "source-db",
        "code": code,
        "name": name,
        "reference product": f"product {name}",
        "location": location,
        "unit": "kilogram",
        "exchanges": exchanges,
        **extra,
    }


def _production(code, name, location):
    return {
        "type": "production",
        "name": name,
        "product": f"product {name}",
        "location": location,
        "unit": "kilogram",
        "amount": 1.0,
        "input": ("source-db", code),
    }


def _synthetic_stores():
    source = [
        _activity(
            "a",
            "consumer",
            "CH",
            [
                _production("a", "consumer", "CH"),
                {
                    "type": "technosphere",
                    "name": "supplier",
                    "product": "product supplier",
                    "location": "CH",
                    "unit": "kilogram",
                    "amount": 1.0,
                    "uncertainty type": 5,
                    "minimum": 0.5,
                    "maximum": 1.5,
                    "input": ("source-db", "b"),
                    "comment": "source | value\nwith newline",
                },
            ],
            comment="source comment",
            _runtime_cache="ignored",
        ),
        _activity(
            "b",
            "supplier",
            "CH",
            [_production("b", "supplier", "CH")],
        ),
        _activity(
            None,
            "semantic duplicate",
            "GLO",
            [_production("missing", "semantic duplicate", "GLO")],
            comment="first",
        ),
        _activity(
            None,
            "semantic duplicate",
            "GLO",
            [_production("missing", "semantic duplicate", "GLO")],
            comment="second",
        ),
    ]
    final = [
        _activity(
            "a",
            "consumer",
            "CH",
            [
                {**_production("a", "consumer", "CH"), "input": ("export-db", "a")},
                {
                    "type": "technosphere",
                    "name": "supplier",
                    "product": "product supplier",
                    "location": "RER",
                    "unit": "kilogram",
                    "amount": 2.0,
                    "uncertainty type": 5,
                    "minimum": 0.75,
                    "maximum": 2.5,
                    "input": ("export-db", "b"),
                    "comment": "final | value\nwith newline",
                },
            ],
            database="export-db",
            comment="final comment",
            _runtime_cache="different but ignored",
        ),
        _activity(
            "b",
            "supplier",
            "RER",
            [_production("b", "supplier", "RER")],
            database="export-db",
        ),
        _activity(
            None,
            "semantic duplicate",
            "GLO",
            [_production("missing", "semantic duplicate", "GLO")],
            comment="second changed",
        ),
        _activity(
            None,
            "semantic duplicate",
            "GLO",
            [_production("missing", "semantic duplicate", "GLO")],
            comment="first",
        ),
        _activity(
            "c",
            "added",
            "GLO",
            [_production("c", "added", "GLO")],
        ),
    ]
    return CompactInventoryStore(source), CompactInventoryStore(final)


def test_structured_change_report_exact_diff_and_workbook(tmp_path):
    source, final = _synthetic_stores()
    collector = ProvenanceCollector("build-id")
    with collector.session(("image", "path", 2050, ()), "electricity"):
        record_change_event(
            type("Transformation", (), {"system_model": "cutoff"})(),
            final.activity(0).to_dict(),
            "updated",
            sector="electricity",
        )

    generated = generate_structured_change_report(
        source_store=source,
        scenarios=(
            ReportScenario(
                identity=("image", "path", 2050, ()),
                store=final,
                provenance_payload=collector.payload_for(("image", "path", 2050, ())),
            ),
        ),
        build_id="1234567890abcdef",
        source_fingerprint="source-fingerprint",
        filepath=tmp_path,
        name="audit.xlsx",
        source_database="source-db",
        source_type="brightway",
        version="3.12",
        system_model="cutoff",
        premise_version="3.0.0",
    )

    artifacts = generated.artifacts
    assert artifacts.status == "passed"
    assert artifacts.workbook_path.is_file()
    assert artifacts.details_path.is_file()

    table = pq.read_table(artifacts.details_path)
    assert table.schema.names == DETAIL_SCHEMA.names
    assert table.schema.remove_metadata() == DETAIL_SCHEMA.remove_metadata()
    assert table.schema.metadata[b"premise_report_schema_version"] == b"2"
    rows = table.to_pylist()
    assert rows
    assert all(row["report_schema_version"] == REPORT_SCHEMA_VERSION for row in rows)
    assert any(row["change_type"] == "supplier relink" for row in rows)
    assert any(row["change_type"] == "amount change" for row in rows)
    assert any(row["change_type"] == "uncertainty change" for row in rows)
    assert any(row["change_type"] == "addition" for row in rows)
    assert any(
        row["changed_field"] == "comment"
        and row["old_value_json"] is not None
        and json.loads(row["old_value_json"]) == "source | value\nwith newline"
        for row in rows
    )
    assert not any(
        row["changed_field"] in {"database", "_runtime_cache"} for row in rows
    )
    assert any(row["transformations"] == ["electricity"] for row in rows)
    assert any(row["transformations"] == ["unattributed"] for row in rows)

    order = [
        (
            row["scenario_order"],
            row["activity_name"] or "",
            row["activity_product"] or "",
            row["activity_location"] or "",
            row["activity_occurrence"],
            row["exchange_type"] or "",
            row["exchange_name"] or "",
            (
                row["exchange_occurrence"]
                if row["exchange_occurrence"] is not None
                else -1
            ),
            row["changed_field"] or "",
            row["change_type"],
        )
        for row in rows
    ]
    assert order == sorted(order)

    workbook = openpyxl.load_workbook(artifacts.workbook_path, read_only=False)
    assert workbook.sheetnames == [
        "Overview",
        "Scenario Summary",
        "Sector Summary",
        "Key Changes",
        "Market Changes",
        "Fallbacks & Proxies",
        "Validation Findings",
        "Validation Coverage",
        "Methodology",
    ]
    assert workbook["Scenario Summary"].freeze_panes == "A2"
    overview = workbook["Overview"]
    audit_row = next(
        row
        for row in range(1, overview.max_row + 1)
        if overview.cell(row, 1).value == "Detailed audit"
    )
    assert overview.cell(audit_row, 2).hyperlink is not None
    assert len(workbook["Scenario Summary"].tables) == 1


def test_details_cache_is_reused_but_workbook_is_refreshed(tmp_path):
    source, final = _synthetic_stores()
    kwargs = dict(
        source_store=source,
        scenarios=(ReportScenario(identity=("image", "path", 2050, ()), store=final),),
        build_id="1234567890abcdef",
        source_fingerprint="source-fingerprint",
        filepath=tmp_path,
        premise_version="3.0.0",
    )
    first = generate_structured_change_report(**kwargs)
    first_bytes = first.artifacts.details_path.read_bytes()
    second = generate_structured_change_report(
        **kwargs, cache_entry=first.cache_entry, status="failed"
    )
    assert second.artifacts.details_path == first.artifacts.details_path
    assert second.artifacts.details_path.read_bytes() == first_bytes
    assert second.artifacts.workbook_path != first.artifacts.workbook_path
    assert second.artifacts.status == "failed"


def test_detail_content_is_stable_excluding_report_and_build_identity(tmp_path):
    source, final = _synthetic_stores()
    scenario = (ReportScenario(identity=("image", "path", 2050, ()), store=final),)
    first = generate_structured_change_report(
        source_store=source,
        scenarios=scenario,
        build_id="first-build-id",
        source_fingerprint="source-fingerprint",
        filepath=tmp_path / "first",
    )
    second = generate_structured_change_report(
        source_store=source,
        scenarios=scenario,
        build_id="second-build-id",
        source_fingerprint="source-fingerprint",
        filepath=tmp_path / "second",
    )

    def normalized_rows(path):
        rows = pq.read_table(path).to_pylist()
        for row in rows:
            row.pop("report_id")
            row.pop("build_id")
        return rows

    assert normalized_rows(first.artifacts.details_path) == normalized_rows(
        second.artifacts.details_path
    )


def test_generate_change_report_before_update_is_clear_runtime_error():
    database = object.__new__(NewDatabase)
    database.scenarios = [{"model": "image", "pathway": "path", "year": 2050}]
    with pytest.raises(RuntimeError, match="Call update"):
        database.generate_change_report()


def test_explicit_report_works_when_automatic_reports_are_disabled(tmp_path):
    source, final = _synthetic_stores()
    database = object.__new__(NewDatabase)
    database.scenarios = [
        {
            "model": "image",
            "pathway": "path",
            "year": 2050,
            "applied functions": ["electricity"],
            "_inventory_store": final,
        }
    ]
    database._source_inventory_store = source
    database._compact_source_checkpoint = None
    database._validation_enabled = False
    database._validation_reports = {}
    database._validation_iam_fingerprints = {}
    database.generate_reports = False
    database.build_id = "1234567890abcdef"
    database.source = "source-db"
    database.source_type = "brightway"
    database.version = "3.12"
    database.system_model = "cutoff"
    before = (
        inventory_store_fingerprint(source),
        inventory_store_fingerprint(final),
    )

    artifacts = database.generate_change_report(filepath=tmp_path)

    assert isinstance(artifacts, premise.ChangeReportArtifacts)
    assert artifacts.workbook_path.is_file()
    assert before == (
        inventory_store_fingerprint(source),
        inventory_store_fingerprint(final),
    )


def test_provenance_round_trips_with_inventory_checkpoint(tmp_path):
    _, store = _synthetic_stores()
    collector = ProvenanceCollector("build-id")
    with collector.session(("image", "path", 2050, ()), "electricity"):
        record_change_event(object(), store.activity(0).to_dict(), "updated")
    payload = collector.payload_for(("image", "path", 2050, ()))
    store._provenance_payload = payload

    checkpoint = store.checkpoint(tmp_path / "scenario.inventory-store")
    reopened = CompactInventoryStore.open(checkpoint)

    assert reopened._provenance_payload == payload


def test_validation_error_can_expose_diagnostic_artifacts(tmp_path):
    issue = ValidationIssue(
        rule_id="TEST.FAILURE",
        severity="error",
        message="invalid inventory",
    )
    result = ValidationRuleResult(
        rule_id=issue.rule_id,
        severity="error",
        applicability="applicable",
        checked_object_count=1,
        issues=(issue,),
    )
    phase = ValidationPhaseResult(
        phase_id="graph:full", kind="graph", rule_results=(result,)
    )
    report = ValidationReport(
        scenario_identity=("image", "path", 2050),
        store_generation=1,
        ruleset_version=1,
        certificate_key="certificate",
        rule_results=(result,),
        phase_results=(phase,),
    )
    error = PremiseValidationError(report)
    artifacts = premise.ChangeReportArtifacts(
        report_id="report",
        status="failed",
        workbook_path=tmp_path / "diagnostic.xlsx",
        details_path=None,
        scenario_identities=(("image", "path", 2050),),
        source_fingerprint="source",
        validation_certificate_keys=("certificate",),
    )

    error.attach_report_artifacts(artifacts)

    assert error.artifacts is artifacts
    assert str(artifacts.workbook_path) in str(error)


def test_validation_failure_automatically_builds_failed_workbook(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    source, final = _synthetic_stores()
    issue = ValidationIssue(
        rule_id="TEST.FAILURE", severity="error", message="invalid inventory"
    )
    result = ValidationRuleResult(
        rule_id=issue.rule_id,
        severity="error",
        applicability="applicable",
        checked_object_count=1,
        issues=(issue,),
    )
    phase = ValidationPhaseResult(
        phase_id="graph:full", kind="graph", rule_results=(result,)
    )
    report = ValidationReport(
        scenario_identity=("image", "path", 2050, ()),
        store_generation=final.generation,
        ruleset_version=1,
        certificate_key="failed-certificate",
        rule_results=(result,),
        phase_results=(phase,),
    )
    error = PremiseValidationError(report)
    scenario = {
        "model": "image",
        "pathway": "path",
        "year": 2050,
        "applied functions": ["electricity"],
        "_inventory_store": final,
    }
    database = object.__new__(NewDatabase)
    database.scenarios = [scenario]
    database._source_inventory_store = source
    database._compact_source_checkpoint = None
    database._validation_enabled = False
    database._validation_reports = {}
    database._validation_iam_fingerprints = {}
    database.generate_reports = True
    database._automatic_report_in_progress = False
    database._change_report_cache = None
    database.build_id = "1234567890abcdef"
    database.source = "source-db"
    database.source_type = "brightway"
    database.version = "3.12"
    database.system_model = "cutoff"

    database._generate_validation_diagnostic(error, scenario, final)

    assert error.artifacts.status == "failed"
    assert error.artifacts.workbook_path.is_file()
    assert str(error.artifacts.workbook_path) in str(error)
    workbook = openpyxl.load_workbook(error.artifacts.workbook_path)
    assert workbook["Overview"]["B3"].value == "failed"
    findings = workbook["Validation Findings"]
    assert any(
        findings.cell(row, 3).value == "TEST.FAILURE"
        for row in range(2, findings.max_row + 1)
    )


def test_export_failure_diagnostic_uses_invalid_runtime_inventory(monkeypatch):
    _, certified = _synthetic_stores()
    invalid_inventory = [
        _activity(
            "invalid",
            "invalid exporter activity",
            "GLO",
            [_production("invalid", "invalid exporter activity", "GLO")],
        )
    ]
    issue = ValidationIssue(
        rule_id="EXPORT.FAILURE", severity="error", message="invalid export graph"
    )
    result = ValidationRuleResult(
        rule_id=issue.rule_id,
        severity="error",
        applicability="applicable",
        checked_object_count=1,
        issues=(issue,),
    )
    phase = ValidationPhaseResult(
        phase_id="export:generic", kind="export", rule_results=(result,)
    )
    report = ValidationReport(
        scenario_identity=("image", "path", 2050, ()),
        store_generation=certified.generation,
        ruleset_version=1,
        certificate_key="certified-key",
        rule_results=(result,),
        phase_results=(phase,),
    )
    definition = {
        "model": "image",
        "pathway": "path",
        "year": 2050,
        "_inventory_store": certified,
        "_validation_report": report.to_dict(),
    }
    runtime = {
        "model": "image",
        "pathway": "path",
        "year": 2050,
        "database": invalid_inventory,
    }
    database = object.__new__(NewDatabase)
    database.inventory_backend = "compact"
    database._validation_reports = {}
    captured = {}

    def capture(error, scenario, store):
        captured.update(error=error, scenario=scenario, store=store)

    monkeypatch.setattr(database, "_generate_validation_diagnostic", capture)
    error = PremiseValidationError(report)

    database._handle_export_validation_error(definition, error, "brightway", runtime)

    assert captured["scenario"] is runtime
    assert len(captured["store"]) == 1
    assert captured["store"].activity(0)["name"] == "invalid exporter activity"
    assert error.report.phase_results[-1].phase_id == "export:brightway"


def _performance_regression_stores(backend=CompactInventoryStore):
    """Synthetic inventory with duplicates, special values and unchanged rows."""
    import copy
    from datetime import date
    import numpy as np

    source, final = _synthetic_stores()
    old, new = source.materialize(), final.materialize()
    old[0]["exchanges"].extend(
        [
            {**old[0]["exchanges"][1], "amount": 3.0, "comment": "duplicate"},
            {**old[0]["exchanges"][1], "amount": 3.0, "comment": "duplicate"},
        ]
    )
    new[0]["exchanges"].extend(
        [
            {**new[0]["exchanges"][1], "amount": 4.0, "comment": "duplicate"},
            {**new[0]["exchanges"][1], "amount": 2.0, "comment": "duplicate"},
        ]
    )
    for index, (before, after) in enumerate(
        [
            (b"a", b"b"),
            (float("nan"), float("inf")),
            (-0.0, 0.0),
            (1, 1.0),
            (np.float64(2), np.float64(3)),
            ({"nested": [1, {"_hidden": 2, "x": "a"}]}, {"nested": [1, {"x": "b"}]}),
            (date(2020, 1, 1), date(2020, 1, 2)),
            (Path("a"), Path("b")),
            ({"a", "b"}, {"b", "c"}),
        ]
    ):
        code = f"special-{index}"
        exchange = {
            "type": "technosphere",
            "name": "provider",
            "product": "p",
            "location": "CH",
            "unit": "kilogram",
            "amount": 1,
            "comment": before,
        }
        old.append(_activity(code, code, "CH", [exchange]))
        new.append(
            _activity(code, code, "CH", [{**exchange, "comment": after, "amount": 2}])
        )
    unchanged = _activity(
        "same",
        "unchanged market",
        "GLO",
        [_production("same", "unchanged market", "GLO")],
    )
    old.append(unchanged)
    new.append(copy.deepcopy(unchanged))
    old.append(_activity("removed", "removed", "GLO", []))
    return backend(old), backend(new)


@pytest.mark.parametrize("backend_name", ["compact", "legacy", "checkpoint"])
def test_optimized_report_matches_baseline_golden(tmp_path, backend_name):
    from premise.inventory_store import (
        InventoryStore,
        LegacyInventoryStore,
        ReadOnlyInventoryStore,
    )

    backend = (
        LegacyInventoryStore if backend_name == "legacy" else CompactInventoryStore
    )
    source, final = _performance_regression_stores(backend)
    if backend_name == "checkpoint":
        source = InventoryStore.open(
            source.checkpoint(tmp_path / "source.inventory-store")
        )
        final = InventoryStore.open(
            final.checkpoint(tmp_path / "final.inventory-store")
        )
    generated = generate_structured_change_report(
        source_store=ReadOnlyInventoryStore(source),
        scenarios=(
            ReportScenario(
                identity=("image", "path", 2050, ()),
                store=ReadOnlyInventoryStore(final),
            ),
        ),
        build_id="golden-build",
        source_fingerprint="golden-source",
        filepath=tmp_path,
    )
    rows = pq.read_table(generated.artifacts.details_path).to_pylist()
    for row in rows:
        row.pop("report_id")
    expected = json.loads(
        (Path(__file__).parent / "fixtures" / "change_report_v2.json").read_text()
    )
    assert rows == expected


def test_unchanged_report_avoids_snapshots_and_second_payload_read(
    tmp_path, monkeypatch
):
    import premise.change_report as report_module

    source, _ = _synthetic_stores()
    counts = []
    original = source._report_activity_payload

    def read(activity_id):
        counts.append(activity_id)
        return original(activity_id)

    monkeypatch.setattr(source, "_report_activity_payload", read)
    monkeypatch.setattr(
        source, "activity", lambda *_: pytest.fail("public snapshot read")
    )
    generated = generate_structured_change_report(
        source_store=source,
        scenarios=(ReportScenario(identity=("image", "p", 2050, ()), store=source),),
        build_id="same",
        source_fingerprint="same",
        filepath=tmp_path,
    )
    assert len(counts) == 2 * len(source)
    assert pq.read_table(generated.artifacts.details_path).num_rows == 0
    assert report_module._hash_skip_safe({"x": [1, 2.0, None]})
    assert not report_module._hash_skip_safe({"x": float("nan")})
    assert not report_module._hash_skip_safe({"x": b"bytes"})


def test_prepared_pairing_hashes_each_exchange_once(monkeypatch):
    import premise.change_report as report_module

    original = report_module._stable_hash
    calls = []

    def hash_value(value):
        calls.append(value)
        return original(value)

    monkeypatch.setattr(report_module, "_stable_hash", hash_value)
    old = [
        report_module._PreparedExchange(
            type="technosphere", name="p", amount=i, comment=str(i)
        )
        for i in range(5)
    ]
    new = [
        report_module._PreparedExchange(
            type="technosphere", name="p", amount=i + 0.5, comment=str(i)
        )
        for i in range(5)
    ]
    report_module._pair_exchange_group(old, new)
    assert len(calls) == 10


def test_report_reader_preserves_columnar_overrides_and_metadata(tmp_path, monkeypatch):
    import copy
    from premise.inventory_store import InventoryStore, ReadOnlyInventoryStore

    source, _ = _performance_regression_stores()
    store = InventoryStore.open(source.checkpoint(tmp_path / "source.inventory-store"))
    # Check nested data through a real reopened columnar mapping, not just dicts.
    exchange = store._state.exchanges[store._state.activity_exchanges[0][1]]
    exchange["amount"] = 9.0
    exchange["nested"] = {"items": [1, 2]}
    del exchange["comment"]
    store._state.exchanges[store._state.activity_exchanges[0][1]] = exchange
    assert store._report_activity_payload(0)["exchanges"][1]["amount"] == 9.0
    before = store.materialize()
    generation = store.generation
    from premise.inventory_store import _COLUMNAR_DELETED

    changes = copy.deepcopy(
        exchange._changes, {id(_COLUMNAR_DELETED): _COLUMNAR_DELETED}
    )
    fingerprint = inventory_store_fingerprint(store)
    monkeypatch.setattr(
        store, "activity", lambda *_: pytest.fail("public snapshot read")
    )
    generate_structured_change_report(
        source_store=ReadOnlyInventoryStore(store),
        scenarios=(
            ReportScenario(
                identity=("image", "p", 2050, ()), store=ReadOnlyInventoryStore(store)
            ),
        ),
        build_id="same",
        source_fingerprint="same",
        filepath=tmp_path,
    )
    assert store.generation == generation
    assert inventory_store_fingerprint(store) == fingerprint
    assert exchange._changes == changes
    # Canonical comparison handles NaN in the synthetic inventory.
    from premise.change_report import _canonical_json

    assert _canonical_json(store.materialize()) == _canonical_json(before)


def test_report_cache_invalidates_for_generation_and_missing_file(tmp_path):
    source, final = _synthetic_stores()
    kwargs = dict(
        source_store=source,
        scenarios=(ReportScenario(identity=("image", "p", 2050, ()), store=final),),
        build_id="cache-test",
        source_fingerprint="source",
        filepath=tmp_path,
    )
    first = generate_structured_change_report(**kwargs)
    # A valid transaction, even without a data edit, advances generation.
    with final.transaction("new generation"):
        pass
    second = generate_structured_change_report(**kwargs, cache_entry=first.cache_entry)
    assert second.artifacts.details_path != first.artifacts.details_path
    second.artifacts.details_path.unlink()
    third = generate_structured_change_report(**kwargs, cache_entry=second.cache_entry)
    assert third.artifacts.details_path.is_file()
    assert third.artifacts.report_id != second.artifacts.report_id


def test_multiple_scenarios_reuse_source_index(tmp_path, monkeypatch):
    source, final = _synthetic_stores()
    calls = []
    original = source._report_activity_payload

    def read(activity_id):
        calls.append(activity_id)
        return original(activity_id)

    monkeypatch.setattr(source, "_report_activity_payload", read)
    scenarios = tuple(
        ReportScenario(identity=("image", "p", year, ()), store=final)
        for year in (2030, 2050)
    )
    report = generate_structured_change_report(
        source_store=source,
        scenarios=scenarios,
        build_id="multi",
        source_fingerprint="source",
        filepath=tmp_path,
    )
    rows = pq.read_table(report.artifacts.details_path).to_pylist()
    first = [row for row in rows if row["scenario_order"] == 0]
    second = [row for row in rows if row["scenario_order"] == 1]
    assert first and len(first) == len(second)
    for left, right in zip(first, second):
        for field in ("scenario_order", "scenario_identity"):
            left.pop(field)
            right.pop(field)
        assert left == right
    # One source indexing pass, and at most one source read per paired activity
    # for each scenario; no indexing pass per scenario.
    assert len(calls) <= 3 * len(source)


def test_generic_store_reporting_reader_fallback():
    from premise.change_report import _report_payload
    from premise.inventory_store import InventoryStore

    class GenericStore:
        backend_name = "third-party"

        def activity(self, activity_id):
            assert activity_id == 7
            return type("Record", (), {"to_dict": lambda self: {"name": "fallback"}})()

    store = GenericStore()
    assert _report_payload(store, 7) == {"name": "fallback"}
    assert InventoryStore._report_activity_payload(store, 7) == {"name": "fallback"}
    from premise.inventory_store import ReadOnlyInventoryStore

    assert _report_payload(ReadOnlyInventoryStore(store), 7) == {"name": "fallback"}


def test_normalization_preserves_special_values_and_stages():
    from datetime import date
    import numpy as np
    from premise.change_report import _canonical_json, _plain

    class Custom:
        def __str__(self):
            return "custom value"

    cases = [
        (b"\x00\xff", '{"__bytes__":"00ff"}'),
        (float("nan"), '{"__float__":"NaN"}'),
        (float("inf"), '{"__float__":"Infinity"}'),
        (-float("inf"), '{"__float__":"-Infinity"}'),
        (-0.0, "-0.0"),
        (np.int64(2), "2"),
        (np.float64(2), "2.0"),
        (np.bool_(True), "true"),
        (date(2020, 1, 1), '"2020-01-01"'),
        (Path("a/b"), '"a/b"'),
        (Custom(), '{"__type__":"Custom","value":"custom value"}'),
        ({"z": 1, "database": "ignored", "_private": 2}, '{"z":1}'),
        ({1: "one", "2": "two"}, '{"1":"one","2":"two"}'),
    ]
    for value, expected in cases:
        assert _canonical_json(value) == expected
    # Retain the established second-pass behavior rather than changing audit
    # semantics as a side effect of caching normalized fields.
    assert _canonical_json(_plain(b"bytes")) == "{}"
    assert _canonical_json(_plain(float("nan"))) == "{}"


def test_special_values_do_not_take_hash_shortcut(tmp_path, monkeypatch):
    import premise.change_report as report_module

    activity = _activity("special", "special", "CH", [], comment=b"bytes")
    store = CompactInventoryStore([activity])
    reads = []
    original = store._report_activity_payload

    def read(activity_id):
        reads.append(activity_id)
        return original(activity_id)

    monkeypatch.setattr(store, "_report_activity_payload", read)
    assert not report_module._activity_index(store)[0][0].hash_skip_safe
    reads.clear()
    generate_structured_change_report(
        source_store=store,
        scenarios=(ReportScenario(identity=("image", "p", 2050, ()), store=store),),
        build_id="special",
        source_fingerprint="source",
        filepath=tmp_path,
    )
    assert len(reads) == 4  # two indexing reads, then both sides of the pair


def test_reporting_does_not_residentize_lazy_activity_metadata(tmp_path):
    from premise.inventory_store import InventoryStore, ReadOnlyInventoryStore

    activity = _activity(
        "a",
        "nested metadata",
        "GLO",
        [],
        parameters={"efficiency": {"amount": 1}},
        classifications=[["ISIC", "example"]],
    )
    store = InventoryStore.open(
        CompactInventoryStore([activity]).checkpoint(
            tmp_path / "nested.inventory-store"
        )
    )
    resident = store._state.activities[0]
    before = dict(dict.items(resident))
    assert "parameters" not in before
    generate_structured_change_report(
        source_store=ReadOnlyInventoryStore(store),
        scenarios=(
            ReportScenario(
                identity=("image", "p", 2050, ()), store=ReadOnlyInventoryStore(store)
            ),
        ),
        build_id="nested",
        source_fingerprint="source",
        filepath=tmp_path,
    )
    assert dict(dict.items(resident)) == before
    assert store._report_activity_payload(0)["parameters"] == activity["parameters"]
    assert dict(dict.items(resident)) == before
