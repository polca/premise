"""Regression boundaries for report serialization and checkpoint readers."""

import json
import random
import subprocess
import sys
from pathlib import Path

import pytest

from premise.change_report import (
    _PreparedExchange,
    _canonical_json,
    _plain,
    _twice_json,
    _visible_exchange,
)
from premise.inventory_store import (
    InventoryStore,
    InventoryStoreCorruptionError,
    InventoryStoreReadOnlyError,
    LegacyInventoryStore,
)


def test_canonical_encoder_matches_normalized_json():
    rng = random.Random(20260910)
    atoms = [
        None,
        False,
        True,
        0,
        1,
        -1,
        0.0,
        -0.0,
        1e-7,
        1e20,
        float("nan"),
        float("inf"),
        b"bytes",
        Path("a/b"),
        'line\n"quote"\\\t',
        "é雪",
        "\ud800",
    ]

    def value(depth):
        if not depth or rng.random() < 0.55:
            return rng.choice(atoms)
        if rng.random() < 0.5:
            return [value(depth - 1) for _ in range(rng.randrange(5))]
        return {
            key: value(depth - 1)
            for key in rng.sample(
                ["a", "z", "_private", "database", "é", "input", "__bytes__"],
                rng.randrange(7),
            )
        }

    values = [value(4) for _ in range(2000)]
    values += [{1: "first", "1": "second"}, {"1": "first", 1: "second"}]
    for item in values:
        assert _twice_json(item) == _canonical_json(_plain(item))
        if isinstance(item, dict):
            assert _PreparedExchange(item).signature == _canonical_json(
                _visible_exchange(item)
            )
        for stage in (item, _plain(item)):
            expected = json.dumps(
                _plain(stage), sort_keys=True, separators=(",", ":"), ensure_ascii=False
            )
            assert _canonical_json(stage) == expected


def test_report_reader_preserves_legacy_payload_and_checksums(tmp_path):
    source = LegacyInventoryStore(
        [
            {
                "name": "activity",
                "reference product": "product",
                "location": "GLO",
                "unit": "kilogram",
                "code": "a",
                "parameters": {"example": 1.0},
                "exchanges": [
                    {
                        "name": "activity",
                        "product": "product",
                        "type": "production",
                        "amount": -0.0,
                        "unit": "kilogram",
                        "location": "GLO",
                        "uncertainty type": 5,
                        "minimum": 0.0,
                        "maximum": 2.0,
                        "custom": {"nested": b"payload"},
                    }
                ],
            }
        ]
    )
    checkpoint = source.checkpoint(tmp_path / "legacy")
    ordinary = InventoryStore.open(checkpoint)
    report = InventoryStore.open_for_reporting(checkpoint)
    assert list(report.iter_materialized()) == list(ordinary.iter_materialized())
    with pytest.raises(InventoryStoreReadOnlyError):
        report.transaction("not allowed")
    with (checkpoint / "metadata.bin").open("ab") as stream:
        stream.write(b"corruption")
    with pytest.raises(InventoryStoreCorruptionError, match="Checksum"):
        InventoryStore.open_for_reporting(checkpoint)


def test_report_import_does_not_initialize_build_dependencies():
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import premise.change_report; assert 'bw2io' not in sys.modules; assert 'premise.new_database' not in sys.modules",
        ],
        cwd=Path(__file__).resolve().parents[1],
        check=True,
    )


def test_lazy_public_exports_preserve_api():
    import premise
    from premise.new_database import NewDatabase

    assert premise.NewDatabase is NewDatabase
    assert set(premise.__all__) <= set(dir(premise))
    for name in premise.__all__:
        assert getattr(premise, name) is not None
    with pytest.raises(AttributeError, match="does_not_exist"):
        premise.does_not_exist


@pytest.mark.parametrize("checkpoint", [False, True])
def test_report_input_reads_effective_values_without_mutation(tmp_path, checkpoint):
    from premise._report_inputs import ReportInput, write_report_input
    from premise.inventory_store import CompactInventoryStore

    source = CompactInventoryStore(
        [
            dict(
                name="source",
                code="a",
                unit="kilogram",
                location="GLO",
                parameters={"nested": [1, 2]},
                exchanges=[
                    dict(
                        type="biosphere",
                        name="example",
                        unit="kilogram",
                        amount=-0.0,
                        categories=("air", "urban"),
                        input=("bio", "flow"),
                        custom=b"bytes",
                    )
                ],
            )
        ]
    )
    if checkpoint:
        source = InventoryStore.open_for_reporting(
            source.checkpoint(tmp_path / "checkpoint")
        )
    before = source._report_activity_payload(0)
    headers = write_report_input(source, tmp_path / "input")
    reader = ReportInput(tmp_path / "input")
    try:
        assert reader._report_activity_payload(0) == before
        assert headers == [(0, "a", ("source", "", "GLO", "kilogram"), 1)]
    finally:
        reader.close()
    assert source._report_activity_payload(0) == before


def test_simapro_releases_export_inventory_and_reuses_source(tmp_path, monkeypatch):
    import weakref
    import premise.new_database as module

    original = [dict(name="original", exchanges=[])]
    loads = []
    exporters = []
    database = object.__new__(module.NewDatabase)
    database.scenarios = [dict(model="image", pathway="p", year=2050)]
    database.version = "3.12"
    database.biosphere_name = "biosphere"
    database.system_model = "cutoff"
    database.inventory_backend = "legacy"
    database.generate_reports = True

    def load_original():
        loads.append(True)
        return original

    class Export:
        def __init__(self, scenario, **kwargs):
            self.db = scenario["database"]
            self.unmatched_category_flows = []
            exporters.append(weakref.ref(self))

        def export_db_to_simapro(self):
            pass

    def reports():
        assert all(reference() is None for reference in exporters)
        view = database._report_source_store()
        assert view._report_database is original
        assert view._resolved_store is None

    monkeypatch.setattr(database, "_load_original_database", load_original)
    monkeypatch.setattr(database, "_ensure_semantic_certification", lambda *_: None)
    monkeypatch.setattr(database, "_record_export_validation_phase", lambda *_: None)
    monkeypatch.setattr(database, "_run_automatic_reports", reports)
    monkeypatch.setattr(module, "Export", Export)
    monkeypatch.setattr(
        module,
        "load_database",
        lambda scenario, **_: {
            **scenario,
            "database": [dict(name="exported", exchanges=[])],
        },
    )
    monkeypatch.setattr(module, "_prepare_database", lambda **_: None)
    monkeypatch.setattr(module, "delete_all_pickles", lambda: None)
    database.write_db_to_simapro(str(tmp_path))
    assert loads == [True]
    assert original == [dict(name="original", exchanges=[])]
