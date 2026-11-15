"""Remaining charger links: source limits, event boundaries and stored export."""

import copy
import csv
from types import SimpleNamespace

import pytest
from bw2io import ExcelImporter

from dev.trails_gap_profiles import remaining_charger_profiles
from premise import trails
from premise.core.filesystem_constants import DATA_DIR
from premise.export import Export
from premise.inventory_store import CompactInventoryStore, LegacyInventoryStore
from premise.new_database import NewDatabase
from premise.temporal_specs import TemporalRegistry


def test_charger_evidence_retains_small_sample_and_allocation_limits():
    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    generated = remaining_charger_profiles()
    for key, profile in generated.items():
        assert registry.profiles[key] == profile
    profile = generated["bernmobil-ch-pantograph-site-vintage-2026"]
    evidence = profile["evidence"]
    assert evidence["status"] == "modelled"
    assert evidence["sample_size"]["count"] == 3
    assert "small-sample" in evidence["quality_flags"]
    sites = evidence["coverage"]["site_cohorts"]
    assert {s["site"] for s in sites} == {
        "Köniz, Weiermatt",
        "Bremgarten, Stuckishaus",
        "Köniz, Blinzern",
    }
    assert all("Elfenau" not in s["site"] for s in sites)
    distribution = profile["distribution"]
    assert sum(distribution["weights"]) == pytest.approx(1)
    assert -sum(
        a * w
        for a, w in zip(
            distribution["offsets_years"], distribution["weights"], strict=True
        )
    ) == pytest.approx(11 / 3)
    allocated = generated["reviewed-truck-allocated-charger-construction-event"]
    assert allocated["evidence"]["status"] == "assumption"
    assert allocated["reference_event"] == "construction"
    assert (
        "independent-charger-vintages-unresolved"
        in allocated["evidence"]["quality_flags"]
    )


@pytest.fixture(scope="module")
def charger_callers():
    """Read real names, products and amounts; isolate reviewed capital inputs."""
    callers = []
    for filename in ("lci-buses.xlsx", "lci-trucks.xlsx"):
        for dataset in ExcelImporter(
            DATA_DIR / "additional_inventories" / filename
        ).data:
            if not dataset["name"].startswith(
                ("transport, passenger bus,", "lorry production,")
            ):
                continue
            exchanges = [
                copy.deepcopy(e)
                for e in dataset["exchanges"]
                if e["type"] == "technosphere" and "EV charger" in e.get("name", "")
            ]
            if exchanges:
                exchanges.extend(
                    copy.deepcopy(e)
                    for e in dataset["exchanges"]
                    if e["type"] == "production"
                )
                # The workbook importer has not yet performed premise's usual
                # provider linking and reference-product field normalisation.
                for exchange in exchanges:
                    exchange["product"] = exchange["reference product"]
                callers.append({**copy.deepcopy(dataset), "exchanges": exchanges})
    return callers


@pytest.mark.parametrize(
    "store_class,persisted",
    [
        (None, False),
        (CompactInventoryStore, False),
        (LegacyInventoryStore, False),
        (CompactInventoryStore, True),
        (LegacyInventoryStore, True),
    ],
)
def test_real_charger_links_keep_amounts_and_export_reviewed_timing(
    charger_callers, store_class, persisted, monkeypatch, tmp_path
):
    datasets = copy.deepcopy(charger_callers)
    suppliers = {}
    for caller in datasets:
        for exchange in caller["exchanges"]:
            if exchange["type"] != "technosphere":
                continue
            product = exchange["product"]
            suppliers[exchange["name"]] = {
                "name": exchange["name"],
                "reference product": product,
                "unit": exchange["unit"],
                "location": exchange["location"],
                "exchanges": [
                    {
                        "name": exchange["name"],
                        "product": product,
                        "unit": exchange["unit"],
                        "location": exchange["location"],
                        "type": "production",
                        "amount": 1,
                    }
                ],
            }
    datasets.extend(suppliers.values())
    before = copy.deepcopy(datasets)
    scenario = {"model": "image", "pathway": "SSP2-M", "year": 2030}
    if store_class is None:
        scenario["database"] = datasets
        database = SimpleNamespace(scenarios=[scenario], database=[])
        monkeypatch.setattr(trails, "load_database", lambda s, _: s)
        monkeypatch.setattr(trails, "dump_database", lambda s: s)
    else:
        store = store_class(datasets)
        if persisted:
            scenario["_inventory_checkpoint"] = store.checkpoint(
                tmp_path / "before.inventory-store"
            )
        else:
            scenario["_inventory_store"] = store
        database = NewDatabase.__new__(NewDatabase)
        database._inventory_api_active = True
        database.scenarios = [scenario]
    obj = trails.TrailsDataPackage.__new__(trails.TrailsDataPackage)
    obj._load_canonical_temporal_data()
    obj.source_version = "3.12"
    obj.datapackage = database
    monkeypatch.chdir(tmp_path)
    # Repeating the operation must not add offsets or alter amounts/topology.
    obj.add_temporal_distributions()
    obj.add_temporal_distributions()
    loaded = trails.load_database(database.scenarios[0], [])
    observed = loaded["database"]
    assert len(observed) == len(before)
    pantographs = allocated = 0
    for old, new in zip(before, observed, strict=True):
        assert old["name"] == new["name"]
        assert len(old["exchanges"]) == len(new["exchanges"])
        for old_e, new_e in zip(old["exchanges"], new["exchanges"], strict=True):
            assert {
                k: v for k, v in new_e.items() if not k.startswith("temporal_")
            } == old_e
            if old_e["type"] != "technosphere":
                assert "temporal_distribution" not in new_e
                continue
            assert new_e["temporal_distribution"] == 6
            if new["name"].startswith("lorry production,"):
                allocated += 1
                assert new_e["temporal_offsets"] == [0.0]
                assert new_e["temporal_weights"] == [1.0]
            elif "pantograph" in new_e["name"]:
                pantographs += 1
                assert new_e["temporal_offsets"] == [-7.0, -3.0, -1.0]
    assert (pantographs, allocated) == (4, 14)
    Export(
        scenario=loaded, filepath=tmp_path / "matrices", version="3.12"
    ).export_db_to_matrices()
    with (tmp_path / "matrices/A_matrix.csv").open() as stream:
        rows = list(csv.DictReader(stream, delimiter=";"))
    assert sum(r["temporal_offsets"] == "[0.0]" for r in rows) == 14
    assert sum(r["temporal_offsets"] == "[-7.0, -3.0, -1.0]" for r in rows) == 4


@pytest.mark.parametrize(
    "caller",
    [
        "lorry production, diesel, 26 metric ton",
        "market for lorry",
        "maintenance, lorry",
        "EV charger, level 3, plugin, 200 kW",
    ],
)
def test_allocated_charger_convention_does_not_spread_to_other_callers(caller):
    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    supplier = {"name": "EV charger, level 3, plugin, 200 kW", "unit": "unit"}
    assert (
        registry.resolve(
            supplier,
            target="supplier",
            context={"ecoinvent_version": "3.12"},
            caller_record={"name": caller},
        )
        is None
    )
