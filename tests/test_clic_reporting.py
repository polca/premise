"""Published reports carry usable recipes, private-source hashes and migration audits."""

import gzip
import hashlib
import json

from openpyxl import Workbook, load_workbook

from premise.core.utils import database_metadata
from premise.inventory.clic_archive import ClicRecipe
from premise.reporting.clic import write_inventory_sheet, write_inventory_sidecars


def test_checkpoint_restores_inventory_manifest_and_recipe(tmp_path):
    from premise.database.new_database import NewDatabase
    from premise.inventory.inventory_store import create_inventory_store, InventoryStore

    definition = scenario(tmp_path)
    store = create_inventory_store(
        [
            {
                "name": "root",
                "reference product": "root",
                "location": "GLO",
                "unit": "kilogram",
                "code": "root",
                "database": "test",
                "exchanges": [
                    {
                        "name": "root",
                        "product": "root",
                        "location": "GLO",
                        "unit": "kilogram",
                        "type": "production",
                        "amount": 1,
                    }
                ],
            }
        ],
        backend="compact",
    )
    store._provenance_payload = {
        "schema_version": 1,
        "build_id": "checkpoint-test",
        "events": [],
        "clic inventories": definition["clic inventories"],
        "clic recipe": definition["clic recipe"],
    }
    opened = InventoryStore.open(store.checkpoint(tmp_path / "inventory-checkpoint"))
    recovered = {key: definition[key] for key in ("model", "pathway", "year")}
    owner = NewDatabase.__new__(NewDatabase)
    owner._restore_scenario_provenance(recovered, opened)
    assert recovered["clic recipe"] == definition["clic recipe"]
    assert recovered["clic inventories"]["datasets"][0]["semantic_version"] == "1.2.0"


def scenario(tmp_path):
    audit = tmp_path / "cached-audit.json.gz"
    audit.write_bytes(gzip.compress(b'{"stages": []}'))
    background = {
        "technosphere": {
            "family": "ecoinvent",
            "version": "3.12",
            "system_model": "cutoff",
        },
        "biosphere": {"family": "ecoinvent", "version": "3.12"},
    }
    row = {
        "slug": "required-root",
        "semantic_version": "1.2.0",
        "version_id": 12,
        "activity_code": "clic-root",
        "role": "root",
        "background": background,
        "license": "CC-BY-4.0",
    }
    return {
        "model": "remind",
        "pathway": "SSP2-NPi",
        "year": 2050,
        "clic inventories": {
            "datasets": [row],
            "target": background,
            "brightpath_version": "test",
            "archive": {"doi": "10.5281/zenodo.test", "fingerprint": "snapshot"},
            "resolution_fingerprint": "graph",
            "hybrid_plan_fingerprint": "hybrid",
            "retained_workbooks": [
                {"file": "licensed.xlsx", "sha256": "a" * 64, "source_version": "3.10"}
            ],
            "cohorts": [
                {
                    "fingerprint": "cohort",
                    "audit_file": str(audit),
                    "audit_sha256": hashlib.sha256(audit.read_bytes()).hexdigest(),
                }
            ],
        },
        "clic recipe": {
            "schema_version": 1,
            "roots": ["required-root"],
            "versions": {"required-root": "1.2.0"},
            "workbooks": {"licensed.xlsx": "a" * 64},
        },
    }


def test_report_sidecars_are_portable_and_recipe_includes_retained_workbooks(tmp_path):
    definition = scenario(tmp_path)
    export = tmp_path / "report.xlsx"
    manifest_path, recipes = write_inventory_sidecars(export, [definition])
    saved = json.loads(manifest_path.read_text())["scenarios"][0]
    audit = saved["inventory"]["cohorts"][0]
    assert audit["audit_file"] == "report.migration-cohort.json.gz"
    assert (
        hashlib.sha256((tmp_path / audit["audit_file"]).read_bytes()).hexdigest()
        == audit["audit_sha256"]
    )
    recipe = ClicRecipe.read(recipes[0])
    assert recipe.versions["required-root"].version == "1.2.0"
    assert recipe.workbooks == {"licensed.xlsx": "a" * 64}
    workbook = Workbook()
    write_inventory_sheet(workbook, [definition], workbook_path=export)
    workbook.save(export)
    sheet = load_workbook(export)["CLIC Inventories"]
    assert sheet.cell(2, 2).value == "required-root"
    assert sheet.cell(2, 3).value == "1.2.0"
    assert sheet.cell(3, 2).value == "licensed.xlsx"
    assert sheet.cell(3, 13).value == "a" * 64


def test_database_exports_preserve_owned_version_metadata(tmp_path):
    definition = scenario(tmp_path)
    metadata = database_metadata([definition], version="3.12", system_model="cutoff")
    assert metadata["inventory_manifest"]["datasets"][0]["semantic_version"] == "1.2.0"
    assert (
        metadata["inventory_recipe"]["workbooks"]
        == definition["clic recipe"]["workbooks"]
    )
    definition["clic inventories"]["datasets"][0]["semantic_version"] = "2.0.0"
    assert metadata["inventory_manifest"]["datasets"][0]["semantic_version"] == "1.2.0"
    json.dumps(metadata)
