"""Licensed end-to-end check for the live CLIC inventory provider."""

from __future__ import annotations

import json
import os
from pathlib import Path

import bw2data
import bw2io
import pytest
from dotenv import load_dotenv

from premise import NewDatabase
from premise.core.filesystem_constants import VARIABLES_DIR
from premise.inventory.activity_maps import InventorySet, get_mapping

load_dotenv()

EI_USERNAME = os.environ.get("EI_USERNAME")
EI_PASSWORD = os.environ.get("EI_PASSWORD")
IAM_KEY = os.environ.get("IAM_FILES_KEY", "")
CLIC_ARCHIVE = os.environ.get("PREMISE_CLIC_ARCHIVE")

pytestmark = [
    pytest.mark.skipif(
        not (EI_USERNAME and EI_PASSWORD and IAM_KEY and CLIC_ARCHIVE),
        reason="ecoinvent credentials, IAM key, and a CLIC archive are required",
    ),
]


@pytest.mark.parametrize("license_policy", ["default", "commercial"])
def test_live_clic_electricity_update_preserves_reviewed_role_filters(
    tmp_path, license_policy
):
    project_name = "ecoinvent-3.12-cutoff"
    biosphere_name = "ecoinvent-3.12-biosphere"
    bw2data.projects.set_current(project_name)
    if project_name not in bw2data.databases:
        bw2io.import_ecoinvent_release(
            version="3.12",
            system_model="cutoff",
            username=EI_USERNAME,
            password=EI_PASSWORD,
            biosphere_name=biosphere_name,
        )

    ndb = NewDatabase(
        scenarios=[{"model": "remind", "pathway": "SSP2-NPi", "year": 2050}],
        source_db=project_name,
        source_version="3.12",
        key=IAM_KEY.encode(),
        system_model="cutoff",
        biosphere_name=biosphere_name,
        generate_reports=False,
        inventory_backend="compact",
        clic_options={
            "archive_path": CLIC_ARCHIVE,
            "cache_dir": tmp_path / "clic",
            "offline": True,
            "freshness": "snapshot",
            "license_policy": license_policy,
        },
    )
    ndb.update(["electricity"])

    assert ndb._clic_loaded_sectors == {"electricity"}
    assert ndb._clic_manifest["requested_sectors"] == ["electricity"]
    assert "electricity" in ndb.scenarios[0]["applied functions"]

    definitions = get_mapping(
        VARIABLES_DIR / "electricity.yaml", "ecoinvent_aliases", model="remind"
    )
    selected = InventorySet(
        ndb._clic_records, version="3.12", model="remind"
    ).generate_sets_from_filters(definitions)
    expected = {}
    for record in ndb._clic_records:
        provenance = record.get("clic provenance", {})
        for binding in provenance.get("role_bindings", ()):
            filename, role = binding.split(":", 1)
            if filename == "electricity.yaml" and role in definitions:
                expected.setdefault(role, set()).add(provenance["slug"])

    assert expected
    for role, slugs in expected.items():
        actual = {
            record["clic provenance"]["slug"]
            for record in selected[role]
            if record.get("clic provenance")
        }
        assert actual == slugs

    recipe_path = ndb.write_inventory_recipe(tmp_path / "inventory-recipe.json")
    recipe = json.loads(recipe_path.read_text(encoding="utf-8"))
    assert set(recipe["roots"]) == set(ndb._clic_lockfile["roots"])
    used_slugs = {row["slug"] for row in ndb._clic_manifest["datasets"]}
    if license_policy == "commercial":
        assert ndb._clic_manifest["license_policy"]["status"] == "passed"
        assert recipe["license_policy"]["name"] == "commercial"
        used_slugs.update(ndb._clic_manifest["commercial_replacements"]["pruned_slugs"])
    assert set(recipe["versions"]) == used_slugs
