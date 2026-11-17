"""Commercial selection checks suppliers, source notes, caches and recipe replay."""

import csv
import json
from copy import deepcopy
from unittest.mock import Mock

import pytest
from openpyxl import Workbook

from premise.database.new_database import NewDatabase
from premise.inventory.clic import ClicInventoryOptions, ClicInventoryProvider
from premise.inventory.clic_archive import ClicArchive, ClicInventoryError
from premise.inventory.clic_licenses import (
    COMMERCIAL_LICENSES,
    POLICY_FINGERPRINT,
    ClicLicenseError,
    assess_commercial_licenses,
)
from premise.reporting.clic import write_inventory_sheet, write_inventory_sidecars
from test_clic_archive import catalog, native, purchase, write_archive
from test_clic_harmonize import provider
from test_clic_reporting import scenario


@pytest.mark.parametrize("license_id", COMMERCIAL_LICENSES)
def test_permitted_licenses_record_conditions_without_modifying_inventory(license_id):
    version = native(1, 1, "root")
    version["selected_license"] = license_id
    archive = catalog(version)
    resolved = archive.resolve(["dataset-1"])
    before = deepcopy(resolved)
    report = assess_commercial_licenses(resolved, archive)
    assert report["status"] == "passed"
    row = report["datasets"][0]
    assert row["attribution_required"] is (license_id != "CC0-1.0")
    assert row["share_alike"] is (license_id == "CC-BY-SA-4.0")
    assert resolved == before


@pytest.mark.parametrize(
    "license_id",
    ["CC-BY-NC-4.0", "CC-BY-NC-SA-4.0", "UNDISCLOSED", "unknown", "", None],
)
def test_restricted_upstream_cannot_hide_behind_permissive_root(license_id, tmp_path):
    root = native(1, 1, "root", exchanges=[purchase("component")])
    component = native(2, 2, "component", exchanges=[purchase("root")])
    component["selected_license"] = license_id
    unrelated = native(3, 3, "unselected")
    unrelated["selected_license"] = "CC-BY-NC-4.0"
    archive = catalog(root, component, unrelated)
    client = ClicInventoryProvider(
        archive=archive,
        options={
            "cache_dir": tmp_path,
            "offline": True,
            "license_policy": "commercial",
        },
    )
    with pytest.raises(ClicLicenseError, match="dataset-1 -> dataset-2") as error:
        client.resolve(["dataset-1"])
    assert len(error.value.issues) == 1
    assert error.value.issues[0]["slug"] == "dataset-2"


@pytest.mark.parametrize(
    "note",
    [
        "CC-BY-NC-ND-4.0",
        "CC BY-NC-SA 4.0",
        "https://creativecommons.org/licenses/by-nc/4.0/",
    ],
)
def test_source_conflict_needs_review(note):
    version = native(1, 1, "root")
    version["existing_license"] = note
    archive = catalog(version)
    report = assess_commercial_licenses(archive.resolve(["dataset-1"]), archive)
    assert report["issues"][0]["reason"] == "source-license conflict needs review"
    assert report["datasets"][0]["source_license"] == note


def test_commercial_policy_is_applied_to_graph_from_default_cache(tmp_path):
    archive = ClicArchive.read(write_archive(tmp_path / "source.zip"))
    # Existing caches need not contain the source-license notes.
    archive.versions[1]["existing_license"] = "CC-BY-NC-ND-4.0"
    archive.resolve = Mock(wraps=archive.resolve)
    client = ClicInventoryProvider(
        archive=archive, options={"cache_dir": tmp_path, "offline": True}
    )
    client.resolve(["dataset-1"])
    client = ClicInventoryProvider(
        archive=archive,
        options={
            "cache_dir": tmp_path,
            "offline": True,
            "license_policy": "commercial",
        },
    )
    with pytest.raises(ClicLicenseError, match="conflict"):
        client.resolve(["dataset-1"])
    assert archive.resolve.call_count == 1


def test_commercial_recipe_replay_restores_checks_and_exports_attribution(
    tmp_path, provider
):
    archive = catalog(native(1, 1, "root"))

    def client(recipe=None, **options):
        return ClicInventoryProvider(
            archive=archive,
            recipe=recipe,
            catalog_provider=provider,
            options={"cache_dir": tmp_path, "offline": True, **options},
        )

    default = client()
    before = default.prepare(
        ["dataset-1"], [], version="3.12", biosphere_codes={}, classifications={}
    )
    assert "license_policy" not in default.manifest
    assert "license_policy" not in default.lockfile
    commercial = client(license_policy="commercial")
    after = commercial.prepare(
        ["dataset-1"], [], version="3.12", biosphere_codes={}, classifications={}
    )
    assert before == after
    assert commercial.manifest["license_policy"]["status"] == "passed"
    recipe = commercial.write_recipe(tmp_path / "recipe.json")
    replay = client(recipe=recipe)
    assert replay.options.license_policy == "commercial"
    replay.prepare(
        ["dataset-1"], [], version="3.12", biosphere_codes={}, classifications={}
    )
    assert replay.manifest["license_policy"] == commercial.manifest["license_policy"]
    definition = scenario(tmp_path)
    definition["clic inventories"]["license_policy"] = commercial.manifest[
        "license_policy"
    ]
    definition["clic recipe"] = commercial.lockfile
    manifest_path, _ = write_inventory_sidecars(tmp_path / "report.xlsx", [definition])
    saved = json.loads(manifest_path.read_text())["scenarios"][0]
    with (tmp_path / saved["license_report_file"]).open() as stream:
        rows = list(csv.DictReader(stream))
    assert rows[0]["slug"] == "dataset-1"
    assert rows[0]["license_url"] == COMMERCIAL_LICENSES["CC-BY-4.0"]
    workbook = Workbook()
    write_inventory_sheet(workbook, [definition])
    assert workbook["CLIC Licenses"].cell(2, 4).value == "CC-BY-4.0"


def test_unknown_policy_and_changed_recipe_policy_fail_before_download():
    with pytest.raises(ValueError, match="license_policy"):
        ClicInventoryOptions(license_policy="anything")
    with pytest.raises(ClicInventoryError, match="policy changed"):
        ClicInventoryProvider(
            recipe={"license_policy": {"name": "commercial", "fingerprint": "old"}}
        )
    assert len(POLICY_FINGERPRINT) == 64


def test_consequential_commercial_build_cannot_fall_back_to_workbooks(tmp_path):
    client = ClicInventoryProvider(
        options={"cache_dir": tmp_path, "license_policy": "commercial"}
    )
    client.get_archive = Mock(side_effect=AssertionError("Must fail before download"))
    with pytest.raises(ClicInventoryError, match="workbook fallback"):
        client.prepare([], [], version="3.12", system_model="consequential")
    client.get_archive.assert_not_called()
    with pytest.raises(ClicInventoryError, match="workbook fallback"):
        NewDatabase(
            scenarios=[],
            source_version="3.12",
            system_model="consequential",
            clic_options={"license_policy": "commercial"},
        )


def test_unsupported_commercial_target_fails_before_archive_download(tmp_path):
    client = ClicInventoryProvider(
        options={"cache_dir": tmp_path, "license_policy": "commercial"}
    )
    client.get_archive = Mock(side_effect=AssertionError("No archive request expected"))
    with pytest.raises(ClicInventoryError, match="3.8 or newer"):
        client.prepare([], [], version="3.7")
    client.get_archive.assert_not_called()
    with pytest.raises(ClicInventoryError, match="3.8 or newer"):
        NewDatabase(
            scenarios=[],
            source_version="3.7",
            clic_options={"license_policy": "commercial"},
        )
