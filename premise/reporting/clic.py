"""Portable CLIC version manifests and recipes for scenario export artifacts."""

from __future__ import annotations

import copy
import csv
import hashlib
import json
from pathlib import Path

from openpyxl.styles import Alignment, Font, PatternFill

from ..inventory.clic_download import _write_json


def _definitions(scenarios):
    for position, scenario in enumerate(scenarios, 1):
        definition = scenario if isinstance(scenario, dict) else scenario.definition
        if definition is not None and definition.get("clic inventories"):
            identity = (
                definition.get("model"),
                definition.get("pathway"),
                definition.get("year"),
            )
            if not isinstance(scenario, dict):
                identity = scenario.identity
            yield position, identity, definition


def write_inventory_sidecars(workbook_path, scenarios):
    """Export all selected versions and one directly reusable recipe per scenario."""
    path = Path(workbook_path)
    manifests, recipes = [], []
    for position, identity, definition in _definitions(scenarios):
        manifest = copy.deepcopy(definition["clic inventories"])
        for cohort in manifest.get("cohorts", ()):
            source = Path(cohort["audit_file"])
            destination = path.with_name(
                f"{path.stem}.migration-{cohort['fingerprint']}.json.gz"
            )
            if source.exists():
                payload = source.read_bytes()
                if hashlib.sha256(payload).hexdigest() != cohort["audit_sha256"]:
                    raise ValueError(
                        f"CLIC migration audit checksum mismatch: {source}"
                    )
                if (
                    not destination.exists()
                    or hashlib.sha256(destination.read_bytes()).hexdigest()
                    != cohort["audit_sha256"]
                ):
                    temporary = destination.with_suffix(destination.suffix + ".tmp")
                    temporary.write_bytes(payload)
                    temporary.replace(destination)
                cohort["audit_file"] = destination.name
            else:
                cohort["audit_available"] = False
        recipe = definition.get("clic recipe")
        recipe_path = None
        if recipe is not None:
            recipe_path = path.with_name(
                f"{path.stem}.inventory-recipe-{position}.json"
            )
            _write_json(recipe_path, recipe)
            recipes.append(recipe_path)
        manifests.append(
            {
                "scenario_number": position,
                "scenario_identity": list(identity),
                "inventory": manifest,
                "recipe_file": recipe_path.name if recipe_path else None,
            }
        )
        policy = manifest.get("license_policy")
        if policy is not None:
            license_path = path.with_name(
                f"{path.stem}.inventory-licenses-{position}.csv"
            )
            columns = (
                list(policy["datasets"][0])
                if policy["datasets"]
                else ["slug", "license"]
            )
            with license_path.open("w", encoding="utf-8", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=columns)
                writer.writeheader()
                writer.writerows(policy["datasets"])
            manifests[-1]["license_report_file"] = license_path.name
        replacements = manifest.get("commercial_replacements", {}).get(
            "substitutions", ()
        )
        if replacements:
            replacement_path = path.with_name(
                f"{path.stem}.inventory-substitutions-{position}.csv"
            )
            with replacement_path.open("w", encoding="utf-8", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=list(replacements[0]))
                writer.writeheader()
                writer.writerows(
                    {
                        key: (
                            json.dumps(value, ensure_ascii=False)
                            if isinstance(value, (list, dict))
                            else value
                        )
                        for key, value in row.items()
                    }
                    for row in replacements
                )
            manifests[-1]["substitution_report_file"] = replacement_path.name
    if not manifests:
        return None, ()
    manifest_path = path.with_suffix(".inventory-manifest.json")
    _write_json(manifest_path, {"schema_version": 1, "scenarios": manifests})
    return manifest_path, tuple(recipes)


def write_inventory_sheet(workbook, scenarios, *, workbook_path=None):
    """Expose actual root/upstream versions in summary and diagnostic workbooks."""
    selected = list(_definitions(scenarios))
    if not selected:
        return
    sheet = workbook.create_sheet("CLIC Inventories")
    columns = [
        "Scenario",
        "Dataset slug",
        "Version",
        "Version ID",
        "Role",
        "License",
        "Source background",
        "Source biosphere",
        "Target background",
        "Target biosphere",
        "Brightpath",
        "Snapshot DOI",
        "Snapshot fingerprint",
        "Resolution fingerprint",
        "Recipe file",
        "Dataset URL",
    ]
    commercial = any(
        "license_policy" in definition["clic inventories"]
        for _, _, definition in selected
    )
    if commercial:
        columns[5] = "Published license (original)"
        columns.extend(["Used inventory license", "Replacement rule"])
    sheet.append(columns)

    def background(value):
        return " / ".join(
            str(value[key])
            for key in ("family", "version", "system_model")
            if key in value
        )

    for position, identity, definition in selected:
        manifest = definition["clic inventories"]
        target = manifest["target"]
        snapshot = manifest["archive"]
        recipe_name = (
            f"{Path(workbook_path).stem}.inventory-recipe-{position}.json"
            if workbook_path is not None and definition.get("clic recipe") is not None
            else None
        )
        for row in manifest["datasets"]:
            sheet.append(
                [
                    " | ".join(str(item) for item in identity),
                    row["slug"],
                    row["semantic_version"],
                    row["version_id"],
                    row["role"],
                    row.get("license"),
                    background(row["background"]["technosphere"]),
                    background(row["background"]["biosphere"]),
                    background(target["technosphere"]),
                    background(target["biosphere"]),
                    manifest["brightpath_version"],
                    snapshot.get("doi"),
                    snapshot.get("fingerprint"),
                    manifest["resolution_fingerprint"],
                    recipe_name,
                    row.get("public_url"),
                    *(
                        [
                            row.get("replacement", {}).get(
                                "inventory_license", row.get("license")
                            ),
                            row.get("replacement", {}).get("rule"),
                        ]
                        if commercial
                        else []
                    ),
                ]
            )
        for code in manifest.get("generated_helpers", ()):
            sheet.append(
                [
                    " | ".join(str(item) for item in identity),
                    code,
                    None,
                    None,
                    "generated helper",
                    None,
                    None,
                    None,
                    background(target["technosphere"]),
                    background(target["biosphere"]),
                    manifest["brightpath_version"],
                    snapshot.get("doi"),
                    snapshot.get("fingerprint"),
                    manifest["resolution_fingerprint"],
                    recipe_name,
                ]
            )
        source_rows = [
            (row, "retained workbook") for row in manifest.get("retained_workbooks", ())
        ]
        source_rows.extend(
            (row, "user inventory") for row in manifest.get("user_inventories", ())
        )
        for row, role in source_rows:
            sheet.append(
                [
                    " | ".join(str(item) for item in identity),
                    row["file"],
                    None,
                    None,
                    role,
                    "See source inventory",
                    row["source_version"],
                    None,
                    background(target["technosphere"]),
                    background(target["biosphere"]),
                    None,
                    None,
                    row["sha256"],
                    manifest.get("hybrid_plan_fingerprint"),
                    recipe_name,
                ]
            )
    sheet.freeze_panes = "A2"
    sheet.auto_filter.ref = sheet.dimensions
    for cell in sheet[1]:
        cell.fill = PatternFill("solid", fgColor="17365D")
        cell.font = Font(bold=True, color="FFFFFF")
        cell.alignment = Alignment(wrap_text=True, vertical="top")
    sheet.row_dimensions[1].height = 32
    for position, width in enumerate(
        [38, 64, 12, 12, 18, 20, 30, 24, 30, 24, 15, 32, 38, 38, 48, 64], 1
    ):
        sheet.column_dimensions[sheet.cell(1, position).column_letter].width = width

    policies = [
        (identity, definition["clic inventories"]["license_policy"])
        for _, identity, definition in selected
        if "license_policy" in definition["clic inventories"]
    ]
    if policies:
        sheet = workbook.create_sheet("CLIC Licenses")
        sheet.append(
            [
                "Scenario",
                "Dataset slug",
                "Version ID",
                "License",
                "License URL",
                "Source license note",
                "Citation",
                "DOI",
                "Attribution required",
                "ShareAlike",
                "Policy scope",
            ]
        )
        for identity, policy in policies:
            for row in policy["datasets"]:
                sheet.append(
                    [
                        " | ".join(str(item) for item in identity),
                        row["slug"],
                        row["version_id"],
                        row["license"],
                        row["license_url"],
                        row["source_license"],
                        row["citation"],
                        row["doi"],
                        row["attribution_required"],
                        row["share_alike"],
                        policy["scope"],
                    ]
                )
        sheet.freeze_panes = "A2"
        sheet.auto_filter.ref = sheet.dimensions
        for cell in sheet[1]:
            cell.font = Font(bold=True)
        for column in sheet.columns:
            sheet.column_dimensions[column[0].column_letter].width = 35

    replacements = [
        (identity, row)
        for _, identity, definition in selected
        for row in definition["clic inventories"]
        .get("commercial_replacements", {})
        .get("substitutions", ())
    ]
    if replacements:
        sheet = workbook.create_sheet("CLIC Replacements")
        sheet.append(
            [
                "Scenario",
                "Dataset slug",
                "Original license",
                "Rule",
                "Native supplier",
                "Amount",
                "Conversion",
                "Limitations",
                "Evidence",
                "Confidence",
                "Geographic proxy",
            ]
        )
        for identity, row in replacements:
            sheet.append(
                [
                    " | ".join(str(item) for item in identity),
                    row["slug"],
                    row["original_license"],
                    row["rule"],
                    " | ".join(row["supplier"]),
                    row["amount"],
                    row["conversion"],
                    row["limitations"],
                    "\n".join(row["evidence"]),
                    row["confidence"],
                    row["geography_proxy"],
                ]
            )
        sheet.freeze_panes = "A2"
        sheet.auto_filter.ref = sheet.dimensions
        for cell in sheet[1]:
            cell.font = Font(bold=True)
        for column in sheet.columns:
            sheet.column_dimensions[column[0].column_letter].width = 40
