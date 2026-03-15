"""Run-manifest validation for the Premise UI scaffold."""

from __future__ import annotations

from premise_ui.core.adapters import get_workflow_adapter
from premise_ui.core.manifests import RunManifest

IMPLEMENTED_WORKFLOWS = {
    "new_database",
    "incremental_database",
    "pathways_datapackage",
}


def validate_run_manifest_payload(payload: dict) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []

    workflow = payload.get("workflow")
    if not workflow:
        errors.append("`workflow` is required.")

    project_name = payload.get("project_name")
    if not project_name:
        errors.append("Configuration Name is required.")

    if errors:
        return errors, warnings

    if workflow not in {"new_database", "incremental_database", "pathways_datapackage"}:
        errors.append(f"Unknown workflow `{workflow}`.")
        return errors, warnings

    if workflow not in IMPLEMENTED_WORKFLOWS:
        errors.append(
            f"Workflow `{workflow}` is not implemented yet in the UI backend scaffold."
        )
        return errors, warnings

    manifest = RunManifest.from_dict(payload)
    adapter = get_workflow_adapter(manifest)
    result = adapter.validate()
    return result.errors, result.warnings
