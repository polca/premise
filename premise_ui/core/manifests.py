"""Manifest models for GUI configurations and worker runs."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from premise_ui.core.project_migrations import (
    CURRENT_GUI_SCHEMA_VERSION,
    migrate_gui_project_payload,
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


WORKFLOW_EXPORT_DEFAULTS: dict[str, dict[str, dict[str, Any]]] = {
    "new_database": {
        "brightway": {"name": ""},
        "matrices": {"filepath": "export"},
        "datapackage": {"name": ""},
        "simapro": {"filepath": "export/simapro"},
        "openlca": {"filepath": "export/olca"},
        "superstructure": {
            "name": "",
            "filepath": "export/scenario-diff",
            "file_format": "csv",
            "preserve_original_column": False,
        },
    },
    "incremental_database": {
        "brightway": {
            "name": "",
            "filepath": "export/incremental",
            "file_format": "csv",
        },
        "matrices": {"filepath": "export"},
        "simapro": {"filepath": "export/simapro"},
        "openlca": {"filepath": "export/olca"},
    },
    "pathways_datapackage": {
        "datapackage": {"name": "pathways"},
    },
}


def normalize_export_config(
    export: dict[str, Any] | None, *, workflow: str = "new_database"
) -> dict[str, Any]:
    payload = dict(export or {})
    workflow_defaults = WORKFLOW_EXPORT_DEFAULTS.get(
        workflow,
        WORKFLOW_EXPORT_DEFAULTS["new_database"],
    )
    default_export_type = next(iter(workflow_defaults))
    export_type = str(payload.get("type", default_export_type))
    if export_type not in workflow_defaults:
        export_type = default_export_type

    options = dict(payload.get("options", {}))
    normalized_options = deepcopy(workflow_defaults[export_type])
    allowed_keys = set(normalized_options)

    for key, value in options.items():
        if key in allowed_keys:
            normalized_options[key] = deepcopy(value)

    return {
        "type": export_type,
        "options": normalized_options,
    }


DEFAULT_NEW_DATABASE_CONFIG = {
    "source_type": "brightway",
    "source_version": "3.12",
    "source_project": "",
    "source_db": "",
    "source_file_path": "",
    "system_model": "cutoff",
    "system_args": {},
    "use_cached_inventories": True,
    "use_cached_database": True,
    "quiet": False,
    "keep_imports_uncertainty": True,
    "keep_source_db_uncertainty": False,
    "gains_scenario": "CLE",
    "use_absolute_efficiency": False,
    "biosphere_name": "biosphere3",
    "generate_reports": True,
    "additional_inventories": [],
    "transformations": None,
    "export": normalize_export_config({"type": "brightway"}, workflow="new_database"),
}

DEFAULT_INCREMENTAL_DATABASE_CONFIG = {
    **deepcopy(DEFAULT_NEW_DATABASE_CONFIG),
    "sectors": [],
    "export": {
        "type": "brightway",
        "options": deepcopy(
            WORKFLOW_EXPORT_DEFAULTS["incremental_database"]["brightway"]
        ),
    },
}

DEFAULT_PATHWAYS_DATAPACKAGE_CONFIG = {
    "source_type": "brightway",
    "source_version": "3.12",
    "source_project": "",
    "source_db": "",
    "source_file_path": "",
    "system_model": "cutoff",
    "system_args": {},
    "gains_scenario": "CLE",
    "use_absolute_efficiency": False,
    "biosphere_name": "biosphere3",
    "generate_reports": True,
    "additional_inventories": [],
    "transformations": None,
    "years": [2030, 2040, 2050],
    "contributors": [{"title": "", "name": "", "email": ""}],
    "export": {
        "type": "datapackage",
        "options": deepcopy(
            WORKFLOW_EXPORT_DEFAULTS["pathways_datapackage"]["datapackage"]
        ),
    },
}

DEFAULT_NEW_DATABASE_SCENARIO_SETS = [
    {
        "name": "default",
        "scenarios": [
            {"model": "remind", "pathway": "SSP2-Base", "year": 2030},
        ],
    }
]

WORKFLOW_DEFAULTS = {
    "new_database": {
        "config": DEFAULT_NEW_DATABASE_CONFIG,
        "scenario_sets": DEFAULT_NEW_DATABASE_SCENARIO_SETS,
    },
    "incremental_database": {
        "config": DEFAULT_INCREMENTAL_DATABASE_CONFIG,
        "scenario_sets": DEFAULT_NEW_DATABASE_SCENARIO_SETS,
    },
    "pathways_datapackage": {
        "config": DEFAULT_PATHWAYS_DATAPACKAGE_CONFIG,
        "scenario_sets": DEFAULT_NEW_DATABASE_SCENARIO_SETS,
    },
}


def _sanitize_workflow_config(
    workflow: str, config: dict[str, Any] | None
) -> dict[str, Any]:
    sanitized = deepcopy(config or {})

    if workflow != "pathways_datapackage":
        sanitized.pop("years", None)
        sanitized.pop("contributors", None)

    if workflow != "incremental_database":
        sanitized.pop("sectors", None)

    return sanitized


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(base)
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = deepcopy(value)
    return merged


def _scenario_sets_to_run_scenarios(
    scenario_sets: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[str]]:
    scenarios: list[dict[str, Any]] = []
    names: list[str] = []

    for index, entry in enumerate(scenario_sets):
        if isinstance(entry, dict) and isinstance(entry.get("scenarios"), list):
            entry_scenarios = entry["scenarios"]
            names.append(str(entry.get("name", f"set-{index + 1}")))
        else:
            entry_scenarios = [entry]
            if isinstance(entry, dict) and entry.get("name"):
                names.append(str(entry["name"]))
            else:
                names.append(f"set-{index + 1}")

        for scenario in entry_scenarios:
            if isinstance(scenario, dict):
                normalized = deepcopy(scenario)
                if normalized.get("filepath") in (None, ""):
                    normalized.pop("filepath", None)
                scenarios.append(normalized)

    return scenarios, names


@dataclass(slots=True)
class GuiProjectManifest:
    schema_version: int = CURRENT_GUI_SCHEMA_VERSION
    project_name: str = "Untitled Premise Configuration"
    workflow: str = "new_database"
    config: dict[str, Any] = field(default_factory=dict)
    scenario_sets: list[dict[str, Any]] = field(default_factory=list)
    run_history: list[dict[str, Any]] = field(default_factory=list)
    ui_state: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def template(cls, workflow: str = "new_database") -> "GuiProjectManifest":
        defaults = WORKFLOW_DEFAULTS.get(workflow, {"config": {}, "scenario_sets": []})
        return cls(
            workflow=workflow,
            config=deepcopy(defaults["config"]),
            scenario_sets=deepcopy(defaults["scenario_sets"]),
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "GuiProjectManifest":
        migrated = migrate_gui_project_payload(data)
        workflow = str(migrated.get("workflow", "new_database"))
        base = cls.template(workflow=workflow)
        merged_config = _deep_merge(base.config, dict(migrated.get("config", {})))
        merged_config = _sanitize_workflow_config(workflow, merged_config)
        merged_config["export"] = normalize_export_config(
            merged_config.get("export"),
            workflow=workflow,
        )
        return cls(
            schema_version=int(
                migrated.get("schema_version", CURRENT_GUI_SCHEMA_VERSION)
            ),
            project_name=str(migrated.get("project_name", base.project_name)),
            workflow=workflow,
            config=merged_config,
            scenario_sets=deepcopy(migrated.get("scenario_sets", base.scenario_sets)),
            run_history=list(migrated.get("run_history", [])),
            ui_state=_deep_merge(base.ui_state, dict(migrated.get("ui_state", {}))),
        )

    def cloned(self) -> "GuiProjectManifest":
        return GuiProjectManifest(
            schema_version=self.schema_version,
            project_name=f"{self.project_name} Copy",
            workflow=self.workflow,
            config=deepcopy(self.config),
            scenario_sets=deepcopy(self.scenario_sets),
            run_history=[],
            ui_state=deepcopy(self.ui_state),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "project_name": self.project_name,
            "workflow": self.workflow,
            "config": self.config,
            "scenario_sets": self.scenario_sets,
            "run_history": self.run_history,
            "ui_state": self.ui_state,
        }


@dataclass(slots=True)
class RunManifest:
    schema_version: int = 1
    run_id: str = field(default_factory=lambda: uuid4().hex)
    created_at: str = field(default_factory=utc_now_iso)
    project_name: str = "Untitled Premise Configuration"
    workflow: str = "new_database"
    project_path: str | None = None
    working_directory: str = ""
    config: dict[str, Any] = field(default_factory=dict)
    scenarios: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RunManifest":
        return cls(
            schema_version=int(data.get("schema_version", 1)),
            run_id=str(data.get("run_id", uuid4().hex)),
            created_at=str(data.get("created_at", utc_now_iso())),
            project_name=str(
                data.get("project_name", "Untitled Premise Configuration")
            ),
            workflow=str(data.get("workflow", "new_database")),
            project_path=data.get("project_path"),
            working_directory=str(data.get("working_directory", "")),
            config=dict(data.get("config", {})),
            scenarios=list(data.get("scenarios", [])),
            metadata=dict(data.get("metadata", {})),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "created_at": self.created_at,
            "project_name": self.project_name,
            "workflow": self.workflow,
            "project_path": self.project_path,
            "working_directory": self.working_directory,
            "config": self.config,
            "scenarios": self.scenarios,
            "metadata": self.metadata,
        }


def build_run_manifest_from_project(
    project: GuiProjectManifest,
    *,
    project_path: str | None = None,
) -> RunManifest:
    scenarios, scenario_set_names = _scenario_sets_to_run_scenarios(
        project.scenario_sets
    )

    working_directory = ""
    resolved_project_path = None
    if project_path:
        project_file = Path(project_path).expanduser().resolve()
        resolved_project_path = str(project_file)
        working_directory = str(project_file.parent)

    return RunManifest(
        project_name=project.project_name,
        workflow=project.workflow,
        project_path=resolved_project_path,
        working_directory=working_directory,
        config=_sanitize_workflow_config(project.workflow, project.config),
        scenarios=scenarios,
        metadata={
            "project_source": "gui_project",
            "scenario_set_names": scenario_set_names,
        },
    )


def validate_run_manifest_payload(
    payload: dict[str, Any],
) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []

    workflow = payload.get("workflow")
    if not workflow:
        errors.append("`workflow` is required.")

    project_name = payload.get("project_name")
    if not project_name:
        errors.append("Configuration Name is required.")

    if not payload.get("scenarios"):
        warnings.append(
            "No scenarios were provided. Dry-run scaffolding can still proceed."
        )

    if not payload.get("config"):
        warnings.append("No workflow config was provided. Using scaffold defaults.")

    return errors, warnings
