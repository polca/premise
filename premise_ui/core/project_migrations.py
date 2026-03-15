"""GUI configuration schema migrations for Premise UI."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Callable

CURRENT_GUI_SCHEMA_VERSION = 1


def migrate_gui_project_payload(data: dict[str, Any]) -> dict[str, Any]:
    """Upgrade a raw GUI project payload to the current schema version."""

    if not isinstance(data, dict):
        raise TypeError("GUI project payload must be a dictionary.")

    payload = deepcopy(data)
    version = _coerce_schema_version(payload.get("schema_version"))

    if version > CURRENT_GUI_SCHEMA_VERSION:
        raise ValueError(
            "Configuration schema version "
            f"{version} is newer than this Premise UI release supports "
            f"(current: {CURRENT_GUI_SCHEMA_VERSION})."
        )

    while version < CURRENT_GUI_SCHEMA_VERSION:
        migrate = _PROJECT_MIGRATIONS.get(version)
        if migrate is None:
            raise ValueError(
                "No migration path is available for configuration schema version "
                f"{version}."
            )
        payload = migrate(payload)
        version = _coerce_schema_version(payload.get("schema_version"))

    payload["schema_version"] = CURRENT_GUI_SCHEMA_VERSION
    return payload


def _coerce_schema_version(value: Any) -> int:
    if value in (None, ""):
        return 0

    try:
        version = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid configuration schema version: {value!r}") from exc

    if version < 0:
        raise ValueError(f"Invalid configuration schema version: {version}")

    return version


def _migrate_v0_to_v1(payload: dict[str, Any]) -> dict[str, Any]:
    migrated = deepcopy(payload)

    legacy_name = migrated.pop("name", None)
    if not migrated.get("project_name") and isinstance(legacy_name, str):
        migrated["project_name"] = legacy_name

    legacy_history = migrated.pop("history", None)
    if not isinstance(migrated.get("run_history"), list) and isinstance(
        legacy_history, list
    ):
        migrated["run_history"] = deepcopy(legacy_history)

    workflow = migrated.get("workflow")
    if workflow is None:
        migrated["workflow"] = "new_database"

    config = migrated.get("config")
    if not isinstance(config, dict):
        config = {}

    export = config.get("export")
    if isinstance(export, str):
        config["export"] = {"type": export, "options": {}}
    elif isinstance(export, dict):
        normalized_export = deepcopy(export)
        if "type" not in normalized_export and "export_type" in normalized_export:
            normalized_export["type"] = normalized_export.pop("export_type")
        if "options" not in normalized_export and "export_options" in normalized_export:
            normalized_export["options"] = normalized_export.pop("export_options")
        config["export"] = normalized_export
    else:
        legacy_export_type = config.pop("export_type", None)
        legacy_export_options = config.pop("export_options", None)
        if legacy_export_type is not None or legacy_export_options is not None:
            config["export"] = {
                "type": legacy_export_type or "brightway",
                "options": legacy_export_options or {},
            }

    migrated["config"] = config

    scenario_sets = migrated.get("scenario_sets")
    legacy_scenarios = migrated.pop("scenarios", None)
    if not isinstance(scenario_sets, list) and isinstance(legacy_scenarios, list):
        scenario_sets = [{"name": "default", "scenarios": legacy_scenarios}]
    migrated["scenario_sets"] = _normalize_legacy_scenario_sets(scenario_sets or [])

    ui_state = migrated.get("ui_state")
    if not isinstance(ui_state, dict):
        ui_state = {}
    ui_state["explorer"] = _normalize_legacy_explorer_ui_state(ui_state.get("explorer"))
    migrated["ui_state"] = ui_state

    migrated["schema_version"] = 1
    return migrated


def _normalize_legacy_scenario_sets(
    scenario_sets: list[Any],
) -> list[dict[str, Any]]:
    normalized_sets: list[dict[str, Any]] = []

    for index, entry in enumerate(scenario_sets):
        if isinstance(entry, dict) and isinstance(entry.get("scenarios"), list):
            scenarios = _normalize_legacy_scenarios(entry.get("scenarios", []))
            name = str(entry.get("name", f"set-{index + 1}"))
        elif isinstance(entry, dict):
            scenario = _normalize_legacy_scenario(entry)
            if scenario is None:
                continue
            scenarios = [scenario]
            name = str(entry.get("name", f"set-{index + 1}"))
        else:
            continue

        normalized_sets.append(
            {
                "name": name,
                "scenarios": scenarios,
            }
        )

    return normalized_sets


def _normalize_legacy_scenarios(scenarios: list[Any]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for scenario in scenarios:
        item = _normalize_legacy_scenario(scenario)
        if item is not None:
            normalized.append(item)
    return normalized


def _normalize_legacy_scenario(scenario: Any) -> dict[str, Any] | None:
    if not isinstance(scenario, dict):
        return None

    normalized = deepcopy(scenario)
    if normalized.get("filepath") in (None, ""):
        normalized.pop("filepath", None)
    return normalized


def _normalize_legacy_explorer_ui_state(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}

    explorer = deepcopy(value)
    for key in (
        "selected_paths",
        "selected_groups",
        "selected_variables",
        "hidden_series",
    ):
        current = explorer.get(key)
        if isinstance(current, str):
            explorer[key] = [current] if current else []
        elif current is None:
            continue
        elif not isinstance(current, list):
            explorer.pop(key, None)

    for key in ("year_start", "year_end", "baseline_year"):
        current = explorer.get(key)
        if isinstance(current, int):
            explorer[key] = str(current)

    return explorer


_PROJECT_MIGRATIONS: dict[int, Callable[[dict[str, Any]], dict[str, Any]]] = {
    0: _migrate_v0_to_v1,
}
