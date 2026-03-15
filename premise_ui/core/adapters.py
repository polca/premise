"""Workflow adapters for executing Premise from the UI worker."""

from __future__ import annotations

import importlib
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from premise_ui.core.credentials import iam_key_value
from premise_ui.core.manifests import RunManifest

SUPPORTED_EXPORT_TYPES = {
    "brightway",
    "matrices",
    "datapackage",
    "simapro",
    "openlca",
    "superstructure",
}

EXPORT_OPTION_KEYS = {
    "brightway": {"name"},
    "matrices": {"filepath"},
    "datapackage": {"name"},
    "simapro": {"filepath"},
    "openlca": {"filepath"},
    "superstructure": {"name", "filepath", "file_format", "preserve_original_column"},
}

INCREMENTAL_EXPORT_TYPES = {"brightway", "matrices", "simapro", "openlca"}
INCREMENTAL_EXPORT_OPTION_KEYS = {
    "brightway": {"name", "filepath", "file_format"},
    "matrices": {"filepath"},
    "simapro": {"filepath"},
    "openlca": {"filepath"},
}
PATHWAYS_EXPORT_TYPES = {"datapackage"}
PATHWAYS_EXPORT_OPTION_KEYS = {"datapackage": {"name"}}

IMPLEMENTED_WORKFLOWS = {
    "new_database",
    "incremental_database",
    "pathways_datapackage",
}


def _normalize_brightway_project_name(project_name: Any) -> str | None:
    if project_name in (None, ""):
        return None

    value = str(project_name)
    if value.startswith("Project: "):
        return value.split("Project: ", 1)[1]
    return value


class WorkflowValidationError(RuntimeError):
    """Raised when a run manifest cannot be executed safely."""

    def __init__(self, errors: list[str], warnings: list[str] | None = None) -> None:
        super().__init__("Run manifest validation failed.")
        self.errors = errors
        self.warnings = warnings or []


@dataclass(slots=True)
class AdapterValidationResult:
    errors: list[str]
    warnings: list[str]

    @property
    def valid(self) -> bool:
        return not self.errors


class WorkflowAdapter:
    """Base adapter for a GUI workflow."""

    workflow_name = ""

    def __init__(self, manifest: RunManifest, writer: Any | None = None) -> None:
        self.manifest = manifest
        self.writer = writer

    def validate(self) -> AdapterValidationResult:
        raise NotImplementedError

    def execute(self) -> dict[str, Any]:
        raise NotImplementedError

    def _emit(
        self,
        event_type: str,
        *,
        phase: str | None = None,
        level: str = "info",
        message: str = "",
        details: dict[str, Any] | None = None,
    ) -> None:
        if self.writer is None:
            return

        self.writer.emit(
            event_type,
            phase=phase,
            level=level,
            message=message,
            details=details,
        )

    def _resolved_iam_key(self) -> Any | None:
        key = self.manifest.config.get("key")
        if key not in (None, ""):
            return key
        return iam_key_value()

    @staticmethod
    def _dedupe_paths(paths: list[Path]) -> list[Path]:
        unique: list[Path] = []
        seen: set[str] = set()
        for path in paths:
            value = str(path.resolve())
            if value in seen:
                continue
            seen.add(value)
            unique.append(path.resolve())
        return unique

    @staticmethod
    def _format_locations(paths: list[Path]) -> str:
        resolved = [str(path.resolve()) for path in paths]
        if not resolved:
            return ""
        if len(resolved) == 1:
            return resolved[0]
        return "; ".join(resolved)


class NewDatabaseAdapter(WorkflowAdapter):
    """Adapter for the `premise.NewDatabase` workflow."""

    workflow_name = "new_database"

    def supported_export_types(self) -> set[str]:
        return SUPPORTED_EXPORT_TYPES

    def export_option_keys(self) -> dict[str, set[str]]:
        return EXPORT_OPTION_KEYS

    def biosphere_required_export_types(self) -> set[str]:
        return {"brightway", "superstructure"}

    def minimum_scenarios_for_export(self, export_type: str) -> int | None:
        if export_type == "superstructure":
            return 2
        return None

    def validate_workflow_config(
        self,
        *,
        config: dict[str, Any],
        export_type: str | None,
        export_options: dict[str, Any],
        errors: list[str],
        warnings: list[str],
    ) -> None:
        transformations = config.get("transformations")
        if transformations is not None and not isinstance(transformations, (list, str)):
            errors.append("`config.transformations` must be a string, a list, or null.")

    def transform_selection(self) -> Any:
        return self.manifest.config.get("transformations")

    @classmethod
    def _load_workflow_class(cls) -> Any:
        return cls._load_new_database_class()

    def validate(self) -> AdapterValidationResult:
        errors: list[str] = []
        warnings: list[str] = []

        config = self.manifest.config
        scenarios = self.manifest.scenarios
        source_type = str(config.get("source_type", "brightway"))
        source_version = config.get("source_version", "3.12")
        source_project = _normalize_brightway_project_name(config.get("source_project"))
        export = config.get("export", {})
        export_type = export.get("type")
        export_options = dict(export.get("options", {}))

        if not scenarios:
            errors.append("At least one scenario is required for `new_database`.")

        if source_type not in {"brightway", "ecospold"}:
            errors.append("Source Type must be either brightway or ecospold.")

        if source_type == "brightway" and not config.get("source_db"):
            errors.append(
                "Brightway Source Database is required when Source Type is set to Brightway."
            )

        if source_type == "ecospold" and not config.get("source_file_path"):
            errors.append(
                "Ecospold Directory is required when Source Type is set to Ecospold."
            )

        if source_project not in (None, "") and not isinstance(source_project, str):
            errors.append("`source_project` must be a string when provided.")

        if not export_type:
            errors.append("`config.export.type` is required.")
        elif export_type not in self.supported_export_types():
            errors.append(
                f"`config.export.type` must be one of {sorted(self.supported_export_types())}."
            )

        minimum_scenarios = self.minimum_scenarios_for_export(str(export_type or ""))
        if minimum_scenarios and len(scenarios) < minimum_scenarios:
            errors.append(
                f"Export `{export_type}` requires at least {minimum_scenarios} configured scenarios."
            )

        export_option_keys = self.export_option_keys()
        if export_type in export_option_keys:
            unexpected = sorted(set(export_options) - export_option_keys[export_type])
            if unexpected:
                warnings.append(
                    f"Ignoring unsupported export options for `{export_type}`: "
                    f"{', '.join(unexpected)}."
                )

        if export_type in {"brightway", "datapackage", "superstructure"}:
            name = export_options.get("name")
            if name not in (None, "") and not isinstance(name, str):
                errors.append(
                    f"`config.export.options.name` must be a string for `{export_type}`."
                )

        if export_type in {"matrices", "simapro", "openlca", "superstructure"}:
            filepath = export_options.get("filepath")
            if filepath not in (None, "") and not isinstance(filepath, str):
                errors.append(
                    f"`config.export.options.filepath` must be a string for `{export_type}`."
                )

        if export_type == "superstructure":
            file_format = export_options.get("file_format")
            if file_format not in (None, "", "excel", "csv", "feather"):
                errors.append(
                    "`config.export.options.file_format` must be one of "
                    "`excel`, `csv`, or `feather`."
                )
            preserve_original = export_options.get("preserve_original_column")
            if preserve_original is not None and not isinstance(preserve_original, bool):
                errors.append(
                    "`config.export.options.preserve_original_column` must be a boolean."
                )

        self.validate_workflow_config(
            config=config,
            export_type=export_type,
            export_options=export_options,
            errors=errors,
            warnings=warnings,
        )

        for index, scenario in enumerate(scenarios):
            missing = [field for field in ("model", "pathway", "year") if field not in scenario]
            if missing:
                errors.append(
                    f"Scenario {index} is missing required fields: {', '.join(missing)}."
                )

        try:
            module = self._load_new_database_module()
        except ImportError as exc:
            errors.append(
                "Premise runtime dependencies are not fully available in this "
                f"environment: {exc}."
            )
            return AdapterValidationResult(errors, warnings)

        try:
            module.check_db_version(source_version)
        except Exception as exc:
            errors.append(str(exc))

        try:
            module.check_system_model(config.get("system_model", "cutoff"))
        except Exception as exc:
            errors.append(str(exc))

        if source_type == "ecospold" and config.get("source_file_path"):
            try:
                module.check_ei_filepath(
                    self._resolve_path_value(str(config["source_file_path"]))
                )
            except Exception as exc:
                errors.append(str(exc))

        if config.get("additional_inventories"):
            try:
                module.check_additional_inventories(
                    self._resolved_additional_inventories()
                )
            except Exception as exc:
                errors.append(str(exc))

        for scenario in scenarios:
            try:
                module.check_scenarios(
                    self._resolved_scenario(scenario),
                    key=self._resolved_iam_key(),
                )
            except Exception as exc:
                errors.append(str(exc))

        if source_type == "brightway":
            try:
                with self._using_brightway_project() as bw2data:
                    projects = [
                        _normalize_brightway_project_name(project)
                        for project in bw2data.projects
                    ]
                    if source_project and source_project not in projects:
                        errors.append(f"Brightway project not found: {source_project}")

                    source_db = str(config.get("source_db", ""))
                    databases = {str(database) for database in getattr(bw2data, "databases", [])}
                    if source_db and source_db not in databases:
                        current_project = getattr(bw2data.projects, "current", None)
                        errors.append(
                            "Brightway database "
                            f"`{source_db}` was not found in project "
                            f"`{str(current_project) if current_project else 'current'}`."
                        )

                    if export_type in self.biosphere_required_export_types():
                        biosphere_name = str(config.get("biosphere_name", "biosphere3"))
                        try:
                            module.check_presence_biosphere_database(biosphere_name)
                        except Exception as exc:
                            errors.append(
                                f"{exc} Adjust Brightway Biosphere Database in the Source tab if your "
                                "Brightway project uses another biosphere database name."
                            )
            except Exception as exc:
                errors.append(str(exc))

        return AdapterValidationResult(errors, warnings)

    def execute(self) -> dict[str, Any]:
        validation = self.validate()
        if not validation.valid:
            raise WorkflowValidationError(validation.errors, validation.warnings)

        workflow_class = self._load_workflow_class()
        kwargs = self._build_init_kwargs()
        export_type, export_options = self._build_export_call()
        transformations = self.transform_selection()

        with self._using_brightway_project():
            self._emit(
                "phase_started",
                phase="instantiate",
                message="Instantiating `premise.NewDatabase`.",
            )
            workflow = workflow_class(**kwargs)
            self._emit(
                "phase_completed",
                phase="instantiate",
                message="`premise.NewDatabase` initialized.",
            )

            self._emit(
                "phase_started",
                phase="transform",
                message="Applying requested Premise transformations.",
            )
            workflow.update(transformations)
            self._emit(
                "phase_completed",
                phase="transform",
                message="Premise transformations completed.",
                details={"transformations": transformations or "all"},
            )

            self._emit(
                "phase_started",
                phase="export",
                message=f"Exporting transformed database to `{export_type}`.",
            )
            self._dispatch_export(workflow, export_type, export_options)
            self._emit(
                "phase_completed",
                phase="export",
                message=f"Export `{export_type}` completed.",
                details={"export_options": export_options},
            )

        return {
            "workflow": self.workflow_name,
            "export_type": export_type,
            "transformations": transformations or "all",
            "output_location": self._output_location_summary(export_type, export_options),
        }

    def _build_init_kwargs(self) -> dict[str, Any]:
        config = self.manifest.config
        kwargs = {
            "scenarios": [self._resolved_scenario(s) for s in self.manifest.scenarios],
            "source_version": str(config.get("source_version", "3.12")),
            "source_type": str(config.get("source_type", "brightway")),
            "source_db": config.get("source_db"),
            "source_file_path": self._resolve_optional_path(
                config.get("source_file_path")
            ),
            "additional_inventories": self._resolved_additional_inventories(),
            "system_model": str(config.get("system_model", "cutoff")),
            "system_args": dict(config.get("system_args", {})),
            "use_cached_inventories": bool(config.get("use_cached_inventories", True)),
            "use_cached_database": bool(config.get("use_cached_database", True)),
            "quiet": bool(config.get("quiet", False)),
            "keep_imports_uncertainty": bool(
                config.get("keep_imports_uncertainty", True)
            ),
            "keep_source_db_uncertainty": bool(
                config.get("keep_source_db_uncertainty", False)
            ),
            "gains_scenario": str(config.get("gains_scenario", "CLE")),
            "use_absolute_efficiency": bool(
                config.get("use_absolute_efficiency", False)
            ),
            "biosphere_name": str(config.get("biosphere_name", "biosphere3")),
            "generate_reports": bool(config.get("generate_reports", True)),
        }

        key = self._resolved_iam_key()
        if key is not None:
            kwargs["key"] = key

        return kwargs

    def _build_export_call(self) -> tuple[str, dict[str, Any]]:
        export = dict(self.manifest.config.get("export", {}))
        export_type = str(export.get("type", ""))
        export_options = {
            key: value
            for key, value in dict(export.get("options", {})).items()
            if key in self.export_option_keys().get(export_type, set())
        }

        if "filepath" in export_options and export_options["filepath"] not in (None, ""):
            export_options["filepath"] = self._resolve_path_value(
                str(export_options["filepath"])
            )
        elif "filepath" in export_options:
            export_options.pop("filepath")

        if "name" in export_options and export_options["name"] in (None, ""):
            export_options.pop("name")

        return export_type, export_options

    def _dispatch_export(
        self, workflow: Any, export_type: str, export_options: dict[str, Any]
    ) -> None:
        if export_type == "brightway":
            workflow.write_db_to_brightway(**export_options)
            return
        if export_type == "matrices":
            workflow.write_db_to_matrices(**export_options)
            return
        if export_type == "datapackage":
            workflow.write_datapackage(**export_options)
            return
        if export_type == "simapro":
            workflow.write_db_to_simapro(**export_options)
            return
        if export_type == "openlca":
            workflow.write_db_to_olca(**export_options)
            return
        if export_type == "superstructure":
            workflow.write_superstructure_db_to_brightway(**export_options)
            return

        raise WorkflowValidationError(
            [f"Unsupported export type: {export_type}."],
            [],
        )

    def _output_location_summary(
        self, export_type: str, export_options: dict[str, Any]
    ) -> str | None:
        output_paths = self._dedupe_paths(
            self._primary_output_paths(export_type, export_options)
        )
        report_paths = self._dedupe_paths(self._report_output_paths())

        if export_type == "brightway":
            project_name = (
                _normalize_brightway_project_name(self.manifest.config.get("source_project"))
                or "current Brightway project"
            )
            database_name = export_options.get("name")
            message = None
            if isinstance(database_name, str) and database_name:
                message = (
                    f'Brightway export target: project "{project_name}" / '
                    f'database "{database_name}".'
                )
            else:
                message = (
                    f'Brightway export target: project "{project_name}" / '
                    "Premise-generated database name(s)."
                )
            if output_paths:
                message += f" Files saved under {self._format_locations(output_paths)}."
            if report_paths:
                message += f" Reports saved under {self._format_locations(report_paths)}."
            return message

        if export_type == "matrices":
            if output_paths:
                message = f"Matrices saved under {self._format_locations(output_paths)}."
                if report_paths:
                    message += f" Reports saved under {self._format_locations(report_paths)}."
                return message
            filepath = export_options.get("filepath")
            if filepath:
                return f"Matrices configured to save under {filepath}."
            if report_paths:
                return f"Reports saved under {self._format_locations(report_paths)}."
            return None

        if export_type == "simapro":
            if output_paths:
                message = f"SimaPro files saved under {self._format_locations(output_paths)}."
                if report_paths:
                    message += f" Reports saved under {self._format_locations(report_paths)}."
                return message
            filepath = export_options.get("filepath")
            if filepath:
                return f"SimaPro files configured to save under {filepath}."
            if report_paths:
                return f"Reports saved under {self._format_locations(report_paths)}."
            return None

        if export_type == "openlca":
            if output_paths:
                message = f"OpenLCA files saved under {self._format_locations(output_paths)}."
                if report_paths:
                    message += f" Reports saved under {self._format_locations(report_paths)}."
                return message
            filepath = export_options.get("filepath")
            if filepath:
                return f"OpenLCA files configured to save under {filepath}."
            if report_paths:
                return f"Reports saved under {self._format_locations(report_paths)}."
            return None

        if export_type == "superstructure":
            if output_paths:
                message = (
                    "Superstructure comparison files saved under "
                    f"{self._format_locations(output_paths)}."
                )
                if report_paths:
                    message += f" Reports saved under {self._format_locations(report_paths)}."
                return message
            filepath = export_options.get("filepath")
            if filepath:
                return f"Superstructure comparison files configured to save under {filepath}."
            if report_paths:
                return f"Reports saved under {self._format_locations(report_paths)}."
            return None

        if export_type == "datapackage":
            if output_paths:
                message = f"Datapackage saved under {self._format_locations(output_paths)}."
                if report_paths:
                    message += f" Reports saved under {self._format_locations(report_paths)}."
                return message
            if report_paths:
                return f"Reports saved under {self._format_locations(report_paths)}."
            return None

        return None

    def _primary_output_paths(
        self, export_type: str, export_options: dict[str, Any]
    ) -> list[Path]:
        export_dir = Path.cwd() / "export"
        explicit_filepath = export_options.get("filepath")

        if export_type == "brightway" and explicit_filepath:
            target = Path(str(explicit_filepath))
            return [target] if target.exists() else []

        if export_type == "matrices":
            if explicit_filepath:
                target = Path(str(explicit_filepath))
                return [target] if target.exists() else []

            scenario_dirs = [
                export_dir / str(scenario["model"]) / str(scenario["pathway"]) / str(scenario["year"])
                for scenario in self.manifest.scenarios
                if all(field in scenario for field in ("model", "pathway", "year"))
            ]
            existing_scenario_dirs = [path for path in scenario_dirs if path.exists()]
            if existing_scenario_dirs:
                return existing_scenario_dirs
            return [export_dir] if export_dir.exists() else []

        if export_type == "simapro":
            if explicit_filepath:
                target = Path(str(explicit_filepath))
                return [target] if target.exists() else []
            default_dir = export_dir / "simapro"
            return [default_dir] if default_dir.exists() else []

        if export_type == "openlca":
            if explicit_filepath:
                target = Path(str(explicit_filepath))
                return [target] if target.exists() else []
            default_dir = export_dir / "olca"
            return [default_dir] if default_dir.exists() else []

        if export_type == "superstructure":
            paths: list[Path] = []
            if explicit_filepath:
                target = Path(str(explicit_filepath))
                if target.exists():
                    paths.append(target)
            default_dir = export_dir / "scenario diff files"
            if default_dir.exists():
                paths.append(default_dir)
            return paths

        if export_type == "datapackage":
            datapackage_dir = export_dir / "datapackage"
            if datapackage_dir.exists():
                zip_name = export_options.get("name")
                if isinstance(zip_name, str) and zip_name:
                    archive = datapackage_dir / f"{zip_name}.zip"
                    if archive.exists():
                        return [archive]
                archives = sorted(datapackage_dir.glob("*.zip"))
                if archives:
                    return archives
                return [datapackage_dir]

        return []

    def _report_output_paths(self) -> list[Path]:
        report_dirs = [
            Path.cwd() / "export" / "scenario_report",
            Path.cwd() / "export" / "change reports",
        ]
        return [path for path in report_dirs if path.exists()]

    def _resolved_scenario(self, scenario: dict[str, Any]) -> dict[str, Any]:
        resolved = dict(scenario)
        if resolved.get("filepath") in (None, ""):
            resolved.pop("filepath", None)
        elif "filepath" in resolved:
            resolved["filepath"] = self._resolve_path_value(str(resolved["filepath"]))
        return resolved

    def _resolved_additional_inventories(self) -> list[dict[str, Any]] | None:
        inventories = self.manifest.config.get("additional_inventories")
        if not inventories:
            return None

        resolved: list[dict[str, Any]] = []
        for inventory in inventories:
            item = dict(inventory)
            if "filepath" in item and item["filepath"] is not None:
                item["filepath"] = self._resolve_path_value(str(item["filepath"]))
            resolved.append(item)
        return resolved

    def _project_base_dir(self) -> Path:
        if self.manifest.project_path:
            return Path(self.manifest.project_path).expanduser().resolve().parent

        if self.manifest.working_directory:
            return Path(self.manifest.working_directory).expanduser().resolve()

        return Path.cwd()

    def _resolve_optional_path(self, value: Any) -> str | None:
        if value in (None, ""):
            return None
        return self._resolve_path_value(str(value))

    def _resolve_path_value(self, value: str) -> str:
        path = Path(value).expanduser()
        if path.is_absolute():
            return str(path.resolve())
        return str((self._project_base_dir() / path).resolve())

    @contextmanager
    def _using_brightway_project(self):
        config = self.manifest.config
        if str(config.get("source_type", "brightway")) != "brightway":
            yield None
            return

        try:
            import bw2data
        except ImportError as exc:
            raise RuntimeError(
                "Brightway runtime dependencies are unavailable in this environment."
            ) from exc

        selected_project = _normalize_brightway_project_name(config.get("source_project"))
        previous_project = getattr(bw2data.projects, "current", None)
        previous_name = _normalize_brightway_project_name(previous_project)

        if selected_project not in (None, ""):
            try:
                bw2data.projects.set_current(selected_project)
            except Exception as exc:
                raise RuntimeError(
                    f"Failed to switch Brightway project to `{selected_project}`: {exc}"
                ) from exc

        try:
            yield bw2data
        finally:
            if (
                previous_name
                and selected_project not in (None, "")
                and previous_name != selected_project
            ):
                try:
                    bw2data.projects.set_current(previous_name)
                except Exception:
                    pass

    @staticmethod
    def _load_new_database_module() -> Any:
        return importlib.import_module("premise.new_database")

    @classmethod
    def _load_new_database_class(cls) -> Any:
        return cls._load_new_database_module().NewDatabase


class IncrementalDatabaseAdapter(NewDatabaseAdapter):
    """Adapter for the `premise.IncrementalDatabase` workflow."""

    workflow_name = "incremental_database"

    def supported_export_types(self) -> set[str]:
        return INCREMENTAL_EXPORT_TYPES

    def export_option_keys(self) -> dict[str, set[str]]:
        return INCREMENTAL_EXPORT_OPTION_KEYS

    def biosphere_required_export_types(self) -> set[str]:
        return {"brightway"}

    def validate_workflow_config(
        self,
        *,
        config: dict[str, Any],
        export_type: str | None,
        export_options: dict[str, Any],
        errors: list[str],
        warnings: list[str],
    ) -> None:
        sectors = config.get("sectors")
        if sectors not in (None, []):
            if not isinstance(sectors, list) or not all(isinstance(item, str) for item in sectors):
                errors.append("`config.sectors` must be a list of incremental sector identifiers.")
            else:
                try:
                    supported = set(self._load_incremental_module().SECTORS)
                except ImportError as exc:
                    errors.append(
                        "Premise incremental workflow dependencies are unavailable in this "
                        f"environment: {exc}."
                    )
                else:
                    unknown = sorted(set(sectors) - supported)
                    if unknown:
                        errors.append(
                            "Unknown incremental sectors: " + ", ".join(unknown) + "."
                        )

        if export_type == "brightway":
            filepath = export_options.get("filepath")
            if filepath not in (None, "") and not isinstance(filepath, str):
                errors.append(
                    "`config.export.options.filepath` must be a string for incremental Brightway export."
                )
            file_format = export_options.get("file_format")
            if file_format not in (None, "", "excel", "csv", "feather"):
                errors.append(
                    "`config.export.options.file_format` must be one of `excel`, `csv`, or `feather`."
                )

    def transform_selection(self) -> Any:
        sectors = self.manifest.config.get("sectors")
        if not sectors:
            return None

        sector_map = self._load_incremental_module().SECTORS
        return {sector_id: sector_map[sector_id] for sector_id in sectors}

    @classmethod
    def _load_workflow_class(cls) -> Any:
        return cls._load_incremental_class()

    def _dispatch_export(
        self, workflow: Any, export_type: str, export_options: dict[str, Any]
    ) -> None:
        if export_type == "brightway":
            workflow.write_increment_db_to_brightway(**export_options)
            return
        super()._dispatch_export(workflow, export_type, export_options)

    def _output_location_summary(
        self, export_type: str, export_options: dict[str, Any]
    ) -> str | None:
        if export_type == "brightway":
            project_name = (
                _normalize_brightway_project_name(self.manifest.config.get("source_project"))
                or "current Brightway project"
            )
            database_name = export_options.get("name") or "Premise-generated database name"
            message = (
                f'Incremental Brightway export target: project "{project_name}" / '
                f'database "{database_name}".'
            )
            output_paths = self._dedupe_paths(self._primary_output_paths(export_type, export_options))
            report_paths = self._dedupe_paths(self._report_output_paths())
            if output_paths:
                message += f" Files saved under {self._format_locations(output_paths)}."
            elif export_options.get("filepath"):
                message += f" Comparison files configured to save under {export_options['filepath']}."
            if report_paths:
                message += f" Reports saved under {self._format_locations(report_paths)}."
            return message
        return super()._output_location_summary(export_type, export_options)

    @staticmethod
    def _load_incremental_module() -> Any:
        return importlib.import_module("premise.incremental")

    @classmethod
    def _load_incremental_class(cls) -> Any:
        return cls._load_incremental_module().IncrementalDatabase


class PathwaysDataPackageAdapter(NewDatabaseAdapter):
    """Adapter for the `premise.PathwaysDataPackage` workflow."""

    workflow_name = "pathways_datapackage"

    def supported_export_types(self) -> set[str]:
        return PATHWAYS_EXPORT_TYPES

    def export_option_keys(self) -> dict[str, set[str]]:
        return PATHWAYS_EXPORT_OPTION_KEYS

    def biosphere_required_export_types(self) -> set[str]:
        return set()

    def validate_workflow_config(
        self,
        *,
        config: dict[str, Any],
        export_type: str | None,
        export_options: dict[str, Any],
        errors: list[str],
        warnings: list[str],
    ) -> None:
        years = config.get("years")
        if not isinstance(years, list) or not years:
            errors.append("`config.years` must be a non-empty list of years.")
        elif not all(isinstance(year, int) for year in years):
            errors.append("`config.years` must contain integers only.")

        contributors = config.get("contributors", [])
        if contributors not in (None, []):
            if not isinstance(contributors, list):
                errors.append("`config.contributors` must be a list of contributor objects.")
            else:
                for index, contributor in enumerate(contributors):
                    if not isinstance(contributor, dict):
                        errors.append(f"Contributor {index + 1} must be an object.")
                        continue
                    for field in ("name", "email"):
                        value = contributor.get(field)
                        if value not in (None, "") and not isinstance(value, str):
                            errors.append(
                                f"Contributor {index + 1} field `{field}` must be a string."
                            )

        name = export_options.get("name")
        if name not in (None, "") and not isinstance(name, str):
            errors.append("`config.export.options.name` must be a string for datapackage export.")

    def execute(self) -> dict[str, Any]:
        validation = self.validate()
        if not validation.valid:
            raise WorkflowValidationError(validation.errors, validation.warnings)

        workflow_class = self._load_workflow_class()
        kwargs = self._build_init_kwargs()
        export_type, export_options = self._build_export_call()
        transformations = self.transform_selection()
        config = self.manifest.config
        contributors = config.get("contributors") or None

        with self._using_brightway_project():
            self._emit(
                "phase_started",
                phase="instantiate",
                message="Instantiating `premise.PathwaysDataPackage`.",
            )
            workflow = workflow_class(**kwargs)
            self._emit(
                "phase_completed",
                phase="instantiate",
                message="`premise.PathwaysDataPackage` initialized.",
            )

            self._emit(
                "phase_started",
                phase="build",
                message="Building the pathways datapackage.",
            )
            workflow.create_datapackage(
                name=export_options.get("name") or f"pathways_{self.manifest.run_id}",
                contributors=contributors,
                transformations=transformations,
            )
            self._emit(
                "phase_completed",
                phase="build",
                message="Pathways datapackage created.",
                details={
                    "years": config.get("years", []),
                    "contributors": len(contributors or []),
                },
            )

        return {
            "workflow": self.workflow_name,
            "export_type": export_type,
            "transformations": transformations or "all",
            "years": config.get("years", []),
            "output_location": self._output_location_summary(export_type, export_options),
        }

    def _build_init_kwargs(self) -> dict[str, Any]:
        config = self.manifest.config
        kwargs = {
            "scenarios": [self._resolved_scenario(s) for s in self.manifest.scenarios],
            "years": list(config.get("years", [])),
            "source_version": str(config.get("source_version", "3.12")),
            "source_type": str(config.get("source_type", "brightway")),
            "source_db": config.get("source_db"),
            "source_file_path": self._resolve_optional_path(
                config.get("source_file_path")
            ),
            "additional_inventories": self._resolved_additional_inventories(),
            "system_model": str(config.get("system_model", "cutoff")),
            "system_args": dict(config.get("system_args", {})),
            "gains_scenario": str(config.get("gains_scenario", "CLE")),
            "use_absolute_efficiency": bool(
                config.get("use_absolute_efficiency", False)
            ),
            "biosphere_name": str(config.get("biosphere_name", "biosphere3")),
            "generate_reports": bool(config.get("generate_reports", True)),
        }
        key = self._resolved_iam_key()
        if key is not None:
            kwargs["key"] = key
        return kwargs

    def _output_location_summary(
        self, export_type: str, export_options: dict[str, Any]
    ) -> str | None:
        if export_type != "datapackage":
            return super()._output_location_summary(export_type, export_options)

        name = export_options.get("name") or f"pathways_{self.manifest.run_id}"
        archive = (Path.cwd() / f"{name}.zip").resolve()
        if archive.exists():
            return f"Datapackage saved under {archive}."
        report_paths = self._dedupe_paths(self._report_output_paths())
        if report_paths:
            return f"Reports saved under {self._format_locations(report_paths)}."
        return None

    @staticmethod
    def _load_workflow_module() -> Any:
        return importlib.import_module("premise.pathways")

    @classmethod
    def _load_workflow_class(cls) -> Any:
        return cls._load_workflow_module().PathwaysDataPackage


def get_workflow_adapter(
    manifest: RunManifest, writer: Any | None = None
) -> WorkflowAdapter:
    if manifest.workflow == "new_database":
        return NewDatabaseAdapter(manifest, writer=writer)
    if manifest.workflow == "incremental_database":
        return IncrementalDatabaseAdapter(manifest, writer=writer)
    if manifest.workflow == "pathways_datapackage":
        return PathwaysDataPackageAdapter(manifest, writer=writer)

    if manifest.workflow not in IMPLEMENTED_WORKFLOWS:
        raise WorkflowValidationError(
            [f"Workflow `{manifest.workflow}` is not implemented yet."],
            [],
        )

    raise WorkflowValidationError(
        [f"Workflow `{manifest.workflow}` is not available."],
        [],
    )
