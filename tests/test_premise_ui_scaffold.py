import json
import sys
from pathlib import Path
from types import SimpleNamespace
from zipfile import ZipFile

import pytest

from premise_ui.core.adapters import (
    IncrementalDatabaseAdapter,
    NewDatabaseAdapter,
    PathwaysDataPackageAdapter,
)
from premise_ui.core.history import remember_project_run, sync_project_run_history
from premise_ui.core.manifests import (
    GuiProjectManifest,
    RunManifest,
    build_run_manifest_from_project,
    utc_now_iso,
)
from premise_ui.core.project_migrations import CURRENT_GUI_SCHEMA_VERSION
from premise_ui.core.recents import (
    clear_recent_state,
    load_recent_state,
    remember_recent_path,
    remember_recent_project,
)
from premise_ui.core.storage import clone_project, load_project, read_json, save_project
from premise_ui.core.validation import validate_run_manifest_payload
from premise_ui.worker.events import read_events
from premise_ui.worker.runner import run_manifest


class DummyProcess:
    def __init__(self):
        self._returncode = None
        self.terminated = False

    def poll(self):
        return self._returncode

    def terminate(self):
        self.terminated = True
        self._returncode = -15


def _enqueue_project_payload(project_path: Path, project_name: str) -> dict:
    return {
        "path": str(project_path),
        "dry_run": True,
        "project": {
            "project_name": project_name,
            "workflow": "new_database",
            "config": {
                "source_db": "ecoinvent-3.12-cutoff",
                "export": {"type": "matrices"},
            },
            "scenario_sets": [
                {
                    "name": "default",
                    "scenarios": [
                        {"model": "remind", "pathway": "SSP2-Base", "year": 2030}
                    ],
                }
            ],
        },
    }


def _fake_app_state():
    return SimpleNamespace(
        processes={},
        run_dirs={},
        job_queue=[],
        active_run_id=None,
    )


def _fake_bw2data(log=None):
    class FakeProject:
        def __init__(self, name):
            self.name = name

        def __str__(self):
            return f"Project: {self.name}"

    class FakeProjects:
        def __init__(self):
            self.current = "project-a"
            self._projects = [FakeProject("project-a"), FakeProject("project-b")]

        def __iter__(self):
            return iter(self._projects)

        def set_current(self, name):
            self.current = name
            if log is not None:
                log.append(("project", name))

    class FakeBw2data:
        def __init__(self):
            self.projects = FakeProjects()
            self._databases = {
                "project-a": {
                    "ecoinvent-3.12-cutoff": object(),
                    "biosphere3": object(),
                },
                "project-b": {
                    "ecoinvent-3.12-cutoff": object(),
                    "biosphere3": object(),
                },
            }

        @property
        def databases(self):
            return self._databases[self.projects.current]

    return FakeBw2data()


def test_project_manifest_roundtrip(tmp_path):
    project = GuiProjectManifest.template()
    project.project_name = "UI scaffold"
    project.ui_state = {
        "explorer": {
            "selected_sector": "Electricity - generation",
            "selected_groups": ["World"],
            "chart_mode": "bar",
        }
    }

    project_path = tmp_path / "project.json"
    save_project(project_path, project)

    loaded = load_project(project_path)
    assert loaded.project_name == "UI scaffold"
    assert loaded.workflow == "new_database"
    assert loaded.ui_state["explorer"]["chart_mode"] == "bar"


def test_project_manifest_migrates_legacy_payload():
    project = GuiProjectManifest.from_dict(
        {
            "schema_version": 0,
            "name": "Legacy config",
            "workflow": "new_database",
            "config": {
                "source_db": "ecoinvent-3.12-cutoff",
                "export_type": "matrices",
                "export_options": {"filepath": "legacy-export"},
                "years": [2030, 2040],
            },
            "scenarios": [
                {
                    "model": "remind",
                    "pathway": "SSP2-Base",
                    "year": 2030,
                    "filepath": "",
                }
            ],
            "history": [{"run_id": "legacy-run"}],
            "ui_state": {
                "explorer": {
                    "selected_paths": "/tmp/remind_SSP2-Base.csv",
                    "selected_groups": "World",
                    "selected_variables": "Electricity",
                    "hidden_series": "REMIND / SSP2-Base / World",
                    "baseline_year": 2030,
                }
            },
        }
    )

    assert project.schema_version == CURRENT_GUI_SCHEMA_VERSION
    assert project.project_name == "Legacy config"
    assert project.config["export"] == {
        "type": "matrices",
        "options": {"filepath": "legacy-export"},
    }
    assert "years" not in project.config
    assert project.scenario_sets == [
        {
            "name": "default",
            "scenarios": [{"model": "remind", "pathway": "SSP2-Base", "year": 2030}],
        }
    ]
    assert project.run_history == [{"run_id": "legacy-run"}]
    assert project.ui_state["explorer"]["selected_paths"] == [
        "/tmp/remind_SSP2-Base.csv"
    ]
    assert project.ui_state["explorer"]["selected_groups"] == ["World"]
    assert project.ui_state["explorer"]["selected_variables"] == ["Electricity"]
    assert project.ui_state["explorer"]["hidden_series"] == [
        "REMIND / SSP2-Base / World"
    ]
    assert project.ui_state["explorer"]["baseline_year"] == "2030"


def test_project_manifest_rejects_future_schema_version():
    with pytest.raises(ValueError, match="newer than this Premise UI release"):
        GuiProjectManifest.from_dict(
            {
                "schema_version": CURRENT_GUI_SCHEMA_VERSION + 1,
                "project_name": "Future",
            }
        )


def test_load_and_resave_project_upgrades_legacy_schema(tmp_path):
    project_path = tmp_path / "legacy.json"
    legacy_payload = {
        "schema_version": 0,
        "name": "Saved legacy",
        "workflow": "new_database",
        "config": {
            "source_db": "ecoinvent-3.12-cutoff",
            "export_type": "matrices",
            "export_options": {"filepath": "legacy-export"},
        },
        "scenario_sets": [
            {
                "name": "default",
                "scenarios": [
                    {
                        "model": "remind",
                        "pathway": "SSP2-Base",
                        "year": 2030,
                        "filepath": "",
                    }
                ],
            }
        ],
    }
    project_path.write_text(json.dumps(legacy_payload, indent=2), encoding="utf-8")

    project = load_project(project_path)
    save_project(project_path, project)
    reloaded_payload = read_json(project_path)

    assert project.schema_version == CURRENT_GUI_SCHEMA_VERSION
    assert reloaded_payload["schema_version"] == CURRENT_GUI_SCHEMA_VERSION
    assert reloaded_payload["project_name"] == "Saved legacy"
    assert reloaded_payload["config"]["export"] == {
        "type": "matrices",
        "options": {"filepath": "legacy-export"},
    }
    assert "filepath" not in reloaded_payload["scenario_sets"][0]["scenarios"][0]


def test_frontend_source_scaffold_exists():
    package_json = json.loads(
        Path("premise_ui/frontend/package.json").read_text(encoding="utf-8")
    )
    api_source = Path("premise_ui/frontend/src/api.ts").read_text(encoding="utf-8")
    app_source = Path("premise_ui/frontend/src/App.tsx").read_text(encoding="utf-8")
    benchmark_source = Path("premise_ui/frontend/src/benchmarkExplorer.ts").read_text(
        encoding="utf-8"
    )
    benchmark_baseline_source = Path(
        "premise_ui/frontend/src/explorerBenchmarkBaseline.ts"
    ).read_text(encoding="utf-8")
    explorer_data_source = Path("premise_ui/frontend/src/explorerData.ts").read_text(
        encoding="utf-8"
    )
    help_source = Path("premise_ui/frontend/src/helpContent.ts").read_text(
        encoding="utf-8"
    )
    explorer_selection_source = Path(
        "premise_ui/frontend/src/explorerSelection.tsx"
    ).read_text(encoding="utf-8")
    plot_widgets_source = Path("premise_ui/frontend/src/plotWidgets.tsx").read_text(
        encoding="utf-8"
    )
    vite_config = Path("premise_ui/frontend/vite.config.ts").read_text(encoding="utf-8")

    assert package_json["scripts"]["build"] == "vite build"
    assert "benchmark:explorer" in package_json["scripts"]
    assert "benchmark:explorer:check" in package_json["scripts"]
    assert 'cache: "no-store"' in api_source
    assert "react" in package_json["dependencies"]
    assert "/api/jobs/enqueue-project" in app_source
    assert "Cancel Run" in app_source
    assert "Add Scenario" in app_source
    assert "Export Type" in app_source
    assert "/api/discovery/brightway" in app_source
    assert "/api/discovery/brightway/project" in app_source
    assert "/api/discovery/scenario-preview" in app_source
    assert "/api/credentials/iam-key" in app_source
    assert "/premise-logo.png" in app_source
    assert "Brightway Project" in app_source
    assert "Brightway Biosphere Database" in app_source
    assert "Select a Brightway biosphere database" in app_source
    assert "Saved value <code>" in app_source
    assert "Used for Brightway export when the biosphere database" in app_source
    assert "Refresh Brightway" in app_source
    assert "Open Config" in app_source
    assert "Save Config" in app_source
    assert "Clone Config" in app_source
    assert "Preview Run Manifest" not in app_source
    assert "Configuration setup" in app_source
    assert "Configuration Name" in app_source
    assert "Config Path" in app_source
    assert "Additional inventories" in app_source
    assert "IAM Scenario Key" in app_source
    assert "Ask for Key" in app_source
    assert "Request for Premise IAM scenario decryption key" in app_source
    assert "PathwaysDataPackage" in app_source
    assert "IncrementalDatabase" in app_source
    assert "Installed IAM scenario files" in app_source
    assert "Scenario descriptions" in app_source
    assert "Download All Known Scenarios" in app_source
    assert "Clear Local Files" in app_source
    assert "/api/discovery/iam-storylines" in app_source
    assert "Retry startup checks" in app_source
    assert "Startup diagnostics" in app_source
    assert "Manual path entry only" in app_source
    assert "All file and directory paths can still be entered manually." in app_source
    assert "Scenario Explorer" in app_source
    assert "/api/scenario-explorer/catalog" in app_source
    assert "/api/discovery/iam-scenarios/local" in app_source
    assert "/api/scenario-explorer/sector-summary" in app_source
    assert "/api/scenario-explorer/compare" in app_source
    assert "Reload Explorer View" in app_source
    assert "Use Config Scenarios" in app_source
    assert "Apply to Config" in app_source
    assert "reconciledLocalScenarioFiles" in app_source
    assert "Explorer Compare Mode" in app_source
    assert "Explorer Chart Mode" in app_source
    assert "Explorer Plot Layout" in app_source
    assert "Year window slider" in app_source
    assert "Normalized to 100%" in app_source
    assert "Stacked area" in app_source
    assert "Grouped bars" in app_source
    assert "Stacked bars" in app_source
    assert "One plot per scenario" in app_source
    assert "lastReloadMs" in app_source
    assert "Performance <strong>{explorerPerformanceSummary}</strong>" in app_source
    assert "buildExplorerBenchmarkSummary" in explorer_data_source
    assert "export function explorerSeriesForSelection" in explorer_data_source
    assert "export function plotSeriesValueDomain" in explorer_data_source
    assert "runBenchmark" in benchmark_source
    assert "buildExplorerBenchmarkSummary" in benchmark_source
    assert "assertExplorerBenchmarkBaseline" in benchmark_source
    assert "console.log(JSON.stringify(result, null, 2))" in benchmark_source
    assert "EXPLORER_BENCHMARK_BASELINE" in benchmark_baseline_source
    assert "series_selection_avg" in benchmark_baseline_source
    assert "export function SeriesPlot" in plot_widgets_source
    assert "export function FullscreenPlotCard" in plot_widgets_source
    assert "export function ExplorerValuesTable" in plot_widgets_source
    assert "export function ExplorerSelectionBoard" in explorer_selection_source
    assert "export function explorerSelectionValues" in explorer_selection_source
    assert "export function sanitizeExplorerSelection" in explorer_selection_source
    assert "Save plot as PNG" in plot_widgets_source
    assert "Enter full screen" in plot_widgets_source
    assert "downloadPlotAsPng" in plot_widgets_source
    assert "Application initialization progress" in app_source
    assert "Preparing initial configuration preview" in app_source
    assert "Deselect all" in explorer_selection_source
    assert "Filter available and selected items" in explorer_selection_source
    assert "explorer-workspace" in app_source
    assert "explorer-plot-grid" in app_source
    assert "Explorer Baseline Year" in app_source
    assert "Explorer Baseline Scenario" in app_source
    assert "Indexed to 100" in app_source
    assert "% change vs baseline" in app_source
    assert "explorer-filter-grid" in app_source
    assert "download-progress-bar" in app_source
    assert "refreshed the scenario selectors from the local IAM folder" in app_source
    assert "scenario-table" in app_source
    assert "/api/jobs/${encodeURIComponent(target.run_id)}/artifact" in app_source
    assert 'params.set("project_path", target.project_path);' in app_source
    assert "Saved artifacts" in app_source
    assert "Monitoring current run" in app_source
    assert "Monitoring run " in app_source
    assert "Monitor" in app_source
    assert "Email Support" in app_source
    assert "mailto:romain.sacchi@psi.ch" in app_source
    assert "pendingMonitorRunIds" in app_source
    assert 'aria-label="Current run progress"' in app_source
    assert "job-progress-track" in app_source
    assert (
        "Queueing will preview and validate the current form automatically."
        in app_source
    )
    assert "Enable Desktop Alerts" in app_source
    assert "completed successfully" in app_source
    assert "Guided flow" in app_source
    assert "Workspace mode" in app_source
    assert "Draft autosaved" in app_source
    assert "field-feedback" in app_source
    assert "Cmd/Ctrl+Enter queues the current draft" in app_source
    assert "HelpText" in app_source
    assert "help-inline" in app_source
    assert "section.configuration_setup" in help_source
    assert "section.scenario_explorer" in help_source
    assert "field.explorer_compare_mode" in help_source
    assert "field.explorer_chart_mode" in help_source
    assert "field.explorer_plot_layout" in help_source
    assert "field.explorer_sector" in help_source
    assert "field.explorer_variable" in help_source
    assert "field.explorer_baseline_year" in help_source
    assert "field.explorer_baseline_scenario" in help_source
    assert "field.configuration_name" in help_source
    assert 'outDir: "dist"' in vite_config


def test_frontend_dist_bundle_exists():
    dist_index = Path("premise_ui/frontend/dist/index.html")
    dist_js_files = sorted(Path("premise_ui/frontend/dist/assets").glob("index-*.js"))
    dist_css_files = sorted(Path("premise_ui/frontend/dist/assets").glob("index-*.css"))

    assert dist_index.exists()
    assert dist_js_files
    assert dist_css_files


def test_list_local_iam_scenarios_prefers_supported_suffix_order(tmp_path, monkeypatch):
    from premise_ui.core.scenario_catalog import list_local_iam_scenarios

    scenario_dir = tmp_path / "iam_output_files"
    scenario_dir.mkdir()
    (scenario_dir / "remind_SSP2-Base.xlsx").write_text("xlsx", encoding="utf-8")
    (scenario_dir / "remind_SSP2-Base.csv").write_text("csv", encoding="utf-8")
    (scenario_dir / "image_SSP2-NPi.mif").write_text("mif", encoding="utf-8")
    (scenario_dir / "ignore-me.txt").write_text("ignore", encoding="utf-8")

    monkeypatch.setattr(
        "premise_ui.core.scenario_catalog._premise_iam_output_dir",
        lambda: scenario_dir,
    )

    scenarios = list_local_iam_scenarios()

    assert scenarios == [
        {
            "id": "image-ssp2-npi",
            "model": "image",
            "pathway": "SSP2-NPi",
            "file_name": "image_SSP2-NPi.mif",
            "path": str((scenario_dir / "image_SSP2-NPi.mif").resolve()),
        },
        {
            "id": "remind-ssp2-base",
            "model": "remind",
            "pathway": "SSP2-Base",
            "file_name": "remind_SSP2-Base.csv",
            "path": str((scenario_dir / "remind_SSP2-Base.csv").resolve()),
        },
    ]


def test_fetch_zenodo_iam_scenario_catalog_parses_archive_file_inventory(monkeypatch):
    from premise_ui.core.scenario_catalog import fetch_zenodo_iam_scenario_catalog

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "id": 18790143,
                "files": [
                    {"key": "message_SSP1-VL.csv"},
                    {"key": "tiam-ucl_SSP2-RCP26.csv"},
                    {"key": "gcam_SSP2-Base.csv"},
                    {"key": "notes.txt"},
                ],
            }

    monkeypatch.setattr(
        "premise_ui.core.scenario_catalog.requests.get",
        lambda *args, **kwargs: FakeResponse(),
    )

    payload = fetch_zenodo_iam_scenario_catalog()

    assert payload["record_id"] == "18790143"
    assert [
        (entry["model"], entry["pathway"], entry["archive_file_name"])
        for entry in payload["scenarios"]
    ] == [
        ("gcam", "SSP2-Base", "gcam_SSP2-Base.csv"),
        ("message", "SSP1-VL", "message_SSP1-VL.csv"),
        ("tiam-ucl", "SSP2-RCP26", "tiam-ucl_SSP2-RCP26.csv"),
    ]


def test_download_all_known_iam_scenarios_skips_existing_supported_extensions(
    tmp_path, monkeypatch
):
    from premise_ui.core.scenario_catalog import download_all_known_iam_scenarios

    scenario_dir = tmp_path / "iam_output_files"
    scenario_dir.mkdir()
    (scenario_dir / "remind_SSP2-NDC.mif").write_text("existing", encoding="utf-8")

    monkeypatch.setattr(
        "premise_ui.core.scenario_catalog._premise_iam_output_dir",
        lambda: scenario_dir,
    )
    monkeypatch.setattr(
        "premise_ui.core.scenario_catalog.load_downloadable_iam_scenario_catalog",
        lambda: {
            "scenarios": [
                {
                    "model": "remind",
                    "pathway": "SSP2-NDC",
                    "availability": "bundled",
                    "archive_file_name": "remind_SSP2-NDC.csv",
                },
                {
                    "model": "image",
                    "pathway": "SSP2-Base",
                    "availability": "download-on-demand",
                    "archive_file_name": "image_SSP2-Base.csv",
                },
                {
                    "model": "remind",
                    "pathway": "SSP2-ignored",
                    "availability": "external",
                },
            ]
        },
    )

    downloaded_calls = []

    def fake_download(file_name, url, download_folder):
        downloaded_calls.append((file_name, url, str(download_folder)))
        target = download_folder / file_name
        target.write_text("downloaded", encoding="utf-8")
        return target

    monkeypatch.setattr(
        "premise_ui.core.scenario_catalog._download_csv",
        fake_download,
    )

    payload = download_all_known_iam_scenarios()

    assert payload["directory"] == str(scenario_dir.resolve())
    assert payload["existing"] == ["remind_SSP2-NDC.mif"]
    assert payload["downloaded"] == ["image_SSP2-Base.csv"]
    assert payload["failed"] == []
    assert downloaded_calls == [
        (
            "image_SSP2-Base.csv",
            "https://zenodo.org/records/18790143/files/image_SSP2-Base.csv",
            str(scenario_dir),
        )
    ]
    assert {entry["file_name"] for entry in payload["scenarios"]} == {
        "image_SSP2-Base.csv",
        "remind_SSP2-NDC.mif",
    }


def test_capabilities_are_derived_from_local_iam_scenarios(monkeypatch):
    from premise_ui.core.capabilities import get_capabilities

    monkeypatch.setattr(
        "premise_ui.core.capabilities.load_premise_constants",
        lambda: {"SUPPORTED_EI_VERSIONS": ["3.11", "3.12", "3.13"]},
    )
    monkeypatch.setattr(
        "premise_ui.core.capabilities.load_premise_version",
        lambda: "test-version",
    )
    monkeypatch.setattr(
        "premise_ui.core.capabilities.load_iam_scenario_catalog",
        lambda: {"scenarios": [{"id": "catalog-entry"}]},
    )
    monkeypatch.setattr(
        "premise_ui.core.capabilities.list_local_iam_scenarios",
        lambda: [
            {
                "id": "image-ssp2-base",
                "model": "image",
                "pathway": "SSP2-Base",
                "file_name": "image_SSP2-Base.csv",
                "path": "/tmp/image_SSP2-Base.csv",
            },
            {
                "id": "remind-ssp2-ndc",
                "model": "remind",
                "pathway": "SSP2-NDC",
                "file_name": "remind_SSP2-NDC.csv",
                "path": "/tmp/remind_SSP2-NDC.csv",
            },
        ],
    )

    payload = get_capabilities()

    assert payload["ecoinvent_versions"] == ["3.11", "3.12"]
    assert payload["iam_models"] == ["image", "remind"]
    assert payload["iam_pathways"] == ["SSP2-Base", "SSP2-NDC"]
    assert len(payload["iam_scenarios"]) == 2
    assert payload["premise_version"] == "test-version"


def test_clear_local_iam_scenarios_removes_supported_files(tmp_path, monkeypatch):
    from premise_ui.core.scenario_catalog import clear_local_iam_scenarios

    scenario_dir = tmp_path / "iam_output_files"
    scenario_dir.mkdir()
    (scenario_dir / "remind_SSP2-Base.csv").write_text("csv", encoding="utf-8")
    (scenario_dir / "image_SSP2-NPi.xlsx").write_text("xlsx", encoding="utf-8")
    (scenario_dir / "notes.txt").write_text("keep", encoding="utf-8")

    monkeypatch.setattr(
        "premise_ui.core.scenario_catalog._premise_iam_output_dir",
        lambda: scenario_dir,
    )

    payload = clear_local_iam_scenarios()

    assert payload["directory"] == str(scenario_dir.resolve())
    assert payload["removed_count"] == 2
    assert set(payload["removed"]) == {"remind_SSP2-Base.csv", "image_SSP2-NPi.xlsx"}
    assert payload["scenarios"] == []
    assert (scenario_dir / "notes.txt").exists()


def test_project_template_has_canonical_new_database_defaults():
    project = GuiProjectManifest.template()

    assert project.workflow == "new_database"
    assert project.config["source_type"] == "brightway"
    assert project.config["export"]["type"] == "brightway"
    assert project.scenario_sets[0]["scenarios"][0]["model"] == "remind"


def test_project_templates_cover_incremental_and_pathways_defaults():
    incremental = GuiProjectManifest.template(workflow="incremental_database")
    pathways = GuiProjectManifest.template(workflow="pathways_datapackage")

    assert incremental.workflow == "incremental_database"
    assert incremental.config["export"]["type"] == "brightway"
    assert incremental.config["sectors"] == []
    assert pathways.workflow == "pathways_datapackage"
    assert pathways.config["export"]["type"] == "datapackage"
    assert pathways.config["years"] == [2030, 2040, 2050]
    assert pathways.config["contributors"][0]["email"] == ""


def test_project_manifest_normalizes_export_options_by_type():
    project = GuiProjectManifest.from_dict(
        {
            "workflow": "new_database",
            "config": {
                "export": {
                    "type": "datapackage",
                    "options": {
                        "filepath": "should-be-ignored",
                        "name": "demo-package",
                    },
                }
            },
        }
    )

    assert project.config["export"]["type"] == "datapackage"
    assert project.config["export"]["options"] == {"name": "demo-package"}


def test_project_manifest_normalizes_export_options_by_workflow():
    incremental = GuiProjectManifest.from_dict(
        {
            "workflow": "incremental_database",
            "config": {
                "export": {
                    "type": "brightway",
                    "options": {
                        "name": "incremental-ui",
                    },
                }
            },
        }
    )
    pathways = GuiProjectManifest.from_dict(
        {
            "workflow": "pathways_datapackage",
            "config": {
                "export": {
                    "type": "brightway",
                    "options": {
                        "filepath": "should-be-ignored",
                    },
                }
            },
        }
    )

    assert incremental.config["export"]["type"] == "brightway"
    assert incremental.config["export"]["options"] == {
        "name": "incremental-ui",
        "filepath": "export/incremental",
        "file_format": "csv",
    }
    assert pathways.config["export"]["type"] == "datapackage"
    assert pathways.config["export"]["options"] == {"name": "pathways"}


def test_project_clone_clears_history(tmp_path):
    project = GuiProjectManifest.template()
    project.run_history.append({"run_id": "abc"})

    source = tmp_path / "source.json"
    target = tmp_path / "target.json"
    save_project(source, project)
    clone_project(source, target)

    cloned = load_project(target)
    assert cloned.run_history == []
    assert cloned.project_name.endswith("Copy")


def test_recent_state_remembers_and_deduplicates(tmp_path, monkeypatch):
    recents_file = tmp_path / "recents.json"
    monkeypatch.setattr("premise_ui.core.recents.RECENTS_FILE", recents_file)

    remember_recent_project(str(tmp_path / "alpha.json"), label="Alpha")
    remember_recent_project(str(tmp_path / "beta.json"), label="Beta")
    remember_recent_project(str(tmp_path / "alpha.json"), label="Alpha")
    remember_recent_path(
        "exports", kind="export_directory", base_path=str(tmp_path / "alpha.json")
    )
    remember_recent_path(
        "exports", kind="export_directory", base_path=str(tmp_path / "alpha.json")
    )

    payload = load_recent_state()

    assert [item["label"] for item in payload["projects"]] == ["Alpha", "Beta"]
    assert len(payload["paths"]) == 1
    assert payload["paths"][0]["kind"] == "export_directory"
    assert payload["paths"][0]["path"].endswith("exports")


def test_recent_state_clear(tmp_path, monkeypatch):
    recents_file = tmp_path / "recents.json"
    monkeypatch.setattr("premise_ui.core.recents.RECENTS_FILE", recents_file)

    remember_recent_project(str(tmp_path / "alpha.json"), label="Alpha")
    cleared = clear_recent_state()

    assert cleared == {"projects": [], "paths": []}
    assert load_recent_state() == {"projects": [], "paths": []}


def test_project_run_history_roundtrip(tmp_path):
    project_path = tmp_path / "project.json"
    save_project(project_path, GuiProjectManifest.template())

    run_dir = tmp_path / ".premise-ui" / "runs" / "run-1"
    run_dir.mkdir(parents=True)
    manifest = RunManifest(
        run_id="run-1",
        created_at=utc_now_iso(),
        project_name="History",
        workflow="new_database",
        project_path=str(project_path),
        config={"export": {"type": "matrices"}},
        scenarios=[{"model": "remind", "pathway": "SSP2-Base", "year": 2030}],
    )

    remember_project_run(
        str(project_path),
        manifest=manifest,
        run_dir=run_dir,
        dry_run=True,
        project_snapshot=GuiProjectManifest.template().to_dict(),
        warnings=["scaffold warning"],
    )
    (run_dir / "result.json").write_text(
        '{"export_type": "matrices"}', encoding="utf-8"
    )
    sync_project_run_history(
        str(project_path),
        run_id="run-1",
        manifest=manifest.to_dict(),
        run_dir=run_dir,
        status="completed",
        events=[
            {
                "timestamp": utc_now_iso(),
                "event_type": "job_completed",
                "message": "done",
            }
        ],
        process_returncode=0,
    )

    project = load_project(project_path)
    entry = project.run_history[0]

    assert entry["run_id"] == "run-1"
    assert entry["status"] == "completed"
    assert entry["dry_run"] is True
    assert entry["result"]["export_type"] == "matrices"
    assert entry["run_dir"] == ".premise-ui/runs/run-1"


def test_build_run_manifest_from_project_flattens_scenario_sets(tmp_path):
    project = GuiProjectManifest.from_dict(
        {
            "project_name": "Build Run",
            "workflow": "new_database",
            "config": {
                "source_db": "ecoinvent-3.12-cutoff",
                "export": {"type": "matrices", "options": {"filepath": "runs/output"}},
            },
            "scenario_sets": [
                {
                    "name": "core",
                    "scenarios": [
                        {"model": "remind", "pathway": "SSP2-Base", "year": 2030},
                        {"model": "image", "pathway": "SSP2-M", "year": 2040},
                    ],
                }
            ],
        }
    )

    manifest = build_run_manifest_from_project(
        project, project_path=str(tmp_path / "project.json")
    )

    assert manifest.project_name == "Build Run"
    assert manifest.project_path.endswith("project.json")
    assert manifest.working_directory == str(tmp_path.resolve())
    assert len(manifest.scenarios) == 2
    assert manifest.metadata["scenario_set_names"] == ["core"]


def test_build_run_manifest_from_project_drops_empty_scenario_filepath(tmp_path):
    project = GuiProjectManifest.from_dict(
        {
            "project_name": "Build Run",
            "workflow": "new_database",
            "config": {
                "source_db": "ecoinvent-3.12-cutoff",
                "export": {"type": "matrices", "options": {"filepath": "runs/output"}},
            },
            "scenario_sets": [
                {
                    "name": "default",
                    "scenarios": [
                        {
                            "model": "remind",
                            "pathway": "SSP2-PkBudg1150",
                            "year": 2030,
                            "filepath": "",
                        }
                    ],
                }
            ],
        }
    )

    manifest = build_run_manifest_from_project(
        project, project_path=str(tmp_path / "project.json")
    )

    assert "filepath" not in manifest.scenarios[0]


def test_build_run_manifest_strips_workflow_specific_config_keys(tmp_path):
    project = GuiProjectManifest.from_dict(
        {
            "project_name": "Build Run",
            "workflow": "new_database",
            "config": {
                "source_db": "ecoinvent-3.12-cutoff",
                "years": [2030, 2040, 2050],
                "contributors": [{"name": "Ada", "email": "ada@example.com"}],
                "export": {"type": "matrices", "options": {"filepath": "runs/output"}},
            },
            "scenario_sets": [
                {
                    "name": "default",
                    "scenarios": [
                        {"model": "remind", "pathway": "SSP2-Base", "year": 2030}
                    ],
                }
            ],
        }
    )

    manifest = build_run_manifest_from_project(
        project, project_path=str(tmp_path / "project.json")
    )

    assert "years" not in manifest.config
    assert "contributors" not in manifest.config


def test_worker_dry_run_writes_events(tmp_path):
    run_dir = tmp_path / "run-1"
    run_dir.mkdir()

    manifest = RunManifest(
        project_name="Dry Run",
        workflow="new_database",
        project_path=str(tmp_path / "project.json"),
    )
    manifest_path = run_dir / "run_manifest.json"
    manifest_path.write_text(
        __import__("json").dumps(manifest.to_dict(), indent=2), encoding="utf-8"
    )

    result = run_manifest(manifest_path, dry_run=True)
    events = read_events(run_dir / "events.jsonl")

    assert result == 0
    assert any(event["event_type"] == "job_started" for event in events)
    assert any(event["event_type"] == "job_completed" for event in events)
    assert (run_dir / "metadata.json").is_file()


def test_validate_manifest_requires_export_type():
    errors, warnings = validate_run_manifest_payload(
        {
            "project_name": "Validate",
            "workflow": "new_database",
            "config": {"source_type": "brightway", "source_db": "ecoinvent"},
            "scenarios": [{"model": "remind", "pathway": "SSP2-Base", "year": 2030}],
        }
    )

    assert any("config.export.type" in error for error in errors)
    assert isinstance(warnings, list)


def test_new_database_validation_uses_ui_labels_for_source_errors(monkeypatch):
    monkeypatch.setitem(sys.modules, "bw2data", _fake_bw2data())
    monkeypatch.setattr(
        NewDatabaseAdapter,
        "_load_new_database_module",
        staticmethod(
            lambda: type(
                "FakeModule",
                (),
                {
                    "check_db_version": staticmethod(lambda version: version),
                    "check_system_model": staticmethod(lambda model: model),
                    "check_scenarios": staticmethod(
                        lambda scenario, key=None: scenario
                    ),
                    "check_additional_inventories": staticmethod(
                        lambda inventories: inventories
                    ),
                    "check_ei_filepath": staticmethod(lambda path: path),
                    "check_presence_biosphere_database": staticmethod(
                        lambda biosphere: biosphere
                    ),
                },
            )()
        ),
    )

    manifest = RunManifest(
        project_name="Validate labels",
        workflow="new_database",
        config={
            "source_type": "brightway",
            "source_version": "3.12",
            "export": {"type": "brightway", "options": {"name": "ui-test-db"}},
        },
        scenarios=[{"model": "remind", "pathway": "SSP2-Base", "year": 2030}],
    )

    errors = NewDatabaseAdapter(manifest).validate().errors

    assert (
        "Brightway Source Database is required when Source Type is set to Brightway."
        in errors
    )


def test_new_database_validation_guides_biosphere_database_override(
    tmp_path, monkeypatch
):
    monkeypatch.setitem(sys.modules, "bw2data", _fake_bw2data())
    monkeypatch.setattr(
        NewDatabaseAdapter,
        "_load_new_database_module",
        staticmethod(
            lambda: type(
                "FakeModule",
                (),
                {
                    "check_db_version": staticmethod(lambda version: version),
                    "check_system_model": staticmethod(lambda model: model),
                    "check_scenarios": staticmethod(
                        lambda scenario, key=None: scenario
                    ),
                    "check_additional_inventories": staticmethod(
                        lambda inventories: inventories
                    ),
                    "check_ei_filepath": staticmethod(lambda path: path),
                    "check_presence_biosphere_database": staticmethod(
                        lambda biosphere: (_ for _ in ()).throw(
                            ValueError(
                                f"Brightway export requires a biosphere database named '{biosphere}'."
                            )
                        )
                    ),
                },
            )()
        ),
    )

    manifest = RunManifest(
        project_name="Validate biosphere guidance",
        workflow="new_database",
        project_path=str(tmp_path / "config.json"),
        config={
            "source_type": "brightway",
            "source_version": "3.12",
            "source_db": "ecoinvent-3.12-cutoff",
            "export": {"type": "brightway", "options": {"name": "ui-test-db"}},
            "biosphere_name": "dummy_bio",
        },
        scenarios=[{"model": "remind", "pathway": "SSP2-Base", "year": 2030}],
    )

    errors = NewDatabaseAdapter(manifest).validate().errors

    assert any(
        "Adjust Brightway Biosphere Database in the Source tab" in error
        for error in errors
    )


def test_new_database_adapter_dispatches_stubbed_workflow(tmp_path, monkeypatch):
    calls = []

    class FakeNewDatabase:
        def __init__(self, **kwargs):
            calls.append(("init", kwargs))

        def update(self, transformations):
            calls.append(("update", transformations))

        def write_db_to_matrices(self, **kwargs):
            calls.append(("matrices", kwargs))
            target = Path(kwargs["filepath"])
            target.mkdir(parents=True, exist_ok=True)
            (target / "A.csv").write_text("a,b\n1,2\n", encoding="utf-8")
            scenario_report = Path.cwd() / "export" / "scenario_report"
            scenario_report.mkdir(parents=True, exist_ok=True)
            (scenario_report / "report.xlsx").write_text("scenario", encoding="utf-8")
            change_report = Path.cwd() / "export" / "change reports"
            change_report.mkdir(parents=True, exist_ok=True)
            (change_report / "change.xlsx").write_text("change", encoding="utf-8")

    monkeypatch.setitem(sys.modules, "bw2data", _fake_bw2data())
    monkeypatch.setattr(
        NewDatabaseAdapter,
        "_load_new_database_class",
        classmethod(lambda cls: FakeNewDatabase),
    )
    monkeypatch.setattr(
        NewDatabaseAdapter,
        "_load_new_database_module",
        staticmethod(
            lambda: type(
                "FakeModule",
                (),
                {
                    "check_db_version": staticmethod(lambda version: version),
                    "check_system_model": staticmethod(lambda model: model),
                    "check_scenarios": staticmethod(
                        lambda scenario, key=None: scenario
                    ),
                    "check_additional_inventories": staticmethod(
                        lambda inventories: inventories
                    ),
                    "check_ei_filepath": staticmethod(lambda path: path),
                    "check_presence_biosphere_database": staticmethod(
                        lambda biosphere: biosphere
                    ),
                },
            )()
        ),
    )

    manifest = RunManifest(
        project_name="Adapter",
        workflow="new_database",
        project_path=str(tmp_path / "project.json"),
        config={
            "source_type": "brightway",
            "source_version": "3.12",
            "source_db": "ecoinvent-3.12-cutoff",
            "transformations": ["electricity"],
            "export": {
                "type": "matrices",
                "options": {"filepath": "outputs/matrices"},
            },
        },
        scenarios=[{"model": "remind", "pathway": "SSP2-Base", "year": 2030}],
    )

    with monkeypatch.context() as context:
        context.chdir(tmp_path)
        result = NewDatabaseAdapter(manifest).execute()

    assert result["export_type"] == "matrices"
    assert (
        str((tmp_path / "outputs" / "matrices").resolve()) in result["output_location"]
    )
    assert (
        str((tmp_path / "export" / "change reports").resolve())
        in result["output_location"]
    )
    assert calls[1] == ("update", ["electricity"])
    assert calls[2][0] == "matrices"
    assert calls[2][1]["filepath"].endswith("outputs/matrices")


def test_new_database_adapter_reports_existing_default_matrix_output_directory(
    tmp_path, monkeypatch
):
    class FakeNewDatabase:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def update(self, transformations):
            return transformations

        def write_db_to_matrices(self, **kwargs):
            target = Path.cwd() / "export" / "remind" / "SSP2-Base" / "2030"
            target.mkdir(parents=True, exist_ok=True)
            (target / "A.csv").write_text("a,b\n1,2\n", encoding="utf-8")
            scenario_report = Path.cwd() / "export" / "scenario_report"
            scenario_report.mkdir(parents=True, exist_ok=True)
            (scenario_report / "report.xlsx").write_text("scenario", encoding="utf-8")
            change_report = Path.cwd() / "export" / "change reports"
            change_report.mkdir(parents=True, exist_ok=True)
            (change_report / "change.xlsx").write_text("change", encoding="utf-8")

    monkeypatch.setitem(sys.modules, "bw2data", _fake_bw2data())
    monkeypatch.setattr(
        NewDatabaseAdapter,
        "_load_new_database_class",
        classmethod(lambda cls: FakeNewDatabase),
    )
    monkeypatch.setattr(
        NewDatabaseAdapter,
        "_load_new_database_module",
        staticmethod(
            lambda: type(
                "FakeModule",
                (),
                {
                    "check_db_version": staticmethod(lambda version: version),
                    "check_system_model": staticmethod(lambda model: model),
                    "check_scenarios": staticmethod(
                        lambda scenario, key=None: scenario
                    ),
                    "check_additional_inventories": staticmethod(
                        lambda inventories: inventories
                    ),
                    "check_ei_filepath": staticmethod(lambda path: path),
                    "check_presence_biosphere_database": staticmethod(
                        lambda biosphere: biosphere
                    ),
                },
            )()
        ),
    )

    manifest = RunManifest(
        project_name="Adapter",
        workflow="new_database",
        project_path=str(tmp_path / "project.json"),
        config={
            "source_type": "brightway",
            "source_version": "3.12",
            "source_db": "ecoinvent-3.12-cutoff",
            "export": {
                "type": "matrices",
                "options": {},
            },
        },
        scenarios=[{"model": "remind", "pathway": "SSP2-Base", "year": 2030}],
    )

    with monkeypatch.context() as context:
        context.chdir(tmp_path)
        result = NewDatabaseAdapter(manifest).execute()

    assert (
        str((tmp_path / "export" / "remind" / "SSP2-Base" / "2030").resolve())
        in result["output_location"]
    )
    assert (
        str((tmp_path / "export" / "change reports").resolve())
        in result["output_location"]
    )


def test_new_database_adapter_switches_selected_brightway_project(
    tmp_path, monkeypatch
):
    calls = []

    class FakeNewDatabase:
        def __init__(self, **kwargs):
            calls.append(("init", kwargs))

        def update(self, transformations):
            calls.append(("update", transformations))

        def write_db_to_brightway(self, **kwargs):
            calls.append(("brightway", kwargs))

    monkeypatch.setitem(sys.modules, "bw2data", _fake_bw2data(log=calls))
    monkeypatch.setattr(
        NewDatabaseAdapter,
        "_load_new_database_class",
        classmethod(lambda cls: FakeNewDatabase),
    )
    monkeypatch.setattr(
        NewDatabaseAdapter,
        "_load_new_database_module",
        staticmethod(
            lambda: type(
                "FakeModule",
                (),
                {
                    "check_db_version": staticmethod(lambda version: version),
                    "check_system_model": staticmethod(lambda model: model),
                    "check_scenarios": staticmethod(
                        lambda scenario, key=None: scenario
                    ),
                    "check_additional_inventories": staticmethod(
                        lambda inventories: inventories
                    ),
                    "check_ei_filepath": staticmethod(lambda path: path),
                    "check_presence_biosphere_database": staticmethod(
                        lambda biosphere: biosphere
                    ),
                },
            )()
        ),
    )

    manifest = RunManifest(
        project_name="Adapter",
        workflow="new_database",
        project_path=str(tmp_path / "project.json"),
        config={
            "source_type": "brightway",
            "source_project": "project-b",
            "source_version": "3.12",
            "source_db": "ecoinvent-3.12-cutoff",
            "export": {
                "type": "brightway",
                "options": {"name": "ui-test-db"},
            },
        },
        scenarios=[{"model": "remind", "pathway": "SSP2-Base", "year": 2030}],
    )

    adapter = NewDatabaseAdapter(manifest)
    result = adapter.execute()

    assert result["export_type"] == "brightway"
    assert calls.count(("project", "project-b")) == 2
    assert calls.count(("project", "project-a")) == 2
    assert any(call[0] == "init" for call in calls)
    assert any(call == ("update", None) for call in calls)
    assert any(call == ("brightway", {"name": "ui-test-db"}) for call in calls)


def test_new_database_adapter_uses_saved_iam_key_when_manifest_omits_it(
    tmp_path, monkeypatch
):
    calls = []
    stored_key = "x" * 44

    class FakeNewDatabase:
        def __init__(self, **kwargs):
            calls.append(("init", kwargs))

        def update(self, transformations):
            calls.append(("update", transformations))

        def write_db_to_matrices(self, **kwargs):
            calls.append(("matrices", kwargs))

    def check_scenarios(scenario, key=None):
        calls.append(("check_scenarios", dict(scenario), key))
        return scenario

    monkeypatch.setitem(sys.modules, "bw2data", _fake_bw2data())
    monkeypatch.setattr("premise_ui.core.adapters.iam_key_value", lambda: stored_key)
    monkeypatch.setattr(
        NewDatabaseAdapter,
        "_load_new_database_class",
        classmethod(lambda cls: FakeNewDatabase),
    )
    monkeypatch.setattr(
        NewDatabaseAdapter,
        "_load_new_database_module",
        staticmethod(
            lambda: type(
                "FakeModule",
                (),
                {
                    "check_db_version": staticmethod(lambda version: version),
                    "check_system_model": staticmethod(lambda model: model),
                    "check_scenarios": staticmethod(check_scenarios),
                    "check_additional_inventories": staticmethod(
                        lambda inventories: inventories
                    ),
                    "check_ei_filepath": staticmethod(lambda path: path),
                    "check_presence_biosphere_database": staticmethod(
                        lambda biosphere: biosphere
                    ),
                },
            )()
        ),
    )

    manifest = RunManifest(
        project_name="Adapter",
        workflow="new_database",
        project_path=str(tmp_path / "project.json"),
        config={
            "source_type": "brightway",
            "source_version": "3.12",
            "source_db": "ecoinvent-3.12-cutoff",
            "export": {
                "type": "matrices",
                "options": {"filepath": "outputs/matrices"},
            },
        },
        scenarios=[
            {
                "model": "remind",
                "pathway": "SSP2-NDC",
                "year": 2030,
                "filepath": "",
            }
        ],
    )

    adapter = NewDatabaseAdapter(manifest)
    result = adapter.execute()

    assert result["export_type"] == "matrices"
    check_call = next(call for call in calls if call[0] == "check_scenarios")
    assert check_call[1].get("filepath") is None
    assert check_call[2] == stored_key
    init_call = next(call for call in calls if call[0] == "init")
    assert init_call[1]["key"] == stored_key


def test_new_database_adapter_dispatches_datapackage_name_only(tmp_path, monkeypatch):
    calls = []

    class FakeNewDatabase:
        def __init__(self, **kwargs):
            calls.append(("init", kwargs))

        def update(self, transformations):
            calls.append(("update", transformations))

        def write_datapackage(self, **kwargs):
            calls.append(("datapackage", kwargs))

    monkeypatch.setitem(sys.modules, "bw2data", _fake_bw2data())
    monkeypatch.setattr(
        NewDatabaseAdapter,
        "_load_new_database_class",
        classmethod(lambda cls: FakeNewDatabase),
    )
    monkeypatch.setattr(
        NewDatabaseAdapter,
        "_load_new_database_module",
        staticmethod(
            lambda: type(
                "FakeModule",
                (),
                {
                    "check_db_version": staticmethod(lambda version: version),
                    "check_system_model": staticmethod(lambda model: model),
                    "check_scenarios": staticmethod(
                        lambda scenario, key=None: scenario
                    ),
                    "check_additional_inventories": staticmethod(
                        lambda inventories: inventories
                    ),
                    "check_ei_filepath": staticmethod(lambda path: path),
                    "check_presence_biosphere_database": staticmethod(
                        lambda biosphere: biosphere
                    ),
                },
            )()
        ),
    )

    manifest = RunManifest(
        project_name="Datapackage",
        workflow="new_database",
        project_path=str(tmp_path / "project.json"),
        config={
            "source_type": "brightway",
            "source_version": "3.12",
            "source_db": "ecoinvent-3.12-cutoff",
            "export": {
                "type": "datapackage",
                "options": {
                    "filepath": "outputs/should-not-pass-through",
                    "name": "scenario-bundle",
                },
            },
        },
        scenarios=[{"model": "remind", "pathway": "SSP2-Base", "year": 2030}],
    )

    adapter = NewDatabaseAdapter(manifest)
    result = adapter.execute()

    assert result["export_type"] == "datapackage"
    assert calls[1] == ("update", None)
    assert calls[2] == ("datapackage", {"name": "scenario-bundle"})


def test_incremental_adapter_dispatches_selected_sectors(tmp_path, monkeypatch):
    calls = []

    class FakeIncrementalDatabase:
        def __init__(self, **kwargs):
            calls.append(("init", kwargs))

        def update(self, sectors):
            calls.append(("update", sectors))

        def write_increment_db_to_brightway(self, **kwargs):
            calls.append(("incremental-brightway", kwargs))

    monkeypatch.setitem(sys.modules, "bw2data", _fake_bw2data())
    monkeypatch.setattr(
        IncrementalDatabaseAdapter,
        "_load_workflow_class",
        classmethod(lambda cls: FakeIncrementalDatabase),
    )
    monkeypatch.setattr(
        IncrementalDatabaseAdapter,
        "_load_new_database_module",
        staticmethod(
            lambda: type(
                "FakeModule",
                (),
                {
                    "check_db_version": staticmethod(lambda version: version),
                    "check_system_model": staticmethod(lambda model: model),
                    "check_scenarios": staticmethod(
                        lambda scenario, key=None: scenario
                    ),
                    "check_additional_inventories": staticmethod(
                        lambda inventories: inventories
                    ),
                    "check_ei_filepath": staticmethod(lambda path: path),
                    "check_presence_biosphere_database": staticmethod(
                        lambda biosphere: biosphere
                    ),
                },
            )()
        ),
    )
    monkeypatch.setattr(
        IncrementalDatabaseAdapter,
        "_load_incremental_module",
        staticmethod(
            lambda: type(
                "IncrementalModule",
                (),
                {
                    "SECTORS": {
                        "electricity": "electricity",
                        "transport": ["cars", "trucks"],
                    }
                },
            )()
        ),
    )

    manifest = RunManifest(
        project_name="Incremental",
        workflow="incremental_database",
        project_path=str(tmp_path / "project.json"),
        config={
            "source_type": "brightway",
            "source_version": "3.12",
            "source_db": "ecoinvent-3.12-cutoff",
            "sectors": ["electricity", "transport"],
            "export": {
                "type": "brightway",
                "options": {
                    "name": "incremental-ui",
                    "filepath": "exports/incremental",
                    "file_format": "csv",
                },
            },
        },
        scenarios=[{"model": "remind", "pathway": "SSP2-Base", "year": 2030}],
    )

    adapter = IncrementalDatabaseAdapter(manifest)
    result = adapter.execute()

    assert result["workflow"] == "incremental_database"
    assert result["export_type"] == "brightway"
    assert calls[1] == (
        "update",
        {"electricity": "electricity", "transport": ["cars", "trucks"]},
    )
    assert calls[2][0] == "incremental-brightway"
    assert calls[2][1]["name"] == "incremental-ui"
    assert calls[2][1]["filepath"].endswith("exports/incremental")
    assert calls[2][1]["file_format"] == "csv"


def test_pathways_adapter_dispatches_datapackage_with_years_and_contributors(
    tmp_path, monkeypatch
):
    calls = []

    class FakePathwaysDataPackage:
        def __init__(self, **kwargs):
            calls.append(("init", kwargs))

        def create_datapackage(self, **kwargs):
            calls.append(("datapackage", kwargs))

    monkeypatch.setitem(sys.modules, "bw2data", _fake_bw2data())
    monkeypatch.setattr(
        PathwaysDataPackageAdapter,
        "_load_workflow_class",
        classmethod(lambda cls: FakePathwaysDataPackage),
    )
    monkeypatch.setattr(
        PathwaysDataPackageAdapter,
        "_load_new_database_module",
        staticmethod(
            lambda: type(
                "FakeModule",
                (),
                {
                    "check_db_version": staticmethod(lambda version: version),
                    "check_system_model": staticmethod(lambda model: model),
                    "check_scenarios": staticmethod(
                        lambda scenario, key=None: scenario
                    ),
                    "check_additional_inventories": staticmethod(
                        lambda inventories: inventories
                    ),
                    "check_ei_filepath": staticmethod(lambda path: path),
                    "check_presence_biosphere_database": staticmethod(
                        lambda biosphere: biosphere
                    ),
                },
            )()
        ),
    )

    manifest = RunManifest(
        project_name="Pathways",
        workflow="pathways_datapackage",
        project_path=str(tmp_path / "project.json"),
        config={
            "source_type": "brightway",
            "source_version": "3.12",
            "source_db": "ecoinvent-3.12-cutoff",
            "years": [2030, 2050],
            "contributors": [
                {"title": "Lead", "name": "Ada", "email": "ada@example.com"}
            ],
            "transformations": ["electricity", "fuels"],
            "export": {
                "type": "datapackage",
                "options": {"name": "pathways-bundle"},
            },
        },
        scenarios=[{"model": "remind", "pathway": "SSP2-Base", "year": 2030}],
    )

    adapter = PathwaysDataPackageAdapter(manifest)
    result = adapter.execute()

    assert result["workflow"] == "pathways_datapackage"
    assert result["export_type"] == "datapackage"
    assert calls[0][1]["years"] == [2030, 2050]
    assert calls[1] == (
        "datapackage",
        {
            "name": "pathways-bundle",
            "contributors": [
                {"title": "Lead", "name": "Ada", "email": "ada@example.com"}
            ],
            "transformations": ["electricity", "fuels"],
        },
    )


def test_worker_non_dry_run_dispatches_adapter(tmp_path, monkeypatch):
    class DummyAdapter:
        def __init__(self, manifest, writer=None):
            self.manifest = manifest
            self.writer = writer

        def validate(self):
            return type("Validation", (), {"errors": [], "warnings": []})()

        def execute(self):
            if self.writer is not None:
                self.writer.emit(
                    "phase_completed",
                    phase="execution",
                    message="Dummy execution completed.",
                )
            return {
                "workflow": self.manifest.workflow,
                "export_type": "matrices",
                "output_location": str(tmp_path / "export" / "matrices"),
            }

    monkeypatch.setattr(
        "premise_ui.worker.runner.get_workflow_adapter",
        lambda manifest, writer=None: DummyAdapter(manifest, writer=writer),
    )

    run_dir = tmp_path / "run-2"
    run_dir.mkdir()
    manifest = RunManifest(
        project_name="Worker",
        workflow="new_database",
        project_path=str(tmp_path / "project.json"),
        config={
            "source_type": "brightway",
            "source_db": "ecoinvent-3.12-cutoff",
            "export": {"type": "matrices"},
        },
        scenarios=[{"model": "remind", "pathway": "SSP2-Base", "year": 2030}],
    )
    manifest_path = run_dir / "run_manifest.json"
    manifest_path.write_text(
        __import__("json").dumps(manifest.to_dict(), indent=2), encoding="utf-8"
    )

    result = run_manifest(manifest_path, dry_run=False)
    events = read_events(run_dir / "events.jsonl")

    assert result == 0
    assert any(event["event_type"] == "job_completed" for event in events)
    assert any(event["event_type"] == "output_location" for event in events)
    assert (run_dir / "result.json").is_file()


def test_worker_validates_inside_run_directory(tmp_path, monkeypatch):
    validate_cwds = []
    execute_cwds = []

    class DummyAdapter:
        def __init__(self, manifest, writer=None):
            self.manifest = manifest
            self.writer = writer

        def validate(self):
            validate_cwds.append(Path.cwd().resolve())
            return type("Validation", (), {"errors": [], "warnings": []})()

        def execute(self):
            execute_cwds.append(Path.cwd().resolve())
            return {
                "workflow": self.manifest.workflow,
                "export_type": "matrices",
                "output_location": str(Path.cwd()),
            }

    monkeypatch.setattr(
        "premise_ui.worker.runner.get_workflow_adapter",
        lambda manifest, writer=None: DummyAdapter(manifest, writer=writer),
    )

    run_dir = tmp_path / "run-cwd"
    run_dir.mkdir()
    manifest = RunManifest(
        project_name="Worker",
        workflow="new_database",
        project_path=str(tmp_path / "project.json"),
        config={
            "source_type": "brightway",
            "source_db": "ecoinvent-3.12-cutoff",
            "export": {"type": "matrices"},
        },
        scenarios=[{"model": "remind", "pathway": "SSP2-Base", "year": 2030}],
    )
    manifest_path = run_dir / "run_manifest.json"
    manifest_path.write_text(json.dumps(manifest.to_dict(), indent=2), encoding="utf-8")

    result = run_manifest(manifest_path, dry_run=False)

    assert result == 0
    assert validate_cwds == [run_dir.resolve()]
    assert execute_cwds == [run_dir.resolve()]


def test_worker_failure_writes_diagnostics(tmp_path, monkeypatch):
    class FailingAdapter:
        def __init__(self, manifest, writer=None):
            self.manifest = manifest
            self.writer = writer

        def validate(self):
            return type("Validation", (), {"errors": [], "warnings": []})()

        def execute(self):
            raise RuntimeError(f"Failure under {tmp_path}")

    monkeypatch.setattr(
        "premise_ui.worker.runner.get_workflow_adapter",
        lambda manifest, writer=None: FailingAdapter(manifest, writer=writer),
    )

    run_dir = tmp_path / "run-failure"
    run_dir.mkdir()
    manifest = RunManifest(
        project_name="Failure",
        workflow="new_database",
        project_path=str(tmp_path / "project.json"),
        config={
            "source_type": "brightway",
            "source_db": "ecoinvent-3.12-cutoff",
            "export": {"type": "matrices"},
        },
        scenarios=[{"model": "remind", "pathway": "SSP2-Base", "year": 2030}],
    )
    manifest_path = run_dir / "run_manifest.json"
    manifest_path.write_text(json.dumps(manifest.to_dict(), indent=2), encoding="utf-8")

    result = run_manifest(manifest_path, dry_run=False)
    diagnostics = read_json(run_dir / "diagnostics.json")

    assert result == 1
    assert diagnostics["kind"] == "execution_failed"
    assert diagnostics["exception_type"] == "RuntimeError"
    assert "Traceback" in diagnostics["traceback"]


def test_job_runtime_promotes_next_run_without_fastapi():
    from premise_ui.core.job_runtime import advance_queue, queue_or_start_job

    app_state = _fake_app_state()
    launched = []
    processes = {}

    def spawn_job(job):
        process = DummyProcess()
        processes[job["run_id"]] = process
        app_state.processes[job["run_id"]] = process
        app_state.active_run_id = job["run_id"]
        launched.append(job["run_id"])

    sync_calls = []
    first_job = {"run_id": "run-1"}
    second_job = {"run_id": "run-2"}

    first_status, first_position = queue_or_start_job(
        app_state, first_job, spawn_job=spawn_job
    )
    second_status, second_position = queue_or_start_job(
        app_state, second_job, spawn_job=spawn_job
    )

    assert first_status == "running"
    assert first_position is None
    assert second_status == "queued"
    assert second_position == 1
    assert launched == ["run-1"]

    processes["run-1"]._returncode = 0
    advance_queue(
        app_state,
        sync_finished_run=lambda run_id: sync_calls.append(run_id),
        spawn_job=spawn_job,
    )

    assert sync_calls == ["run-1"]
    assert app_state.active_run_id == "run-2"
    assert launched == ["run-1", "run-2"]


def test_job_runtime_can_remove_queued_run():
    from premise_ui.core.job_runtime import pop_queued_job, queue_position

    job_queue = [{"run_id": "run-1"}, {"run_id": "run-2"}]

    assert queue_position(job_queue, "run-2") == 2
    assert pop_queued_job(job_queue, "run-2") == {"run_id": "run-2"}
    assert queue_position(job_queue, "run-2") is None
    assert job_queue == [{"run_id": "run-1"}]


def test_native_dialog_state_without_tkinter(monkeypatch):
    from premise_ui.core import dialogs

    def _raise_import_error():
        raise ImportError("tk unavailable")

    monkeypatch.setattr(dialogs, "_load_tkinter", _raise_import_error)

    payload = dialogs.native_dialog_state()

    assert payload["available"] is False
    assert payload["backend"] == "none"
    assert payload["manual_path_entry"] is True
    assert "Tkinter" in payload["detail"]


def test_native_dialog_state_on_linux_without_display(monkeypatch):
    from premise_ui.core import dialogs

    monkeypatch.setattr(dialogs, "_load_tkinter", lambda: (object(), object()))
    monkeypatch.setattr(dialogs.sys, "platform", "linux")
    monkeypatch.delenv("DISPLAY", raising=False)
    monkeypatch.delenv("WAYLAND_DISPLAY", raising=False)

    payload = dialogs.native_dialog_state()

    assert payload["available"] is False
    assert payload["backend"] == "tkinter"
    assert payload["manual_path_entry"] is True
    assert "No graphical desktop session" in payload["detail"]


def test_status_from_events_marks_terminated_process_as_cancelled():
    from premise_ui.core.job_runtime import status_from_events

    assert status_from_events([], process_returncode=-15) == "cancelled"


def test_create_app_health_and_capabilities():
    fastapi = pytest.importorskip("fastapi")
    fastapi_testclient = pytest.importorskip("fastapi.testclient")
    assert fastapi

    from premise_ui.api.app import create_app

    client = fastapi_testclient.TestClient(create_app())

    health = client.get("/api/health")
    capabilities = client.get("/api/capabilities")

    assert health.status_code == 200
    assert health.json()["service"] == "premise-ui"
    assert capabilities.status_code == 200
    assert "workflows" in capabilities.json()
    assert "iam_scenarios" in capabilities.json()
    assert capabilities.json()["iam_scenario_catalog"]


def test_create_app_still_serves_health_and_environment_when_explorer_backend_fails(
    monkeypatch,
):
    fastapi = pytest.importorskip("fastapi")
    fastapi_testclient = pytest.importorskip("fastapi.testclient")
    assert fastapi

    import importlib
    from premise_ui.api.routes import scenario_explorer as scenario_explorer_routes

    scenario_explorer_routes._scenario_explorer_core.cache_clear()

    def _import_module(name):
        if name == "premise_ui.core.scenario_explorer":
            raise ImportError("explorer backend unavailable")
        return importlib.import_module(name)

    monkeypatch.setattr(scenario_explorer_routes, "import_module", _import_module)

    from premise_ui.api.app import create_app

    client = fastapi_testclient.TestClient(create_app())

    health = client.get("/api/health")
    environment = client.get("/api/environment")
    explorer_catalog = client.get("/api/scenario-explorer/catalog")

    assert health.status_code == 200
    assert environment.status_code == 200
    assert "python_version" in environment.json()
    assert "dialogs" in environment.json()
    assert "native_path_dialogs" in environment.json()["dialogs"]
    assert explorer_catalog.status_code == 503
    assert (
        "Scenario Explorer backend is unavailable" in explorer_catalog.json()["detail"]
    )


def test_environment_route_reports_dialog_capability(monkeypatch):
    fastapi = pytest.importorskip("fastapi")
    fastapi_testclient = pytest.importorskip("fastapi.testclient")
    assert fastapi

    monkeypatch.setattr(
        "premise_ui.api.routes.capabilities.native_dialog_state",
        lambda: {
            "available": False,
            "backend": "tkinter",
            "detail": "No graphical desktop session was detected for native file dialogs.",
            "manual_path_entry": True,
        },
    )

    from premise_ui.api.app import create_app

    client = fastapi_testclient.TestClient(create_app())
    environment = client.get("/api/environment")

    assert environment.status_code == 200
    payload = environment.json()
    assert payload["dialogs"]["native_path_dialogs"] == {
        "available": False,
        "backend": "tkinter",
        "detail": "No graphical desktop session was detected for native file dialogs.",
        "manual_path_entry": True,
    }


def test_brightway_project_switch_route(monkeypatch):
    fastapi = pytest.importorskip("fastapi")
    fastapi_testclient = pytest.importorskip("fastapi.testclient")
    assert fastapi

    class FakeProject:
        def __init__(self, name):
            self.name = name

        def __str__(self):
            return f"Project: {self.name}"

    class FakeProjects:
        def __init__(self):
            self.current = "project-a"
            self._projects = [FakeProject("project-a"), FakeProject("project-b")]

        def __iter__(self):
            return iter(self._projects)

        def set_current(self, name):
            self.current = name

    class FakeBw2data:
        def __init__(self):
            self.projects = FakeProjects()
            self._databases = {
                "project-a": {"legacy-db": object()},
                "project-b": {
                    "biosphere3": object(),
                    "ecoinvent-3.12-cutoff": object(),
                },
            }

        @property
        def databases(self):
            return self._databases[self.projects.current]

    monkeypatch.setitem(sys.modules, "bw2data", FakeBw2data())

    from premise_ui.api.app import create_app

    client = fastapi_testclient.TestClient(create_app())
    discovery = client.post("/api/discovery/brightway")
    response = client.post(
        "/api/discovery/brightway/project",
        json={"project_name": "Project: project-b"},
    )

    assert discovery.status_code == 200
    assert discovery.json()["projects"] == ["project-a", "project-b"]
    assert response.status_code == 200
    assert response.json()["current_project"] == "project-b"
    assert response.json()["projects"] == ["project-a", "project-b"]
    assert response.json()["databases"] == ["biosphere3", "ecoinvent-3.12-cutoff"]


def test_scenario_preview_route_returns_years_and_series(tmp_path):
    fastapi = pytest.importorskip("fastapi")
    fastapi_testclient = pytest.importorskip("fastapi.testclient")
    assert fastapi

    scenario_file = tmp_path / "remind_SSP2-Base.csv"
    scenario_file.write_text(
        "\n".join(
            [
                "model,scenario,region,variable,unit,2020,2030,2040",
                "remind,SSP2-Base,World,Final Energy,EJ/yr,400,430,455",
                "remind,SSP2-Base,World,Electricity,TWh,24000,28000,32000",
            ]
        ),
        encoding="utf-8",
    )

    from premise_ui.api.app import create_app

    client = fastapi_testclient.TestClient(create_app())
    response = client.post(
        "/api/discovery/scenario-preview",
        json={"path": str(scenario_file)},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["inferred_model"] == "remind"
    assert payload["inferred_pathway"] == "SSP2-Base"
    assert payload["years"] == [2020, 2030, 2040]
    assert payload["series"][0]["points"][0] == {"year": 2020, "value": 400.0}
    assert "Final Energy" in payload["series"][0]["label"]


def test_download_all_iam_scenarios_route(monkeypatch):
    fastapi = pytest.importorskip("fastapi")
    fastapi_testclient = pytest.importorskip("fastapi.testclient")
    assert fastapi

    monkeypatch.setattr(
        "premise_ui.api.routes.discovery.start_download_all_known_iam_scenarios",
        lambda: {
            "job_id": "job-1",
            "status": "running",
            "directory": "/tmp/iam_output_files",
            "started_at": utc_now_iso(),
            "updated_at": utc_now_iso(),
            "total_count": 4,
            "processed_count": 1,
            "progress": 0.25,
            "current_file": "image_SSP2-NPi.csv",
            "current_file_progress": 0.5,
            "downloaded": ["remind_SSP2-Base.csv"],
            "existing": [],
            "failed": [],
            "scenarios": [
                {
                    "id": "remind-ssp2-base",
                    "model": "remind",
                    "pathway": "SSP2-Base",
                    "file_name": "remind_SSP2-Base.csv",
                    "path": "/tmp/iam_output_files/remind_SSP2-Base.csv",
                }
            ],
        },
    )

    from premise_ui.api.app import create_app

    client = fastapi_testclient.TestClient(create_app())
    response = client.post("/api/discovery/iam-scenarios/download-all")

    assert response.status_code == 200
    payload = response.json()
    assert payload["job_id"] == "job-1"
    assert payload["status"] == "running"
    assert payload["directory"] == "/tmp/iam_output_files"
    assert payload["downloaded"] == ["remind_SSP2-Base.csv"]
    assert payload["processed_count"] == 1


def test_iam_storylines_route(monkeypatch):
    fastapi = pytest.importorskip("fastapi")
    fastapi_testclient = pytest.importorskip("fastapi.testclient")
    assert fastapi

    monkeypatch.setattr(
        "premise_ui.api.routes.discovery.load_iam_storyline_catalog",
        lambda: {
            "schema_version": 1,
            "updated_at": "2026-03-14",
            "source": "test",
            "storylines": [
                {
                    "id": "remind-ssp2-base",
                    "model": "remind",
                    "pathway": "SSP2-Base",
                    "label": "remind / SSP2-Base",
                    "description": "Test description",
                    "scope_note": "Test note",
                }
            ],
        },
    )

    from premise_ui.api.app import create_app

    client = fastapi_testclient.TestClient(create_app())
    response = client.get("/api/discovery/iam-storylines")

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == 1
    assert payload["storylines"][0]["pathway"] == "SSP2-Base"
    assert payload["storylines"][0]["description"] == "Test description"


def test_download_all_iam_scenarios_status_route(monkeypatch):
    fastapi = pytest.importorskip("fastapi")
    fastapi_testclient = pytest.importorskip("fastapi.testclient")
    assert fastapi

    monkeypatch.setattr(
        "premise_ui.api.routes.discovery.get_download_all_known_iam_scenarios_status",
        lambda job_id: {
            "job_id": job_id,
            "status": "completed",
            "directory": "/tmp/iam_output_files",
            "started_at": utc_now_iso(),
            "updated_at": utc_now_iso(),
            "total_count": 2,
            "processed_count": 2,
            "progress": 1.0,
            "current_file": None,
            "current_file_progress": None,
            "downloaded": ["remind_SSP2-Base.csv"],
            "existing": ["image_SSP2-NPi.mif"],
            "failed": [],
            "scenarios": [],
        },
    )

    from premise_ui.api.app import create_app

    client = fastapi_testclient.TestClient(create_app())
    response = client.get("/api/discovery/iam-scenarios/download-all/job-1")

    assert response.status_code == 200
    assert response.json()["job_id"] == "job-1"
    assert response.json()["status"] == "completed"


def test_clear_iam_scenarios_route(monkeypatch):
    fastapi = pytest.importorskip("fastapi")
    fastapi_testclient = pytest.importorskip("fastapi.testclient")
    assert fastapi

    monkeypatch.setattr(
        "premise_ui.api.routes.discovery.current_download_job",
        lambda: None,
    )
    monkeypatch.setattr(
        "premise_ui.api.routes.discovery.clear_local_iam_scenarios",
        lambda: {
            "directory": "/tmp/iam_output_files",
            "removed": ["remind_SSP2-Base.csv"],
            "removed_count": 1,
            "scenarios": [],
        },
    )

    from premise_ui.api.app import create_app

    client = fastapi_testclient.TestClient(create_app())
    response = client.post("/api/discovery/iam-scenarios/clear")

    assert response.status_code == 200
    assert response.json()["removed_count"] == 1
    assert response.json()["removed"] == ["remind_SSP2-Base.csv"]


def test_scenario_explorer_catalog_route(monkeypatch):
    fastapi = pytest.importorskip("fastapi")
    fastapi_testclient = pytest.importorskip("fastapi.testclient")
    assert fastapi

    monkeypatch.setattr(
        "premise_ui.api.routes.scenario_explorer.get_scenario_explorer_catalog",
        lambda: {
            "scenarios": [
                {
                    "id": "remind-ssp2-base",
                    "model": "remind",
                    "pathway": "SSP2-Base",
                    "file_name": "remind_SSP2-Base.csv",
                    "path": "/tmp/iam_output_files/remind_SSP2-Base.csv",
                }
            ],
            "sectors": [
                {
                    "id": "Electricity - generation",
                    "label": "Electricity - generation",
                    "unit": "Exajoules (EJ)",
                    "explanation": "Generated volumes of electricity.",
                }
            ],
        },
    )

    from premise_ui.api.app import create_app

    client = fastapi_testclient.TestClient(create_app())
    response = client.get("/api/scenario-explorer/catalog")

    assert response.status_code == 200
    payload = response.json()
    assert payload["scenarios"][0]["model"] == "remind"
    assert payload["sectors"][0]["id"] == "Electricity - generation"


def test_scenario_explorer_sector_summary_route(monkeypatch):
    fastapi = pytest.importorskip("fastapi")
    fastapi_testclient = pytest.importorskip("fastapi.testclient")
    assert fastapi

    captured = {}

    def _summarize(scenario_paths, sector, **kwargs):
        captured["scenario_paths"] = scenario_paths
        captured["sector"] = sector
        captured["kwargs"] = kwargs
        return {
            "sector": sector,
            "label": "Exajoules (EJ)",
            "regions": ["World"],
            "variables": ["Biogas CHP"],
            "years": [2020, 2030],
            "scenarios": [
                {
                    "scenario_id": "remind::SSP2-Base::remind_SSP2-Base",
                    "model": "remind",
                    "pathway": "SSP2-Base",
                    "groups": [
                        {
                            "name": "World",
                            "group_type": "region",
                            "series": [
                                {
                                    "variable": "Biogas CHP",
                                    "points": [
                                        {"year": 2020, "value": 1.0},
                                        {"year": 2030, "value": 2.0},
                                    ],
                                }
                            ],
                        }
                    ],
                }
            ],
        }

    monkeypatch.setattr(
        "premise_ui.api.routes.scenario_explorer.summarize_scenario_explorer_sector",
        _summarize,
    )

    from premise_ui.api.app import create_app

    client = fastapi_testclient.TestClient(create_app())
    response = client.post(
        "/api/scenario-explorer/sector-summary",
        json={
            "scenario_paths": ["/tmp/iam_output_files/remind_SSP2-Base.csv"],
            "sector": "Electricity - generation",
            "group_names": ["World"],
            "variables": ["Biogas CHP"],
            "regions": ["World"],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["sector"] == "Electricity - generation"
    assert payload["regions"] == ["World"]
    assert payload["variables"] == ["Biogas CHP"]
    assert captured["kwargs"]["group_names"] == ["World"]
    assert captured["kwargs"]["variables"] == ["Biogas CHP"]
    assert payload["scenarios"][0]["groups"][0]["series"][0]["points"][0] == {
        "year": 2020,
        "value": 1.0,
    }


def test_scenario_explorer_compare_route(monkeypatch):
    fastapi = pytest.importorskip("fastapi")
    fastapi_testclient = pytest.importorskip("fastapi.testclient")
    assert fastapi

    captured = {}

    def _compare(scenario_paths, sector, **kwargs):
        captured["scenario_paths"] = scenario_paths
        captured["sector"] = sector
        captured["kwargs"] = kwargs
        return {
            "compare_mode": kwargs["compare_mode"],
            "baseline_year": kwargs["baseline_year"],
            "baseline_scenario_id": kwargs["baseline_scenario_id"],
            "baseline_scenario_label": "REMIND / SSP2-Base",
            "summary": {
                "sector": sector,
                "scenarios": [
                    {"scenario_id": "remind::SSP2-Base::remind_SSP2-Base"},
                    {"scenario_id": "image::SSP2-NPi::image_SSP2-NPi"},
                ],
            },
        }

    monkeypatch.setattr(
        "premise_ui.api.routes.scenario_explorer.compare_scenario_explorer_sector",
        _compare,
    )

    from premise_ui.api.app import create_app

    client = fastapi_testclient.TestClient(create_app())
    response = client.post(
        "/api/scenario-explorer/compare",
        json={
            "scenario_paths": [
                "/tmp/iam_output_files/remind_SSP2-Base.csv",
                "/tmp/iam_output_files/image_SSP2-NPi.mif",
            ],
            "sector": "Electricity - generation",
            "compare_mode": "delta",
            "baseline_year": 2030,
            "baseline_scenario_id": "remind::SSP2-Base::remind_SSP2-Base",
            "group_names": ["World"],
            "variables": ["Biogas CHP"],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["compare_mode"] == "delta"
    assert payload["baseline_year"] == 2030
    assert payload["baseline_scenario_id"] == "remind::SSP2-Base::remind_SSP2-Base"
    assert len(payload["summary"]["scenarios"]) == 2
    assert captured["sector"] == "Electricity - generation"
    assert captured["kwargs"]["compare_mode"] == "delta"
    assert captured["kwargs"]["baseline_year"] == 2030
    assert captured["kwargs"]["group_names"] == ["World"]
    assert captured["kwargs"]["variables"] == ["Biogas CHP"]
    assert (
        captured["kwargs"]["baseline_scenario_id"]
        == "remind::SSP2-Base::remind_SSP2-Base"
    )


def test_scenario_explorer_compare_route_returns_bad_request_for_invalid_baseline(
    monkeypatch,
):
    fastapi = pytest.importorskip("fastapi")
    fastapi_testclient = pytest.importorskip("fastapi.testclient")
    assert fastapi

    def _raise(*args, **kwargs):
        raise ValueError("Baseline scenario not found: missing")

    monkeypatch.setattr(
        "premise_ui.api.routes.scenario_explorer.compare_scenario_explorer_sector",
        _raise,
    )

    from premise_ui.api.app import create_app

    client = fastapi_testclient.TestClient(create_app())
    response = client.post(
        "/api/scenario-explorer/compare",
        json={
            "scenario_paths": ["/tmp/iam_output_files/remind_SSP2-Base.csv"],
            "sector": "Electricity - generation",
            "compare_mode": "delta",
            "baseline_scenario_id": "missing",
        },
    )

    assert response.status_code == 400
    assert "Baseline scenario not found" in response.json()["detail"]


def test_iam_key_route_persists_without_keyring(tmp_path, monkeypatch):
    fastapi = pytest.importorskip("fastapi")
    fastapi_testclient = pytest.importorskip("fastapi.testclient")
    assert fastapi

    monkeypatch.delenv("IAM_FILES_KEY", raising=False)
    monkeypatch.setattr(
        "premise_ui.core.credentials.CREDENTIALS_FILE",
        tmp_path / "credentials.json",
    )
    monkeypatch.setattr("premise_ui.core.credentials._load_keyring", lambda: None)

    from premise_ui.api.app import create_app

    client = fastapi_testclient.TestClient(create_app())

    saved = client.post(
        "/api/credentials/iam-key",
        json={"value": "secret-key", "remember": True},
    )
    fetched = client.get("/api/credentials/iam-key")
    cleared = client.delete("/api/credentials/iam-key")

    assert saved.status_code == 200
    assert saved.json()["has_value"] is True
    assert saved.json()["backend"] == "file"
    assert saved.json()["persisted"] is True
    assert fetched.status_code == 200
    assert fetched.json()["value"] == "secret-key"
    assert cleared.status_code == 200
    assert cleared.json()["has_value"] is False
    assert not (tmp_path / "credentials.json").exists()


def test_environment_route_reports_saved_iam_key_after_reload(tmp_path, monkeypatch):
    fastapi = pytest.importorskip("fastapi")
    fastapi_testclient = pytest.importorskip("fastapi.testclient")
    assert fastapi

    monkeypatch.delenv("IAM_FILES_KEY", raising=False)
    monkeypatch.setattr(
        "premise_ui.core.credentials.CREDENTIALS_FILE",
        tmp_path / "credentials.json",
    )
    monkeypatch.setattr("premise_ui.core.credentials._load_keyring", lambda: None)

    from premise_ui.api.app import create_app

    client = fastapi_testclient.TestClient(create_app())
    saved = client.post(
        "/api/credentials/iam-key",
        json={"value": "secret-key", "remember": True},
    )
    assert saved.status_code == 200

    monkeypatch.delenv("IAM_FILES_KEY", raising=False)

    reloaded_client = fastapi_testclient.TestClient(create_app())
    environment = reloaded_client.get("/api/environment")

    assert environment.status_code == 200
    assert environment.json()["credentials"]["IAM_FILES_KEY"] is True


def test_create_app_serves_frontend_index():
    fastapi = pytest.importorskip("fastapi")
    fastapi_testclient = pytest.importorskip("fastapi.testclient")
    assert fastapi

    from premise_ui.api.app import create_app

    client = fastapi_testclient.TestClient(create_app())
    response = client.get("/")

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert '<div id="root"></div>' in response.text
    assert "<title>Premise UI</title>" in response.text
    assert (
        "./assets/index-" in response.text
        or "frontend assets are not bundled in this checkout" in response.text.lower()
    )


def test_dialog_path_route(monkeypatch):
    fastapi = pytest.importorskip("fastapi")
    fastapi_testclient = pytest.importorskip("fastapi.testclient")
    assert fastapi

    monkeypatch.setattr(
        "premise_ui.api.routes.dialogs.open_path_dialog",
        lambda **kwargs: "/tmp/example/project.json",
    )

    from premise_ui.api.app import create_app

    client = fastapi_testclient.TestClient(create_app())
    response = client.post(
        "/api/dialogs/path",
        json={
            "mode": "save_file",
            "title": "Choose configuration file",
            "initial_path": "/tmp/example/project.json",
            "default_extension": ".json",
            "filters": [{"label": "JSON files", "pattern": "*.json"}],
        },
    )

    assert response.status_code == 200
    assert response.json()["selected_path"] == "/tmp/example/project.json"
    assert response.json()["cancelled"] is False


def test_dialog_path_route_handles_unavailable_dialog(monkeypatch):
    fastapi = pytest.importorskip("fastapi")
    fastapi_testclient = pytest.importorskip("fastapi.testclient")
    assert fastapi

    from premise_ui.core.dialogs import DialogUnavailableError

    def _raise_dialog_error(**kwargs):
        raise DialogUnavailableError(
            "A native file dialog is unavailable in this environment."
        )

    monkeypatch.setattr(
        "premise_ui.api.routes.dialogs.open_path_dialog",
        _raise_dialog_error,
    )

    from premise_ui.api.app import create_app

    client = fastapi_testclient.TestClient(create_app())
    response = client.post(
        "/api/dialogs/path",
        json={"mode": "open_directory", "title": "Choose export directory"},
    )

    assert response.status_code == 503
    assert "native file dialog" in response.json()["detail"]


def test_diagnostics_run_details_route(tmp_path):
    fastapi = pytest.importorskip("fastapi")
    fastapi_testclient = pytest.importorskip("fastapi.testclient")
    assert fastapi

    project_path = tmp_path / "project.json"
    save_project(project_path, GuiProjectManifest.template())
    run_dir = tmp_path / ".premise-ui" / "runs" / "run-1"
    run_dir.mkdir(parents=True)
    (run_dir / "diagnostics.json").write_text(
        json.dumps(
            {
                "kind": "execution_failed",
                "message": f"Failure at {tmp_path}",
                "traceback": f"Traceback\nFile \"{tmp_path / 'secret.py'}\"",
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "stderr.log").write_text(
        f"stderr path {tmp_path / 'stderr.log'}\n", encoding="utf-8"
    )
    (run_dir / "stdout.log").write_text("stdout ok\n", encoding="utf-8")
    (run_dir / "metadata.json").write_text(
        json.dumps({"run_id": "run-1"}), encoding="utf-8"
    )
    (run_dir / "events.jsonl").write_text(
        json.dumps(
            {
                "timestamp": utc_now_iso(),
                "run_id": "run-1",
                "event_type": "job_failed",
                "level": "error",
                "phase": "execution",
                "message": f"failed at {tmp_path / 'secret.py'}",
                "details": {"exception_type": "RuntimeError"},
            }
        )
        + "\n",
        encoding="utf-8",
    )

    from premise_ui.api.app import create_app

    client = fastapi_testclient.TestClient(create_app())
    response = client.post(
        "/api/diagnostics/run-details",
        json={
            "run_id": "run-1",
            "project_path": str(project_path),
            "run_dir": ".premise-ui/runs/run-1",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["diagnostics"]["kind"] == "execution_failed"
    assert "<path>" in payload["diagnostics"]["traceback"]
    assert "<path>" in payload["stderr_tail"]


def test_support_bundle_route_exports_zip(tmp_path):
    fastapi = pytest.importorskip("fastapi")
    fastapi_testclient = pytest.importorskip("fastapi.testclient")
    assert fastapi

    project = GuiProjectManifest.template()
    project.run_history = [
        {
            "run_id": "run-1",
            "status": "failed",
            "run_dir": ".premise-ui/runs/run-1",
            "project_snapshot": {
                "project_name": "Bundle Project",
                "workflow": "new_database",
                "config": {
                    "export": {
                        "type": "matrices",
                        "options": {"filepath": str(tmp_path / "export")},
                    }
                },
                "scenario_sets": [{"name": "default", "scenarios": []}],
            },
        }
    ]
    project_path = tmp_path / "project.json"
    save_project(project_path, project)

    run_dir = tmp_path / ".premise-ui" / "runs" / "run-1"
    run_dir.mkdir(parents=True)
    (run_dir / "diagnostics.json").write_text(
        json.dumps(
            {
                "kind": "execution_failed",
                "traceback": f"Traceback at {tmp_path / 'secret.py'}",
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "metadata.json").write_text(
        json.dumps({"run_id": "run-1"}), encoding="utf-8"
    )
    (run_dir / "run_manifest.json").write_text(
        json.dumps(
            {
                "run_id": "run-1",
                "project_path": str(project_path),
                "workflow": "new_database",
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "events.jsonl").write_text(
        json.dumps(
            {
                "timestamp": utc_now_iso(),
                "run_id": "run-1",
                "event_type": "job_failed",
                "message": f"failed at {tmp_path / 'secret.py'}",
                "details": {},
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "stdout.log").write_text(
        f"stdout at {tmp_path / 'stdout.log'}\n", encoding="utf-8"
    )
    (run_dir / "stderr.log").write_text(
        f"stderr at {tmp_path / 'stderr.log'}\n", encoding="utf-8"
    )

    from premise_ui.api.app import create_app

    client = fastapi_testclient.TestClient(create_app())
    response = client.get(
        f"/api/jobs/run-1/support-bundle?project_path={project_path}&run_dir=.premise-ui/runs/run-1"
    )

    assert response.status_code == 200
    assert "application/zip" in response.headers["content-type"]

    bundle_path = tmp_path / "bundle.zip"
    bundle_path.write_bytes(response.content)
    with ZipFile(bundle_path) as archive:
        assert sorted(archive.namelist()) == [
            "stderr.log",
            "stdout.log",
            "summary.json",
        ]
        summary = json.loads(archive.read("summary.json"))
        assert summary["run_id"] == "run-1"
        assert "<path>" in summary["diagnostics"]["traceback"]
        assert (
            "<path>"
            in summary["project_config"]["config"]["export"]["options"]["filepath"]
        )
        assert "<path>" in archive.read("stderr.log").decode("utf-8")


def test_artifact_route_returns_run_file(tmp_path):
    fastapi = pytest.importorskip("fastapi")
    fastapi_testclient = pytest.importorskip("fastapi.testclient")
    assert fastapi

    project_path = tmp_path / "project.json"
    save_project(project_path, GuiProjectManifest.template())
    run_dir = tmp_path / ".premise-ui" / "runs" / "run-1"
    run_dir.mkdir(parents=True)
    (run_dir / "run_manifest.json").write_text(
        json.dumps(
            {
                "run_id": "run-1",
                "project_path": str(project_path),
                "workflow": "new_database",
            }
        ),
        encoding="utf-8",
    )
    artifact_path = run_dir / "exports" / "matrix.csv"
    artifact_path.parent.mkdir(parents=True)
    artifact_path.write_text("a,b\n1,2\n", encoding="utf-8")

    from premise_ui.api.app import create_app

    client = fastapi_testclient.TestClient(create_app())
    response = client.get(
        f"/api/jobs/run-1/artifact?path=exports/matrix.csv&project_path={project_path}&run_dir=.premise-ui/runs/run-1"
    )

    assert response.status_code == 200
    assert response.text == "a,b\n1,2\n"
    assert "matrix.csv" in response.headers["content-disposition"]


def test_recents_route_roundtrip(tmp_path, monkeypatch):
    fastapi = pytest.importorskip("fastapi")
    fastapi_testclient = pytest.importorskip("fastapi.testclient")
    assert fastapi

    monkeypatch.setattr(
        "premise_ui.core.recents.RECENTS_FILE", tmp_path / "recents.json"
    )

    from premise_ui.api.app import create_app

    client = fastapi_testclient.TestClient(create_app())

    remember = client.post(
        "/api/recents/remember",
        json={
            "kind": "export_directory",
            "path": "exports",
            "label": "Matrices export",
            "base_path": str(tmp_path / "project.json"),
        },
    )
    listed = client.get("/api/recents")
    cleared = client.delete("/api/recents")

    assert remember.status_code == 200
    assert remember.json()["recent_paths"][0]["kind"] == "export_directory"
    assert listed.status_code == 200
    assert listed.json()["recent_paths"][0]["label"] == "Matrices export"
    assert cleared.status_code == 200
    assert cleared.json() == {"recent_projects": [], "recent_paths": []}


def test_project_run_manifest_route():
    fastapi = pytest.importorskip("fastapi")
    fastapi_testclient = pytest.importorskip("fastapi.testclient")
    assert fastapi

    from premise_ui.api.app import create_app

    client = fastapi_testclient.TestClient(create_app())
    response = client.post(
        "/api/projects/run-manifest",
        json={
            "path": "/tmp/project.json",
            "project": {
                "project_name": "Route Build",
                "workflow": "new_database",
                "config": {
                    "source_db": "ecoinvent-3.12-cutoff",
                    "export": {"type": "matrices"},
                },
                "scenario_sets": [
                    {
                        "name": "default",
                        "scenarios": [
                            {"model": "remind", "pathway": "SSP2-Base", "year": 2030}
                        ],
                    }
                ],
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["run_manifest"]["project_name"] == "Route Build"
    assert payload["run_manifest"]["config"]["export"]["type"] == "matrices"


def test_project_history_route(tmp_path):
    fastapi = pytest.importorskip("fastapi")
    fastapi_testclient = pytest.importorskip("fastapi.testclient")
    assert fastapi

    project = GuiProjectManifest.template()
    project.run_history = [{"run_id": "run-123", "status": "completed"}]
    project_path = tmp_path / "project.json"
    save_project(project_path, project)

    from premise_ui.api.app import create_app

    client = fastapi_testclient.TestClient(create_app())
    response = client.post("/api/projects/history", json={"path": str(project_path)})

    assert response.status_code == 200
    assert response.json()["run_history"][0]["run_id"] == "run-123"


def test_enqueue_project_route(tmp_path, monkeypatch):
    fastapi = pytest.importorskip("fastapi")
    fastapi_testclient = pytest.importorskip("fastapi.testclient")
    assert fastapi

    launched = {}
    project_path = tmp_path / "project.json"
    save_project(project_path, GuiProjectManifest.template())

    monkeypatch.setattr(
        "premise_ui.api.routes.jobs.validate_run_manifest_payload",
        lambda payload: ([], []),
    )

    def fake_popen(command, cwd):
        launched["command"] = command
        launched["cwd"] = str(cwd)
        return DummyProcess()

    monkeypatch.setattr("premise_ui.api.routes.jobs.subprocess.Popen", fake_popen)

    from premise_ui.api.app import create_app

    client = fastapi_testclient.TestClient(create_app())
    response = client.post(
        "/api/jobs/enqueue-project",
        json=_enqueue_project_payload(project_path, "Queue Project"),
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["run_manifest"]["project_name"] == "Queue Project"
    assert payload["dry_run"] is True
    assert payload["status"] == "running"
    assert payload["queue_position"] is None
    assert launched["command"][-1] == "--dry-run"
    assert load_project(project_path).run_history[0]["run_id"] == payload["run_id"]


def test_enqueue_project_route_queues_second_run_until_first_finishes(
    tmp_path, monkeypatch
):
    fastapi = pytest.importorskip("fastapi")
    fastapi_testclient = pytest.importorskip("fastapi.testclient")
    assert fastapi

    launched = []
    processes = []
    project_path_a = tmp_path / "project-a.json"
    project_path_b = tmp_path / "project-b.json"
    save_project(project_path_a, GuiProjectManifest.template())
    save_project(project_path_b, GuiProjectManifest.template())

    monkeypatch.setattr(
        "premise_ui.api.routes.jobs.validate_run_manifest_payload",
        lambda payload: ([], []),
    )

    def fake_popen(command, cwd):
        process = DummyProcess()
        launched.append({"command": command, "cwd": str(cwd)})
        processes.append(process)
        return process

    monkeypatch.setattr("premise_ui.api.routes.jobs.subprocess.Popen", fake_popen)

    from premise_ui.api.app import create_app

    client = fastapi_testclient.TestClient(create_app())
    first = client.post(
        "/api/jobs/enqueue-project",
        json=_enqueue_project_payload(project_path_a, "First"),
    )
    second = client.post(
        "/api/jobs/enqueue-project",
        json=_enqueue_project_payload(project_path_b, "Second"),
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["status"] == "running"
    assert second.json()["status"] == "queued"
    assert second.json()["queue_position"] == 1
    assert len(launched) == 1

    processes[0]._returncode = 0
    status = client.get(f"/api/jobs/{second.json()['run_id']}")

    assert status.status_code == 200
    assert status.json()["status"] == "running"
    assert status.json()["queue_position"] is None
    assert len(launched) == 2
    assert load_project(project_path_a).run_history[0]["status"] == "completed"


def test_cancel_route_cancels_queued_run(tmp_path, monkeypatch):
    fastapi = pytest.importorskip("fastapi")
    fastapi_testclient = pytest.importorskip("fastapi.testclient")
    assert fastapi

    project_path_a = tmp_path / "project-a.json"
    project_path_b = tmp_path / "project-b.json"
    save_project(project_path_a, GuiProjectManifest.template())
    save_project(project_path_b, GuiProjectManifest.template())

    monkeypatch.setattr(
        "premise_ui.api.routes.jobs.validate_run_manifest_payload",
        lambda payload: ([], []),
    )

    monkeypatch.setattr(
        "premise_ui.api.routes.jobs.subprocess.Popen",
        lambda command, cwd: DummyProcess(),
    )

    from premise_ui.api.app import create_app

    client = fastapi_testclient.TestClient(create_app())
    first = client.post(
        "/api/jobs/enqueue-project",
        json=_enqueue_project_payload(project_path_a, "First"),
    )
    second = client.post(
        "/api/jobs/enqueue-project",
        json=_enqueue_project_payload(project_path_b, "Second"),
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert second.json()["status"] == "queued"

    cancel = client.post("/api/jobs/cancel", json={"run_id": second.json()["run_id"]})
    status = client.get(f"/api/jobs/{second.json()['run_id']}")
    run_dir = tmp_path / ".premise-ui" / "runs" / second.json()["run_id"]
    events = read_events(run_dir / "events.jsonl")

    assert cancel.status_code == 200
    assert cancel.json()["status"] == "cancelled"
    assert status.status_code == 200
    assert status.json()["status"] == "cancelled"
    assert any(event["event_type"] == "job_cancelled" for event in events)
    assert load_project(project_path_b).run_history[0]["status"] == "cancelled"
