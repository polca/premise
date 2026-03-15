export const HELP_CONTENT = {
  "section.configuration_setup": {
    title: "Configuration setup",
    description:
      "Top-level settings for naming, saving, previewing, and queueing a Premise configuration.",
  },
  "field.configuration_name": {
    title: "Configuration Name",
    description:
      "Human-readable name for this Premise configuration. It is shown in the UI and stored in the configuration file.",
  },
  "field.workflow": {
    title: "Workflow",
    description:
      "Select which Premise workflow this configuration should run: NewDatabase, IncrementalDatabase, or PathwaysDataPackage.",
  },
  "field.config_path": {
    title: "Config Path",
    description:
      "Location of the JSON configuration file used to save, reopen, clone, and track run history for this setup.",
  },
  "field.queue_as_dry_run": {
    title: "Queue as dry run",
    description:
      "Queue a worker that validates and exercises the run pipeline without executing the heavy Premise transformation workflow.",
  },
  "field.expose_advanced_controls": {
    title: "Expose advanced controls",
    description:
      "Reveal advanced source, execution, and export settings that are usually hidden in the compact view.",
  },
  "section.manifest_preview": {
    title: "Manifest preview",
    description:
      "Backend-generated run payload and validation result that will be frozen and passed to the worker when the run is queued.",
  },
  "section.source_database": {
    title: "Source database",
    description:
      "Choose where Premise should load the original ecoinvent data from before applying IAM-driven transformations.",
  },
  "field.source_type": {
    title: "Source Type",
    description:
      "Choose whether the source data comes from an existing Brightway project or from ecospold files on disk.",
  },
  "field.ecoinvent_version": {
    title: "Ecoinvent Version",
    description:
      "Version of ecoinvent that the source database, inventories, mappings, and exports should be compatible with.",
  },
  "field.system_model": {
    title: "System Model",
    description:
      "Select the ecoinvent system model to use for the source data and for downstream Premise transformations.",
  },
  "field.gains_scenario": {
    title: "GAINS Scenario",
    description:
      "Select the GAINS air-pollution scenario used by Premise where those emissions updates are supported.",
  },
  "field.brightway_project": {
    title: "Brightway Project",
    description:
      "Brightway workspace that contains the source database and, when applicable, receives Brightway exports.",
  },
  "field.brightway_source_database": {
    title: "Brightway Source Database",
    description:
      "Database inside the selected Brightway project that Premise should use as the starting ecoinvent source.",
  },
  "field.brightway_biosphere_database": {
    title: "Brightway Biosphere Database",
    description:
      "Name of the biosphere database present in the selected Brightway project and used by Premise export checks.",
  },
  "field.ecospold_directory": {
    title: "Ecospold Directory",
    description:
      "Directory containing ecospold source files when you are not starting from a Brightway project.",
  },
  "section.additional_inventories": {
    title: "Additional inventories",
    description:
      "Optional extra inventory files that Premise should import and merge into the workflow before export.",
  },
  "field.inventory_file": {
    title: "Inventory File",
    description:
      "Path to an additional inventory workbook or file that should be imported alongside the source database.",
  },
  "field.inventory_ecoinvent_version": {
    title: "Inventory Ecoinvent Version",
    description:
      "Ecoinvent version that the additional inventory file was prepared against.",
  },
  "field.inventory_region_duplicate": {
    title: "Region duplicate",
    description:
      "Allow region duplication behavior for additional inventories when the import helper supports it.",
  },
  "section.advanced_source_execution_settings": {
    title: "Advanced source and execution settings",
    description:
      "Low-level Premise options for caching, uncertainty handling, biosphere naming, reporting, and runtime arguments.",
  },
  "field.system_args": {
    title: "System Args (JSON)",
    description:
      "Raw JSON object of advanced system arguments passed into the selected Premise workflow.",
  },
  "section.scenario_sets": {
    title: "Scenario sets",
    description:
      "Groups of IAM scenarios stored in this configuration. These define the model, pathway, year, and optional file path inputs.",
  },
  "section.scenario_descriptions": {
    title: "Scenario descriptions",
    description:
      "Plain-language storyline notes for the known IAM scenarios, intended to help users understand what each scenario broadly represents before selecting it.",
  },
  "field.set_name": {
    title: "Set Name",
    description:
      "Human-readable name for a scenario set inside the configuration.",
  },
  "field.scenario_storyline": {
    title: "Scenario description selector",
    description:
      "Choose a known scenario family to read its bundled plain-language description and scope note.",
  },
  "field.bulk_add_years": {
    title: "Bulk Add Years",
    description:
      "Comma-separated year list used to clone the current scenario pattern across multiple target years.",
  },
  "section.installed_iam_scenario_files": {
    title: "Installed IAM scenario files",
    description:
      "Scenario files currently available in Premise's local IAM output directory and therefore selectable in the scenario table.",
  },
  "section.scenario_preview": {
    title: "Scenario preview",
    description:
      "Quick inspection panel for a selected IAM file, including inferred metadata and sampled time-series content.",
  },
  "section.scenario_explorer": {
    title: "Scenario Explorer",
    description:
      "Embedded IAM summary explorer for comparing the installed local scenarios by sector, region or sub-scenario, variable, and year range.",
  },
  "field.explorer_scenarios": {
    title: "Explorer Scenarios",
    description:
      "Select one or more installed IAM scenarios to inspect. Selecting multiple scenarios overlays them in the explorer view.",
  },
  "field.explorer_compare_mode": {
    title: "Explorer Compare Mode",
    description:
      "Choose how the selected scenarios should be compared: raw values, values normalized to sum to 100% within each scenario, indexed values, absolute delta against a baseline scenario, or percent change against a baseline scenario.",
  },
  "field.explorer_sector": {
    title: "Explorer Sector",
    description:
      "Choose which IAM summary sector to inspect, using the same sector catalog that powers Premise's Excel summary report.",
  },
  "field.explorer_group": {
    title: "Explorer Group",
    description:
      "Choose one or more regions or battery sub-scenarios to include in the chart and values table. Drag items in the selected column to change plotting order.",
  },
  "field.explorer_variable": {
    title: "Explorer Variable",
    description:
      "Choose one or more IAM variables within the selected sector to plot and compare. Drag selected variables to change the legend and plotting order.",
  },
  "field.explorer_year_start": {
    title: "Explorer Year Start",
    description:
      "Optional lower year bound applied when requesting the scenario summary slice from the backend.",
  },
  "field.explorer_year_end": {
    title: "Explorer Year End",
    description:
      "Optional upper year bound applied when requesting the scenario summary slice from the backend.",
  },
  "field.explorer_baseline_year": {
    title: "Explorer Baseline Year",
    description:
      "Reference year used when the explorer indexes each series to 100. The closest available scenario year is used if the exact year is not present.",
  },
  "field.explorer_baseline_scenario": {
    title: "Explorer Baseline Scenario",
    description:
      "Scenario used as the comparison baseline when displaying delta or percent-change views across multiple installed IAM scenarios.",
  },
  "field.explorer_chart_mode": {
    title: "Explorer Chart Mode",
    description:
      "Switch between line, stacked-area, grouped-bar, and stacked-bar views depending on the sector and comparison style you want to inspect.",
  },
  "field.explorer_plot_layout": {
    title: "Explorer Plot Layout",
    description:
      "Choose whether the selected scenarios are drawn together in one chart or split into one chart per scenario for easier side-by-side reading.",
  },
  "section.pathways_output_years": {
    title: "Pathways output years",
    description:
      "Years for which the PathwaysDataPackage workflow should materialize outputs from the configured scenarios.",
  },
  "section.incremental_sectors": {
    title: "Incremental sectors",
    description:
      "Sector subset used by IncrementalDatabase when you want to limit the run to specific incremental updates.",
  },
  "section.transformation_checklist": {
    title: "Transformation checklist",
    description:
      "Choose which Premise transformations are active. Premise still owns the execution order.",
  },
  "field.run_all_transformations": {
    title: "Run all transformations",
    description:
      "Keep the default behavior where Premise applies every supported transformation instead of a selected subset.",
  },
  "section.selection_summary": {
    title: "Selection summary",
    description:
      "Compact summary of the current transformation or sector scope for the active workflow.",
  },
  "section.export_target": {
    title: "Export target",
    description:
      "Configure the single export target that will be produced when this run finishes.",
  },
  "field.export_type": {
    title: "Export Type",
    description:
      "Choose the output format or destination for the current run, such as Brightway, matrices, datapackage, or other exporters.",
  },
  "field.export_name": {
    title: "Export Name",
    description:
      "Name passed to the selected export method when that exporter supports a named output.",
  },
  "field.export_path": {
    title: "Export Path",
    description:
      "Directory used by path-based export targets when the exporter writes files to disk.",
  },
  "field.difference_file_format": {
    title: "Difference File Format",
    description:
      "File format used for the superstructure scenario-difference artifact.",
  },
  "field.preserve_original_column": {
    title: "Preserve original column",
    description:
      "Keep the original scenario-difference column when writing superstructure comparison outputs.",
  },
  "section.contributors_and_metadata": {
    title: "Contributors and metadata",
    description:
      "Contributor records written into the generated pathways datapackage metadata.",
  },
  "field.contributor_title": {
    title: "Title",
    description:
      "Contributor title stored in datapackage metadata.",
  },
  "field.contributor_name": {
    title: "Name",
    description:
      "Contributor name stored in datapackage metadata.",
  },
  "field.contributor_email": {
    title: "Email",
    description:
      "Contributor email stored in datapackage metadata.",
  },
  "section.current_worker": {
    title: "Current worker",
    description:
      "Live status panel for the run currently being monitored, including queue state, phase progression, and artifact discovery.",
  },
  "section.events": {
    title: "Events",
    description:
      "Structured worker event stream emitted by the backend while a run is queued, executing, or finishing.",
  },
  "field.severity": {
    title: "Severity",
    description:
      "Filter the event list by log level so you can focus on informational, warning, or error messages.",
  },
  "section.artifacts": {
    title: "Artifacts",
    description:
      "Files discovered under the run directory, including exports, reports, logs, and generated support files.",
  },
  "section.run_history": {
    title: "Run history",
    description:
      "Saved run records for the current configuration, including snapshots, statuses, artifacts, and diagnostics references.",
  },
  "section.technical_details": {
    title: "Technical details",
    description:
      "Structured diagnostics payload for the selected run, including metadata, result data, and redacted error information.",
  },
  "section.redacted_logs": {
    title: "Redacted logs",
    description:
      "Tail view of stdout and stderr logs with sensitive paths redacted for support and troubleshooting.",
  },
  "section.support_workflow": {
    title: "Support workflow",
    description:
      "Download a support bundle with diagnostics, events, logs, and metadata for sharing or later inspection.",
  },
  "section.iam_scenario_key": {
    title: "IAM Scenario Key",
    description:
      "Store the decryption key used to access encrypted IAM scenario files when Premise needs to download or open them.",
  },
  "field.iam_files_key": {
    title: "IAM_FILES_KEY",
    description:
      "Stored decryption key value used by the UI worker when Premise needs access to encrypted IAM scenario data.",
  },
  "section.environment": {
    title: "Environment",
    description:
      "Summary of local runtime metadata and which credentials are currently visible to the UI service.",
  },
  "section.recents": {
    title: "Recents",
    description:
      "Recently used configurations and paths remembered across sessions for faster reopening and path reuse.",
  },
} as const;

export type HelpKey = keyof typeof HELP_CONTENT;
