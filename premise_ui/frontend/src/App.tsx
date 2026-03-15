import { startTransition, useDeferredValue, useEffect, useMemo, useRef, useState } from "react";
import type { MouseEvent, ReactNode } from "react";

import { apiRequest } from "./api";
import {
  explorerScenarioPlots,
  explorerSeriesForSelection,
  filterPlotSeriesByYearWindow,
  normalizePlotSeriesToShare,
  plotSeriesValueDomain,
  plotSeriesYearBounds,
} from "./explorerData";
import type { ExplorerChartMode as ExplorerDataChartMode, PlotSeries } from "./explorerData";
import {
  ExplorerSelectionBoard,
  explorerSelectionValues,
  sanitizeExplorerSelection,
} from "./explorerSelection";
import type { ExplorerSelectionMode } from "./explorerSelection";
import { HELP_CONTENT } from "./helpContent";
import type { HelpKey } from "./helpContent";
import {
  ExplorerValuesTable as ExplorerValuesTableWidget,
  FullscreenPlotCard as ExplorerFullscreenPlotCard,
  SeriesPlot as ExplorerSeriesPlot,
} from "./plotWidgets";
import type {
  AdditionalInventory,
  BrightwayDiscoveryPayload,
  CapabilitiesPayload,
  ClearLocalScenariosPayload,
  Contributor,
  DiagnosticsPayload,
  DownloadAllScenariosPayload,
  ExportConfig,
  ExportOptions,
  EnvironmentPayload,
  GuiProjectManifest,
  HealthPayload,
  IamScenarioStoryline,
  IamScenarioStorylineCatalogPayload,
  JobStatusPayload,
  LocalIamScenarioEntry,
  LocalIamScenariosPayload,
  MessageState,
  PathDialogOptions,
  RecentEntry,
  RecentsPayload,
  RunDiagnosticsTarget,
  RunEvent,
  RunHistoryEntry,
  RunManifest,
  Scenario,
  ScenarioExplorerCatalogPayload,
  ScenarioExplorerComparePayload,
  ScenarioExplorerSummaryPayload,
  ScenarioPreviewPayload,
  ScenarioSet,
  StoredCredentialPayload,
  TransformationEntry,
  ValidationPayload,
} from "./types";

type WorkspaceTab =
  | "overview"
  | "source"
  | "scenarios"
  | "explorer"
  | "transformations"
  | "export"
  | "monitor"
  | "history"
  | "troubleshooting";

type LogFilter = "all" | "info" | "warning" | "error";
type WorkspaceMode = "basic" | "expert";

type CompletionNotice = {
  runId: string;
  detail: string;
  outputLocation: string | null;
};

type InitializationProgress = {
  active: boolean;
  completed: number;
  total: number;
  label: string;
  detail: string;
};

type AutosaveSnapshot = {
  saved_at: string;
  project: GuiProjectManifest;
  project_path: string;
  dry_run: boolean;
  workspace_mode: WorkspaceMode;
  active_tab: WorkspaceTab;
};

type ExplorerCompareMode =
  | "single"
  | "share_100"
  | "overlay"
  | "indexed"
  | "delta"
  | "percent_change";

type ExplorerChartMode = ExplorerDataChartMode;
type ExplorerPlotLayout = "combined" | "per_scenario";

type ExplorerUiState = {
  selected_paths: string[];
  selected_sector: string;
  selected_groups: string[];
  selected_variables: string[];
  group_selection_mode: ExplorerSelectionMode;
  variable_selection_mode: ExplorerSelectionMode;
  compare_mode: ExplorerCompareMode;
  baseline_scenario_id: string;
  baseline_year: string;
  year_start: string;
  year_end: string;
  chart_mode: ExplorerChartMode;
  plot_layout: ExplorerPlotLayout;
  hidden_series: string[];
};

type GuideStep = {
  id: string;
  label: string;
  tab: WorkspaceTab;
};

function createInitializationProgress(
  total: number,
  completed: number,
  label: string,
  detail?: string,
): InitializationProgress {
  return {
    active: true,
    completed,
    total,
    label,
    detail: detail ?? `${completed} of ${total} initialization steps complete.`,
  };
}

function loadAutosaveSnapshot(): AutosaveSnapshot | null {
  if (typeof window === "undefined") {
    return null;
  }
  try {
    const raw = window.localStorage.getItem(AUTOSAVE_STORAGE_KEY);
    if (!raw) {
      return null;
    }
    return JSON.parse(raw) as AutosaveSnapshot;
  } catch {
    return null;
  }
}

function persistAutosaveSnapshot(snapshot: AutosaveSnapshot) {
  if (typeof window === "undefined") {
    return;
  }
  try {
    window.localStorage.setItem(AUTOSAVE_STORAGE_KEY, JSON.stringify(snapshot));
  } catch {
    // Ignore local storage failures.
  }
}

function autosaveTimestampLabel(value: string | null): string {
  if (!value) {
    return "Autosave inactive";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return "Draft autosaved";
  }
  return `Draft autosaved ${parsed.toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
  })}`;
}

const VALIDATION_FIELD_HINTS: Record<string, string[]> = {
  project_name: ["configuration name", "`project_name`"],
  source_project: ["brightway project", "`source_project`"],
  source_db: ["brightway source database", "`source_db`", "database"],
  biosphere_name: ["brightway biosphere database", "biosphere"],
  source_file_path: ["ecospold directory", "`source_file_path`"],
  export_type: ["`config.export.type`", "export target"],
  export_name: ["`config.export.options.name`", "export name", "database name"],
  export_path: ["`config.export.options.filepath`", "export path", "directory"],
  scenario_sets: ["no scenarios", "scenario", "pathway", "model"],
  contributors: ["`config.contributors`", "contributor"],
  years: ["`config.years`", "year"],
};

function validationMessagesForField(
  validation: ValidationPayload | null,
  fieldKey: string,
): { errors: string[]; warnings: string[] } {
  if (!validation) {
    return { errors: [], warnings: [] };
  }
  const hints = VALIDATION_FIELD_HINTS[fieldKey] || [];
  const matchesField = (message: string) => {
    const normalized = message.toLowerCase();
    return hints.some((hint) => normalized.includes(hint));
  };
  return {
    errors: validation.errors.filter(matchesField),
    warnings: validation.warnings.filter(matchesField),
  };
}

const EMPTY_RECENTS: RecentsPayload = {
  recent_projects: [],
  recent_paths: [],
};

const EMPTY_BRIGHTWAY_DISCOVERY: BrightwayDiscoveryPayload = {
  available: false,
  current_project: null,
  projects: [],
  databases: [],
};

const EMPTY_CAPABILITIES: CapabilitiesPayload = {
  workflows: [],
  source_types: [],
  export_types: [],
  workflow_export_types: {},
  ecoinvent_versions: [],
  iam_models: [],
  iam_pathways: [],
  iam_scenarios: [],
  iam_scenario_catalog: [],
  transformation_catalog: [],
  incremental_sector_catalog: [],
  premise_version: "",
};

const EMPTY_ENVIRONMENT: EnvironmentPayload = {
  python_version: "",
  platform: "",
  credentials: {},
  dialogs: {
    native_path_dialogs: {
      available: false,
      backend: "none",
      detail: "",
      manual_path_entry: true,
    },
  },
};

const EMPTY_EXPLORER_CATALOG: ScenarioExplorerCatalogPayload = {
  scenarios: [],
  sectors: [],
};

const EMPTY_IAM_STORYLINE_CATALOG: IamScenarioStorylineCatalogPayload = {
  schema_version: 1,
  updated_at: "",
  source: "",
  storylines: [],
};

const EMPTY_STORED_CREDENTIAL: StoredCredentialPayload = {
  name: "IAM_FILES_KEY",
  has_value: false,
  value: "",
  source: "missing",
  persisted: false,
  backend: "missing",
};

const AUTOSAVE_STORAGE_KEY = "premise-ui.autosave.v1";

const ADVANCED_TAB_IDS = new Set<WorkspaceTab>(["history"]);

const GUIDED_WORKFLOW_STEPS: GuideStep[] = [
  { id: "step-source", label: "Source", tab: "source" },
  { id: "step-scenarios", label: "Scenarios", tab: "scenarios" },
  { id: "step-transformations", label: "Transformations", tab: "transformations" },
  { id: "step-export", label: "Export", tab: "export" },
  { id: "step-run", label: "Run", tab: "monitor" },
];

const WORKFLOW_LABELS: Record<string, string> = {
  new_database: "NewDatabase",
  incremental_database: "IncrementalDatabase",
  pathways_datapackage: "PathwaysDataPackage",
};

const WORKFLOW_EXPORT_DEFAULTS: Record<string, Record<string, ExportOptions>> = {
  new_database: {
    brightway: { name: "" },
    matrices: { filepath: "export" },
    datapackage: { name: "" },
    simapro: { filepath: "export/simapro" },
    openlca: { filepath: "export/olca" },
    superstructure: {
      name: "",
      filepath: "export/scenario-diff",
      file_format: "csv",
      preserve_original_column: false,
    },
  },
  incremental_database: {
    brightway: {
      name: "",
      filepath: "export/incremental",
      file_format: "csv",
    },
    matrices: { filepath: "export" },
    simapro: { filepath: "export/simapro" },
    openlca: { filepath: "export/olca" },
  },
  pathways_datapackage: {
    datapackage: { name: "pathways" },
  },
};

const PATH_BASED_EXPORT_TYPES = new Set([
  "matrices",
  "simapro",
  "openlca",
  "superstructure",
  "brightway",
]);

const NAME_BASED_EXPORT_TYPES = new Set([
  "brightway",
  "datapackage",
  "superstructure",
]);

const SUPERSTRUCTURE_FILE_FORMATS = ["csv", "excel", "feather"];

function suppressHelpEvent(event: MouseEvent<HTMLButtonElement>) {
  event.preventDefault();
  event.stopPropagation();
}

function suppressCollapsibleToggle(event: MouseEvent<HTMLElement>) {
  event.preventDefault();
  event.stopPropagation();
}

function HelpTooltip({ helpKey }: { helpKey: HelpKey }) {
  const entry = HELP_CONTENT[helpKey];
  return (
    <span className="help-anchor">
      <button
        type="button"
        className="help-icon"
        aria-label={`Help: ${entry.title}`}
        onMouseDown={suppressHelpEvent}
        onClick={suppressHelpEvent}
      >
        ?
      </button>
      <span className="help-popover" role="tooltip">
        <strong>{entry.title}</strong>
        <span>{entry.description}</span>
      </span>
    </span>
  );
}

function HelpText({
  helpKey,
  children,
  className = "help-inline",
}: {
  helpKey: HelpKey;
  children: ReactNode;
  className?: string;
}) {
  return (
    <span className={className}>
      <span>{children}</span>
      <HelpTooltip helpKey={helpKey} />
    </span>
  );
}

function CollapsiblePanel({
  title,
  subtitle,
  actions,
  defaultOpen = true,
  lazyMount = false,
  children,
}: {
  title: ReactNode;
  subtitle?: ReactNode;
  actions?: ReactNode;
  defaultOpen?: boolean;
  lazyMount?: boolean;
  children: ReactNode;
}) {
  const [open, setOpen] = useState(defaultOpen);

  return (
    <details className="explorer-panel" open={open}>
      <summary
        className="explorer-panel-summary"
        onClick={(event) => {
          event.preventDefault();
          setOpen((current) => !current);
        }}
      >
        <div className="explorer-panel-heading">
          <strong>{title}</strong>
          {subtitle ? <span className="subtle">{subtitle}</span> : null}
        </div>
        {actions ? (
          <div
            className="action-cluster compact-actions"
            onClick={suppressCollapsibleToggle}
            onMouseDown={suppressCollapsibleToggle}
          >
            {actions}
          </div>
        ) : null}
      </summary>
      {!lazyMount || open ? <div className="explorer-panel-body">{children}</div> : null}
    </details>
  );
}

function CollapsibleCard({
  title,
  subtitle,
  actions,
  defaultOpen = true,
  children,
}: {
  title: ReactNode;
  subtitle?: ReactNode;
  actions?: ReactNode;
  defaultOpen?: boolean;
  children: ReactNode;
}) {
  const [open, setOpen] = useState(defaultOpen);

  return (
    <article className="card collapsible-card" data-open={open ? "true" : "false"}>
      <div className="card-head">
        <div>
          <h2>{title}</h2>
          {subtitle ? <p className="subtle">{subtitle}</p> : null}
        </div>
        <div className="action-cluster compact-actions">
          {actions}
          <button
            className="button subtle-button"
            type="button"
            onClick={() => setOpen((current) => !current)}
          >
            {open ? "Collapse" : "Expand"}
          </button>
        </div>
      </div>
      {open ? children : <div className="inline-note">Collapsed to keep this tab lighter.</div>}
    </article>
  );
}

function FieldFeedback({
  validation,
  fieldKey,
  hint,
}: {
  validation: ValidationPayload | null;
  fieldKey: string;
  hint?: string | null;
}) {
  const messages = validationMessagesForField(validation, fieldKey);
  const error = messages.errors[0];
  const warning = messages.warnings[0];

  if (error) {
    return <div className="field-feedback" data-tone="error">{error}</div>;
  }
  if (warning) {
    return <div className="field-feedback" data-tone="warning">{warning}</div>;
  }
  if (hint) {
    return <div className="field-feedback" data-tone="neutral">{hint}</div>;
  }
  return null;
}

function latestEventOfType(events: RunEvent[], eventType: string): RunEvent | null {
  for (let index = events.length - 1; index >= 0; index -= 1) {
    if (events[index].event_type === eventType) {
      return events[index];
    }
  }
  return null;
}

function latestOutputLocation(events: RunEvent[]): string | null {
  return latestEventOfType(events, "output_location")?.message || null;
}

const WORKSPACE_TABS: Array<{
  id: WorkspaceTab;
  label: string;
  marker: string;
  summary: string;
}> = [
  {
    id: "overview",
    label: "Overview",
    marker: "OV",
    summary: "Configuration identity, workflow switch, manifest preview, and queue controls.",
  },
  {
    id: "source",
    label: "Source",
    marker: "01",
    summary: "Source database selection, Brightway or ecospold paths, inventories, and advanced source settings.",
  },
  {
    id: "scenarios",
    label: "Scenarios",
    marker: "02",
    summary: "Scenario sets, custom IAM files, preview metadata, and year helpers.",
  },
  {
    id: "explorer",
    label: "Scenario Explorer",
    marker: "EX",
    summary: "Read-only IAM sector exploration with local scenario overlays, sector explanations, and embedded charts.",
  },
  {
    id: "transformations",
    label: "Transformations",
    marker: "03",
    summary: "Checklist-based selection with workflow-aware defaults and ordering preserved by Premise.",
  },
  {
    id: "export",
    label: "Export",
    marker: "04",
    summary: "Export target, target-specific options, reports, and datapackage metadata.",
  },
  {
    id: "monitor",
    label: "Run Monitor",
    marker: "05",
    summary: "Current run status, queue position, phase events, and artifacts.",
  },
  {
    id: "history",
    label: "History",
    marker: "HI",
    summary: "Saved configuration runs, reusable snapshots, and historical artifacts.",
  },
  {
    id: "troubleshooting",
    label: "Troubleshooting",
    marker: "TS",
    summary: "Diagnostics, redacted logs, and support bundle export.",
  },
];

const HELP_TEXT: Record<
  WorkspaceTab,
  {
    title: string;
    body: string;
    bullets: string[];
  }
> = {
  overview: {
    title: "Configuration workspace",
    body: "The overview tab keeps the run-defining controls in one compact place and shows the exact worker manifest that will be frozen when you queue a job.",
    bullets: [
      "Switch workflows here when you want to reuse the same source and scenario skeleton across NewDatabase, IncrementalDatabase, or PathwaysDataPackage.",
      "Queueing will preview and validate the current form automatically.",
      "The manifest preview is the authoritative payload sent to the worker.",
    ],
  },
  source: {
    title: "Source setup",
    body: "Source settings depend on whether you start from Brightway or ecospold. Brightway-specific requirements only apply when Brightway is selected.",
    bullets: [
      "Use Refresh Brightway after changing projects outside the app.",
      "Additional inventories are stored inside the GUI configuration and resolved relative to the configuration file when possible.",
      "Advanced options expose caching, uncertainty, biosphere, and report controls.",
    ],
  },
  scenarios: {
    title: "Scenario batches",
    body: "Scenario sets let you keep grouped IAM configurations in one configuration while still queueing one frozen run manifest at a time.",
    bullets: [
      "Model and pathway selectors now reflect only the files present in Premise's local IAM output directory.",
      "Use the bulk download button to fetch the known Zenodo scenario files into that directory.",
      "Each scenario can point to a custom IAM file for validation and preview.",
      "Bulk year helpers clone the last scenario in a set over multiple years.",
      "Scenario preview shows inferred model/pathway metadata and sample numeric series from the selected IAM file.",
    ],
  },
  explorer: {
    title: "Scenario Explorer",
    body: "Read-only explorer for understanding what local IAM scenarios contain before using them in a Premise run.",
    bullets: [
      "The explorer reuses the same sector catalog and metadata as Premise's Excel summary report.",
      "Selecting multiple scenarios overlays them in the same chart for direct comparison.",
      "Region or battery sub-scenario, variable, and year range can be narrowed before loading a summary slice.",
    ],
  },
  transformations: {
    title: "Transformation scope",
    body: "Premise owns execution order. The UI only chooses which sectors are active and keeps the default “all sectors” path available.",
    bullets: [
      "NewDatabase and Pathways use the full transformation catalog.",
      "IncrementalDatabase uses the incremental sector catalog instead.",
      "Empty incremental sector selection means “run all incremental sectors”.",
    ],
  },
  export: {
    title: "Export surface",
    body: "Each run produces exactly one export target. The visible options adapt to the selected workflow and the selected export type.",
    bullets: [
      "Brightway is the default export target for NewDatabase and IncrementalDatabase.",
      "Superstructure is only offered when at least two scenarios are configured.",
      "Brightway and datapackage names can be left blank when Premise can generate them.",
      "Pathways contributor metadata is entered here because it affects the generated datapackage.",
    ],
  },
  monitor: {
    title: "Run monitor",
    body: "Only one worker runs at a time. Additional jobs wait in the local queue and keep their frozen run configuration.",
    bullets: [
      "The event view can be filtered by severity.",
      "Cancelling keeps logs and partial files for debugging.",
      "Artifacts remain downloadable after completion.",
    ],
  },
  history: {
    title: "Configuration history",
    body: "Saved configurations record queued and completed runs together with artifact references and a reusable configuration snapshot.",
    bullets: [
      "Use Config loads the frozen snapshot from a prior run back into the editor.",
      "Details opens historical diagnostics without rerunning the job.",
      "Artifacts listed here are the ones recorded in the saved configuration file.",
    ],
  },
  troubleshooting: {
    title: "Troubleshooting",
    body: "This tab is the support surface for failed or suspicious runs. It exposes redacted diagnostics, logs, and support-bundle export.",
    bullets: [
      "Support bundles include metadata, diagnostics, events, and redacted logs.",
      "Diagnostics can be loaded for the active run or for a historical run from configuration history.",
      "Absolute paths are redacted in the exported support bundle.",
    ],
  },
};

const FALLBACK_SCENARIO: Scenario = {
  model: "remind",
  pathway: "SSP2-Base",
  year: 2030,
  filepath: "",
};

const FALLBACK_PROJECTS: Record<string, GuiProjectManifest> = {
  new_database: {
    schema_version: 1,
    project_name: "Untitled Premise Configuration",
    workflow: "new_database",
    config: {
      source_type: "brightway",
      source_version: "3.12",
      source_project: "",
      source_db: "",
      source_file_path: "",
      system_model: "cutoff",
      system_args: {},
      use_cached_inventories: true,
      use_cached_database: true,
      quiet: false,
      keep_imports_uncertainty: true,
      keep_source_db_uncertainty: false,
      gains_scenario: "CLE",
      use_absolute_efficiency: false,
      biosphere_name: "biosphere3",
      generate_reports: true,
      additional_inventories: [],
      transformations: null,
      export: {
        type: "brightway",
        options: { name: "" },
      },
    },
    scenario_sets: [
      {
        name: "default",
        scenarios: [FALLBACK_SCENARIO],
      },
    ],
    run_history: [],
    ui_state: {},
  },
  incremental_database: {
    schema_version: 1,
    project_name: "Untitled Premise Configuration",
    workflow: "incremental_database",
    config: {
      source_type: "brightway",
      source_version: "3.12",
      source_project: "",
      source_db: "",
      source_file_path: "",
      system_model: "cutoff",
      system_args: {},
      use_cached_inventories: true,
      use_cached_database: true,
      quiet: false,
      keep_imports_uncertainty: true,
      keep_source_db_uncertainty: false,
      gains_scenario: "CLE",
      use_absolute_efficiency: false,
      biosphere_name: "biosphere3",
      generate_reports: true,
      additional_inventories: [],
      transformations: null,
      sectors: [],
      export: {
        type: "brightway",
        options: {
          name: "",
          filepath: "export/incremental",
          file_format: "csv",
        },
      },
    },
    scenario_sets: [
      {
        name: "default",
        scenarios: [FALLBACK_SCENARIO],
      },
    ],
    run_history: [],
    ui_state: {},
  },
  pathways_datapackage: {
    schema_version: 1,
    project_name: "Untitled Premise Configuration",
    workflow: "pathways_datapackage",
    config: {
      source_type: "brightway",
      source_version: "3.12",
      source_project: "",
      source_db: "",
      source_file_path: "",
      system_model: "cutoff",
      system_args: {},
      use_cached_inventories: true,
      use_cached_database: true,
      quiet: false,
      keep_imports_uncertainty: true,
      keep_source_db_uncertainty: false,
      gains_scenario: "CLE",
      use_absolute_efficiency: false,
      biosphere_name: "biosphere3",
      generate_reports: true,
      additional_inventories: [],
      transformations: null,
      years: [2030, 2040, 2050],
      contributors: [{ title: "", name: "", email: "" }],
      export: {
        type: "datapackage",
        options: { name: "pathways" },
      },
    },
    scenario_sets: [
      {
        name: "default",
        scenarios: [FALLBACK_SCENARIO],
      },
    ],
    run_history: [],
    ui_state: {},
  },
};

type ExplorerPerformanceMetrics = {
  lastReloadMs: number | null;
  lastFilterMs: number | null;
  lastChartPaintMs: number | null;
};

function cloneJson<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

function performanceNow(): number {
  if (typeof performance !== "undefined" && typeof performance.now === "function") {
    return performance.now();
  }
  return Date.now();
}

function normalizeStringList(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return Array.from(
    new Set(value.filter((entry): entry is string => typeof entry === "string" && entry.trim())),
  );
}

function stringListKey(values: string[]): string {
  return values.join("\u0001");
}

function reconciledLocalScenarioFiles(
  capabilitiesScenarios: LocalIamScenarioEntry[] | null | undefined,
  explorerScenarios: LocalIamScenarioEntry[] | null | undefined,
): LocalIamScenarioEntry[] {
  if (explorerScenarios?.length) {
    return explorerScenarios;
  }
  return capabilitiesScenarios || [];
}

function capabilitiesWithLocalScenarios(
  payload: CapabilitiesPayload,
  localScenarios: LocalIamScenarioEntry[],
): CapabilitiesPayload {
  return {
    ...payload,
    iam_scenarios: localScenarios,
    iam_models: uniqueStrings(localScenarios.map((entry) => entry.model)),
    iam_pathways: uniqueStrings(localScenarios.map((entry) => entry.pathway)),
  };
}

function catalogWithLocalScenarios(
  payload: ScenarioExplorerCatalogPayload,
  localScenarios: LocalIamScenarioEntry[],
): ScenarioExplorerCatalogPayload {
  return {
    ...payload,
    scenarios: localScenarios,
  };
}

function normalizeBrightwayProjectName(value?: string | null): string {
  if (!value) {
    return "";
  }
  return value.startsWith("Project: ") ? value.slice("Project: ".length) : value;
}

function normalizeScenario(scenario?: Partial<Scenario>): Scenario {
  return {
    model: scenario?.model || FALLBACK_SCENARIO.model,
    pathway: scenario?.pathway || FALLBACK_SCENARIO.pathway,
    year: Number(scenario?.year || FALLBACK_SCENARIO.year),
    filepath: typeof scenario?.filepath === "string" ? scenario.filepath : "",
  };
}

function normalizeScenarioSet(scenarioSet?: Partial<ScenarioSet>): ScenarioSet {
  const scenarios =
    scenarioSet?.scenarios && scenarioSet.scenarios.length
      ? scenarioSet.scenarios.map((scenario) => normalizeScenario(scenario))
      : [normalizeScenario()];

  return {
    name: scenarioSet?.name || "default",
    scenarios,
  };
}

function normalizeScenarioSets(value?: ScenarioSet[]): ScenarioSet[] {
  if (!Array.isArray(value) || !value.length) {
    return [normalizeScenarioSet()];
  }
  return value.map((entry) => normalizeScenarioSet(entry));
}

function normalizeTransformationSelection(
  value: GuiProjectManifest["config"]["transformations"],
): string[] | null {
  if (value == null || value === "") {
    return null;
  }
  if (typeof value === "string") {
    return [value];
  }
  if (!Array.isArray(value)) {
    return null;
  }
  const cleaned = value
    .filter((entry): entry is string => typeof entry === "string" && entry.trim().length > 0)
    .map((entry) => entry.trim());
  return cleaned.length ? cleaned : [];
}

function normalizeAdditionalInventory(
  inventory?: Partial<AdditionalInventory>,
): AdditionalInventory {
  return {
    filepath: typeof inventory?.filepath === "string" ? inventory.filepath : "",
    "ecoinvent version":
      typeof inventory?.["ecoinvent version"] === "string"
        ? inventory["ecoinvent version"]
        : "3.12",
    region_duplicate: Boolean(inventory?.region_duplicate),
  };
}

function normalizeContributor(contributor?: Partial<Contributor>): Contributor {
  return {
    title: typeof contributor?.title === "string" ? contributor.title : "",
    name: typeof contributor?.name === "string" ? contributor.name : "",
    email: typeof contributor?.email === "string" ? contributor.email : "",
  };
}

function normalizeYears(value?: number[]): number[] {
  if (!Array.isArray(value) || !value.length) {
    return [2030, 2040, 2050];
  }
  const cleaned = Array.from(
    new Set(
      value
        .map((entry) => Number(entry))
        .filter((entry) => Number.isInteger(entry) && entry >= 2005 && entry <= 2100),
    ),
  ).sort((left, right) => left - right);
  return cleaned.length ? cleaned : [2030, 2040, 2050];
}

function normalizeExportConfig(
  workflow: string,
  exportConfig?: Partial<ExportConfig>,
): ExportConfig {
  const workflowDefaults =
    WORKFLOW_EXPORT_DEFAULTS[workflow] || WORKFLOW_EXPORT_DEFAULTS.new_database;
  const defaultExportType = Object.keys(workflowDefaults)[0];
  const exportType =
    exportConfig?.type && exportConfig.type in workflowDefaults
      ? exportConfig.type
      : defaultExportType;

  const normalizedOptions: ExportOptions = {
    ...workflowDefaults[exportType],
  };

  const sourceOptions = exportConfig?.options ?? {};
  for (const [key, value] of Object.entries(sourceOptions)) {
    if (key in normalizedOptions) {
      normalizedOptions[key] = value;
    }
  }

  return {
    type: exportType,
    options: normalizedOptions,
  };
}

function uniqueStrings(values: string[]): string[] {
  return Array.from(new Set(values.filter((entry) => entry.trim().length > 0)));
}

function workflowTemplate(workflow: string): GuiProjectManifest {
  return cloneJson(FALLBACK_PROJECTS[workflow] || FALLBACK_PROJECTS.new_database);
}

function normalizeProject(project: GuiProjectManifest): GuiProjectManifest {
  const workflow =
    typeof project.workflow === "string" && project.workflow in FALLBACK_PROJECTS
      ? project.workflow
      : "new_database";
  const template = workflowTemplate(workflow);
  const config = project.config ?? template.config;
  const contributors =
    Array.isArray(config.contributors) && config.contributors.length
      ? config.contributors.map((entry) => normalizeContributor(entry))
      : template.config.contributors
        ? template.config.contributors.map((entry) => normalizeContributor(entry))
        : undefined;
  const normalizedConfig: GuiProjectManifest["config"] = {
    ...template.config,
    ...config,
    source_project: normalizeBrightwayProjectName(config.source_project),
    system_args:
      config.system_args && typeof config.system_args === "object"
        ? cloneJson(config.system_args as Record<string, unknown>)
        : {},
    additional_inventories: Array.isArray(config.additional_inventories)
      ? config.additional_inventories.map((entry) =>
          normalizeAdditionalInventory(entry),
        )
      : [],
    transformations: normalizeTransformationSelection(config.transformations),
    export: normalizeExportConfig(workflow, config.export),
  };

  if (workflow === "incremental_database") {
    normalizedConfig.sectors = Array.isArray(config.sectors)
      ? config.sectors.filter((entry): entry is string => typeof entry === "string")
      : [];
  } else {
    delete normalizedConfig.sectors;
  }

  if (workflow === "pathways_datapackage") {
    normalizedConfig.years = normalizeYears(config.years);
    normalizedConfig.contributors = contributors;
  } else {
    delete normalizedConfig.years;
    delete normalizedConfig.contributors;
  }

  return {
    ...template,
    ...project,
    workflow,
    config: normalizedConfig,
    scenario_sets: normalizeScenarioSets(project.scenario_sets),
    run_history: Array.isArray(project.run_history) ? project.run_history : [],
    ui_state:
      project.ui_state && typeof project.ui_state === "object" ? project.ui_state : {},
  };
}

function normalizeExplorerUiState(value: unknown): ExplorerUiState {
  const source =
    value && typeof value === "object" ? (value as Record<string, unknown>) : {};
  const compareMode =
    typeof source.compare_mode === "string"
      ? (source.compare_mode as ExplorerCompareMode)
      : "single";
  const selectedGroups = normalizeStringList(source.selected_groups);
  const selectedVariables = normalizeStringList(source.selected_variables);
  const chartMode =
    source.chart_mode === "stacked_area" ||
    source.chart_mode === "stacked_bar" ||
    source.chart_mode === "bar"
      ? (source.chart_mode as ExplorerChartMode)
      : "line";
  const groupSelectionMode =
    source.group_selection_mode === "all" ||
    source.group_selection_mode === "custom" ||
    source.group_selection_mode === "none"
      ? (source.group_selection_mode as ExplorerSelectionMode)
      : selectedGroups.length
        ? "custom"
        : "all";
  const variableSelectionMode =
    source.variable_selection_mode === "all" ||
    source.variable_selection_mode === "custom" ||
    source.variable_selection_mode === "none"
      ? (source.variable_selection_mode as ExplorerSelectionMode)
      : selectedVariables.length
        ? "custom"
        : "all";
  const plotLayout =
    source.plot_layout === "per_scenario" || source.plot_layout === "combined"
      ? (source.plot_layout as ExplorerPlotLayout)
      : "per_scenario";

  return {
    selected_paths: normalizeStringList(source.selected_paths),
    selected_sector:
      typeof source.selected_sector === "string" ? source.selected_sector : "",
    selected_groups: selectedGroups,
    selected_variables: selectedVariables,
    group_selection_mode: groupSelectionMode,
    variable_selection_mode: variableSelectionMode,
    compare_mode:
      compareMode === "share_100" ||
      compareMode === "overlay" ||
      compareMode === "indexed" ||
      compareMode === "delta" ||
      compareMode === "percent_change"
        ? compareMode
        : "single",
    baseline_scenario_id:
      typeof source.baseline_scenario_id === "string" ? source.baseline_scenario_id : "",
    baseline_year: typeof source.baseline_year === "string" ? source.baseline_year : "",
    year_start: typeof source.year_start === "string" ? source.year_start : "",
    year_end: typeof source.year_end === "string" ? source.year_end : "",
    chart_mode: chartMode,
    plot_layout: plotLayout,
    hidden_series: normalizeStringList(source.hidden_series),
  };
}

function explorerUiStateFromProject(project: GuiProjectManifest): ExplorerUiState {
  const uiState =
    project.ui_state && typeof project.ui_state === "object"
      ? (project.ui_state as Record<string, unknown>)
      : {};
  return normalizeExplorerUiState(uiState.explorer);
}

function projectWithExplorerUiState(
  project: GuiProjectManifest,
  explorerUiState: ExplorerUiState,
): GuiProjectManifest {
  return normalizeProject({
    ...project,
    ui_state: {
      ...(project.ui_state && typeof project.ui_state === "object" ? project.ui_state : {}),
      explorer: cloneJson(explorerUiState),
    },
  });
}

function mergeProjectIntoTemplate(
  templateProject: GuiProjectManifest,
  currentProject: GuiProjectManifest,
): GuiProjectManifest {
  const next = normalizeProject(templateProject);
  const current = normalizeProject(currentProject);
  const sharedConfigKeys = [
    "source_type",
    "source_version",
    "source_project",
    "source_db",
    "source_file_path",
    "system_model",
    "system_args",
    "use_cached_inventories",
    "use_cached_database",
    "quiet",
    "keep_imports_uncertainty",
    "keep_source_db_uncertainty",
    "gains_scenario",
    "use_absolute_efficiency",
    "biosphere_name",
    "generate_reports",
    "additional_inventories",
  ] as const;

  next.project_name = current.project_name;
  next.scenario_sets = cloneJson(current.scenario_sets);
  next.ui_state = cloneJson(current.ui_state);

  for (const key of sharedConfigKeys) {
    next.config[key] = cloneJson(current.config[key]);
  }

  if (next.workflow !== "incremental_database") {
    next.config.transformations = cloneJson(current.config.transformations);
  }

  if (next.workflow === "incremental_database") {
    next.config.sectors = Array.isArray(current.config.sectors)
      ? cloneJson(current.config.sectors)
      : [];
  }

  if (next.workflow === "pathways_datapackage") {
    next.config.transformations = cloneJson(current.config.transformations);
    next.config.years = normalizeYears(current.config.years);
    next.config.contributors =
      Array.isArray(current.config.contributors) && current.config.contributors.length
        ? current.config.contributors.map((entry) => normalizeContributor(entry))
        : [{ title: "", name: "", email: "" }];
  }

  const currentExport = current.config.export;
  if (
    currentExport?.type &&
    currentExport.type in (WORKFLOW_EXPORT_DEFAULTS[next.workflow] || {})
  ) {
    next.config.export = normalizeExportConfig(next.workflow, currentExport);
  }

  return normalizeProject(next);
}

function nextExportOptions(
  workflow: string,
  exportType: string,
  currentOptions: ExportOptions,
): ExportOptions {
  const normalized = normalizeExportConfig(workflow, { type: exportType }).options;

  for (const key of Object.keys(normalized)) {
    if (key in currentOptions) {
      normalized[key] = currentOptions[key];
    }
  }

  return normalized;
}

function workflowLabel(workflow: string): string {
  return WORKFLOW_LABELS[workflow] || workflow;
}

function exportTypeLabel(exportType: string): string {
  switch (exportType) {
    case "brightway":
      return "Brightway";
    case "matrices":
      return "Matrices";
    case "datapackage":
      return "Datapackage";
    case "simapro":
      return "SimaPro";
    case "openlca":
      return "OpenLCA";
    case "superstructure":
      return "Superstructure";
    default:
      return exportType;
  }
}

function exportPathLabel(exportType: string): string {
  switch (exportType) {
    case "matrices":
      return "Matrices Export Path";
    case "simapro":
      return "SimaPro Export Path";
    case "openlca":
      return "OpenLCA Export Path";
    case "superstructure":
      return "Scenario Difference Export Path";
    case "brightway":
      return "Incremental Export Directory";
    default:
      return "Export Path";
  }
}

function exportPathPlaceholder(workflow: string, exportType: string): string {
  const value = WORKFLOW_EXPORT_DEFAULTS[workflow]?.[exportType]?.filepath;
  return typeof value === "string" && value ? value : "export";
}

function exportNameLabel(exportType: string): string {
  switch (exportType) {
    case "brightway":
      return "Brightway Database Name";
    case "datapackage":
      return "Datapackage Name";
    case "superstructure":
      return "Superstructure Database Name";
    default:
      return "Export Name";
  }
}

function exportNamePlaceholder(exportType: string): string {
  switch (exportType) {
    case "brightway":
      return "Leave blank to use Premise-generated names";
    case "datapackage":
      return "Leave blank to use Premise-generated package name";
    case "superstructure":
      return "Leave blank to use Premise-generated database name";
    default:
      return "";
  }
}

function buildProjectPayload(
  project: GuiProjectManifest,
  runHistory: RunHistoryEntry[],
): GuiProjectManifest {
  return {
    ...project,
    run_history: runHistory,
    ui_state: project.ui_state ?? {},
  };
}

function looksAbsolutePath(value: string): boolean {
  return value.startsWith("/") || /^[A-Za-z]:[\\/]/.test(value);
}

function formatTimestamp(value?: string): string {
  if (!value) {
    return "";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString();
}

function formatRunState(status: string, queuePosition: number | null): string {
  if (status === "queued" && queuePosition) {
    return `queued #${queuePosition}`;
  }
  return status;
}

function isTerminalRunStatus(status: string): boolean {
  return ["completed", "failed", "cancelled"].includes(status);
}

const JOB_PHASE_PROGRESS: Record<string, number> = {
  queue: 0.06,
  bootstrap: 0.14,
  validation: 0.28,
  instantiate: 0.42,
  build: 0.56,
  transform: 0.62,
  dry_run: 0.72,
  execution: 0.72,
  export: 0.84,
  output: 0.92,
  finalize: 1.0,
};

const JOB_PHASE_LABELS: Record<string, string> = {
  queue: "Queued",
  bootstrap: "Bootstrapping",
  validation: "Validating",
  instantiate: "Preparing workflow",
  build: "Building outputs",
  transform: "Applying transformations",
  dry_run: "Dry run",
  execution: "Executing workflow",
  export: "Exporting outputs",
  output: "Finalizing outputs",
  finalize: "Finishing",
};

function currentPhaseEvent(events: RunEvent[]): RunEvent | null {
  for (let index = events.length - 1; index >= 0; index -= 1) {
    if (events[index].phase) {
      return events[index];
    }
  }
  return null;
}

function jobProgressState(
  runId: string | null,
  status: string,
  queuePosition: number | null,
  events: RunEvent[],
): {
  visible: boolean;
  percent: number;
  label: string;
  detail: string;
  tone: "neutral" | "warning" | "success" | "error";
  active: boolean;
} {
  if (!runId) {
    return {
      visible: false,
      percent: 0,
      label: "",
      detail: "",
      tone: "neutral",
      active: false,
    };
  }

  const latestPhase = currentPhaseEvent(events);
  const latestMessage = latestPhase?.message || events[events.length - 1]?.message || "";

  if (status === "queued") {
    return {
      visible: true,
      percent: 6,
      label: queuePosition ? `Queued #${queuePosition}` : "Queued",
      detail: latestMessage || "Waiting for the current worker to finish.",
      tone: "neutral",
      active: true,
    };
  }

  if (status === "cancelling") {
    return {
      visible: true,
      percent: 96,
      label: "Cancelling",
      detail: latestMessage || "Waiting for the worker process to stop.",
      tone: "warning",
      active: true,
    };
  }

  if (status === "completed") {
    return {
      visible: true,
      percent: 100,
      label: "Completed",
      detail: latestMessage || "Workflow execution completed successfully.",
      tone: "success",
      active: false,
    };
  }

  if (status === "failed") {
    return {
      visible: true,
      percent: 100,
      label: "Failed",
      detail: latestMessage || "Workflow execution failed.",
      tone: "error",
      active: false,
    };
  }

  if (status === "cancelled") {
    return {
      visible: true,
      percent: 100,
      label: "Cancelled",
      detail: latestMessage || "Run cancelled.",
      tone: "warning",
      active: false,
    };
  }

  const phase = latestPhase?.phase || "bootstrap";
  let percent = Math.round((JOB_PHASE_PROGRESS[phase] || 0.18) * 100);
  if (latestPhase?.event_type === "phase_completed" && percent < 96) {
    percent = Math.min(96, percent + 4);
  }
  const orderedPhases = Object.keys(JOB_PHASE_PROGRESS);
  const phaseIndex = Math.max(orderedPhases.indexOf(phase), 0);
  const remainingCount = Math.max(orderedPhases.length - phaseIndex - 1, 0);
  const phaseLabel = JOB_PHASE_LABELS[phase] || "Running";

  return {
    visible: true,
    percent,
    label: phaseLabel,
    detail: latestMessage
      ? `${latestMessage} Phase ${phaseIndex + 1} of ${orderedPhases.length}; ${remainingCount} remaining.`
      : `${phaseLabel}. Phase ${phaseIndex + 1} of ${orderedPhases.length}; ${remainingCount} remaining.`,
    tone: "neutral",
    active: true,
  };
}

function formatEventLine(event: RunEvent): string {
  const phase = event.phase ? ` / ${event.phase}` : "";
  return `[${event.timestamp}] ${event.event_type}${phase} :: ${event.message || ""}`;
}

function eventMatchesFilter(event: RunEvent, filter: LogFilter): boolean {
  if (filter === "all") {
    return true;
  }

  const level = (event.level || "info").toLowerCase();
  if (filter === "warning") {
    return level === "warning" || level === "error";
  }
  return level === filter;
}

function buildSupportBundleUrl(target: RunDiagnosticsTarget | null): string | null {
  if (!target?.run_id) {
    return null;
  }

  const params = new URLSearchParams();
  if (target.project_path) {
    params.set("project_path", target.project_path);
  }
  if (target.run_dir) {
    params.set("run_dir", target.run_dir);
  }

  const query = params.toString();
  return `/api/jobs/${encodeURIComponent(target.run_id)}/support-bundle${query ? `?${query}` : ""}`;
}

function buildSupportEmailUrl(target: RunDiagnosticsTarget | null): string | null {
  if (!target?.run_id) {
    return null;
  }

  const subject = `Premise support request for run ${target.run_id}`;
  const bodyLines = [
    "Hello Romain,",
    "",
    "I would like help with this Premise run.",
    "",
    `Run ID: ${target.run_id}`,
    target.run_dir ? `Run directory: ${target.run_dir}` : null,
    "",
    "The support bundle download should open alongside this email draft.",
    "Please attach the downloaded bundle before sending.",
  ].filter(Boolean);

  const params = new URLSearchParams({
    subject,
    body: bodyLines.join("\n"),
  });
  return `mailto:romain.sacchi@psi.ch?${params.toString()}`;
}

function buildIamKeyRequestEmailUrl(): string {
  const params = new URLSearchParams({
    subject: "Request for Premise IAM scenario decryption key",
    body: [
      "Hello Romain,",
      "",
      "I would like to request access to the Premise IAM scenario decryption key.",
      "",
      "Best regards,",
    ].join("\n"),
  });
  return `mailto:romain.sacchi@psi.ch?${params.toString()}`;
}

function buildArtifactUrl(
  target: RunDiagnosticsTarget | null,
  artifactPath: string,
): string | null {
  if (!target?.run_id) {
    return null;
  }

  const params = new URLSearchParams();
  params.set("path", artifactPath);
  if (target.project_path) {
    params.set("project_path", target.project_path);
  }
  if (target.run_dir) {
    params.set("run_dir", target.run_dir);
  }
  return `/api/jobs/${encodeURIComponent(target.run_id)}/artifact?${params.toString()}`;
}

function buildHistoryTarget(
  entry: RunHistoryEntry,
  projectPath: string,
): RunDiagnosticsTarget | null {
  if (!entry.run_id) {
    return null;
  }

  return {
    run_id: entry.run_id,
    project_path: projectPath || null,
    run_dir: entry.run_dir || null,
  };
}

function scenarioKey(setIndex: number, scenarioIndex: number): string {
  return `${setIndex}:${scenarioIndex}`;
}

function parseScenarioKey(value: string | null): { setIndex: number; scenarioIndex: number } | null {
  if (!value) {
    return null;
  }
  const [setPart, scenarioPart] = value.split(":").map((entry) => Number(entry));
  if (!Number.isInteger(setPart) || !Number.isInteger(scenarioPart)) {
    return null;
  }
  return { setIndex: setPart, scenarioIndex: scenarioPart };
}

function parseYearBatch(value: string): number[] {
  return Array.from(
    new Set(
      value
        .split(",")
        .map((entry) => Number(entry.trim()))
        .filter((entry) => Number.isInteger(entry) && entry >= 2005 && entry <= 2100),
    ),
  ).sort((left, right) => left - right);
}

function runSummaryLabel(entry: RunHistoryEntry): string {
  return `${entry.workflow || "workflow"} / ${entry.export_type || "export"} / ${entry.scenario_count || 0} scenario${entry.scenario_count === 1 ? "" : "s"}`;
}

function knownScenarioPathways(
  scenarios: LocalIamScenarioEntry[],
  model: string,
  fallbackPathways: string[],
  currentPathway: string,
): string[] {
  const catalogPathways = uniqueStrings(
    scenarios
      .filter((entry) => entry.model === model)
      .map((entry) => entry.pathway),
  );

  if (catalogPathways.length) {
    if (catalogPathways.includes(currentPathway)) {
      return uniqueStrings([currentPathway, ...catalogPathways]);
    }
    return catalogPathways;
  }

  return uniqueStrings([currentPathway, ...fallbackPathways]);
}

function nextMonitorCandidate(
  history: RunHistoryEntry[],
  currentRunId: string | null,
): RunHistoryEntry | null {
  const statusRank: Record<string, number> = {
    running: 0,
    cancelling: 1,
    queued: 2,
  };

  const candidates = history.filter((entry) => {
    const status = String(entry.status || "");
    return entry.run_id !== currentRunId && status in statusRank;
  });

  if (!candidates.length) {
    return null;
  }

  return [...candidates].sort((left, right) => {
    const leftRank = statusRank[String(left.status || "")] ?? Number.MAX_SAFE_INTEGER;
    const rightRank = statusRank[String(right.status || "")] ?? Number.MAX_SAFE_INTEGER;
    if (leftRank !== rightRank) {
      return leftRank - rightRank;
    }
    const leftTime = new Date(left.created_at || 0).getTime();
    const rightTime = new Date(right.created_at || 0).getTime();
    return leftTime - rightTime;
  })[0];
}

function syncProjectScenariosToAvailableFiles(
  project: GuiProjectManifest,
  localScenarios: LocalIamScenarioEntry[],
): GuiProjectManifest {
  if (!localScenarios.length) {
    return normalizeProject(project);
  }

  const nextProject = normalizeProject(project);
  nextProject.scenario_sets = normalizeScenarioSets(nextProject.scenario_sets).map((scenarioSet) => ({
    ...scenarioSet,
    scenarios: scenarioSet.scenarios.map((scenario) => {
      const currentMatch = localScenarios.some(
        (entry) => entry.model === scenario.model && entry.pathway === scenario.pathway,
      );
      if (currentMatch) {
        return scenario;
      }

      const sameModel = localScenarios.find((entry) => entry.model === scenario.model);
      const fallback = sameModel || localScenarios[0];
      return normalizeScenario({
        ...scenario,
        model: fallback.model,
        pathway: fallback.pathway,
      });
    }),
  }));
  return normalizeProject(nextProject);
}

function defaultExplorerScenarioPaths(
  project: GuiProjectManifest,
  localScenarios: LocalIamScenarioEntry[],
): string[] {
  if (!localScenarios.length) {
    return [];
  }

  const orderedMatches: string[] = [];
  for (const scenarioSet of normalizeScenarioSets(project.scenario_sets)) {
    for (const scenario of scenarioSet.scenarios) {
      const direct = scenario.filepath
        ? localScenarios.find((entry) => entry.path === scenario.filepath)
        : null;
      const inferred =
        direct ||
        localScenarios.find(
          (entry) => entry.model === scenario.model && entry.pathway === scenario.pathway,
        );
      if (inferred?.path && !orderedMatches.includes(inferred.path)) {
        orderedMatches.push(inferred.path);
      }
    }
  }

  return orderedMatches.length ? orderedMatches : [localScenarios[0].path];
}

function resolvedExplorerUiState(
  project: GuiProjectManifest,
  localScenarios: LocalIamScenarioEntry[],
  sectorIds: string[],
): ExplorerUiState {
  const saved = explorerUiStateFromProject(project);
  const validPaths = saved.selected_paths.filter((path) =>
    localScenarios.some((entry) => entry.path === path),
  );
  const selectedPaths = validPaths.length
    ? validPaths
    : defaultExplorerScenarioPaths(project, localScenarios);
  const availableCompareModes = explorerCompareModesForCount(selectedPaths.length);
  const compareMode = availableCompareModes.includes(saved.compare_mode)
    ? saved.compare_mode
    : availableCompareModes[0];

  return {
    ...saved,
    selected_paths: selectedPaths,
    selected_sector: sectorIds.includes(saved.selected_sector)
      ? saved.selected_sector
      : sectorIds[0] || "",
    compare_mode: compareMode,
  };
}

function scenarioLabelFromPath(path: string, scenarios: LocalIamScenarioEntry[]): string {
  const entry = scenarios.find((item) => item.path === path);
  if (!entry) {
    return path;
  }
  return `${entry.model} / ${entry.pathway}`;
}

function storylineMatchKey(model: string, pathway: string): string {
  return `${model.trim().toLowerCase()}::${pathway.trim()}`;
}

function storylineMatchesScenario(
  storyline: Pick<IamScenarioStoryline, "model" | "pathway">,
  scenario: Pick<Scenario, "model" | "pathway">,
): boolean {
  return storylineMatchKey(storyline.model, storyline.pathway) === storylineMatchKey(scenario.model, scenario.pathway);
}

function storylineSections(storyline: IamScenarioStoryline | null): Array<{
  title: string;
  content: string;
}> {
  if (!storyline) {
    return [];
  }
  if (storyline.sections?.length) {
    return storyline.sections.filter(
      (entry) => Boolean(entry.title?.trim()) && Boolean(entry.content?.trim()),
    );
  }
  const fallback: Array<{ title: string; content: string }> = [];
  if (storyline.description?.trim()) {
    fallback.push({
      title: "Overview",
      content: storyline.description.trim(),
    });
  }
  if (storyline.scope_note?.trim()) {
    fallback.push({
      title: "Data Note",
      content: storyline.scope_note.trim(),
    });
  }
  return fallback;
}

function scenarioExplorerIdFromEntry(entry: LocalIamScenarioEntry): string {
  const stem = entry.file_name.replace(/\.[^.]+$/, "");
  return `${entry.model}::${entry.pathway}::${stem}`;
}

function explorerGroupOptions(summary: ScenarioExplorerSummaryPayload | null): string[] {
  if (!summary) {
    return [];
  }
  return summary.group_by === "subscenario"
    ? summary.subscenarios || []
    : summary.regions || [];
}

function explorerGroupLabel(summary: ScenarioExplorerSummaryPayload | null): string {
  return summary?.group_by === "subscenario" ? "Battery Sub-scenario" : "Region";
}

function explorerCompareModeLabel(mode: ExplorerCompareMode): string {
  switch (mode) {
    case "single":
      return "Absolute values";
    case "share_100":
      return "Normalized to 100%";
    case "overlay":
      return "Absolute overlay";
    case "indexed":
      return "Indexed to 100";
    case "delta":
      return "Delta vs baseline";
    case "percent_change":
      return "% change vs baseline";
    default:
      return mode;
  }
}

function explorerCompareModesForCount(
  scenarioCount: number,
): ExplorerCompareMode[] {
  if (scenarioCount <= 1) {
    return ["single", "share_100", "indexed"];
  }
  return ["overlay", "share_100", "indexed", "delta", "percent_change"];
}

function explorerCompareModeUsesBackendCompare(mode: ExplorerCompareMode): boolean {
  return mode === "overlay" || mode === "indexed" || mode === "delta" || mode === "percent_change";
}

function explorerChartModeLabel(mode: ExplorerChartMode): string {
  switch (mode) {
    case "stacked_area":
      return "Stacked area";
    case "stacked_bar":
      return "Stacked bars";
    case "bar":
      return "Grouped bars";
    default:
      return "Line";
  }
}

function explorerPlotLayoutLabel(layout: ExplorerPlotLayout): string {
  return layout === "per_scenario" ? "One plot per scenario" : "Combined plot";
}

function defaultExplorerChartMode(
  summary: ScenarioExplorerSummaryPayload | null,
  compareMode: ExplorerCompareMode,
): ExplorerChartMode {
  if (compareMode === "delta" || compareMode === "percent_change") {
    return "line";
  }
  if (compareMode === "share_100") {
    return "stacked_area";
  }
  if (
    summary?.sector?.includes("generation") ||
    summary?.sector?.includes("Transport") ||
    summary?.sector?.includes("steel") ||
    summary?.sector?.includes("cement")
  ) {
    return "stacked_area";
  }
  return "line";
}

function parseExplorerYearValue(value: string): number | null {
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }
  const parsed = Number(trimmed);
  return Number.isFinite(parsed) ? parsed : null;
}

function PreviewChart({ preview }: { preview: ScenarioPreviewPayload | null }) {
  if (!preview) {
    return <div className="empty-state">Select a scenario file preview to inspect its metadata.</div>;
  }

  return (
    <ExplorerSeriesPlot
      series={(preview.series || []).map((entry) => ({
        ...entry,
        scenarioLabel: preview.file_name,
      }))}
      emptyMessage="No numeric preview series were found in the selected IAM file."
      ariaLabel="Scenario preview line chart"
    />
  );
}

export default function App() {
  const [health, setHealth] = useState<HealthPayload | null>(null);
  const [environment, setEnvironment] = useState<EnvironmentPayload | null>(null);
  const [capabilities, setCapabilities] = useState<CapabilitiesPayload | null>(null);
  const [iamKeyState, setIamKeyState] = useState<StoredCredentialPayload | null>(null);
  const [iamKeyInput, setIamKeyInput] = useState("");
  const [showIamKey, setShowIamKey] = useState(false);
  const [brightwayDiscovery, setBrightwayDiscovery] =
    useState<BrightwayDiscoveryPayload>(EMPTY_BRIGHTWAY_DISCOVERY);
  const [recents, setRecents] = useState<RecentsPayload>(EMPTY_RECENTS);
  const [draftProject, setDraftProject] = useState<GuiProjectManifest>(
    workflowTemplate("new_database"),
  );
  const [projectPath, setProjectPath] = useState("");
  const [projectHistory, setProjectHistory] = useState<RunHistoryEntry[]>([]);
  const [manifestPreview, setManifestPreview] = useState<RunManifest | null>(null);
  const [validation, setValidation] = useState<ValidationPayload | null>(null);
  const [message, setMessage] = useState<MessageState | null>(null);
  const [initializationProgress, setInitializationProgress] = useState<InitializationProgress>(
    () =>
      createInitializationProgress(1, 0, "Initializing interface", "Starting startup checks."),
  );
  const [startupIssues, setStartupIssues] = useState<string[]>([]);
  const [formDirty, setFormDirty] = useState(true);
  const [dryRun, setDryRun] = useState(false);
  const [currentRunId, setCurrentRunId] = useState<string | null>(null);
  const [currentRunStatus, setCurrentRunStatus] = useState("idle");
  const [currentQueuePosition, setCurrentQueuePosition] = useState<number | null>(null);
  const [currentEvents, setCurrentEvents] = useState<RunEvent[]>([]);
  const [artifacts, setArtifacts] = useState<string[]>([]);
  const [pendingMonitorRunIds, setPendingMonitorRunIds] = useState<string[]>([]);
  const [diagnostics, setDiagnostics] = useState<DiagnosticsPayload | null>(null);
  const [diagnosticsTarget, setDiagnosticsTarget] = useState<RunDiagnosticsTarget | null>(null);
  const [activeTab, setActiveTab] = useState<WorkspaceTab>("overview");
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [autosaveAt, setAutosaveAt] = useState<string | null>(null);
  const [logFilter, setLogFilter] = useState<LogFilter>("all");
  const [scenarioDownload, setScenarioDownload] =
    useState<DownloadAllScenariosPayload | null>(null);
  const [localIamScenarios, setLocalIamScenarios] = useState<LocalIamScenarioEntry[] | null>(null);
  const [scenarioStorylineCatalog, setScenarioStorylineCatalog] =
    useState<IamScenarioStorylineCatalogPayload>(EMPTY_IAM_STORYLINE_CATALOG);
  const [selectedScenarioStorylineId, setSelectedScenarioStorylineId] = useState("");
  const [scenarioExplorerCatalog, setScenarioExplorerCatalog] =
    useState<ScenarioExplorerCatalogPayload | null>(null);
  const [scenarioExplorerSummary, setScenarioExplorerSummary] =
    useState<ScenarioExplorerSummaryPayload | null>(null);
  const [scenarioExplorerComparison, setScenarioExplorerComparison] =
    useState<ScenarioExplorerComparePayload | null>(null);
  const [scenarioExplorerCompareMode, setScenarioExplorerCompareMode] =
    useState<ExplorerCompareMode>("single");
  const [scenarioExplorerSelectedPaths, setScenarioExplorerSelectedPaths] = useState<string[]>([]);
  const [scenarioExplorerSelectedSector, setScenarioExplorerSelectedSector] = useState("");
  const [scenarioExplorerSelectedGroups, setScenarioExplorerSelectedGroups] = useState<string[]>(
    [],
  );
  const [scenarioExplorerGroupSelectionMode, setScenarioExplorerGroupSelectionMode] =
    useState<ExplorerSelectionMode>("all");
  const [scenarioExplorerSelectedVariables, setScenarioExplorerSelectedVariables] = useState<
    string[]
  >([]);
  const [scenarioExplorerVariableSelectionMode, setScenarioExplorerVariableSelectionMode] =
    useState<ExplorerSelectionMode>("all");
  const [scenarioExplorerBaselineScenarioId, setScenarioExplorerBaselineScenarioId] =
    useState("");
  const [scenarioExplorerBaselineYear, setScenarioExplorerBaselineYear] = useState("");
  const [scenarioExplorerYearStart, setScenarioExplorerYearStart] = useState("");
  const [scenarioExplorerYearEnd, setScenarioExplorerYearEnd] = useState("");
  const [scenarioExplorerChartMode, setScenarioExplorerChartMode] =
    useState<ExplorerChartMode>("line");
  const [scenarioExplorerPlotLayout, setScenarioExplorerPlotLayout] =
    useState<ExplorerPlotLayout>("per_scenario");
  const [scenarioExplorerHiddenSeries, setScenarioExplorerHiddenSeries] = useState<string[]>([]);
  const [scenarioExplorerLoading, setScenarioExplorerLoading] = useState(false);
  const [scenarioExplorerDirty, setScenarioExplorerDirty] = useState(true);
  const [scenarioExplorerPerformance, setScenarioExplorerPerformance] =
    useState<ExplorerPerformanceMetrics>({
      lastReloadMs: null,
      lastFilterMs: null,
      lastChartPaintMs: null,
    });
  const [scenarioPreviews, setScenarioPreviews] = useState<
    Record<string, ScenarioPreviewPayload>
  >({});
  const [selectedPreviewKey, setSelectedPreviewKey] = useState<string | null>(null);
  const [previewLoadingKey, setPreviewLoadingKey] = useState<string | null>(null);
  const [scenarioBatchInputs, setScenarioBatchInputs] = useState<Record<number, string>>({});
  const [completionNotice, setCompletionNotice] = useState<CompletionNotice | null>(null);
  const [desktopNotificationsEnabled, setDesktopNotificationsEnabled] = useState<boolean>(
    () => typeof window !== "undefined" && "Notification" in window && Notification.permission === "granted",
  );
  const runStatusByIdRef = useRef<Record<string, string>>({});
  const localScenarioResyncAttemptedRef = useRef(false);
  const hasInitializedWorkspaceRef = useRef(false);
  const saveProjectShortcutRef = useRef<(() => Promise<unknown>) | null>(null);
  const queueProjectShortcutRef = useRef<(() => Promise<unknown>) | null>(null);
  const explorerReloadStartedAtRef = useRef<number | null>(null);
  const explorerFilterStartedAtRef = useRef<number | null>(null);
  const explorerRenderStartedAtRef = useRef<number | null>(null);
  const explorerUpdatePendingRef = useRef(false);

  const workflow = draftProject.workflow;
  const scenarioSets = normalizeScenarioSets(draftProject.scenario_sets);
  const totalScenarioCount = scenarioSets.reduce(
    (count, entry) => count + entry.scenarios.length,
    0,
  );
  const workflowExportTypes =
    capabilities?.workflow_export_types?.[workflow] ||
    Object.keys(WORKFLOW_EXPORT_DEFAULTS[workflow] || WORKFLOW_EXPORT_DEFAULTS.new_database);
  const availableExportTypes = workflowExportTypes.filter(
    (entry) => entry !== "superstructure" || totalScenarioCount > 1,
  );
  const exportConfig = normalizeExportConfig(workflow, draftProject.config.export);
  const exportType = availableExportTypes.includes(exportConfig.type)
    ? exportConfig.type
    : availableExportTypes[0] || exportConfig.type;
  const exportOptions =
    exportType === exportConfig.type
      ? exportConfig.options
      : normalizeExportConfig(workflow, { type: exportType }).options;
  const sourceType = draftProject.config.source_type || "brightway";
  const selectedBrightwayProject =
    draftProject.config.source_project || brightwayDiscovery.current_project || "";
  const brightwayDatabases =
    sourceType === "brightway" && selectedBrightwayProject
      ? brightwayDiscovery.databases
      : [];
  const selectedBiosphereDatabase = brightwayDatabases.includes(draftProject.config.biosphere_name)
    ? draftProject.config.biosphere_name
    : "";
  const missingBiosphereDatabase =
    Boolean(draftProject.config.biosphere_name) &&
    Boolean(selectedBrightwayProject) &&
    !brightwayDatabases.includes(draftProject.config.biosphere_name);
  const selectedTransformations = normalizeTransformationSelection(
    draftProject.config.transformations,
  );
  const selectedSectors =
    Array.isArray(draftProject.config.sectors) && draftProject.config.sectors.length
      ? draftProject.config.sectors
      : [];
  const capabilityLocalScenarioFiles = capabilities?.iam_scenarios || [];
  const localScenarioFiles =
    localIamScenarios ??
    reconciledLocalScenarioFiles(
      capabilityLocalScenarioFiles,
      scenarioExplorerCatalog?.scenarios,
    );
  const scenarioStorylineFilterKeys = useMemo(() => {
    const keys = new Set<string>();
    for (const entry of capabilities?.iam_scenario_catalog || []) {
      keys.add(storylineMatchKey(entry.model, entry.pathway));
    }
    for (const entry of localScenarioFiles) {
      keys.add(storylineMatchKey(entry.model, entry.pathway));
    }
    for (const scenarioSet of scenarioSets) {
      for (const scenario of scenarioSet.scenarios) {
        if (scenario.model && scenario.pathway) {
          keys.add(storylineMatchKey(scenario.model, scenario.pathway));
        }
      }
    }
    return keys;
  }, [capabilities?.iam_scenario_catalog, localScenarioFiles, scenarioSets]);
  const scenarioStorylines = useMemo(() => {
    const storylines = scenarioStorylineCatalog.storylines || [];
    if (!storylines.length) {
      return [];
    }
    if (!scenarioStorylineFilterKeys.size) {
      return storylines;
    }
    const filtered = storylines.filter((entry) =>
      scenarioStorylineFilterKeys.has(storylineMatchKey(entry.model, entry.pathway)),
    );
    return filtered.length ? filtered : storylines;
  }, [scenarioStorylineCatalog.storylines, scenarioStorylineFilterKeys]);
  const defaultScenarioStorylineId = useMemo(() => {
    for (const scenarioSet of scenarioSets) {
      for (const scenario of scenarioSet.scenarios) {
        const match = scenarioStorylines.find((entry) => storylineMatchesScenario(entry, scenario));
        if (match) {
          return match.id;
        }
      }
    }
    return scenarioStorylines[0]?.id || "";
  }, [scenarioSets, scenarioStorylines]);
  const selectedScenarioStoryline = useMemo(
    () =>
      scenarioStorylines.find((entry) => entry.id === selectedScenarioStorylineId) ||
      scenarioStorylines.find((entry) => entry.id === defaultScenarioStorylineId) ||
      null,
    [defaultScenarioStorylineId, scenarioStorylines, selectedScenarioStorylineId],
  );
  const selectedScenarioStorylineSections = useMemo(
    () => storylineSections(selectedScenarioStoryline),
    [selectedScenarioStoryline],
  );
  const selectedScenarioStorylineAvailableLocally = Boolean(
    selectedScenarioStoryline &&
      localScenarioFiles.some((entry) =>
        storylineMatchesScenario(selectedScenarioStoryline, entry),
      ),
  );
  const selectedScenarioStorylineKnownToCatalog = Boolean(
    selectedScenarioStoryline &&
      (capabilities?.iam_scenario_catalog || []).some((entry) =>
        storylineMatchesScenario(selectedScenarioStoryline, entry),
      ),
  );
  const scenarioModelOptions = uniqueStrings(localScenarioFiles.map((entry) => entry.model));
  const scenarioDownloadInProgress = scenarioDownload?.status === "running";
  const scenarioDownloadPercent = Math.round((scenarioDownload?.progress || 0) * 100);
  const explorerCatalogScenarios = localScenarioFiles;
  const explorerCatalogSectors = scenarioExplorerCatalog?.sectors || [];
  const explorerAvailableCompareModes = explorerCompareModesForCount(
    scenarioExplorerSelectedPaths.length,
  );
  const explorerSelectedScenarioEntries = explorerCatalogScenarios.filter((entry) =>
    scenarioExplorerSelectedPaths.includes(entry.path),
  );
  const explorerScenarioSummaries = scenarioExplorerSummary?.scenarios || [];
  const explorerBaselineChoices =
    scenarioExplorerCompareMode === "delta" || scenarioExplorerCompareMode === "percent_change"
      ? (explorerScenarioSummaries.length
          ? explorerScenarioSummaries.map((entry) => ({
              scenario_id: entry.scenario_id,
              label: `${entry.model.toUpperCase()} / ${entry.pathway}`,
            }))
          : explorerSelectedScenarioEntries.map((entry) => ({
              scenario_id: scenarioExplorerIdFromEntry(entry),
              label: `${entry.model.toUpperCase()} / ${entry.pathway}`,
            })))
      : [];
  const explorerGroupChoices = explorerGroupOptions(scenarioExplorerSummary);
  const deferredExplorerSelectedGroups = useDeferredValue(scenarioExplorerSelectedGroups);
  const deferredExplorerGroupSelectionMode = useDeferredValue(
    scenarioExplorerGroupSelectionMode,
  );
  const explorerSelectedGroupLabels = explorerSelectionValues(
    scenarioExplorerGroupSelectionMode,
    scenarioExplorerSelectedGroups,
    explorerGroupChoices,
  );
  const deferredExplorerSelectedGroupLabels = explorerSelectionValues(
    deferredExplorerGroupSelectionMode,
    deferredExplorerSelectedGroups,
    explorerGroupChoices,
  );
  const deferredExplorerSelectedVariables = useDeferredValue(
    scenarioExplorerSelectedVariables,
  );
  const deferredExplorerVariableSelectionMode = useDeferredValue(
    scenarioExplorerVariableSelectionMode,
  );
  const explorerSelectedVariableLabels = useMemo(
    () =>
      explorerSelectionValues(
        scenarioExplorerVariableSelectionMode,
        scenarioExplorerSelectedVariables,
        scenarioExplorerSummary?.variables || [],
      ),
    [
      scenarioExplorerVariableSelectionMode,
      scenarioExplorerSelectedVariables,
      scenarioExplorerSummary,
    ],
  );
  const deferredExplorerSelectedVariableLabels = useMemo(
    () =>
      explorerSelectionValues(
        deferredExplorerVariableSelectionMode,
        deferredExplorerSelectedVariables,
        scenarioExplorerSummary?.variables || [],
      ),
    [
      deferredExplorerVariableSelectionMode,
      deferredExplorerSelectedVariables,
      scenarioExplorerSummary,
    ],
  );
  const explorerRequestedYearStart = parseExplorerYearValue(scenarioExplorerYearStart);
  const explorerRequestedYearEnd = parseExplorerYearValue(scenarioExplorerYearEnd);
  const deferredExplorerRequestedYearStart = useDeferredValue(explorerRequestedYearStart);
  const deferredExplorerRequestedYearEnd = useDeferredValue(explorerRequestedYearEnd);
  const explorerPlotSeriesBase = useMemo(
    () =>
      filterPlotSeriesByYearWindow(
        explorerSeriesForSelection(
          scenarioExplorerSummary,
          deferredExplorerSelectedGroupLabels,
          deferredExplorerSelectedVariableLabels,
        ),
        deferredExplorerRequestedYearStart,
        deferredExplorerRequestedYearEnd,
      ),
    [
      scenarioExplorerSummary,
      deferredExplorerSelectedGroupLabels,
      deferredExplorerSelectedVariableLabels,
      deferredExplorerRequestedYearStart,
      deferredExplorerRequestedYearEnd,
    ],
  );
  const explorerPlotSeries = useMemo(
    () =>
      scenarioExplorerCompareMode === "share_100"
        ? normalizePlotSeriesToShare(explorerPlotSeriesBase)
        : explorerPlotSeriesBase,
    [scenarioExplorerCompareMode, explorerPlotSeriesBase],
  );
  const explorerRenderedYearBounds = useMemo(
    () => plotSeriesYearBounds(explorerPlotSeries),
    [explorerPlotSeries],
  );
  const explorerVisiblePlotSeries = useMemo(
    () =>
      explorerPlotSeries.filter((entry) => !scenarioExplorerHiddenSeries.includes(entry.label)),
    [explorerPlotSeries, scenarioExplorerHiddenSeries],
  );
  const explorerSharedPlotDomain = useMemo(
    () =>
      plotSeriesValueDomain(
        explorerVisiblePlotSeries.length ? explorerVisiblePlotSeries : explorerPlotSeries,
        scenarioExplorerChartMode,
      ),
    [explorerVisiblePlotSeries, explorerPlotSeries, scenarioExplorerChartMode],
  );
  const explorerPlotDomain =
    scenarioExplorerCompareMode === "share_100"
      ? { min: 0, max: 100 }
      : explorerSharedPlotDomain;
  const explorerScenarioPlotPanels = useMemo(
    () =>
      explorerScenarioPlots(explorerPlotSeries).map((panel) => ({
        ...panel,
        visibleSeries: panel.series.filter(
          (entry) => !scenarioExplorerHiddenSeries.includes(entry.label),
        ),
      })),
    [explorerPlotSeries, scenarioExplorerHiddenSeries],
  );
  const explorerPlotLabelKey = useMemo(
    () => stringListKey(explorerPlotSeries.map((entry) => entry.label)),
    [explorerPlotSeries],
  );
  const explorerYearBounds = scenarioExplorerSummary?.years.length
    ? {
        min: scenarioExplorerSummary.years[0],
        max: scenarioExplorerSummary.years[scenarioExplorerSummary.years.length - 1],
      }
    : { min: 2005, max: 2100 };
  const explorerUpdatePending =
    stringListKey(deferredExplorerSelectedGroupLabels) !==
      stringListKey(explorerSelectedGroupLabels) ||
    stringListKey(deferredExplorerSelectedVariableLabels) !==
      stringListKey(explorerSelectedVariableLabels) ||
    deferredExplorerRequestedYearStart !== explorerRequestedYearStart ||
    deferredExplorerRequestedYearEnd !== explorerRequestedYearEnd;
  const transformationCatalog = capabilities?.transformation_catalog || [];
  const incrementalSectorCatalog = capabilities?.incremental_sector_catalog || [];
  const selectedPreview =
    (selectedPreviewKey && scenarioPreviews[selectedPreviewKey]) ||
    (Object.keys(scenarioPreviews).length
      ? scenarioPreviews[Object.keys(scenarioPreviews)[0]]
      : null);
  const selectedPreviewIndices = parseScenarioKey(selectedPreviewKey);
  const filteredEvents = currentEvents.filter((event) => eventMatchesFilter(event, logFilter));
  const eventLines = filteredEvents.map((event) => formatEventLine(event));
  const jobStateLabel = currentRunId
    ? formatRunState(currentRunStatus, currentQueuePosition)
    : "Idle";
  const initializationPercent = initializationProgress.total
    ? Math.round((initializationProgress.completed / initializationProgress.total) * 100)
    : 0;
  const topJobProgress = jobProgressState(
    currentRunId,
    currentRunStatus,
    currentQueuePosition,
    currentEvents,
  );
  const currentRunTarget = currentRunId ? { run_id: currentRunId } : null;
  const cancelDisabled =
    !currentRunId ||
    ["idle", "completed", "failed", "cancelled", "cancelling"].includes(
      currentRunStatus,
    );
  const validationBadge = validation
    ? validation.valid
      ? validation.warnings.length
        ? `Valid with ${validation.warnings.length} warning${validation.warnings.length > 1 ? "s" : ""}`
        : "Valid"
      : `${validation.errors.length} validation error${validation.errors.length > 1 ? "s" : ""}`
    : "No preview yet";
  const validationTone = validation
    ? validation.valid
      ? validation.warnings.length
        ? "warning"
        : "success"
      : "error"
    : "neutral";
  const workspaceMode: WorkspaceMode = showAdvanced ? "expert" : "basic";
  const visibleTabs = showAdvanced
    ? WORKSPACE_TABS
    : WORKSPACE_TABS.filter((tab) => !ADVANCED_TAB_IDS.has(tab.id));
  const sourceStepReady =
    sourceType === "brightway"
      ? Boolean(selectedBrightwayProject && draftProject.config.source_db)
      : Boolean(draftProject.config.source_file_path);
  const scenariosStepReady = scenarioSets.every(
    (entry) =>
      entry.scenarios.length &&
      entry.scenarios.every(
        (scenario) => Boolean(scenario.model && scenario.pathway && scenario.year),
      ),
  );
  const transformationsStepReady =
    workflow === "incremental_database" ? true : selectedTransformations == null || selectedTransformations.length > 0;
  const exportStepReady = Boolean(exportType);
  const runStepReady = Boolean(validation?.valid);
  const guidedSteps = GUIDED_WORKFLOW_STEPS.map((step) => {
    const ready =
      step.tab === "source"
        ? sourceStepReady
        : step.tab === "scenarios"
          ? scenariosStepReady
          : step.tab === "transformations"
            ? transformationsStepReady
            : step.tab === "export"
              ? exportStepReady
              : runStepReady;
    return {
      ...step,
      ready,
      active: activeTab === step.tab,
    };
  });
  const autosaveLabel = autosaveTimestampLabel(autosaveAt);
  const sourceDbDisabledReason = !selectedBrightwayProject
    ? "Choose a Brightway project first."
    : brightwayDatabases.length
      ? null
      : "No databases were found in the selected Brightway project.";
  const biosphereDisabledReason = !selectedBrightwayProject
    ? "Choose a Brightway project first."
    : brightwayDatabases.length
      ? null
      : "No databases were found in the selected Brightway project.";
  const explorerReloadDisabledReason =
    scenarioExplorerLoading
      ? "The explorer summary is already loading."
      : !scenarioExplorerSelectedPaths.length
        ? "Select at least one installed scenario."
        : !scenarioExplorerSelectedSector
          ? "Choose a sector to load."
          : null;
  const cancelRunDisabledReason = !currentRunId
    ? "No run is currently selected."
    : isTerminalRunStatus(currentRunStatus)
      ? "This run has already finished."
      : currentRunStatus === "cancelling"
        ? "Cancellation is already in progress."
        : null;
  const diagnosticsSummaryText = diagnostics
    ? JSON.stringify(
        {
          run_id: diagnostics.run_id,
          run_dir: diagnostics.run_dir,
          metadata: diagnostics.metadata,
          diagnostics: diagnostics.diagnostics,
          result: diagnostics.result,
          last_event:
            diagnostics.events && diagnostics.events.length
              ? diagnostics.events[diagnostics.events.length - 1]
              : null,
        },
        null,
        2,
      )
    : "Select a run to inspect technical details.";
  const diagnosticsLogsText = diagnostics
    ? [
        String(diagnostics.diagnostics?.traceback || ""),
        diagnostics.stderr_tail || "",
        diagnostics.stdout_tail || "",
      ]
        .filter(Boolean)
        .join("\n\n") || "No log output available."
    : "No diagnostics loaded.";
  const credentials =
    environment == null
      ? "Checking environment..."
      : Object.entries(environment.credentials)
          .filter(([, present]) => present)
          .map(([key]) => key)
          .join(", ") || "No credentials detected";
  const dialogCapability = environment?.dialogs?.native_path_dialogs;
  const dialogCapabilityLabel =
    environment == null
      ? "Checking dialog support..."
      : dialogCapability?.available
        ? "Native dialogs available"
        : "Manual path entry only";
  const iamKeySourceLabel =
    iamKeyState == null
      ? "Checking IAM key..."
      : iamKeyState.has_value
        ? iamKeyState.backend === "keyring"
          ? "Stored in local keyring"
          : iamKeyState.backend === "file"
            ? "Stored in local UI file"
            : "Loaded from environment"
        : "No key saved";
  const currentOutputLocation =
    currentRunId &&
    (diagnostics?.run_id || diagnosticsTarget?.run_id) === currentRunId &&
    diagnostics?.result &&
    typeof diagnostics.result.output_location === "string"
      ? diagnostics.result.output_location
      : null;
  const activeHelp = HELP_TEXT[activeTab];
  const desktopNotificationsAvailable =
    typeof window !== "undefined" && "Notification" in window;
  const queueShortcutHint = dryRun
    ? "Cmd/Ctrl+Enter queues the current draft as a dry run."
    : "Cmd/Ctrl+Enter queues the current draft.";
  const saveShortcutHint = "Cmd/Ctrl+S saves the current configuration file.";
  const explorerPerformanceSummary = [
    scenarioExplorerPerformance.lastReloadMs != null
      ? `Reload ${Math.round(scenarioExplorerPerformance.lastReloadMs)} ms`
      : null,
    scenarioExplorerPerformance.lastFilterMs != null
      ? `Filter ${Math.round(scenarioExplorerPerformance.lastFilterMs)} ms`
      : null,
    scenarioExplorerPerformance.lastChartPaintMs != null
      ? `Chart ${Math.round(scenarioExplorerPerformance.lastChartPaintMs)} ms`
      : null,
  ]
    .filter((entry): entry is string => Boolean(entry))
    .join(" / ");
  saveProjectShortcutRef.current = () => saveProject();
  queueProjectShortcutRef.current = () => queueProject();

  function resetRunMonitor() {
    setCurrentRunId(null);
    setCurrentRunStatus("idle");
    setCurrentQueuePosition(null);
    setCurrentEvents([]);
    setArtifacts([]);
    setPendingMonitorRunIds([]);
    setCompletionNotice(null);
    runStatusByIdRef.current = {};
  }

  function focusRunMonitor(
    runId: string,
    options?: {
      status?: string;
      queuePosition?: number | null;
      clearRunData?: boolean;
    },
  ) {
    setPendingMonitorRunIds((current) => current.filter((entry) => entry !== runId));
    setCurrentRunId(runId);
    setCurrentRunStatus(options?.status || "queued");
    setCurrentQueuePosition(options?.queuePosition ?? null);
    runStatusByIdRef.current[runId] = options?.status || runStatusByIdRef.current[runId] || "queued";
    if (options?.clearRunData ?? true) {
      setCurrentEvents([]);
      setArtifacts([]);
    }
    setDiagnostics(null);
    setDiagnosticsTarget({ run_id: runId });
  }

  async function notifyRunCompleted(runId: string, events: RunEvent[]) {
    const detail =
      latestEventOfType(events, "job_completed")?.message ||
      currentPhaseEvent(events)?.message ||
      "Workflow execution completed successfully.";
    const outputLocation = latestOutputLocation(events);

    setCompletionNotice({
      runId,
      detail,
      outputLocation,
    });
    setMessage({
      text: `Run ${runId} completed successfully.`,
      tone: "success",
    });

    if (
      desktopNotificationsEnabled &&
      desktopNotificationsAvailable &&
      typeof Notification !== "undefined"
    ) {
      try {
        const lines = [detail];
        if (outputLocation) {
          lines.push(outputLocation);
        }
        new Notification("Premise run completed", {
          body: lines.join("\n"),
          tag: `premise-run-${runId}`,
        });
      } catch (error) {
        console.error(error);
      }
    }
  }

  async function enableDesktopNotifications() {
    if (!desktopNotificationsAvailable || typeof Notification === "undefined") {
      setMessage({
        text: "Desktop notifications are not available in this browser.",
        tone: "error",
      });
      return;
    }

    const permission = await Notification.requestPermission();
    setDesktopNotificationsEnabled(permission === "granted");
    setMessage({
      text:
        permission === "granted"
          ? "Desktop notifications enabled for successful runs."
          : "Desktop notification permission was not granted.",
      tone: permission === "granted" ? "success" : "info",
    });
  }

  function startSupportWorkflow() {
    const bundleUrl = buildSupportBundleUrl(diagnosticsTarget);
    const emailUrl = buildSupportEmailUrl(diagnosticsTarget);
    if (!bundleUrl || !emailUrl) {
      setMessage({
        text: "Select a run with diagnostics before starting the support workflow.",
        tone: "error",
      });
      return;
    }

    window.open(bundleUrl, "_blank", "noopener");
    window.location.href = emailUrl;
    setMessage({
      text: "Opened a support email draft and started the support-bundle download. Attach the downloaded bundle before sending.",
      tone: "success",
    });
  }

  function clearDiagnostics() {
    setDiagnostics(null);
    setDiagnosticsTarget(null);
  }

  function clearPreviewState() {
    setScenarioPreviews({});
    setSelectedPreviewKey(null);
    setScenarioBatchInputs({});
  }

  function markFormDirty() {
    setFormDirty(true);
    setValidation(null);
  }

  function updateDraftProject(nextProject: GuiProjectManifest) {
    setDraftProject(normalizeProject(nextProject));
    markFormDirty();
  }

  function updateScenarioSets(nextScenarioSets: ScenarioSet[]) {
    updateDraftProject({
      ...draftProject,
      scenario_sets: nextScenarioSets,
    });
  }

  function updateConfig(patch: Record<string, unknown>) {
    updateDraftProject({
      ...draftProject,
      config: {
        ...draftProject.config,
        ...patch,
      },
    });
  }

  function updateExportConfig(nextExport: ExportConfig) {
    updateConfig({
      export: normalizeExportConfig(workflow, nextExport),
    });
  }

  function updateExportType(nextExportType: string) {
    updateExportConfig({
      type: nextExportType,
      options: nextExportOptions(workflow, nextExportType, exportOptions),
    });
  }

  function updateExportOption(key: keyof ExportOptions, value: unknown) {
    updateExportConfig({
      type: exportType,
      options: {
        ...exportOptions,
        [key]: value,
      },
    });
  }

  function updateScenarioSet(setIndex: number, nextSet: ScenarioSet) {
    updateScenarioSets(
      scenarioSets.map((entry, index) =>
        index === setIndex ? normalizeScenarioSet(nextSet) : entry,
      ),
    );
  }

  function updateScenario(
    setIndex: number,
    scenarioIndex: number,
    patch: Partial<Scenario>,
  ) {
    updateScenarioSet(setIndex, {
      ...scenarioSets[setIndex],
      scenarios: scenarioSets[setIndex].scenarios.map((scenario, index) =>
        index === scenarioIndex ? normalizeScenario({ ...scenario, ...patch }) : scenario,
      ),
    });
  }

  function addScenarioSet() {
    const nextIndex = scenarioSets.length + 1;
    updateScenarioSets([
      ...scenarioSets,
      normalizeScenarioSet({
        name: `set-${nextIndex}`,
        scenarios: [cloneJson(scenarioSets[scenarioSets.length - 1]?.scenarios[0] || FALLBACK_SCENARIO)],
      }),
    ]);
  }

  function removeScenarioSet(setIndex: number) {
    if (scenarioSets.length === 1) {
      setMessage({
        text: "At least one scenario set is required.",
        tone: "error",
      });
      return;
    }

    updateScenarioSets(scenarioSets.filter((_, index) => index !== setIndex));
  }

  function addScenario(setIndex: number) {
    const seed =
      scenarioSets[setIndex].scenarios[scenarioSets[setIndex].scenarios.length - 1] ||
      FALLBACK_SCENARIO;
    updateScenarioSet(setIndex, {
      ...scenarioSets[setIndex],
      scenarios: [...scenarioSets[setIndex].scenarios, normalizeScenario(seed)],
    });
  }

  function removeScenario(setIndex: number, scenarioIndex: number) {
    if (scenarioSets[setIndex].scenarios.length === 1) {
      setMessage({
        text: "Each scenario set must keep at least one scenario.",
        tone: "error",
      });
      return;
    }

    updateScenarioSet(setIndex, {
      ...scenarioSets[setIndex],
      scenarios: scenarioSets[setIndex].scenarios.filter(
        (_, index) => index !== scenarioIndex,
      ),
    });
  }

  function applyYearBatchToScenarioSet(setIndex: number, years: number[]) {
    if (!years.length) {
      setMessage({
        text: "Enter at least one valid year between 2005 and 2100.",
        tone: "error",
      });
      return;
    }

    const setEntry = scenarioSets[setIndex];
    const seed = setEntry.scenarios[setEntry.scenarios.length - 1] || FALLBACK_SCENARIO;
    const existingKeys = new Set(
      setEntry.scenarios.map(
        (scenario) =>
          `${scenario.model}|${scenario.pathway}|${scenario.filepath || ""}|${scenario.year}`,
      ),
    );
    const additions = years
      .map((year) =>
        normalizeScenario({
          ...seed,
          year,
        }),
      )
      .filter(
        (scenario) =>
          !existingKeys.has(
            `${scenario.model}|${scenario.pathway}|${scenario.filepath || ""}|${scenario.year}`,
          ),
      );

    if (!additions.length) {
      setMessage({
        text: "Those years are already present for the selected scenario pattern.",
        tone: "error",
      });
      return;
    }

    updateScenarioSet(setIndex, {
      ...setEntry,
      scenarios: [...setEntry.scenarios, ...additions],
    });
    setScenarioBatchInputs((current) => ({
      ...current,
      [setIndex]: "",
    }));
  }

  function updateAdditionalInventory(index: number, patch: Partial<AdditionalInventory>) {
    const next = (
      Array.isArray(draftProject.config.additional_inventories)
        ? draftProject.config.additional_inventories
        : []
    ).map((entry, entryIndex) =>
      entryIndex === index
        ? normalizeAdditionalInventory({ ...entry, ...patch })
        : normalizeAdditionalInventory(entry),
    );
    updateConfig({ additional_inventories: next });
  }

  function addAdditionalInventory() {
    const current = Array.isArray(draftProject.config.additional_inventories)
      ? draftProject.config.additional_inventories
      : [];
    updateConfig({
      additional_inventories: [...current, normalizeAdditionalInventory()],
    });
  }

  function removeAdditionalInventory(index: number) {
    const current = Array.isArray(draftProject.config.additional_inventories)
      ? draftProject.config.additional_inventories
      : [];
    updateConfig({
      additional_inventories: current.filter((_, entryIndex) => entryIndex !== index),
    });
  }

  function addPathwaysYear() {
    const currentYears = normalizeYears(draftProject.config.years);
    const seedYear = currentYears[currentYears.length - 1] || 2030;
    updateConfig({
      years: [...currentYears, seedYear + 10],
    });
  }

  function updatePathwaysYear(index: number, value: number) {
    const currentYears = normalizeYears(draftProject.config.years);
    const nextYears = currentYears.map((entry, entryIndex) =>
      entryIndex === index ? value : entry,
    );
    updateConfig({ years: nextYears });
  }

  function removePathwaysYear(index: number) {
    const currentYears = normalizeYears(draftProject.config.years);
    if (currentYears.length === 1) {
      setMessage({
        text: "Pathways datapackage requires at least one output year.",
        tone: "error",
      });
      return;
    }
    updateConfig({
      years: currentYears.filter((_, entryIndex) => entryIndex !== index),
    });
  }

  function addContributor() {
    const current = Array.isArray(draftProject.config.contributors)
      ? draftProject.config.contributors
      : [];
    updateConfig({
      contributors: [...current, normalizeContributor()],
    });
  }

  function updateContributor(index: number, patch: Partial<Contributor>) {
    const current = Array.isArray(draftProject.config.contributors)
      ? draftProject.config.contributors
      : [];
    updateConfig({
      contributors: current.map((entry, entryIndex) =>
        entryIndex === index ? normalizeContributor({ ...entry, ...patch }) : entry,
      ),
    });
  }

  function removeContributor(index: number) {
    const current = Array.isArray(draftProject.config.contributors)
      ? draftProject.config.contributors
      : [];
    if (current.length === 1) {
      updateConfig({
        contributors: [{ title: "", name: "", email: "" }],
      });
      return;
    }
    updateConfig({
      contributors: current.filter((_, entryIndex) => entryIndex !== index),
    });
  }

  function toggleTransformSelection(entryId: string) {
    const allIds = transformationCatalog.map((entry) => entry.id);
    if (!allIds.length) {
      return;
    }

    const current = selectedTransformations == null ? allIds : selectedTransformations;
    const next = current.includes(entryId)
      ? current.filter((entry) => entry !== entryId)
      : [...current, entryId];

    updateConfig({
      transformations: next.length === allIds.length ? null : next,
    });
  }

  function toggleAllTransformations(enabled: boolean) {
    updateConfig({
      transformations: enabled ? null : [],
    });
  }

  function toggleIncrementalSector(entryId: string) {
    const allIds = incrementalSectorCatalog.map((entry) => entry.id);
    if (!allIds.length) {
      return;
    }

    const current = selectedSectors.length ? selectedSectors : allIds;
    const next = current.includes(entryId)
      ? current.filter((entry) => entry !== entryId)
      : [...current, entryId];

    updateConfig({
      sectors: next.length === allIds.length ? [] : next,
    });
  }

  function toggleAllIncrementalSectors(enabled: boolean) {
    updateConfig({
      sectors: enabled ? [] : [],
    });
  }

  async function choosePathDialog(options: PathDialogOptions): Promise<string | null> {
    try {
      const response = await apiRequest<{
        selected_path: string | null;
      }>("/api/dialogs/path", {
        method: "POST",
        body: JSON.stringify(options),
      });
      return response.selected_path ?? null;
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error);
      if (
        !dialogCapability?.available ||
        detail.includes("native file dialog") ||
        detail.includes("Tkinter")
      ) {
        const guidance = dialogCapability?.manual_path_entry
          ? "You can keep editing the path manually in the field."
          : "Enter the path manually instead.";
        setMessage({
          text: `${dialogCapability?.detail || detail} ${guidance}`,
          tone: "info",
        });
        return null;
      }
      throw error;
    }
  }

  async function rememberRecent(
    kind: "project" | "source_directory" | "export_directory" | "scenario_file" | "other",
    path: string,
    options?: { label?: string; basePath?: string | null },
  ) {
    if (!path) {
      return;
    }

    const payload = await apiRequest<RecentsPayload>("/api/recents/remember", {
      method: "POST",
      body: JSON.stringify({
        kind,
        path,
        label: options?.label ?? null,
        base_path: options?.basePath ?? null,
      }),
    });
    setRecents(payload);
  }

  async function refreshRecents() {
    const payload = await apiRequest<RecentsPayload>("/api/recents");
    setRecents(payload);
    return payload;
  }

  async function saveIamKey() {
    const payload = await apiRequest<StoredCredentialPayload>("/api/credentials/iam-key", {
      method: "POST",
      body: JSON.stringify({
        value: iamKeyInput,
        remember: true,
      }),
    });
    setIamKeyState(payload);
    setIamKeyInput(payload.value || "");
    await apiRequest<EnvironmentPayload>("/api/environment")
      .then((nextEnvironment) => setEnvironment(nextEnvironment))
      .catch((error: Error) => console.error(error));
    setMessage({
      text:
        payload.backend === "keyring"
          ? "IAM scenario decryption key saved to the local system keyring."
          : "IAM scenario decryption key saved for this machine and loaded into the current session.",
      tone: "success",
    });
    return payload;
  }

  async function clearIamKeyState() {
    const payload = await apiRequest<StoredCredentialPayload>("/api/credentials/iam-key", {
      method: "DELETE",
    });
    setIamKeyState(payload);
    setIamKeyInput(payload.value || "");
    await apiRequest<EnvironmentPayload>("/api/environment")
      .then((nextEnvironment) => setEnvironment(nextEnvironment))
      .catch((error: Error) => console.error(error));
    setMessage({
      text:
        payload.has_value && payload.backend === "environment"
          ? "The current IAM key is managed by the environment, so only the UI-stored copy was cleared."
          : "IAM scenario decryption key cleared from the UI store.",
      tone: "success",
    });
    return payload;
  }

  async function refreshBrightwayDiscovery() {
    const payload = await apiRequest<BrightwayDiscoveryPayload>(
      "/api/discovery/brightway",
      {
        method: "POST",
      },
    );
    setBrightwayDiscovery(payload);
    return payload;
  }

  async function refreshScenarioExplorerCatalog() {
    const payload = await apiRequest<ScenarioExplorerCatalogPayload>(
      "/api/scenario-explorer/catalog",
    );
    return payload;
  }

  async function refreshLocalIamScenarios() {
    const payload = await apiRequest<LocalIamScenariosPayload>(
      "/api/discovery/iam-scenarios/local",
    );
    return payload.scenarios;
  }

  async function retryStartupChecks() {
    const totalSteps = 8;
    let completedSteps = 0;
    const warnings: string[] = [];
    setStartupIssues([]);
    const advance = (label: string, detail?: string) => {
      completedSteps += 1;
      setInitializationProgress(
        createInitializationProgress(totalSteps, completedSteps, label, detail),
      );
    };
    const runStep = async <T,>(
      label: string,
      request: () => Promise<T>,
      fallback: T,
    ): Promise<T> => {
      try {
        const payload = await request();
        advance(label);
        return payload;
      } catch (error) {
        const detail = error instanceof Error ? error.message : String(error);
        warnings.push(`${label}: ${detail}`);
        advance(label, `Continuing without this startup step. ${detail}`);
        return fallback;
      }
    };

    setInitializationProgress(
      createInitializationProgress(
        totalSteps,
        0,
        "Retrying startup checks",
        "Refreshing environment, Brightway discovery, and local scenario metadata.",
      ),
    );

    const [
      healthPayload,
      environmentPayload,
      capabilitiesPayload,
      explorerCatalogPayload,
      localScenarios,
      iamKeyPayload,
      brightwayPayload,
      recentsPayload,
    ] = await Promise.all([
      runStep(
        "Connected to the local service",
        () => apiRequest<HealthPayload>("/api/health"),
        health,
      ),
      runStep(
        "Loaded environment details",
        () => apiRequest<EnvironmentPayload>("/api/environment"),
        environment ?? EMPTY_ENVIRONMENT,
      ),
      runStep(
        "Loaded UI capabilities",
        () => apiRequest<CapabilitiesPayload>("/api/capabilities"),
        capabilities ?? EMPTY_CAPABILITIES,
      ),
      runStep(
        "Loaded Scenario Explorer catalog",
        () => refreshScenarioExplorerCatalog(),
        scenarioExplorerCatalog ?? EMPTY_EXPLORER_CATALOG,
      ),
      runStep(
        "Discovered local IAM scenario files",
        () => refreshLocalIamScenarios(),
        localIamScenarios ?? [],
      ),
      runStep(
        "Loaded stored IAM credentials",
        () => apiRequest<StoredCredentialPayload>("/api/credentials/iam-key"),
        iamKeyState ?? EMPTY_STORED_CREDENTIAL,
      ),
      runStep(
        "Loaded Brightway projects and databases",
        () => refreshBrightwayDiscovery(),
        brightwayDiscovery,
      ),
      runStep(
        "Loaded recent configurations",
        () => refreshRecents(),
        recents,
      ),
    ]);

    const refreshedLocalScenarios = reconciledLocalScenarioFiles(
      localScenarios,
      reconciledLocalScenarioFiles(
        capabilitiesPayload?.iam_scenarios,
        explorerCatalogPayload?.scenarios,
      ),
    );

    setHealth(healthPayload);
    setEnvironment(environmentPayload);
    setCapabilities(
      capabilitiesWithLocalScenarios(capabilitiesPayload, refreshedLocalScenarios),
    );
    setLocalIamScenarios(refreshedLocalScenarios);
    setScenarioExplorerCatalog(
      catalogWithLocalScenarios(explorerCatalogPayload, refreshedLocalScenarios),
    );
    setIamKeyState(iamKeyPayload);
    setIamKeyInput(iamKeyPayload.value || "");
    setBrightwayDiscovery(brightwayPayload);
    setRecents(recentsPayload);
    setStartupIssues(warnings);
    setInitializationProgress((current) => ({
      ...current,
      label: warnings.length ? "Startup checks completed with issues" : "Startup checks refreshed",
      detail: warnings.length
        ? `${warnings.length} startup check${warnings.length === 1 ? "" : "s"} still need attention.`
        : "Environment, discovery, and local metadata are up to date.",
    }));
    window.setTimeout(() => {
      setInitializationProgress((current) => ({ ...current, active: false }));
    }, 400);
    setMessage({
      text: warnings.length
        ? `Startup checks completed with partial data:\n${warnings.join("\n")}`
        : "Startup checks refreshed successfully.",
      tone: warnings.length ? "info" : "success",
    });
  }

  function toggleScenarioExplorerPath(path: string) {
    setScenarioExplorerSelectedPaths((current) => {
      const next = current.includes(path)
        ? current.filter((entry) => entry !== path)
        : [...current, path];
      return next;
    });
    setScenarioExplorerSummary(null);
    setScenarioExplorerComparison(null);
    setScenarioExplorerHiddenSeries([]);
    setScenarioExplorerDirty(true);
  }

  function useConfigurationScenariosForExplorer() {
    const nextPaths = defaultExplorerScenarioPaths(draftProject, explorerCatalogScenarios);
    setScenarioExplorerSelectedPaths(nextPaths);
    setScenarioExplorerSummary(null);
    setScenarioExplorerComparison(null);
    setScenarioExplorerDirty(true);
    setMessage({
      text: nextPaths.length
        ? `Loaded ${nextPaths.length} local scenario${nextPaths.length === 1 ? "" : "s"} from the current configuration into the explorer.`
        : "No local installed scenarios matched the current configuration.",
      tone: nextPaths.length ? "success" : "warning",
    });
  }

  function currentExplorerUiState(): ExplorerUiState {
    return {
      selected_paths: scenarioExplorerSelectedPaths,
      selected_sector: scenarioExplorerSelectedSector,
      selected_groups: scenarioExplorerSelectedGroups,
      selected_variables: scenarioExplorerSelectedVariables,
      group_selection_mode: scenarioExplorerGroupSelectionMode,
      variable_selection_mode: scenarioExplorerVariableSelectionMode,
      compare_mode: scenarioExplorerCompareMode,
      baseline_scenario_id: scenarioExplorerBaselineScenarioId,
      baseline_year: scenarioExplorerBaselineYear,
      year_start: scenarioExplorerYearStart,
      year_end: scenarioExplorerYearEnd,
      chart_mode: scenarioExplorerChartMode,
      plot_layout: scenarioExplorerPlotLayout,
      hidden_series: scenarioExplorerHiddenSeries,
    };
  }

  function applyExplorerUiStateToControls(nextState: ExplorerUiState) {
    setScenarioExplorerSelectedPaths(nextState.selected_paths);
    setScenarioExplorerSelectedSector(nextState.selected_sector);
    setScenarioExplorerSelectedGroups(nextState.selected_groups);
    setScenarioExplorerSelectedVariables(nextState.selected_variables);
    setScenarioExplorerGroupSelectionMode(nextState.group_selection_mode);
    setScenarioExplorerVariableSelectionMode(nextState.variable_selection_mode);
    setScenarioExplorerCompareMode(nextState.compare_mode);
    setScenarioExplorerBaselineScenarioId(nextState.baseline_scenario_id);
    setScenarioExplorerBaselineYear(nextState.baseline_year);
    setScenarioExplorerYearStart(nextState.year_start);
    setScenarioExplorerYearEnd(nextState.year_end);
    setScenarioExplorerChartMode(nextState.chart_mode);
    setScenarioExplorerPlotLayout(nextState.plot_layout);
    setScenarioExplorerHiddenSeries(nextState.hidden_series);
  }

  function projectWithCurrentExplorerUiState(
    projectOverride?: GuiProjectManifest,
    explorerStateOverride?: ExplorerUiState,
  ) {
    return projectWithExplorerUiState(
      normalizeProject(projectOverride ?? draftProject),
      explorerStateOverride ?? currentExplorerUiState(),
    );
  }

  function markExplorerRenderRequested() {
    explorerRenderStartedAtRef.current = performanceNow();
  }

  function markExplorerFilterRequested() {
    explorerFilterStartedAtRef.current = performanceNow();
    markExplorerRenderRequested();
  }

  function toggleExplorerHiddenSeries(label: string) {
    markExplorerRenderRequested();
    setScenarioExplorerHiddenSeries((current) =>
      current.includes(label)
        ? current.filter((entry) => entry !== label)
        : [...current, label],
    );
  }

  function clearExplorerHiddenSeries() {
    markExplorerRenderRequested();
    setScenarioExplorerHiddenSeries([]);
  }

  function setExplorerGroupMode(mode: ExplorerSelectionMode) {
    markExplorerFilterRequested();
    setScenarioExplorerGroupSelectionMode(mode);
    if (mode !== "custom") {
      setScenarioExplorerSelectedGroups([]);
    }
    setScenarioExplorerHiddenSeries([]);
  }

  function setExplorerGroupSelection(values: string[]) {
    markExplorerFilterRequested();
    setScenarioExplorerGroupSelectionMode(values.length ? "custom" : "none");
    setScenarioExplorerSelectedGroups(values);
    setScenarioExplorerHiddenSeries([]);
  }

  function setExplorerVariableMode(mode: ExplorerSelectionMode) {
    markExplorerFilterRequested();
    setScenarioExplorerVariableSelectionMode(mode);
    if (mode !== "custom") {
      setScenarioExplorerSelectedVariables([]);
    }
    setScenarioExplorerHiddenSeries([]);
  }

  function setExplorerVariableSelection(values: string[]) {
    markExplorerFilterRequested();
    setScenarioExplorerVariableSelectionMode(values.length ? "custom" : "none");
    setScenarioExplorerSelectedVariables(values);
    setScenarioExplorerHiddenSeries([]);
  }

  function applyExplorerScenariosToConfiguration() {
    if (!explorerSelectedScenarioEntries.length) {
      setMessage({
        text: "Select at least one Scenario Explorer file before applying it to the configuration.",
        tone: "error",
      });
      return;
    }

    const nextScenarioSets = normalizeScenarioSets(draftProject.scenario_sets);
    const targetSet = nextScenarioSets[0] || { name: "default", scenarios: [] };
    const existingYears = targetSet.scenarios.length
      ? targetSet.scenarios.map((entry) => entry.year)
      : [FALLBACK_SCENARIO.year];

    nextScenarioSets[0] = {
      ...targetSet,
      name: targetSet.name || "default",
      scenarios: explorerSelectedScenarioEntries.map((entry, index) =>
        normalizeScenario({
          model: entry.model,
          pathway: entry.pathway,
          year: existingYears[index % existingYears.length] || FALLBACK_SCENARIO.year,
          filepath: entry.path,
        }),
      ),
    };

    updateDraftProject({
      ...draftProject,
      scenario_sets: nextScenarioSets,
    });
    setMessage({
      text: `Applied ${explorerSelectedScenarioEntries.length} Scenario Explorer selection${explorerSelectedScenarioEntries.length === 1 ? "" : "s"} to the first configuration scenario set.`,
      tone: "success",
    });
  }

  async function loadScenarioExplorerSummary() {
    if (!scenarioExplorerSelectedPaths.length) {
      setMessage({
        text: "Select at least one installed IAM scenario before loading the Scenario Explorer.",
        tone: "error",
      });
      return null;
    }
    if (!scenarioExplorerSelectedSector) {
      setMessage({
        text: "Select a sector before loading the Scenario Explorer.",
        tone: "error",
      });
      return null;
    }

    const parsedYearStart = scenarioExplorerYearStart.trim()
      ? Number(scenarioExplorerYearStart)
      : null;
    const parsedYearEnd = scenarioExplorerYearEnd.trim()
      ? Number(scenarioExplorerYearEnd)
      : null;
    const yearStart =
      parsedYearStart != null && Number.isFinite(parsedYearStart) ? parsedYearStart : null;
    const yearEnd =
      parsedYearEnd != null && Number.isFinite(parsedYearEnd) ? parsedYearEnd : null;
    const parsedBaselineYear = scenarioExplorerBaselineYear.trim()
      ? Number(scenarioExplorerBaselineYear)
      : null;
    const baselineYear =
      parsedBaselineYear != null && Number.isFinite(parsedBaselineYear)
        ? parsedBaselineYear
        : null;
    explorerReloadStartedAtRef.current = performanceNow();
    markExplorerRenderRequested();
    setScenarioExplorerLoading(true);
    try {
      if (explorerCompareModeUsesBackendCompare(scenarioExplorerCompareMode)) {
        const payload = await apiRequest<ScenarioExplorerComparePayload>(
          "/api/scenario-explorer/compare",
          {
            method: "POST",
            body: JSON.stringify({
              scenario_paths: scenarioExplorerSelectedPaths,
              sector: scenarioExplorerSelectedSector,
              compare_mode: scenarioExplorerCompareMode,
              baseline_year: baselineYear,
              baseline_scenario_id:
                scenarioExplorerCompareMode === "delta" ||
                scenarioExplorerCompareMode === "percent_change"
                  ? scenarioExplorerBaselineScenarioId || null
                  : null,
              year_start: yearStart,
              year_end: yearEnd,
            }),
          },
        );
        const summary = payload.summary;
        const nextGroupSelection = sanitizeExplorerSelection(
          scenarioExplorerGroupSelectionMode,
          scenarioExplorerSelectedGroups,
          explorerGroupOptions(summary),
        );
        const nextVariableSelection = sanitizeExplorerSelection(
          scenarioExplorerVariableSelectionMode,
          scenarioExplorerSelectedVariables,
          summary.variables,
        );
        setScenarioExplorerComparison(payload);
        setScenarioExplorerSummary(summary);
        setScenarioExplorerBaselineScenarioId(payload.baseline_scenario_id || "");
        setScenarioExplorerBaselineYear(
          payload.baseline_year != null ? String(payload.baseline_year) : "",
        );
        setScenarioExplorerGroupSelectionMode(nextGroupSelection.mode);
        setScenarioExplorerSelectedGroups(nextGroupSelection.selected);
        setScenarioExplorerVariableSelectionMode(nextVariableSelection.mode);
        setScenarioExplorerSelectedVariables(nextVariableSelection.selected);
        setScenarioExplorerHiddenSeries([]);
        setScenarioExplorerChartMode((current) =>
          current || defaultExplorerChartMode(summary, scenarioExplorerCompareMode),
        );
      } else {
        const payload = await apiRequest<ScenarioExplorerSummaryPayload>(
          "/api/scenario-explorer/sector-summary",
          {
            method: "POST",
            body: JSON.stringify({
              scenario_paths: scenarioExplorerSelectedPaths,
              sector: scenarioExplorerSelectedSector,
              year_start: yearStart,
              year_end: yearEnd,
            }),
          },
        );
        const nextGroupSelection = sanitizeExplorerSelection(
          scenarioExplorerGroupSelectionMode,
          scenarioExplorerSelectedGroups,
          explorerGroupOptions(payload),
        );
        const nextVariableSelection = sanitizeExplorerSelection(
          scenarioExplorerVariableSelectionMode,
          scenarioExplorerSelectedVariables,
          payload.variables,
        );
        setScenarioExplorerComparison(null);
        setScenarioExplorerSummary(payload);
        setScenarioExplorerGroupSelectionMode(nextGroupSelection.mode);
        setScenarioExplorerSelectedGroups(nextGroupSelection.selected);
        setScenarioExplorerVariableSelectionMode(nextVariableSelection.mode);
        setScenarioExplorerSelectedVariables(nextVariableSelection.selected);
        setScenarioExplorerHiddenSeries([]);
        setScenarioExplorerChartMode((current) =>
          current || defaultExplorerChartMode(payload, scenarioExplorerCompareMode),
        );
      }

      setScenarioExplorerDirty(false);
      if (explorerReloadStartedAtRef.current != null) {
        setScenarioExplorerPerformance((current) => ({
          ...current,
          lastReloadMs: performanceNow() - explorerReloadStartedAtRef.current!,
        }));
        explorerReloadStartedAtRef.current = null;
      }
      setMessage({
        text:
          !explorerCompareModeUsesBackendCompare(scenarioExplorerCompareMode)
            ? `Loaded Scenario Explorer summary for ${scenarioExplorerSelectedSector}.`
            : `Loaded ${explorerCompareModeLabel(scenarioExplorerCompareMode).toLowerCase()} view for ${scenarioExplorerSelectedSector}.`,
        tone: "success",
      });
    } finally {
      explorerReloadStartedAtRef.current = null;
      setScenarioExplorerLoading(false);
    }
    return null;
  }

  async function downloadAllIamScenarios() {
    const payload = await apiRequest<DownloadAllScenariosPayload>(
      "/api/discovery/iam-scenarios/download-all",
      {
        method: "POST",
      },
    );
    setScenarioDownload(payload);
    setMessage({
      text:
        payload.status === "running"
          ? `Downloading IAM scenarios into ${payload.directory}.`
          : `Scenario download status refreshed for ${payload.directory}.`,
      tone: "info",
    });
    return payload;
  }

  async function clearLocalIamScenarios() {
    if (
      !window.confirm(
        "Remove all local IAM scenario files from premise/data/iam_output_files?",
      )
    ) {
      return null;
    }

    const payload = await apiRequest<ClearLocalScenariosPayload>(
      "/api/discovery/iam-scenarios/clear",
      {
        method: "POST",
      },
    );
    const [nextCapabilities, nextExplorerCatalog] = await Promise.all([
      apiRequest<CapabilitiesPayload>("/api/capabilities"),
      refreshScenarioExplorerCatalog(),
    ]);
    const refreshedLocalScenarios = await refreshLocalIamScenarios().catch(() =>
      reconciledLocalScenarioFiles(
        nextCapabilities.iam_scenarios,
        nextExplorerCatalog.scenarios,
      ),
    );
    setCapabilities(capabilitiesWithLocalScenarios(nextCapabilities, refreshedLocalScenarios));
    setScenarioExplorerCatalog(
      catalogWithLocalScenarios(nextExplorerCatalog, refreshedLocalScenarios),
    );
    setLocalIamScenarios(refreshedLocalScenarios);
    setScenarioExplorerSelectedPaths([]);
    setScenarioExplorerSelectedGroups([]);
    setScenarioExplorerSelectedVariables([]);
    setScenarioExplorerGroupSelectionMode("all");
    setScenarioExplorerVariableSelectionMode("all");
    setScenarioExplorerHiddenSeries([]);
    setScenarioExplorerSummary(null);
    setScenarioExplorerComparison(null);
    setScenarioExplorerDirty(true);
    setScenarioDownload(null);
    setMessage({
      text: payload.removed_count
        ? `Removed ${payload.removed_count} local IAM file${payload.removed_count === 1 ? "" : "s"} from ${payload.directory}.`
        : `No local IAM scenario files were present in ${payload.directory}.`,
      tone: "success",
    });

    await previewProject(draftProject, projectPath, projectHistory).catch((error: Error) =>
      console.error(error),
    );
    return payload;
  }

  async function refreshScenarioDownload(jobId: string) {
    const payload = await apiRequest<DownloadAllScenariosPayload>(
      `/api/discovery/iam-scenarios/download-all/${encodeURIComponent(jobId)}`,
    );
    setScenarioDownload(payload);
    return payload;
  }

  async function finalizeScenarioDownload(payload: DownloadAllScenariosPayload) {
    const [nextCapabilities, nextExplorerCatalog] = await Promise.all([
      apiRequest<CapabilitiesPayload>("/api/capabilities"),
      refreshScenarioExplorerCatalog(),
    ]);
    const refreshedLocalScenarios = await refreshLocalIamScenarios().catch(() =>
      reconciledLocalScenarioFiles(
        nextCapabilities.iam_scenarios,
        nextExplorerCatalog.scenarios,
      ),
    );
    const nextProject = syncProjectScenariosToAvailableFiles(
      draftProject,
      refreshedLocalScenarios,
    );
    setCapabilities(capabilitiesWithLocalScenarios(nextCapabilities, refreshedLocalScenarios));
    setScenarioExplorerCatalog(
      catalogWithLocalScenarios(nextExplorerCatalog, refreshedLocalScenarios),
    );
    setLocalIamScenarios(refreshedLocalScenarios);
    setDraftProject(nextProject);
    setValidation(null);
    setFormDirty(true);
    setScenarioExplorerDirty(true);
    setMessage({
      text: payload.failed.length
        ? `Downloaded ${payload.downloaded.length} scenario files to ${payload.directory}, kept ${payload.existing.length} existing files, saw ${payload.failed.length} download failure${payload.failed.length === 1 ? "" : "s"}, and refreshed the scenario selectors from the local IAM folder.`
        : `Scenario download complete. Downloaded ${payload.downloaded.length} new file${payload.downloaded.length === 1 ? "" : "s"} to ${payload.directory}, kept ${payload.existing.length} existing file${payload.existing.length === 1 ? "" : "s"}, and refreshed the scenario selectors from the local IAM folder.`,
      tone: payload.failed.length ? "error" : "success",
    });

    await previewProject(nextProject, projectPath, projectHistory).catch((error: Error) =>
      console.error(error),
    );
    return payload;
  }

  async function selectBrightwayProject(projectName: string) {
    const payload = await apiRequest<BrightwayDiscoveryPayload>(
      "/api/discovery/brightway/project",
      {
        method: "POST",
        body: JSON.stringify({ project_name: projectName }),
      },
    );
    setBrightwayDiscovery(payload);

    const availableDatabases = new Set(payload.databases);
    const currentSourceDb = draftProject.config.source_db;
    updateDraftProject({
      ...draftProject,
      config: {
        ...draftProject.config,
        source_project: payload.current_project || projectName,
        source_db:
          currentSourceDb && availableDatabases.has(currentSourceDb)
            ? currentSourceDb
            : "",
      },
    });
    setMessage({
      text: payload.available
        ? `Loaded ${payload.databases.length} Brightway database${payload.databases.length === 1 ? "" : "s"} from project ${payload.current_project || projectName}.`
        : "Brightway discovery is unavailable in this environment.",
      tone: payload.available ? "success" : "error",
    });
    return payload;
  }

  async function refreshProjectHistory(pathOverride?: string) {
    const targetPath = pathOverride ?? projectPath;
    if (!targetPath) {
      setProjectHistory([]);
      return [];
    }

    const response = await apiRequest<{ run_history: RunHistoryEntry[] }>(
      "/api/projects/history",
      {
        method: "POST",
        body: JSON.stringify({ path: targetPath }),
      },
    );
    setProjectHistory(response.run_history ?? []);
    return response.run_history ?? [];
  }

  async function loadDiagnostics(target: RunDiagnosticsTarget) {
    const payload = await apiRequest<DiagnosticsPayload>(
      "/api/diagnostics/run-details",
      {
        method: "POST",
        body: JSON.stringify(target),
      },
    );
    setDiagnosticsTarget(target);
    setDiagnostics(payload);
    return payload;
  }

  async function refreshDiagnosticsForCurrentRun(runId: string) {
    if (diagnosticsTarget?.run_id && diagnosticsTarget.run_id !== runId) {
      return;
    }
    await loadDiagnostics({ run_id: runId });
  }

  async function previewProject(
    projectOverride?: GuiProjectManifest,
    pathOverride?: string,
    historyOverride?: RunHistoryEntry[],
    explorerStateOverride?: ExplorerUiState,
  ): Promise<ValidationPayload> {
    const project = buildProjectPayload(
      projectWithCurrentExplorerUiState(projectOverride, explorerStateOverride),
      historyOverride ?? projectHistory,
    );
    const targetPath = pathOverride ?? projectPath;

    setMessage(null);
    const manifestResponse = await apiRequest<{
      run_manifest: RunManifest;
    }>("/api/projects/run-manifest", {
      method: "POST",
      body: JSON.stringify({
        path: targetPath || null,
        project,
      }),
    });
    setManifestPreview(manifestResponse.run_manifest);

    const validationResponse = await apiRequest<ValidationPayload>(
      "/api/jobs/validate",
      {
        method: "POST",
        body: JSON.stringify({
          run_manifest: manifestResponse.run_manifest,
        }),
      },
    );
    setValidation(validationResponse);
    setFormDirty(false);

    if (validationResponse.valid) {
      if (validationResponse.warnings.length) {
        setMessage({
          text: validationResponse.warnings.join("\n"),
          tone: "success",
        });
      } else {
        setMessage(null);
      }

      const basePath = targetPath || null;
      if (
        project.config.source_type === "ecospold" &&
        project.config.source_file_path &&
        (looksAbsolutePath(project.config.source_file_path) || basePath)
      ) {
        await rememberRecent("source_directory", project.config.source_file_path, {
          label: "Ecospold source",
          basePath,
        }).catch((error: Error) => console.error(error));
      }

      for (const scenarioSet of project.scenario_sets) {
        for (const scenario of scenarioSet.scenarios) {
          if (
            scenario.filepath &&
            (looksAbsolutePath(scenario.filepath) || basePath)
          ) {
            await rememberRecent("scenario_file", scenario.filepath, {
              label: `${scenario.model} / ${scenario.pathway}`,
              basePath,
            }).catch((error: Error) => console.error(error));
          }
        }
      }

      if (
        PATH_BASED_EXPORT_TYPES.has(project.config.export.type) &&
        typeof project.config.export.options.filepath === "string" &&
        project.config.export.options.filepath &&
        (looksAbsolutePath(project.config.export.options.filepath) || basePath)
      ) {
        await rememberRecent("export_directory", project.config.export.options.filepath, {
          label: `${exportTypeLabel(project.config.export.type)} export`,
          basePath,
        }).catch((error: Error) => console.error(error));
      }

      return validationResponse;
    }

    setMessage({
      text: validationResponse.errors.join("\n"),
      tone: "error",
    });
    return validationResponse;
  }

  async function browseProjectPath(mode: "save_file" | "open_file" = "save_file") {
    const selectedPath = await choosePathDialog({
      mode,
      title:
        mode === "open_file"
          ? "Open Premise configuration"
          : "Choose Premise configuration file",
      initial_path: projectPath || null,
      default_extension: ".json",
      filters: [
        { label: "JSON files", pattern: "*.json" },
        { label: "All files", pattern: "*.*" },
      ],
    });
    if (!selectedPath) {
      return null;
    }

    setProjectPath(selectedPath);
    markFormDirty();
    return selectedPath;
  }

  async function browseSourceDirectory() {
    const selectedPath = await choosePathDialog({
      mode: "open_directory",
      title: "Choose ecospold directory",
      initial_path: draftProject.config.source_file_path || null,
      must_exist: true,
    });
    if (!selectedPath) {
      return;
    }
    updateConfig({
      source_type: "ecospold",
      source_file_path: selectedPath,
    });
  }

  async function browseExportDirectory() {
    if (!PATH_BASED_EXPORT_TYPES.has(exportType)) {
      setMessage({
        text: `${exportTypeLabel(exportType)} does not use a filesystem export path.`,
        tone: "error",
      });
      return;
    }

    const currentPath =
      typeof exportOptions.filepath === "string" ? exportOptions.filepath : "";
    const selectedPath = await choosePathDialog({
      mode: "open_directory",
      title: `Choose ${exportTypeLabel(exportType)} export directory`,
      initial_path: currentPath || projectPath || null,
      must_exist: true,
    });
    if (!selectedPath) {
      return;
    }
    updateExportOption("filepath", selectedPath);
  }

  async function browseScenarioFile(setIndex: number, scenarioIndex: number) {
    const currentScenario = scenarioSets[setIndex].scenarios[scenarioIndex];
    const selectedPath = await choosePathDialog({
      mode: "open_file",
      title: "Choose IAM scenario file",
      initial_path: currentScenario.filepath || null,
      must_exist: true,
      filters: [
        { label: "IAM files", pattern: "*.csv" },
        { label: "IAM files", pattern: "*.mif" },
        { label: "Excel files", pattern: "*.xlsx" },
        { label: "Excel files", pattern: "*.xls" },
        { label: "All files", pattern: "*.*" },
      ],
    });
    if (!selectedPath) {
      return;
    }
    updateScenario(setIndex, scenarioIndex, { filepath: selectedPath });
    await previewScenarioFile(setIndex, scenarioIndex, selectedPath);
  }

  async function browseAdditionalInventory(index: number) {
    const currentInventory = Array.isArray(draftProject.config.additional_inventories)
      ? draftProject.config.additional_inventories[index]
      : undefined;
    const selectedPath = await choosePathDialog({
      mode: "open_file",
      title: "Choose additional inventory file",
      initial_path: currentInventory?.filepath || null,
      must_exist: true,
      filters: [
        { label: "Spreadsheet files", pattern: "*.xlsx" },
        { label: "Spreadsheet files", pattern: "*.xls" },
        { label: "CSV files", pattern: "*.csv" },
        { label: "All files", pattern: "*.*" },
      ],
    });
    if (!selectedPath) {
      return;
    }
    updateAdditionalInventory(index, { filepath: selectedPath });
  }

  async function previewScenarioFile(
    setIndex: number,
    scenarioIndex: number,
    pathOverride?: string,
  ) {
    const path = pathOverride || scenarioSets[setIndex].scenarios[scenarioIndex].filepath;
    if (!path) {
      setMessage({
        text: "Choose a scenario file before requesting a preview.",
        tone: "error",
      });
      return;
    }

    const key = scenarioKey(setIndex, scenarioIndex);
    setPreviewLoadingKey(key);
    try {
      const payload = await apiRequest<ScenarioPreviewPayload>(
        "/api/discovery/scenario-preview",
        {
          method: "POST",
          body: JSON.stringify({ path }),
        },
      );
      setScenarioPreviews((current) => ({
        ...current,
        [key]: payload,
      }));
      setSelectedPreviewKey(key);
      setActiveTab("scenarios");
      setMessage({
        text: `Loaded scenario preview for ${payload.file_name}.`,
        tone: "success",
      });
      await rememberRecent("scenario_file", path, {
        label: payload.file_name,
        basePath: projectPath || null,
      }).catch((error: Error) => console.error(error));
    } finally {
      setPreviewLoadingKey(null);
    }
  }

  async function loadTemplate(workflowOverride = workflow, preserveCurrent = false) {
    const response = await apiRequest<{ project: GuiProjectManifest }>(
      `/api/projects/template?workflow=${encodeURIComponent(workflowOverride)}`,
    );
    const availableLocalScenarios = reconciledLocalScenarioFiles(
      capabilities?.iam_scenarios,
      scenarioExplorerCatalog?.scenarios,
    );

    let nextProject = syncProjectScenariosToAvailableFiles(
      response.project,
      availableLocalScenarios,
    );
    if (preserveCurrent) {
      nextProject = mergeProjectIntoTemplate(
        nextProject,
        projectWithCurrentExplorerUiState(),
      );
    }

    if (
      nextProject.config.source_type === "brightway" &&
      !nextProject.config.source_project &&
      brightwayDiscovery.current_project
    ) {
      nextProject.config.source_project = brightwayDiscovery.current_project;
    }
    const nextExplorerState = resolvedExplorerUiState(
      nextProject,
      explorerCatalogScenarios,
      explorerCatalogSectors.map((entry) => entry.id),
    );

    startTransition(() => {
      setDraftProject(nextProject);
      applyExplorerUiStateToControls(nextExplorerState);
      setProjectPath("");
      setProjectHistory([]);
      setManifestPreview(null);
      setValidation(null);
      setMessage({
        text: preserveCurrent
          ? `Switched to ${workflowLabel(workflowOverride)} and carried over shared settings.`
          : `Loaded a fresh ${workflowLabel(workflowOverride)} template.`,
        tone: "success",
      });
      clearDiagnostics();
      clearPreviewState();
      resetRunMonitor();
      setFormDirty(true);
      setActiveTab("overview");
    });

    await previewProject(nextProject, "", [], nextExplorerState);
  }

  async function openProject(pathOverride?: string) {
    const targetPath =
      pathOverride || projectPath || (await browseProjectPath("open_file"));
    if (!targetPath) {
      return;
    }

    const response = await apiRequest<{
      path: string;
      project: GuiProjectManifest;
    }>("/api/projects/open", {
      method: "POST",
      body: JSON.stringify({ path: targetPath }),
    });
    const nextProject = normalizeProject(response.project);
    const nextHistory = response.project.run_history ?? [];

    if (
      nextProject.config.source_type === "brightway" &&
      nextProject.config.source_project &&
      nextProject.config.source_project !== brightwayDiscovery.current_project
    ) {
      try {
        const payload = await apiRequest<BrightwayDiscoveryPayload>(
          "/api/discovery/brightway/project",
          {
            method: "POST",
            body: JSON.stringify({
              project_name: nextProject.config.source_project,
            }),
          },
        );
        setBrightwayDiscovery(payload);
        if (!payload.databases.includes(nextProject.config.source_db)) {
          nextProject.config.source_db = "";
        }
      } catch (error) {
        console.error(error);
      }
    } else if (
      nextProject.config.source_type === "brightway" &&
      !nextProject.config.source_project &&
      brightwayDiscovery.current_project
    ) {
      nextProject.config.source_project = brightwayDiscovery.current_project;
    }
    const nextExplorerState = resolvedExplorerUiState(
      nextProject,
      explorerCatalogScenarios,
      explorerCatalogSectors.map((entry) => entry.id),
    );

    startTransition(() => {
      setProjectPath(response.path);
      setDraftProject(nextProject);
      applyExplorerUiStateToControls(nextExplorerState);
      setProjectHistory(nextHistory);
      setMessage({
        text: `Loaded configuration from ${response.path}`,
        tone: "success",
      });
      clearDiagnostics();
      clearPreviewState();
      resetRunMonitor();
      setFormDirty(true);
      setActiveTab("overview");
    });

    await refreshRecents().catch((error: Error) => console.error(error));
    await previewProject(nextProject, response.path, nextHistory, nextExplorerState);
  }

  async function saveProject() {
    const targetPath = projectPath || (await browseProjectPath("save_file"));
    if (!targetPath) {
      return null;
    }

    const project = buildProjectPayload(
      projectWithCurrentExplorerUiState(),
      projectHistory,
    );
    const response = await apiRequest<{
      path: string;
      project: GuiProjectManifest;
    }>("/api/projects/save", {
      method: "POST",
      body: JSON.stringify({
        path: targetPath,
        project,
      }),
    });

    setProjectPath(response.path);
    setMessage({
      text: `Configuration saved to ${response.path}`,
      tone: "success",
    });
    void refreshRecents().catch((error: Error) => console.error(error));
    void refreshProjectHistory(response.path).catch((error: Error) =>
      console.error(error),
    );
    return response;
  }

  async function cloneCurrentProject() {
    const targetPath = await choosePathDialog({
      mode: "save_file",
      title: "Clone Premise configuration to...",
      initial_path: projectPath || null,
      default_extension: ".json",
      filters: [
        { label: "JSON files", pattern: "*.json" },
        { label: "All files", pattern: "*.*" },
      ],
    });
    if (!targetPath) {
      return;
    }

    if (projectPath) {
      const response = await apiRequest<{
        path: string;
        project: GuiProjectManifest;
      }>("/api/projects/clone", {
        method: "POST",
        body: JSON.stringify({
          source_path: projectPath,
          target_path: targetPath,
        }),
      });
      const nextProject = normalizeProject(response.project);
      setProjectPath(response.path);
      setDraftProject(nextProject);
      setProjectHistory(nextProject.run_history ?? []);
      setMessage({
        text: `Cloned configuration to ${response.path}`,
        tone: "success",
      });
      await refreshRecents().catch((error: Error) => console.error(error));
      await previewProject(nextProject, response.path, []);
      return;
    }

    const nextProject = normalizeProject({
      ...projectWithCurrentExplorerUiState(),
      project_name: `${draftProject.project_name} Copy`,
      run_history: [],
    });
    const response = await apiRequest<{
      path: string;
      project: GuiProjectManifest;
    }>("/api/projects/save", {
      method: "POST",
      body: JSON.stringify({
        path: targetPath,
        project: nextProject,
      }),
    });

    setProjectPath(response.path);
    setDraftProject(normalizeProject(response.project));
    setProjectHistory([]);
    setMessage({
      text: `Cloned configuration to ${response.path}`,
      tone: "success",
    });
    await refreshRecents().catch((error: Error) => console.error(error));
    await previewProject(nextProject, response.path, []);
  }

  async function queueProject() {
    const currentValidation =
      formDirty || validation == null ? await previewProject() : validation;

    if (!currentValidation.valid) {
      return;
    }

    const payloadProject = buildProjectPayload(
      projectWithCurrentExplorerUiState(),
      projectHistory,
    );
    const response = await apiRequest<{
      run_id: string;
      status: string;
      queue_position: number | null;
      dry_run: boolean;
    }>("/api/jobs/enqueue-project", {
      method: "POST",
      body: JSON.stringify({
        path: projectPath || null,
        project: payloadProject,
        dry_run: dryRun,
      }),
    });

    const keepCurrentMonitor =
      response.status === "queued" &&
      Boolean(currentRunId) &&
      currentRunId !== response.run_id &&
      !isTerminalRunStatus(currentRunStatus);

    if (!keepCurrentMonitor) {
      focusRunMonitor(response.run_id, {
        status: response.status || "queued",
        queuePosition: response.queue_position,
      });
    } else {
      runStatusByIdRef.current[response.run_id] = response.status || "queued";
      setPendingMonitorRunIds((current) =>
        current.includes(response.run_id) ? current : [...current, response.run_id],
      );
    }
    setActiveTab("monitor");
    setMessage({
      text:
        response.status === "queued" && response.queue_position
          ? keepCurrentMonitor
            ? `${response.dry_run ? "Dry-run" : "Live"} worker queued at position ${response.queue_position} with run ID ${response.run_id}. Monitoring current run ${currentRunId} until it finishes.`
            : `${response.dry_run ? "Dry-run" : "Live"} worker queued at position ${response.queue_position} with run ID ${response.run_id}.`
          : `${response.dry_run ? "Dry-run" : "Live"} worker started with run ID ${response.run_id}.`,
      tone: "success",
    });

    await refreshProjectHistory(projectPath).catch((error: Error) =>
      console.error(error),
    );
  }

  async function cancelCurrentRun() {
    if (!currentRunId) {
      setMessage({
        text: "No current run is selected for cancellation.",
        tone: "error",
      });
      return;
    }

    const response = await apiRequest<{ run_id: string; status: string }>(
      "/api/jobs/cancel",
      {
        method: "POST",
        body: JSON.stringify({ run_id: currentRunId }),
      },
    );

    setCurrentRunStatus(response.status);
    setCurrentQueuePosition(null);
    runStatusByIdRef.current[currentRunId] = response.status;
    setMessage({
      text: `Run ${response.run_id} is ${response.status}.`,
      tone: "success",
    });

    await refreshProjectHistory(projectPath).catch((error: Error) =>
      console.error(error),
    );
  }

  async function loadHistorySnapshot(runId: string) {
    const entry = projectHistory.find((item) => item.run_id === runId);
    if (!entry?.project_snapshot) {
      setMessage({
        text: "No reusable configuration snapshot is available for that history entry.",
        tone: "error",
      });
      return;
    }

    const nextProject = normalizeProject(entry.project_snapshot);
    startTransition(() => {
      setDraftProject(nextProject);
      setMessage({
        text: `Loaded configuration snapshot from run ${runId}.`,
        tone: "success",
      });
      clearPreviewState();
      setFormDirty(true);
      setActiveTab("overview");
    });

    await previewProject(nextProject, projectPath, projectHistory);
  }

  async function loadHistoryDetails(runId: string) {
    const entry = projectHistory.find((item) => item.run_id === runId);
    if (!entry?.run_dir) {
      setMessage({
        text: "No diagnostics are available for that history entry.",
        tone: "error",
      });
      return;
    }

    await loadDiagnostics({
      run_id: entry.run_id,
      project_path: projectPath || null,
      run_dir: entry.run_dir,
    });
    setActiveTab("troubleshooting");
  }

  function applyRecentPath(entry: RecentEntry) {
    if (entry.kind === "source_directory") {
      updateConfig({
        source_type: "ecospold",
        source_file_path: entry.path,
      });
      setActiveTab("source");
      return;
    }

    if (entry.kind === "export_directory") {
      updateExportOption("filepath", entry.path);
      setActiveTab("export");
      return;
    }

    if (entry.kind === "scenario_file") {
      updateScenario(0, 0, { filepath: entry.path });
      void previewScenarioFile(0, 0, entry.path).catch((error: Error) =>
        setMessage({ text: error.message, tone: "error" }),
      );
      return;
    }

    setMessage({
      text: `No handler is wired for recent path kind \`${entry.kind || "other"}\` yet.`,
      tone: "error",
    });
  }

  function applyPreviewMetadata() {
    if (!selectedPreview || !selectedPreviewIndices) {
      return;
    }
    updateScenario(selectedPreviewIndices.setIndex, selectedPreviewIndices.scenarioIndex, {
      model:
        selectedPreview.inferred_model ||
        scenarioSets[selectedPreviewIndices.setIndex].scenarios[
          selectedPreviewIndices.scenarioIndex
        ].model,
      pathway:
        selectedPreview.inferred_pathway ||
        scenarioSets[selectedPreviewIndices.setIndex].scenarios[
          selectedPreviewIndices.scenarioIndex
        ].pathway,
    });
    setMessage({
      text: "Applied inferred model and pathway values from the scenario preview.",
      tone: "success",
    });
  }

  function applyPreviewYears() {
    if (!selectedPreview || !selectedPreviewIndices) {
      return;
    }
    applyYearBatchToScenarioSet(selectedPreviewIndices.setIndex, selectedPreview.years);
  }

  useEffect(() => {
    if (!desktopNotificationsAvailable || typeof Notification === "undefined") {
      return;
    }
    setDesktopNotificationsEnabled(Notification.permission === "granted");
  }, [desktopNotificationsAvailable]);

  useEffect(() => {
    if (showAdvanced) {
      return;
    }
    if (ADVANCED_TAB_IDS.has(activeTab)) {
      setActiveTab("overview");
    }
  }, [activeTab, showAdvanced]);

  useEffect(() => {
    if (!hasInitializedWorkspaceRef.current || typeof window === "undefined") {
      return undefined;
    }

    const timer = window.setTimeout(() => {
      const savedAt = new Date().toISOString();
      persistAutosaveSnapshot({
        saved_at: savedAt,
        project: buildProjectPayload(projectWithCurrentExplorerUiState(), projectHistory),
        project_path: projectPath,
        dry_run: dryRun,
        workspace_mode: workspaceMode,
        active_tab: activeTab,
      });
      setAutosaveAt(savedAt);
    }, 280);

    return () => {
      window.clearTimeout(timer);
    };
  }, [
    activeTab,
    draftProject,
    dryRun,
    projectHistory,
    projectPath,
    scenarioExplorerBaselineScenarioId,
    scenarioExplorerBaselineYear,
    scenarioExplorerChartMode,
    scenarioExplorerCompareMode,
    scenarioExplorerGroupSelectionMode,
    scenarioExplorerHiddenSeries,
    scenarioExplorerPlotLayout,
    scenarioExplorerSelectedGroups,
    scenarioExplorerSelectedPaths,
    scenarioExplorerSelectedSector,
    scenarioExplorerSelectedVariables,
    scenarioExplorerVariableSelectionMode,
    scenarioExplorerYearEnd,
    scenarioExplorerYearStart,
    workspaceMode,
  ]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return undefined;
    }

    const handleKeydown = (event: KeyboardEvent) => {
      if (event.defaultPrevented || (!event.metaKey && !event.ctrlKey)) {
        return;
      }

      const normalizedKey = event.key.toLowerCase();
      if (normalizedKey === "s") {
        event.preventDefault();
        const savePromise = saveProjectShortcutRef.current?.();
        if (savePromise) {
          void savePromise.catch((error: Error) =>
            setMessage({ text: error.message, tone: "error" }),
          );
        }
        return;
      }

      if (event.key === "Enter") {
        event.preventDefault();
        const queuePromise = queueProjectShortcutRef.current?.();
        if (queuePromise) {
          void queuePromise.catch((error: Error) =>
            setMessage({ text: error.message, tone: "error" }),
          );
        }
      }
    };

    window.addEventListener("keydown", handleKeydown);
    return () => {
      window.removeEventListener("keydown", handleKeydown);
    };
  }, []);

  useEffect(() => {
    if (exportConfig.type !== exportType) {
      updateExportType(exportType);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [exportType]);

  useEffect(() => {
    if (
      sourceType === "brightway" &&
      !draftProject.config.source_project &&
      brightwayDiscovery.current_project
    ) {
      updateConfig({
        source_project: brightwayDiscovery.current_project,
      });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sourceType, brightwayDiscovery.current_project]);

  useEffect(() => {
    if (!explorerCatalogScenarios.length) {
      setScenarioExplorerSelectedPaths([]);
      setScenarioExplorerSelectedGroups([]);
      setScenarioExplorerSelectedVariables([]);
      setScenarioExplorerGroupSelectionMode("all");
      setScenarioExplorerVariableSelectionMode("all");
      setScenarioExplorerHiddenSeries([]);
      setScenarioExplorerSummary(null);
      setScenarioExplorerComparison(null);
      setScenarioExplorerDirty(true);
      return;
    }

    setScenarioExplorerSelectedPaths((current) => {
      const valid = current.filter((path) =>
        explorerCatalogScenarios.some((entry) => entry.path === path),
      );
      return valid.length
        ? valid
        : defaultExplorerScenarioPaths(draftProject, explorerCatalogScenarios);
    });
  }, [draftProject, explorerCatalogScenarios]);

  useEffect(() => {
    if (!explorerCatalogSectors.length) {
      setScenarioExplorerSelectedSector("");
      setScenarioExplorerSelectedGroups([]);
      setScenarioExplorerSelectedVariables([]);
      setScenarioExplorerGroupSelectionMode("all");
      setScenarioExplorerVariableSelectionMode("all");
      setScenarioExplorerHiddenSeries([]);
      setScenarioExplorerSummary(null);
      setScenarioExplorerComparison(null);
      setScenarioExplorerDirty(true);
      return;
    }

    setScenarioExplorerSelectedSector((current) =>
      explorerCatalogSectors.some((entry) => entry.id === current)
        ? current
        : explorerCatalogSectors[0].id,
    );
  }, [explorerCatalogSectors]);

  useEffect(() => {
    if (!explorerAvailableCompareModes.includes(scenarioExplorerCompareMode)) {
      setScenarioExplorerCompareMode(explorerAvailableCompareModes[0]);
      setScenarioExplorerHiddenSeries([]);
      setScenarioExplorerSummary(null);
      setScenarioExplorerComparison(null);
      setScenarioExplorerDirty(true);
    }
  }, [explorerAvailableCompareModes, scenarioExplorerCompareMode]);

  useEffect(() => {
    if (
      localScenarioResyncAttemptedRef.current ||
      localIamScenarios != null ||
      localScenarioFiles.length > 0
    ) {
      return;
    }

    localScenarioResyncAttemptedRef.current = true;
    void Promise.all([
      apiRequest<CapabilitiesPayload>("/api/capabilities"),
      refreshScenarioExplorerCatalog(),
      refreshLocalIamScenarios(),
    ])
      .then(([nextCapabilities, nextExplorerCatalog, nextLocalScenarios]) => {
        const refreshedLocalScenarios = reconciledLocalScenarioFiles(
          nextLocalScenarios,
          reconciledLocalScenarioFiles(
            nextCapabilities.iam_scenarios,
            nextExplorerCatalog.scenarios,
          ),
        );
        if (!refreshedLocalScenarios.length) {
          return;
        }

        setCapabilities(
          capabilitiesWithLocalScenarios(nextCapabilities, refreshedLocalScenarios),
        );
        setLocalIamScenarios(refreshedLocalScenarios);
        setScenarioExplorerCatalog(
          catalogWithLocalScenarios(nextExplorerCatalog, refreshedLocalScenarios),
        );
      })
      .catch((error: Error) => console.error(error));
  }, [localIamScenarios, localScenarioFiles.length]);

  useEffect(() => {
    if (!explorerBaselineChoices.length) {
      setScenarioExplorerBaselineScenarioId("");
      return;
    }

    setScenarioExplorerBaselineScenarioId((current) =>
      explorerBaselineChoices.some((entry) => entry.scenario_id === current)
        ? current
        : explorerBaselineChoices[0].scenario_id,
    );
  }, [explorerBaselineChoices]);

  useEffect(() => {
    const visibleLabels = new Set(explorerPlotSeries.map((entry) => entry.label));
    setScenarioExplorerHiddenSeries((current) =>
      {
        const next = current.filter((entry) => visibleLabels.has(entry));
        return stringListKey(next) === stringListKey(current) ? current : next;
      },
    );
  }, [explorerPlotLabelKey]);

  useEffect(() => {
    if (explorerUpdatePending) {
      explorerUpdatePendingRef.current = true;
      return;
    }

    if (explorerUpdatePendingRef.current && explorerFilterStartedAtRef.current != null) {
      setScenarioExplorerPerformance((current) => ({
        ...current,
        lastFilterMs: performanceNow() - explorerFilterStartedAtRef.current!,
      }));
      explorerFilterStartedAtRef.current = null;
    }
    explorerUpdatePendingRef.current = false;
  }, [explorerUpdatePending]);

  useEffect(() => {
    if (!scenarioStorylines.length) {
      if (selectedScenarioStorylineId) {
        setSelectedScenarioStorylineId("");
      }
      return;
    }
    if (
      selectedScenarioStorylineId &&
      scenarioStorylines.some((entry) => entry.id === selectedScenarioStorylineId)
    ) {
      return;
    }
    setSelectedScenarioStorylineId(defaultScenarioStorylineId || scenarioStorylines[0].id);
  }, [defaultScenarioStorylineId, scenarioStorylines, selectedScenarioStorylineId]);

  useEffect(() => {
    if (
      activeTab !== "explorer" ||
      scenarioExplorerLoading ||
      explorerRenderStartedAtRef.current == null
    ) {
      return;
    }

    let secondAnimationFrame: number | null = null;
    const animationFrame =
      typeof window !== "undefined"
        ? window.requestAnimationFrame(() => {
            secondAnimationFrame = window.requestAnimationFrame(() => {
              if (explorerRenderStartedAtRef.current == null) {
                return;
              }
              setScenarioExplorerPerformance((current) => ({
                ...current,
                lastChartPaintMs: performanceNow() - explorerRenderStartedAtRef.current!,
              }));
              explorerRenderStartedAtRef.current = null;
            });
          })
        : null;

    return () => {
      if (typeof window !== "undefined") {
        if (animationFrame != null) {
          window.cancelAnimationFrame(animationFrame);
        }
        if (secondAnimationFrame != null) {
          window.cancelAnimationFrame(secondAnimationFrame);
        }
      }
    };
  }, [
    activeTab,
    scenarioExplorerLoading,
    scenarioExplorerSummary,
    scenarioExplorerComparison,
    scenarioExplorerChartMode,
    scenarioExplorerPlotLayout,
    scenarioExplorerHiddenSeries,
    explorerPlotLabelKey,
    explorerUpdatePending,
  ]);

  useEffect(() => {
    if (
      activeTab !== "explorer" ||
      scenarioExplorerLoading ||
      scenarioExplorerSummary != null ||
      !scenarioExplorerDirty ||
      !scenarioExplorerSelectedPaths.length ||
      !scenarioExplorerSelectedSector
    ) {
      return;
    }

    void loadScenarioExplorerSummary().catch((error: Error) =>
      setMessage({ text: error.message, tone: "error" }),
    );
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    activeTab,
    scenarioExplorerLoading,
    scenarioExplorerSummary,
    scenarioExplorerDirty,
    scenarioExplorerSelectedSector,
    scenarioExplorerSelectedPaths,
  ]);

  useEffect(() => {
    let cancelled = false;
    let hideInitializationTimer: number | null = null;

    async function initialize() {
      try {
        const totalSteps = 11;
        let completedSteps = 0;
        const startupWarnings: string[] = [];
        const advanceInitialization = (label: string, detail?: string) => {
          completedSteps += 1;
          if (cancelled) {
            return;
          }
          setInitializationProgress(
            createInitializationProgress(totalSteps, completedSteps, label, detail),
          );
        };
        const runInitializationStep = async <T,>(
          label: string,
          request: () => Promise<T>,
          fallback: T,
        ): Promise<T> => {
          try {
            const payload = await request();
            advanceInitialization(label);
            return payload;
          } catch (error) {
            const detail = error instanceof Error ? error.message : String(error);
            startupWarnings.push(`${label}: ${detail}`);
            advanceInitialization(label, `Continuing without this startup step. ${detail}`);
            return fallback;
          }
        };

        setInitializationProgress(
          createInitializationProgress(
            totalSteps,
            0,
            "Initializing interface",
            "Starting startup checks.",
          ),
        );
        setStartupIssues([]);

        const [
          healthPayload,
          environmentPayload,
          capabilitiesPayload,
          explorerCatalogPayload,
          localScenariosPayload,
          storylinesPayload,
          iamKeyPayload,
          brightwayPayload,
          recentsPayload,
          templatePayload,
        ] = await Promise.all([
          runInitializationStep(
            "Connected to the local service",
            () => apiRequest<HealthPayload>("/api/health"),
            null as HealthPayload | null,
          ),
          runInitializationStep(
            "Loaded environment details",
            () => apiRequest<EnvironmentPayload>("/api/environment"),
            EMPTY_ENVIRONMENT,
          ),
          runInitializationStep(
            "Loaded UI capabilities",
            () => apiRequest<CapabilitiesPayload>("/api/capabilities"),
            EMPTY_CAPABILITIES,
          ),
          runInitializationStep(
            "Loaded Scenario Explorer catalog",
            () => apiRequest<ScenarioExplorerCatalogPayload>("/api/scenario-explorer/catalog"),
            EMPTY_EXPLORER_CATALOG,
          ),
          runInitializationStep(
            "Discovered local IAM scenario files",
            () => apiRequest<LocalIamScenariosPayload>("/api/discovery/iam-scenarios/local"),
            { scenarios: [] },
          ),
          runInitializationStep(
            "Loaded bundled scenario descriptions",
            () => apiRequest<IamScenarioStorylineCatalogPayload>("/api/discovery/iam-storylines"),
            EMPTY_IAM_STORYLINE_CATALOG,
          ),
          runInitializationStep(
            "Loaded stored IAM credentials",
            () => apiRequest<StoredCredentialPayload>("/api/credentials/iam-key"),
            EMPTY_STORED_CREDENTIAL,
          ),
          runInitializationStep(
            "Loaded Brightway projects and databases",
            () =>
              apiRequest<BrightwayDiscoveryPayload>("/api/discovery/brightway", {
                method: "POST",
              }),
            EMPTY_BRIGHTWAY_DISCOVERY,
          ),
          runInitializationStep(
            "Loaded recent configurations",
            () => apiRequest<RecentsPayload>("/api/recents"),
            EMPTY_RECENTS,
          ),
          runInitializationStep(
            "Loaded default configuration template",
            () =>
              apiRequest<{ project: GuiProjectManifest }>(
                "/api/projects/template?workflow=new_database",
              ),
            { project: workflowTemplate("new_database") },
          ),
        ]);

        if (cancelled) {
          return;
        }

        const initialLocalScenarios = reconciledLocalScenarioFiles(
          localScenariosPayload.scenarios,
          reconciledLocalScenarioFiles(
            capabilitiesPayload.iam_scenarios,
            explorerCatalogPayload.scenarios,
          ),
        );
        const autosaveSnapshot = loadAutosaveSnapshot();
        let templateProject = syncProjectScenariosToAvailableFiles(
          templatePayload.project,
          initialLocalScenarios,
        );
        let initialProjectPath = "";
        let initialDryRun = false;
        let initialWorkspaceMode: WorkspaceMode = "basic";
        let initialTab: WorkspaceTab = "overview";
        let initialHistory: RunHistoryEntry[] = [];

        if (autosaveSnapshot?.project) {
          templateProject = syncProjectScenariosToAvailableFiles(
            normalizeProject(autosaveSnapshot.project),
            initialLocalScenarios,
          );
          initialProjectPath = autosaveSnapshot.project_path || "";
          initialDryRun = Boolean(autosaveSnapshot.dry_run);
          initialWorkspaceMode =
            autosaveSnapshot.workspace_mode === "expert" ? "expert" : "basic";
          initialTab =
            typeof autosaveSnapshot.active_tab === "string" &&
            WORKSPACE_TABS.some((tab) => tab.id === autosaveSnapshot.active_tab)
              ? autosaveSnapshot.active_tab
              : "overview";
          initialHistory = autosaveSnapshot.project.run_history ?? [];
        }

        if (
          templateProject.config.source_type === "brightway" &&
          !templateProject.config.source_project &&
          brightwayPayload.current_project
        ) {
          templateProject.config.source_project = brightwayPayload.current_project;
        }
        const nextExplorerState = resolvedExplorerUiState(
          templateProject,
          explorerCatalogPayload.scenarios || [],
          (explorerCatalogPayload.sectors || []).map((entry) => entry.id),
        );
        setHealth(healthPayload);
        setEnvironment(environmentPayload);
        setCapabilities(
          capabilitiesWithLocalScenarios(capabilitiesPayload, initialLocalScenarios),
        );
        setLocalIamScenarios(initialLocalScenarios);
        setScenarioStorylineCatalog(storylinesPayload);
        setScenarioExplorerCatalog(
          catalogWithLocalScenarios(explorerCatalogPayload, initialLocalScenarios),
        );
        applyExplorerUiStateToControls(nextExplorerState);
        setScenarioExplorerSummary(null);
        setScenarioExplorerComparison(null);
        setScenarioExplorerDirty(true);
        setIamKeyState(iamKeyPayload);
        setIamKeyInput(iamKeyPayload.value || "");
        setBrightwayDiscovery(brightwayPayload);
        setRecents(recentsPayload);
        setStartupIssues(startupWarnings);
        setDraftProject(templateProject);
        setProjectPath(initialProjectPath);
        setProjectHistory(initialHistory);
        setManifestPreview(null);
        setValidation(null);
        setMessage(
          autosaveSnapshot?.project
            ? {
                text: autosaveSnapshot.saved_at
                  ? `Restored autosaved draft from ${formatTimestamp(autosaveSnapshot.saved_at)}.`
                  : "Restored the last autosaved draft.",
                tone: "success",
              }
            : null,
        );
        setCompletionNotice(null);
        clearDiagnostics();
        clearPreviewState();
        resetRunMonitor();
        setFormDirty(true);
        setDryRun(initialDryRun);
        setShowAdvanced(initialWorkspaceMode === "expert");
        setAutosaveAt(autosaveSnapshot?.saved_at || null);
        setActiveTab(
          initialWorkspaceMode === "basic" && ADVANCED_TAB_IDS.has(initialTab)
            ? "overview"
            : initialTab,
        );

        setInitializationProgress(
          createInitializationProgress(
            totalSteps,
            completedSteps,
            "Preparing initial configuration preview",
            `${completedSteps} of ${totalSteps} initialization steps complete.`,
          ),
        );
        await previewProject(
          templateProject,
          initialProjectPath,
          initialHistory,
          nextExplorerState,
        );
        if (cancelled) {
          return;
        }
        if (autosaveSnapshot?.project) {
          setMessage({
            text: autosaveSnapshot.saved_at
              ? `Restored autosaved draft from ${formatTimestamp(autosaveSnapshot.saved_at)}.`
              : "Restored the last autosaved draft.",
            tone: "success",
          });
        } else if (startupWarnings.length) {
          setMessage({
            text: `Startup completed with partial data:\n${startupWarnings.join("\n")}`,
            tone: "info",
          });
        }
        hasInitializedWorkspaceRef.current = true;
        advanceInitialization(
          "Initialization complete",
          "The workspace is ready for review and queueing.",
        );
        hideInitializationTimer = window.setTimeout(() => {
          if (!cancelled) {
            setInitializationProgress((current) => ({ ...current, active: false }));
          }
        }, 500);
      } catch (error) {
        if (!cancelled) {
          setInitializationProgress((current) => ({ ...current, active: false }));
          setStartupIssues([
            error instanceof Error ? error.message : String(error),
          ]);
          setMessage({
            text: error instanceof Error ? error.message : String(error),
            tone: "error",
          });
        }
      }
    }

    void initialize();
    return () => {
      cancelled = true;
      if (hideInitializationTimer !== null) {
        window.clearTimeout(hideInitializationTimer);
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    if (!scenarioDownload?.job_id || scenarioDownload.status !== "running") {
      return undefined;
    }

    let cancelled = false;
    let pollTimer: number | null = null;

    const refresh = async () => {
      try {
        const payload = await refreshScenarioDownload(scenarioDownload.job_id);
        if (cancelled) {
          return;
        }

        if (payload.status !== "running") {
          if (pollTimer !== null) {
            window.clearInterval(pollTimer);
            pollTimer = null;
          }
          await finalizeScenarioDownload(payload);
        }
      } catch (error) {
        if (!cancelled) {
          setMessage({
            text: error instanceof Error ? error.message : String(error),
            tone: "error",
          });
        }
      }
    };

    void refresh();
    pollTimer = window.setInterval(() => {
      void refresh();
    }, 500);

    return () => {
      cancelled = true;
      if (pollTimer !== null) {
        window.clearInterval(pollTimer);
      }
    };
  }, [scenarioDownload?.job_id, scenarioDownload?.status, draftProject, projectHistory, projectPath]);

  useEffect(() => {
    if (!currentRunId) {
      return undefined;
    }

    let cancelled = false;
    let pollTimer: number | null = null;

    const refresh = async () => {
      try {
        const status = await apiRequest<JobStatusPayload>(`/api/jobs/${currentRunId}`);
        if (cancelled) {
          return;
        }

        const previousStatus = runStatusByIdRef.current[currentRunId];
        runStatusByIdRef.current[currentRunId] = status.status;

        setCurrentRunStatus(status.status);
        setCurrentQueuePosition(status.queue_position);
        setCurrentEvents(status.events ?? []);

        if (
          status.status === "completed" &&
          previousStatus != null &&
          !isTerminalRunStatus(previousStatus)
        ) {
          await notifyRunCompleted(currentRunId, status.events ?? []);
        }

        const artifactPayload = await apiRequest<{ artifacts: string[] }>(
          `/api/jobs/${currentRunId}/artifacts`,
        );
        if (cancelled) {
          return;
        }

        setArtifacts(artifactPayload.artifacts ?? []);
        const refreshedHistory = await refreshProjectHistory(projectPath).catch(
          (error: Error) => {
            console.error(error);
            return [];
          },
        );
        await refreshDiagnosticsForCurrentRun(currentRunId).catch((error: Error) =>
          console.error(error),
        );

        if (isTerminalRunStatus(status.status)) {
          const nextRun = nextMonitorCandidate(refreshedHistory, currentRunId);
          const fallbackRunId = pendingMonitorRunIds.find((entry) => entry !== currentRunId);
          const targetRunId = nextRun?.run_id || fallbackRunId || null;
          if (targetRunId) {
            if (pollTimer !== null) {
              window.clearInterval(pollTimer);
              pollTimer = null;
            }
            focusRunMonitor(targetRunId, {
              status: nextRun?.status || "queued",
              queuePosition: null,
            });
            return;
          }

          if (pollTimer !== null) {
            window.clearInterval(pollTimer);
            pollTimer = null;
          }
        }
      } catch (error) {
        if (!cancelled) {
          setMessage({
            text: error instanceof Error ? error.message : String(error),
            tone: "error",
          });
        }
      }
    };

    void refresh();
    pollTimer = window.setInterval(() => {
      void refresh();
    }, 1000);

    return () => {
      cancelled = true;
      if (pollTimer !== null) {
        window.clearInterval(pollTimer);
      }
    };
  }, [
    currentRunId,
    projectPath,
    diagnosticsTarget?.run_id,
    diagnosticsTarget?.run_dir,
    pendingMonitorRunIds,
  ]);

  useEffect(() => {
    if (!currentRunId) {
      return undefined;
    }

    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const socket = new WebSocket(
      `${protocol}//${window.location.host}/api/jobs/events?run_id=${encodeURIComponent(currentRunId)}`,
    );

    socket.onmessage = (messageEvent) => {
      try {
        const event = JSON.parse(messageEvent.data) as RunEvent;
        setCurrentEvents((current) => {
          const existing = current.some(
            (entry) =>
              entry.timestamp === event.timestamp &&
              entry.event_type === event.event_type &&
              entry.message === event.message,
          );
          if (existing) {
            return current;
          }
          return [...current.slice(-159), event];
        });
      } catch (error) {
        console.error(error);
      }
    };

    return () => {
      socket.close();
    };
  }, [currentRunId]);

  const phaseSummaries = Array.from(
    filteredEvents.reduce((entries, event) => {
      if (!event.phase) {
        return entries;
      }
      entries.set(event.phase, event);
      return entries;
    }, new Map<string, RunEvent>()).values(),
  );

  return (
    <main className="app-shell">
      <header className="masthead">
        <div className="masthead-top">
          <div className="brand-lockup">
            <img className="brand-logo" src="/premise-logo.png" alt="Premise logo" />
            <div>
              <div className="eyebrow">Premise UI / Local worker-backed browser app</div>
              <h1>{draftProject.project_name || "Untitled Premise Configuration"}</h1>
              <p className="lede">
                Compact workspace for Premise workflows with configuration persistence,
                scenario previews, live worker monitoring, and workflow-specific export controls.
              </p>
            </div>
          </div>

          <div className="action-cluster">
            <button
              className="button subtle-button"
              type="button"
              onClick={() => {
                void loadTemplate(workflow, false).catch((error: Error) =>
                  setMessage({ text: error.message, tone: "error" }),
                );
              }}
            >
              Load Template
            </button>
            <button
              className="button subtle-button"
              type="button"
              onClick={() => {
                void openProject().catch((error: Error) =>
                  setMessage({ text: error.message, tone: "error" }),
                );
              }}
            >
              Open Config
            </button>
            <button
              className="button subtle-button"
              type="button"
              onClick={() => {
                void saveProject().catch((error: Error) =>
                  setMessage({ text: error.message, tone: "error" }),
                );
              }}
            >
              Save Config
            </button>
            <button
              className="button subtle-button"
              type="button"
              onClick={() => {
                void cloneCurrentProject().catch((error: Error) =>
                  setMessage({ text: error.message, tone: "error" }),
                );
              }}
            >
              Clone Config
            </button>
            <button
              className="button primary"
              type="button"
              title={queueShortcutHint}
              onClick={() => {
                void queueProject().catch((error: Error) =>
                  setMessage({ text: error.message, tone: "error" }),
                );
              }}
            >
              Queue Worker Run
            </button>
          </div>
        </div>

        <div className="status-strip">
          <span className="pill" data-tone="neutral">
            {workflowLabel(workflow)}
          </span>
          <span className="pill" data-tone={validationTone}>
            {validationBadge}
          </span>
          <span className="pill" data-tone="neutral">
            {totalScenarioCount} scenario{totalScenarioCount === 1 ? "" : "s"}
          </span>
          <span className="pill" data-tone="neutral">
            {exportTypeLabel(exportType)}
          </span>
          <span className="pill" data-tone={currentRunId ? "warning" : "neutral"}>
            {jobStateLabel}
          </span>
          <span className="pill" data-tone="neutral">
            {health ? `Premise ${health.premise_version}` : "Checking service..."}
          </span>
          <span className="pill" data-tone="neutral">
            {workspaceMode === "expert" ? "Expert mode" : "Basic mode"}
          </span>
          <span className="pill" data-tone="neutral">
            {autosaveLabel}
          </span>
        </div>

        <div className="subtle banner-note">
          Queueing will preview and validate the current form automatically.
        </div>

        <div className="notification-toolbar">
          {desktopNotificationsAvailable ? (
            desktopNotificationsEnabled ? null : (
              <button
                className="button subtle-button"
                type="button"
                onClick={() => {
                  void enableDesktopNotifications().catch((error: Error) =>
                    setMessage({ text: error.message, tone: "error" }),
                  );
                }}
              >
                Enable Desktop Alerts
              </button>
            )
          ) : (
            <span className="subtle">Desktop alerts unavailable in this browser.</span>
          )}
        </div>

        {message ? (
          <div className="message" data-tone={message.tone}>
            {message.text}
          </div>
        ) : null}

        {completionNotice ? (
          <div className="completion-notice" role="status" aria-live="polite">
            <div className="completion-notice-head">
              <div>
                <strong>Run {completionNotice.runId} completed</strong>
                <div className="subtle">{completionNotice.detail}</div>
              </div>
              <button
                className="button subtle-button"
                type="button"
                onClick={() => setCompletionNotice(null)}
              >
                Dismiss
              </button>
            </div>
            {completionNotice.outputLocation ? (
              <div className="inline-note">
                <strong>Output location.</strong> {completionNotice.outputLocation}
              </div>
            ) : null}
          </div>
        ) : null}

        {initializationProgress.active ? (
          <div
            className="job-progress init-progress"
            data-tone="neutral"
            data-active={initializationProgress.completed < initializationProgress.total}
          >
            <div className="job-progress-head">
              <strong>{initializationProgress.label}</strong>
              <span>{initializationPercent}%</span>
            </div>
            <div
              className="job-progress-track"
              role="progressbar"
              aria-label="Application initialization progress"
              aria-valuemin={0}
              aria-valuemax={100}
              aria-valuenow={initializationPercent}
            >
              <div
                className="job-progress-fill"
                style={{ width: `${initializationPercent}%` }}
              />
            </div>
            <div className="job-progress-meta">
              <span>Startup</span>
              <span>{initializationProgress.detail}</span>
            </div>
          </div>
        ) : null}

        {startupIssues.length ? (
          <div className="status-box startup-diagnostics" data-tone="warning">
            <div className="card-head">
              <div>
                <strong>Startup diagnostics</strong>
                <div className="subtle">
                  Some initialization checks did not complete. The app can continue, but parts of
                  the interface may stay incomplete until these checks succeed.
                </div>
              </div>
              <button
                className="button subtle-button"
                type="button"
                onClick={() => {
                  void retryStartupChecks().catch((error: Error) =>
                    setMessage({ text: error.message, tone: "error" }),
                  );
                }}
              >
                Retry startup checks
              </button>
            </div>
            <ul>
              {startupIssues.map((issue) => (
                <li key={issue}>{issue}</li>
              ))}
            </ul>
          </div>
        ) : null}

        {topJobProgress.visible ? (
          <div className="job-progress" data-tone={topJobProgress.tone} data-active={topJobProgress.active}>
            <div className="job-progress-head">
              <strong>{topJobProgress.label}</strong>
              <span>{topJobProgress.percent}%</span>
            </div>
            <div
              className="job-progress-track"
              role="progressbar"
              aria-label="Current run progress"
              aria-valuemin={0}
              aria-valuemax={100}
              aria-valuenow={topJobProgress.percent}
            >
              <div
                className="job-progress-fill"
                style={{ width: `${topJobProgress.percent}%` }}
              />
            </div>
            <div className="job-progress-meta">
              <span>Run {currentRunId}</span>
              <span>{topJobProgress.detail}</span>
            </div>
          </div>
        ) : null}
      </header>

      <nav className="tabbar" role="tablist" aria-label="Workspace tabs">
        {visibleTabs.map((tab) => (
          <button
            key={tab.id}
            className="tab-button"
            data-active={activeTab === tab.id}
            role="tab"
            type="button"
            title={tab.summary}
            onClick={() => setActiveTab(tab.id)}
          >
            <span className="tab-marker" aria-hidden="true">
              {tab.marker}
            </span>
            <span>{tab.label}</span>
          </button>
        ))}
      </nav>

      <div className="workspace-toolbar">
        <div className="guided-flow-card">
          <div className="guided-flow-head">
            <div>
              <strong>Guided flow</strong>
              <div className="subtle">
                Work through source, scenarios, transformations, export, then queue the run.
              </div>
            </div>
            <div className="subtle">
              {guidedSteps.filter((step) => step.ready).length}/{guidedSteps.length} ready
            </div>
          </div>
          <div className="guided-flow">
            {guidedSteps.map((step, index) => (
              <button
                key={step.id}
                className="guided-step"
                data-active={step.active ? "true" : "false"}
                data-ready={step.ready ? "true" : "false"}
                type="button"
                onClick={() => setActiveTab(step.tab)}
              >
                <span className="guided-step-index">{index + 1}</span>
                <span className="guided-step-label">{step.label}</span>
                <span className="guided-step-status">
                  {step.ready ? "Ready" : "Needs input"}
                </span>
              </button>
            ))}
          </div>
        </div>

        <div className="sticky-actions" role="region" aria-label="Sticky workspace actions">
          <div className="sticky-actions-meta">
            <div className="segmented-control" aria-label="Workspace mode">
              <button
                className="segmented-button"
                data-active={workspaceMode === "basic"}
                type="button"
                onClick={() => setShowAdvanced(false)}
              >
                Basic
              </button>
              <button
                className="segmented-button"
                data-active={workspaceMode === "expert"}
                type="button"
                onClick={() => setShowAdvanced(true)}
              >
                Expert
              </button>
            </div>
            <div className="sticky-actions-copy">
              <strong>Workspace mode</strong>
              <span className="subtle">
                {workspaceMode === "expert"
                  ? "History stays visible, along with the denser execution and source controls."
                  : "Keeps the common run path focused while leaving troubleshooting available when runs need attention."}
              </span>
            </div>
          </div>
          <div className="sticky-actions-main">
            <span className="subtle">{autosaveLabel}</span>
            <span className="subtle">{saveShortcutHint}</span>
            <button
              className="button subtle-button"
              type="button"
              title={saveShortcutHint}
              onClick={() => {
                void saveProject().catch((error: Error) =>
                  setMessage({ text: error.message, tone: "error" }),
                );
              }}
            >
              Save Config
            </button>
            <button
              className="button primary"
              type="button"
              title={queueShortcutHint}
              onClick={() => {
                void queueProject().catch((error: Error) =>
                  setMessage({ text: error.message, tone: "error" }),
                );
              }}
            >
              Queue Worker Run
            </button>
          </div>
        </div>
      </div>

      <section className="workspace-grid">
        <div className="main-column">
          {activeTab === "overview" ? (
            <div className="card-stack">
              <article className="card">
                <div className="card-head">
                  <div>
                    <h2>
                      <HelpText
                        helpKey="section.configuration_setup"
                        className="help-inline help-inline-heading"
                      >
                        Configuration setup
                      </HelpText>
                    </h2>
                    <p className="subtle">
                      Switch workflows, point to a configuration file, choose dry run versus live execution,
                      and keep the editable configuration compact.
                    </p>
                  </div>
                  <span className="pill" data-tone="neutral">
                    {workspaceMode === "expert" ? "Expert mode" : "Basic mode"}
                  </span>
                </div>

                <div className="form-grid compact-grid">
                  <div className="field">
                    <label htmlFor="projectName">
                      <HelpText helpKey="field.configuration_name">
                        Configuration Name
                      </HelpText>
                    </label>
                    <input
                      id="projectName"
                      type="text"
                      value={draftProject.project_name}
                      onChange={(event) =>
                        updateDraftProject({
                          ...draftProject,
                          project_name: event.target.value,
                        })
                      }
                    />
                    <FieldFeedback
                      validation={validation}
                      fieldKey="project_name"
                      hint="Used in saved configuration files, history snapshots, and generated export labels."
                    />
                  </div>

                  <div className="field">
                    <label htmlFor="workflow">
                      <HelpText helpKey="field.workflow">Workflow</HelpText>
                    </label>
                    <select
                      id="workflow"
                      value={workflow}
                      onChange={(event) => {
                        void loadTemplate(event.target.value, true).catch((error: Error) =>
                          setMessage({ text: error.message, tone: "error" }),
                        );
                      }}
                    >
                      {(capabilities?.workflows || Object.keys(WORKFLOW_LABELS)).map((entry) => (
                        <option key={entry} value={entry}>
                          {workflowLabel(entry)}
                        </option>
                      ))}
                    </select>
                    <div className="field-feedback" data-tone="neutral">
                      Switch workflows without losing the shared source and scenario skeleton.
                    </div>
                  </div>

                  <div className="field field-span-2">
                    <label htmlFor="projectPath">
                      <HelpText helpKey="field.config_path">Config Path</HelpText>
                    </label>
                    <div className="path-control">
                      <input
                        id="projectPath"
                        type="text"
                        value={projectPath}
                        placeholder="/path/to/configuration.json"
                        onChange={(event) => {
                          setProjectPath(event.target.value);
                          markFormDirty();
                        }}
                      />
                      <button
                        className="button subtle-button"
                        type="button"
                        onClick={() => {
                          void browseProjectPath("save_file").catch((error: Error) =>
                            setMessage({ text: error.message, tone: "error" }),
                          );
                        }}
                      >
                        Browse
                      </button>
                    </div>
                    <div className="field-feedback" data-tone="neutral">
                      Saving the configuration file enables portable history and reusable run snapshots.
                    </div>
                  </div>

                  <div className="field toggle-field">
                    <label className="switch" htmlFor="dryRun">
                      <input
                        id="dryRun"
                        type="checkbox"
                        checked={dryRun}
                        onChange={(event) => setDryRun(event.target.checked)}
                      />
                      <HelpText helpKey="field.queue_as_dry_run">
                        Queue as dry run
                      </HelpText>
                    </label>
                    <div className="field-feedback" data-tone="neutral">
                      Dry runs validate and instantiate the workflow without writing transformed outputs.
                    </div>
                  </div>

                  <div className="field toggle-field">
                    <div className="field">
                      <label htmlFor="workspaceMode">
                        <HelpText helpKey="field.expose_advanced_controls">
                          Workspace Mode
                        </HelpText>
                      </label>
                      <select
                        id="workspaceMode"
                        value={workspaceMode}
                        onChange={(event) =>
                          setShowAdvanced(event.target.value === "expert")
                        }
                      >
                        <option value="basic">Basic</option>
                        <option value="expert">Expert</option>
                      </select>
                      <div className="field-feedback" data-tone="neutral">
                        Basic mode hides dense support tabs. Expert mode keeps everything visible.
                      </div>
                    </div>
                  </div>
                </div>

                <div className="inline-note">
                  Queueing validates automatically. Save with <code>Cmd/Ctrl+S</code> and queue with{" "}
                  <code>Cmd/Ctrl+Enter</code>.
                </div>
              </article>

              <CollapsibleCard
                title={
                  <HelpText
                    helpKey="section.manifest_preview"
                    className="help-inline help-inline-heading"
                  >
                    Manifest preview
                  </HelpText>
                }
                subtitle="Backend-generated run manifest and preflight result for the current configuration state."
                defaultOpen={false}
                actions={
                  <div className="metric-strip">
                    <span className="metric-chip">
                      Run ID <strong>{currentRunId || "pending"}</strong>
                    </span>
                    <span className="metric-chip">
                      History <strong>{projectHistory.length}</strong>
                    </span>
                  </div>
                }
              >
                <div className="validation-summary">
                  {validation?.errors?.length ? (
                    <div className="status-box" data-tone="error">
                      <strong>Errors</strong>
                      <ul>
                        {validation.errors.map((entry) => (
                          <li key={entry}>{entry}</li>
                        ))}
                      </ul>
                    </div>
                  ) : null}
                  {validation?.warnings?.length ? (
                    <div className="status-box" data-tone="warning">
                      <strong>Warnings</strong>
                      <ul>
                        {validation.warnings.map((entry) => (
                          <li key={entry}>{entry}</li>
                        ))}
                      </ul>
                    </div>
                  ) : null}
                </div>

                <pre className="code-block manifest-block">
                  {manifestPreview
                    ? JSON.stringify(manifestPreview, null, 2)
                    : "Manifest preview will appear here."}
                </pre>
              </CollapsibleCard>
            </div>
          ) : null}

          {activeTab === "source" ? (
            <div className="card-stack">
              <article className="card">
                <div className="card-head">
                  <div>
                    <h2>
                      <HelpText
                        helpKey="section.source_database"
                        className="help-inline help-inline-heading"
                      >
                        Source database
                      </HelpText>
                    </h2>
                    <p className="subtle">
                      Select the source type, ecoinvent version, and the concrete Brightway or ecospold source location.
                    </p>
                  </div>
                  <span className="pill" data-tone="neutral">
                    {sourceType}
                  </span>
                </div>

                <div className="form-grid compact-grid">
                  <div className="field">
                    <label htmlFor="sourceType">
                      <HelpText helpKey="field.source_type">Source Type</HelpText>
                    </label>
                    <select
                      id="sourceType"
                      value={sourceType}
                      onChange={(event) => {
                        const nextSourceType = event.target.value;
                        updateConfig({
                          source_type: nextSourceType,
                          source_project:
                            nextSourceType === "brightway"
                              ? draftProject.config.source_project ||
                                brightwayDiscovery.current_project ||
                                ""
                              : draftProject.config.source_project,
                        });
                      }}
                    >
                      {(capabilities?.source_types || ["brightway", "ecospold"]).map((entry) => (
                        <option key={entry} value={entry}>
                          {entry}
                        </option>
                      ))}
                    </select>
                    <div className="field-feedback" data-tone="neutral">
                      Choose Brightway when the source database already exists in a Brightway project. Choose Ecospold when importing directly from raw files.
                    </div>
                  </div>

                  <div className="field">
                    <label htmlFor="sourceVersion">
                      <HelpText helpKey="field.ecoinvent_version">
                        Ecoinvent Version
                      </HelpText>
                    </label>
                    <select
                      id="sourceVersion"
                      value={draftProject.config.source_version}
                      onChange={(event) => updateConfig({ source_version: event.target.value })}
                    >
                      {(capabilities?.ecoinvent_versions.length
                        ? capabilities.ecoinvent_versions
                        : [draftProject.config.source_version]
                      ).map((entry) => (
                        <option key={entry} value={entry}>
                          {entry}
                        </option>
                      ))}
                    </select>
                  </div>

                  <div className="field">
                    <label htmlFor="systemModel">
                      <HelpText helpKey="field.system_model">System Model</HelpText>
                    </label>
                    <select
                      id="systemModel"
                      value={draftProject.config.system_model}
                      onChange={(event) => updateConfig({ system_model: event.target.value })}
                    >
                      <option value="cutoff">cutoff</option>
                      <option value="consequential">consequential</option>
                    </select>
                    <div className="field-feedback" data-tone="neutral">
                      Matches the ecoinvent system model used to build the source database.
                    </div>
                  </div>

                  <div className="field">
                    <label htmlFor="gainsScenario">
                      <HelpText helpKey="field.gains_scenario">GAINS Scenario</HelpText>
                    </label>
                    <select
                      id="gainsScenario"
                      value={draftProject.config.gains_scenario}
                      onChange={(event) => updateConfig({ gains_scenario: event.target.value })}
                    >
                      <option value="CLE">CLE</option>
                      <option value="MFR">MFR</option>
                    </select>
                    <div className="field-feedback" data-tone="neutral">
                      Affects the air-pollution pathway assumptions used by the relevant transformations.
                    </div>
                  </div>

                  {sourceType === "brightway" ? (
                    <>
                      <div className="field">
                        <label htmlFor="sourceProject">
                          <HelpText helpKey="field.brightway_project">
                            Brightway Project
                          </HelpText>
                        </label>
                        {brightwayDiscovery.available && brightwayDiscovery.projects.length ? (
                          <select
                            id="sourceProject"
                            value={selectedBrightwayProject}
                            onChange={(event) => {
                              const nextProject = event.target.value;
                              if (!nextProject) {
                                updateConfig({
                                  source_project: "",
                                  source_db: "",
                                });
                                return;
                              }
                              void selectBrightwayProject(nextProject).catch((error: Error) =>
                                setMessage({ text: error.message, tone: "error" }),
                              );
                            }}
                          >
                            <option value="">Select a Brightway project</option>
                            {brightwayDiscovery.projects.map((entry) => (
                              <option key={entry} value={entry}>
                                {entry}
                              </option>
                            ))}
                          </select>
                        ) : (
                          <input
                            id="sourceProject"
                            type="text"
                            value={draftProject.config.source_project}
                            placeholder="Current Brightway project"
                            onChange={(event) => updateConfig({ source_project: event.target.value })}
                          />
                        )}
                        <FieldFeedback
                          validation={validation}
                          fieldKey="source_project"
                          hint={
                            brightwayDiscovery.available
                              ? "Choose the Brightway project that contains the source and biosphere databases."
                              : "Brightway discovery is unavailable, so the project name must be entered manually."
                          }
                        />
                      </div>

                      <div className="field">
                        <label htmlFor="sourceDb">
                          <HelpText helpKey="field.brightway_source_database">
                            Brightway Source Database
                          </HelpText>
                        </label>
                        {brightwayDiscovery.available ? (
                          <select
                            id="sourceDb"
                            value={draftProject.config.source_db}
                            disabled={!selectedBrightwayProject || !brightwayDatabases.length}
                            onChange={(event) => updateConfig({ source_db: event.target.value })}
                          >
                              <option value="">
                                {!selectedBrightwayProject
                                  ? "Select a Brightway project first"
                                  : brightwayDatabases.length
                                    ? "Select an ecoinvent database"
                                    : "No databases found in the selected project"}
                              </option>
                            {brightwayDatabases.map((entry) => (
                              <option key={entry} value={entry}>
                                {entry}
                              </option>
                            ))}
                          </select>
                        ) : (
                          <input
                            id="sourceDb"
                            type="text"
                            value={draftProject.config.source_db}
                            placeholder="ecoinvent-3.12-cutoff"
                            onChange={(event) => updateConfig({ source_db: event.target.value })}
                          />
                        )}
                        <FieldFeedback
                          validation={validation}
                          fieldKey="source_db"
                          hint={
                            sourceDbDisabledReason ||
                            "Select the ecoinvent database inside the chosen Brightway project."
                          }
                        />
                      </div>

                      <div className="field">
                        <label htmlFor="biosphereName">
                          <HelpText helpKey="field.brightway_biosphere_database">
                            Brightway Biosphere Database
                          </HelpText>
                        </label>
                        {brightwayDiscovery.available ? (
                          <>
                            <select
                              id="biosphereName"
                              value={selectedBiosphereDatabase}
                              disabled={!selectedBrightwayProject || !brightwayDatabases.length}
                              onChange={(event) =>
                                updateConfig({ biosphere_name: event.target.value })
                              }
                            >
                              <option value="">
                                {!selectedBrightwayProject
                                  ? "Select a Brightway project first"
                                  : brightwayDatabases.length
                                    ? "Select a Brightway biosphere database"
                                    : "No databases found in the selected project"}
                              </option>
                              {brightwayDatabases.map((entry) => (
                                <option key={entry} value={entry}>
                                  {entry}
                                </option>
                              ))}
                            </select>
                            {missingBiosphereDatabase ? (
                              <div className="subtle">
                                Saved value <code>{draftProject.config.biosphere_name}</code> was
                                not found in the selected Brightway project.
                              </div>
                            ) : null}
                          </>
                        ) : (
                          <input
                            id="biosphereName"
                            type="text"
                            value={draftProject.config.biosphere_name}
                            placeholder="biosphere3"
                            onChange={(event) =>
                              updateConfig({ biosphere_name: event.target.value })
                            }
                          />
                        )}
                        <FieldFeedback
                          validation={validation}
                          fieldKey="biosphere_name"
                          hint={
                            biosphereDisabledReason ||
                            "Set this only when the biosphere database uses a non-default name in the selected Brightway project."
                          }
                        />
                      </div>

                      <div className="field field-span-2">
                        <div className="inline-actions">
                          <span className="subtle">
                            {brightwayDiscovery.available
                              ? selectedBrightwayProject
                                ? `Brightway project: ${selectedBrightwayProject}`
                                : "Brightway is available, but no current project was reported."
                              : "Brightway discovery is unavailable in this environment."}
                          </span>
                          <button
                            className="button subtle-button"
                            type="button"
                            onClick={() => {
                              void refreshBrightwayDiscovery()
                                .then((payload) => {
                                  if (
                                    draftProject.config.source_type === "brightway" &&
                                    !draftProject.config.source_project &&
                                    payload.current_project
                                  ) {
                                    updateConfig({
                                      source_project: payload.current_project,
                                    });
                                  }
                                  setMessage({
                                    text: payload.available
                                      ? `Loaded ${payload.projects.length} Brightway project${payload.projects.length === 1 ? "" : "s"} and ${payload.databases.length} database${payload.databases.length === 1 ? "" : "s"}.`
                                      : "Brightway discovery is unavailable in this environment.",
                                    tone: payload.available ? "success" : "error",
                                  });
                                })
                                .catch((error: Error) =>
                                  setMessage({ text: error.message, tone: "error" }),
                                );
                            }}
                          >
                            Refresh Brightway
                          </button>
                        </div>
                        <div className="subtle">
                          Used for Brightway export when the biosphere database in the selected
                          Brightway project is not named <code>biosphere3</code>.
                        </div>
                        <div className="field-feedback" data-tone="neutral">
                          Refresh after creating or changing Brightway projects outside the UI.
                        </div>
                      </div>
                    </>
                  ) : (
                    <div className="field field-span-2">
                      <label htmlFor="sourceFilePath">
                        <HelpText helpKey="field.ecospold_directory">
                          Ecospold Directory
                        </HelpText>
                      </label>
                      <div className="path-control">
                        <input
                          id="sourceFilePath"
                          type="text"
                          value={draftProject.config.source_file_path}
                          placeholder="/path/to/ecospold/files"
                          onChange={(event) =>
                            updateConfig({ source_file_path: event.target.value })
                          }
                        />
                        <button
                          className="button subtle-button"
                          type="button"
                          onClick={() => {
                            void browseSourceDirectory().catch((error: Error) =>
                              setMessage({ text: error.message, tone: "error" }),
                            );
                          }}
                        >
                          Browse
                        </button>
                      </div>
                      <FieldFeedback
                        validation={validation}
                        fieldKey="source_file_path"
                        hint="Point to the directory containing the ecospold source files for this run."
                      />
                    </div>
                  )}
                </div>
              </article>

              <CollapsibleCard
                title={
                  <HelpText
                    helpKey="section.additional_inventories"
                    className="help-inline help-inline-heading"
                  >
                    Additional inventories
                  </HelpText>
                }
                subtitle="Add extra inventory files to import with the base source database. Each entry is validated before the worker launches."
                defaultOpen={Boolean((draftProject.config.additional_inventories || []).length)}
                actions={
                  <button className="button secondary" type="button" onClick={() => addAdditionalInventory()}>
                    Add Inventory
                  </button>
                }
              >
                <FieldFeedback
                  validation={validation}
                  fieldKey="source_file_path"
                  hint="Inventory file paths are stored relative to the configuration file whenever possible."
                />
                <div className="stack">
                  {(draftProject.config.additional_inventories || []).length ? (
                    (draftProject.config.additional_inventories || []).map((entry, index) => (
                      <div className="item-card" key={`inventory-${index}`}>
                        <div className="form-grid compact-grid">
                          <div className="field field-span-2">
                            <label htmlFor={`inventory-file-${index}`}>
                              <HelpText helpKey="field.inventory_file">Inventory File</HelpText>
                            </label>
                            <div className="path-control">
                              <input
                                id={`inventory-file-${index}`}
                                type="text"
                                value={entry.filepath}
                                placeholder="/path/to/inventory.xlsx"
                                onChange={(event) =>
                                  updateAdditionalInventory(index, { filepath: event.target.value })
                                }
                              />
                              <button
                                className="button subtle-button"
                                type="button"
                                onClick={() => {
                                  void browseAdditionalInventory(index).catch((error: Error) =>
                                    setMessage({ text: error.message, tone: "error" }),
                                  );
                                }}
                              >
                                Browse
                              </button>
                            </div>
                          </div>

                          <div className="field">
                            <label htmlFor={`inventory-version-${index}`}>
                              <HelpText helpKey="field.inventory_ecoinvent_version">
                                Ecoinvent Version
                              </HelpText>
                            </label>
                            <select
                              id={`inventory-version-${index}`}
                              value={entry["ecoinvent version"]}
                              onChange={(event) =>
                                updateAdditionalInventory(index, {
                                  "ecoinvent version": event.target.value,
                                })
                              }
                            >
                              {(capabilities?.ecoinvent_versions.length
                                ? capabilities.ecoinvent_versions
                                : ["3.12"]
                              ).map((version) => (
                                <option key={version} value={version}>
                                  {version}
                                </option>
                              ))}
                            </select>
                          </div>

                          <div className="field toggle-field">
                            <label className="switch" htmlFor={`inventory-region-${index}`}>
                              <input
                                id={`inventory-region-${index}`}
                                type="checkbox"
                                checked={Boolean(entry.region_duplicate)}
                                onChange={(event) =>
                                  updateAdditionalInventory(index, {
                                    region_duplicate: event.target.checked,
                                  })
                                }
                              />
                              <HelpText helpKey="field.inventory_region_duplicate">
                                Duplicate regional variants
                              </HelpText>
                            </label>
                          </div>
                        </div>

                        <div className="inline-actions">
                          <span className="subtle">
                            Stored relative to the configuration file when possible.
                          </span>
                          <button
                            className="button subtle-button"
                            type="button"
                            onClick={() => removeAdditionalInventory(index)}
                          >
                            Remove
                          </button>
                        </div>
                      </div>
                    ))
                  ) : (
                    <div className="empty-state">
                      No additional inventories configured yet. Keep this panel collapsed unless extra spreadsheet imports are needed for this run.
                    </div>
                  )}
                </div>
              </CollapsibleCard>

              {showAdvanced ? (
                <CollapsibleCard
                  title={
                    <HelpText
                      helpKey="section.advanced_source_execution_settings"
                      className="help-inline help-inline-heading"
                    >
                      Advanced source and execution settings
                    </HelpText>
                  }
                  subtitle="These settings map directly to Premise options that affect imports, caching, uncertainty handling, and report generation."
                  defaultOpen={false}
                >
                  <div className="form-grid compact-grid">
                    <div className="field">
                      <label htmlFor="systemArgs">
                        <HelpText helpKey="field.system_args">System Args (JSON)</HelpText>
                      </label>
                      <input
                        id="systemArgs"
                        type="text"
                        value={JSON.stringify(draftProject.config.system_args || {})}
                        onChange={(event) => {
                          try {
                            const parsed = JSON.parse(event.target.value || "{}") as Record<
                              string,
                              unknown
                            >;
                            updateConfig({ system_args: parsed });
                          } catch {
                            setMessage({
                              text: "System Args must be valid JSON.",
                              tone: "error",
                            });
                          }
                        }}
                      />
                    </div>
                  </div>

                  <div className="checkbox-grid compact-checkbox-grid">
                    {[
                      {
                        key: "generate_reports",
                        label: "Generate reports",
                        description: "Keep Premise summary and change reports enabled.",
                      },
                      {
                        key: "use_cached_inventories",
                        label: "Use cached inventories",
                        description: "Reuse cached additional inventory parsing when possible.",
                      },
                      {
                        key: "use_cached_database",
                        label: "Use cached source database",
                        description: "Reuse cached source database materialization when possible.",
                      },
                      {
                        key: "keep_imports_uncertainty",
                        label: "Keep imported uncertainty",
                        description: "Retain uncertainty data from additional inventories.",
                      },
                      {
                        key: "keep_source_db_uncertainty",
                        label: "Keep source uncertainty",
                        description: "Retain uncertainty data from the source database.",
                      },
                      {
                        key: "use_absolute_efficiency",
                        label: "Use absolute efficiency",
                        description: "Use absolute efficiency values in the relevant transformations.",
                      },
                      {
                        key: "quiet",
                        label: "Quiet mode",
                        description: "Reduce verbose output from Premise where supported.",
                      },
                    ].map((entry) => (
                      <label className="check-card" key={entry.key}>
                        <input
                          type="checkbox"
                          checked={Boolean(draftProject.config[entry.key])}
                          onChange={(event) =>
                            updateConfig({
                              [entry.key]: event.target.checked,
                            })
                          }
                        />
                        <div>
                          <strong>{entry.label}</strong>
                          <span>{entry.description}</span>
                        </div>
                      </label>
                    ))}
                  </div>
                </CollapsibleCard>
              ) : null}
            </div>
          ) : null}

          {activeTab === "scenarios" ? (
            <div className="card-stack">
              <article className="card">
                <div className="card-head">
                  <div>
                    <h2>
                      <HelpText
                        helpKey="section.scenario_sets"
                        className="help-inline help-inline-heading"
                      >
                        Scenario sets
                      </HelpText>
                    </h2>
                    <p className="subtle">
                      Keep multiple named scenario groups inside one configuration. The worker manifest expands them in order, and each set stays editable as a compact table.
                    </p>
                  </div>
                  <button className="button secondary" type="button" onClick={() => addScenarioSet()}>
                    Add Scenario Set
                  </button>
                </div>

                <FieldFeedback
                  validation={validation}
                  fieldKey="scenario_sets"
                  hint="Bulk year helpers clone the last row pattern, so scenario tables stay compact even for multiple years."
                />

                <div className="stack">
                  {scenarioSets.map((scenarioSet, setIndex) => (
                    <div className="item-card scenario-set-card" key={`${scenarioSet.name}-${setIndex}`}>
                      <div className="scenario-set-toolbar">
                        <div>
                          <h3>{scenarioSet.name || `set-${setIndex + 1}`}</h3>
                          <p className="subtle">
                            {scenarioSet.scenarios.length} scenario
                            {scenarioSet.scenarios.length === 1 ? "" : "s"} in this set.
                          </p>
                        </div>

                        <div className="scenario-set-controls">
                          <div className="field">
                            <label htmlFor={`scenario-set-name-${setIndex}`}>
                              <HelpText helpKey="field.set_name">Set Name</HelpText>
                            </label>
                            <input
                              id={`scenario-set-name-${setIndex}`}
                              type="text"
                              value={scenarioSet.name}
                              onChange={(event) =>
                                updateScenarioSet(setIndex, {
                                  ...scenarioSet,
                                  name: event.target.value,
                                })
                              }
                            />
                          </div>

                          <div className="field">
                            <label htmlFor={`scenario-batch-${setIndex}`}>
                              <HelpText helpKey="field.bulk_add_years">Bulk Add Years</HelpText>
                            </label>
                            <div className="path-control">
                              <input
                                id={`scenario-batch-${setIndex}`}
                                type="text"
                                value={scenarioBatchInputs[setIndex] || ""}
                                placeholder="2030, 2040, 2050"
                                onKeyDown={(event) => {
                                  if (event.key !== "Enter") {
                                    return;
                                  }
                                  event.preventDefault();
                                  applyYearBatchToScenarioSet(
                                    setIndex,
                                    parseYearBatch(scenarioBatchInputs[setIndex] || ""),
                                  );
                                }}
                                onChange={(event) =>
                                  setScenarioBatchInputs((current) => ({
                                    ...current,
                                    [setIndex]: event.target.value,
                                  }))
                                }
                              />
                              <button
                                className="button subtle-button"
                                type="button"
                                onClick={() =>
                                  applyYearBatchToScenarioSet(
                                    setIndex,
                                    parseYearBatch(scenarioBatchInputs[setIndex] || ""),
                                  )
                                }
                              >
                                Add Year Batch
                              </button>
                            </div>
                          </div>

                          <div className="action-cluster compact-actions">
                            <button
                              className="button subtle-button"
                              type="button"
                              onClick={() => addScenario(setIndex)}
                            >
                              Add Scenario
                            </button>
                            <button
                              className="button subtle-button"
                              type="button"
                              onClick={() => removeScenarioSet(setIndex)}
                              disabled={scenarioSets.length === 1}
                            >
                              Remove Set
                            </button>
                          </div>
                        </div>
                      </div>

                      <div className="table-wrap">
                        <table className="scenario-table">
                          <thead>
                            <tr>
                              <th scope="col">Model</th>
                              <th scope="col">Pathway</th>
                              <th scope="col">Year</th>
                              <th scope="col">Custom IAM File</th>
                              <th scope="col">Preview</th>
                              <th scope="col">Row</th>
                            </tr>
                          </thead>
                          <tbody>
                            {scenarioSet.scenarios.map((scenario, scenarioIndex) => {
                              const previewKey = scenarioKey(setIndex, scenarioIndex);
                              const preview = scenarioPreviews[previewKey];
                              const modelOptions = scenarioModelOptions;
                              const pathwayOptions = knownScenarioPathways(
                                localScenarioFiles,
                                scenario.model,
                                [],
                                "",
                              );
                              const selectedModelValue = modelOptions.includes(scenario.model)
                                ? scenario.model
                                : "";
                              const selectedPathwayValue = pathwayOptions.includes(
                                scenario.pathway,
                              )
                                ? scenario.pathway
                                : "";
                              const scenarioInstalled = localScenarioFiles.some(
                                (entry) =>
                                  entry.model === scenario.model &&
                                  entry.pathway === scenario.pathway,
                              );

                              return (
                                <tr key={previewKey}>
                                  <td>
                                    <select
                                      aria-label={`IAM model for scenario ${scenarioIndex + 1}`}
                                      value={selectedModelValue}
                                      disabled={!modelOptions.length}
                                      onChange={(event) =>
                                        updateScenario(setIndex, scenarioIndex, {
                                          model: event.target.value,
                                          pathway: knownScenarioPathways(
                                            localScenarioFiles,
                                            event.target.value,
                                            [],
                                            "",
                                          )[0] || "",
                                        })
                                      }
                                    >
                                      {!modelOptions.length ? (
                                        <option value="" disabled>
                                          No local scenarios
                                        </option>
                                      ) : null}
                                      {modelOptions.length && !selectedModelValue ? (
                                        <option value="" disabled>
                                          Select a local model
                                        </option>
                                      ) : null}
                                      {modelOptions.map((entry) => (
                                        <option key={entry} value={entry}>
                                          {entry}
                                        </option>
                                      ))}
                                    </select>
                                  </td>
                                  <td>
                                    <select
                                      aria-label={`Pathway for scenario ${scenarioIndex + 1}`}
                                      value={selectedPathwayValue}
                                      disabled={!selectedModelValue || !pathwayOptions.length}
                                      onChange={(event) =>
                                        updateScenario(setIndex, scenarioIndex, {
                                          pathway: event.target.value,
                                        })
                                      }
                                    >
                                      {!selectedModelValue ? (
                                        <option value="" disabled>
                                          Select a model first
                                        </option>
                                      ) : null}
                                      {selectedModelValue && !selectedPathwayValue ? (
                                        <option value="" disabled>
                                          Select a local pathway
                                        </option>
                                      ) : null}
                                      {pathwayOptions.map((entry) => (
                                        <option key={entry} value={entry}>
                                          {entry}
                                        </option>
                                      ))}
                                    </select>
                                  </td>
                                  <td className="scenario-year-cell">
                                    <input
                                      aria-label={`Year for scenario ${scenarioIndex + 1}`}
                                      type="number"
                                      min={2005}
                                      max={2100}
                                      value={scenario.year}
                                      onChange={(event) =>
                                        updateScenario(setIndex, scenarioIndex, {
                                          year: Number(event.target.value),
                                        })
                                      }
                                    />
                                  </td>
                                  <td>
                                    <div className="scenario-file-cell">
                                      <input
                                        aria-label={`Custom IAM file for scenario ${scenarioIndex + 1}`}
                                        type="text"
                                        value={scenario.filepath || ""}
                                        placeholder="Optional custom IAM file"
                                        onChange={(event) =>
                                          updateScenario(setIndex, scenarioIndex, {
                                            filepath: event.target.value,
                                          })
                                        }
                                      />
                                      <button
                                        className="button subtle-button"
                                        type="button"
                                        onClick={() => {
                                          void browseScenarioFile(setIndex, scenarioIndex).catch(
                                            (error: Error) =>
                                              setMessage({
                                                text: error.message,
                                                tone: "error",
                                              }),
                                          );
                                        }}
                                      >
                                        Browse
                                      </button>
                                    </div>
                                  </td>
                                  <td>
                                    <div className="scenario-preview-cell">
                                      <button
                                        className="button subtle-button"
                                        type="button"
                                        disabled={!scenario.filepath || previewLoadingKey === previewKey}
                                        onClick={() => {
                                          void previewScenarioFile(setIndex, scenarioIndex).catch(
                                            (error: Error) =>
                                              setMessage({
                                                text: error.message,
                                                tone: "error",
                                              }),
                                          );
                                        }}
                                      >
                                        {previewLoadingKey === previewKey ? "Previewing..." : "Preview"}
                                      </button>
                                      {preview ? (
                                        <button
                                          className="button subtle-button"
                                          type="button"
                                          onClick={() => setSelectedPreviewKey(previewKey)}
                                        >
                                          View
                                        </button>
                                      ) : null}
                                      <span className="scenario-preview-status">
                                        {preview
                                          ? preview.file_name
                                          : scenario.filepath
                                            ? "No preview loaded"
                                            : scenarioInstalled
                                              ? "Using local Premise file"
                                              : "Not installed locally"}
                                      </span>
                                    </div>
                                  </td>
                                  <td className="scenario-row-actions">
                                    <button
                                      className="button subtle-button"
                                      type="button"
                                      onClick={() => removeScenario(setIndex, scenarioIndex)}
                                      disabled={scenarioSet.scenarios.length === 1}
                                    >
                                      Remove
                                    </button>
                                  </td>
                                </tr>
                              );
                            })}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  ))}
                </div>
              </article>

              <CollapsibleCard
                title={
                  <HelpText
                    helpKey="section.scenario_descriptions"
                    className="help-inline help-inline-heading"
                  >
                    Scenario descriptions
                  </HelpText>
                }
                subtitle="Read a bundled plain-language description of a known scenario before choosing it for a run."
                defaultOpen={true}
                actions={
                  selectedScenarioStoryline ? (
                    <>
                      <span
                        className="pill"
                        data-tone={
                          selectedScenarioStorylineAvailableLocally
                            ? "success"
                            : selectedScenarioStorylineKnownToCatalog
                              ? "neutral"
                              : "warning"
                        }
                      >
                        {selectedScenarioStorylineAvailableLocally
                          ? "Available locally"
                          : selectedScenarioStorylineKnownToCatalog
                            ? "Known downloadable scenario"
                            : "Description only"}
                      </span>
                      {defaultScenarioStorylineId &&
                      selectedScenarioStoryline.id !== defaultScenarioStorylineId ? (
                        <button
                          className="button subtle-button"
                          type="button"
                          onClick={() => setSelectedScenarioStorylineId(defaultScenarioStorylineId)}
                        >
                          Use Config Scenario
                        </button>
                      ) : null}
                    </>
                  ) : null
                }
              >
                {scenarioStorylines.length ? (
                  <div className="storyline-card">
                    <div className="storyline-toolbar">
                      <div className="stack">
                        <HelpText helpKey="field.scenario_storyline" className="help-inline">
                          Scenario description
                        </HelpText>
                        <select
                          aria-label="Scenario description selector"
                          value={selectedScenarioStoryline?.id || ""}
                          onChange={(event) => setSelectedScenarioStorylineId(event.target.value)}
                        >
                          {scenarioStorylines.map((entry) => (
                            <option key={entry.id} value={entry.id}>
                              {entry.label}
                            </option>
                          ))}
                        </select>
                      </div>
                    </div>
                    {selectedScenarioStoryline ? (
                      <div className="storyline-detail">
                        <div className="storyline-head">
                          <strong>{selectedScenarioStoryline.label}</strong>
                          {selectedScenarioStoryline.source_heading ? (
                            <span className="subtle">{selectedScenarioStoryline.source_heading}</span>
                          ) : (
                            <span className="subtle">
                              Plain-language interpretation for this scenario family
                            </span>
                          )}
                        </div>
                        {selectedScenarioStorylineSections.length ? (
                          <div className="storyline-sections">
                            {selectedScenarioStorylineSections.map((entry) =>
                              entry.title === "Data Note" ? (
                                <div
                                  key={`${selectedScenarioStoryline.id}-${entry.title}`}
                                  className="status-box"
                                  data-tone="warning"
                                >
                                  <strong>{entry.title}.</strong> {entry.content}
                                </div>
                              ) : (
                                <section
                                  key={`${selectedScenarioStoryline.id}-${entry.title}`}
                                  className="storyline-section"
                                >
                                  <h3>{entry.title}</h3>
                                  <p>{entry.content}</p>
                                </section>
                              ),
                            )}
                          </div>
                        ) : (
                          <div className="empty-state compact-empty">
                            No detailed description is available for this scenario in the current release.
                          </div>
                        )}
                      </div>
                    ) : null}
                  </div>
                ) : (
                  <div className="empty-state compact-empty">
                    No bundled scenario descriptions are available for the current release.
                  </div>
                )}
              </CollapsibleCard>

              <CollapsibleCard
                title={
                  <HelpText
                    helpKey="section.installed_iam_scenario_files"
                    className="help-inline help-inline-heading"
                  >
                    Installed IAM scenario files
                  </HelpText>
                }
                subtitle={
                  <>
                    These selectors reflect only the scenario files currently present in{" "}
                    <code>premise/data/iam_output_files</code>.
                  </>
                }
                defaultOpen={false}
                actions={
                  <>
                    <span className="pill" data-tone="neutral">
                      {localScenarioFiles.length} local file{localScenarioFiles.length === 1 ? "" : "s"}
                    </span>
                    <button
                      className="button secondary"
                      type="button"
                      disabled={scenarioDownloadInProgress}
                      onClick={() => {
                        void downloadAllIamScenarios().catch((error: Error) =>
                          setMessage({ text: error.message, tone: "error" }),
                        );
                      }}
                    >
                      {scenarioDownloadInProgress ? "Downloading..." : "Download All Known Scenarios"}
                    </button>
                    <button
                      className="button subtle-button"
                      type="button"
                      disabled={scenarioDownloadInProgress}
                      onClick={() => {
                        void clearLocalIamScenarios().catch((error: Error) =>
                          setMessage({ text: error.message, tone: "error" }),
                        );
                      }}
                    >
                      Clear Local Files
                    </button>
                  </>
                }
              >

                {scenarioDownload ? (
                  <div className="download-progress-card">
                    <div className="download-progress-head">
                      <strong>
                        {scenarioDownloadInProgress ? "Download in progress" : "Last download"}
                      </strong>
                      <span className="subtle">
                        {scenarioDownload.processed_count}/{scenarioDownload.total_count || 0} files
                      </span>
                    </div>
                    <div
                      className="download-progress-bar"
                      role="progressbar"
                      aria-valuemin={0}
                      aria-valuemax={100}
                      aria-valuenow={scenarioDownloadPercent}
                      aria-label="IAM scenario download progress"
                    >
                      <span
                        className="download-progress-fill"
                        style={{ width: `${scenarioDownloadPercent}%` }}
                      />
                    </div>
                    <div className="download-progress-meta">
                      <span>{scenarioDownloadPercent}% complete</span>
                      <span>
                        {scenarioDownload.current_file
                          ? `Current file: ${scenarioDownload.current_file}`
                          : `Directory: ${scenarioDownload.directory}`}
                      </span>
                    </div>
                  </div>
                ) : null}

                {localScenarioFiles.length ? (
                  <div className="table-wrap">
                    <table className="compact-table">
                      <thead>
                        <tr>
                          <th scope="col">Model</th>
                          <th scope="col">Pathway</th>
                          <th scope="col">File</th>
                          <th scope="col">Location</th>
                        </tr>
                      </thead>
                      <tbody>
                        {localScenarioFiles.map((entry) => (
                          <tr key={entry.id}>
                            <td>{entry.model}</td>
                            <td>{entry.pathway}</td>
                            <td>{entry.file_name}</td>
                            <td>{entry.path}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <div className="empty-state compact-empty">
                    No IAM scenario files were found locally. Use the download button to populate the directory.
                  </div>
                )}
              </CollapsibleCard>

              <CollapsibleCard
                title={
                  <HelpText
                    helpKey="section.scenario_preview"
                    className="help-inline help-inline-heading"
                  >
                    Scenario preview
                  </HelpText>
                }
                subtitle="Preview the selected IAM file to validate inferred metadata and inspect sample numeric series before queueing a run."
                defaultOpen={Boolean(selectedPreview)}
                actions={
                  <div className="action-cluster compact-actions">
                    <button
                      className="button subtle-button"
                      type="button"
                      disabled={!selectedPreview}
                      onClick={() => applyPreviewMetadata()}
                    >
                      Apply Inferred Model/Pathway
                    </button>
                    <button
                      className="button subtle-button"
                      type="button"
                      disabled={!selectedPreview || !selectedPreviewIndices}
                      onClick={() => applyPreviewYears()}
                    >
                      Add Preview Years
                    </button>
                  </div>
                }
              >

                {selectedPreview ? (
                  <>
                    <div className="metric-strip">
                      <span className="metric-chip">
                        File <strong>{selectedPreview.file_name}</strong>
                      </span>
                      <span className="metric-chip">
                        Years <strong>{selectedPreview.years.join(", ") || "-"}</strong>
                      </span>
                      <span className="metric-chip">
                        Inferred model <strong>{selectedPreview.inferred_model || "-"}</strong>
                      </span>
                      <span className="metric-chip">
                        Inferred pathway <strong>{selectedPreview.inferred_pathway || "-"}</strong>
                      </span>
                    </div>

                    <PreviewChart preview={selectedPreview} />

                    <div className="status-box" data-tone="neutral">
                      <strong>Detected columns</strong>
                      <div className="tag-cloud">
                        {selectedPreview.columns.map((entry) => (
                          <span className="mini-tag" key={entry}>
                            {entry}
                          </span>
                        ))}
                      </div>
                    </div>
                  </>
                ) : (
                  <div className="empty-state">
                    Add a custom IAM file to any scenario and click Preview to inspect it here.
                  </div>
                )}
              </CollapsibleCard>

              {workflow === "pathways_datapackage" ? (
                <CollapsibleCard
                  title={
                    <HelpText
                      helpKey="section.pathways_output_years"
                      className="help-inline help-inline-heading"
                    >
                      Pathways output years
                    </HelpText>
                  }
                  subtitle="Pathways datapackage expands each configured scenario across these output years."
                  defaultOpen={false}
                  actions={
                    <button className="button secondary" type="button" onClick={() => addPathwaysYear()}>
                      Add Year
                    </button>
                  }
                >
                  <FieldFeedback
                    validation={validation}
                    fieldKey="years"
                    hint="Use a compact set of milestone years. Press Enter in the last row to add another year quickly."
                  />
                  <div className="stack">
                    {normalizeYears(draftProject.config.years).map((year, index) => (
                      <div className="inline-form" key={`pathways-year-${index}`}>
                        <input
                          type="number"
                          min={2005}
                          max={2100}
                          value={year}
                          onKeyDown={(event) => {
                            if (event.key !== "Enter") {
                              return;
                            }
                            event.preventDefault();
                            if (index === normalizeYears(draftProject.config.years).length - 1) {
                              addPathwaysYear();
                            }
                          }}
                          onChange={(event) =>
                            updatePathwaysYear(index, Number(event.target.value))
                          }
                        />
                        <button
                          className="button subtle-button"
                          type="button"
                          onClick={() => removePathwaysYear(index)}
                        >
                          Remove
                        </button>
                      </div>
                    ))}
                  </div>
                </CollapsibleCard>
              ) : null}
            </div>
          ) : null}

          {activeTab === "explorer" ? (
            <div className="card-stack">
              <article className="card">
                <div className="card-head">
                  <div>
                    <h2>
                      <HelpText
                        helpKey="section.scenario_explorer"
                        className="help-inline help-inline-heading"
                      >
                        Scenario Explorer
                      </HelpText>
                    </h2>
                    <p className="subtle">
                      Explore installed IAM scenarios in a denser analysis workspace. Scenario,
                      sector, and compare mode define the backend summary; region, variable, and
                      year-window filtering update instantly in the browser.
                    </p>
                  </div>
                  <div className="action-cluster compact-actions">
                    <span className="pill" data-tone="neutral">
                      {explorerCompareModeLabel(scenarioExplorerCompareMode)}
                    </span>
                    <button
                      className="button subtle-button"
                      type="button"
                      onClick={() => useConfigurationScenariosForExplorer()}
                      disabled={!explorerCatalogScenarios.length}
                    >
                      Use Config Scenarios
                    </button>
                    <button
                      className="button subtle-button"
                      type="button"
                      onClick={() => applyExplorerScenariosToConfiguration()}
                      disabled={!explorerSelectedScenarioEntries.length}
                    >
                      Apply to Config
                    </button>
                    <button
                      className="button secondary"
                      type="button"
                      title={explorerReloadDisabledReason || "Reload the backend summary with the latest scenario, sector, and compare settings."}
                      disabled={
                        scenarioExplorerLoading ||
                        !scenarioExplorerSelectedPaths.length ||
                        !scenarioExplorerSelectedSector
                      }
                      onClick={() => {
                        void loadScenarioExplorerSummary().catch((error: Error) =>
                          setMessage({ text: error.message, tone: "error" }),
                        );
                      }}
                    >
                      {scenarioExplorerLoading ? "Loading..." : "Reload Explorer View"}
                    </button>
                  </div>
                </div>

                {explorerReloadDisabledReason ? (
                  <div className="field-feedback" data-tone="neutral">
                    {explorerReloadDisabledReason}
                  </div>
                ) : null}

                {scenarioExplorerDirty && scenarioExplorerSummary ? (
                  <div className="inline-note">
                    Source controls changed. Reload Explorer View to refresh the backend summary
                    for the current scenario set, sector, comparison mode, or to fetch years that
                    are outside the currently loaded range.
                  </div>
                ) : null}

                <div className="explorer-workspace">
                  <div className="explorer-sidebar">
                    <CollapsiblePanel
                      title={
                        <HelpText helpKey="field.explorer_scenarios">
                          Explorer Scenarios
                        </HelpText>
                      }
                      subtitle={`${scenarioExplorerSelectedPaths.length} selected`}
                      actions={
                        <>
                          <button
                            className="button subtle-button"
                            type="button"
                            disabled={!explorerCatalogScenarios.length}
                            onClick={() => {
                              setScenarioExplorerSelectedPaths(
                                explorerCatalogScenarios.map((entry) => entry.path),
                              );
                              setScenarioExplorerSummary(null);
                              setScenarioExplorerComparison(null);
                              setScenarioExplorerHiddenSeries([]);
                              setScenarioExplorerDirty(true);
                            }}
                          >
                            Select all
                          </button>
                          <button
                            className="button subtle-button"
                            type="button"
                            disabled={!scenarioExplorerSelectedPaths.length}
                            onClick={() => {
                              setScenarioExplorerSelectedPaths([]);
                              setScenarioExplorerSummary(null);
                              setScenarioExplorerComparison(null);
                              setScenarioExplorerHiddenSeries([]);
                              setScenarioExplorerDirty(true);
                            }}
                          >
                            Deselect all
                          </button>
                        </>
                      }
                    >
                      {explorerCatalogScenarios.length ? (
                        <div className="explorer-scenario-list">
                          {explorerCatalogScenarios.map((entry) => {
                            const active = scenarioExplorerSelectedPaths.includes(entry.path);
                            return (
                              <label
                                className="explorer-scenario-row"
                                data-active={active ? "true" : "false"}
                                key={entry.path}
                              >
                                <input
                                  aria-label={`Use ${entry.model} ${entry.pathway} in the Scenario Explorer`}
                                  type="checkbox"
                                  checked={active}
                                  onChange={() => toggleScenarioExplorerPath(entry.path)}
                                />
                                <div>
                                  <strong>{entry.model}</strong>
                                  <span>{entry.pathway}</span>
                                </div>
                                <span className="subtle">{entry.file_name}</span>
                              </label>
                            );
                          })}
                        </div>
                      ) : (
                        <div className="empty-state compact-empty">
                          No installed IAM scenarios are available for the Scenario Explorer yet.
                          Use the download controls in the Scenarios tab to populate
                          <code>premise/data/iam_output_files</code>.
                        </div>
                      )}
                    </CollapsiblePanel>

                    <CollapsiblePanel
                      title="Explorer controls"
                      subtitle="Sector, comparison mode, chart mode, and year window"
                    >
                      <div className="form-grid compact-grid explorer-filter-grid">
                        <div className="field">
                          <label htmlFor="scenarioExplorerCompareMode">
                            <HelpText helpKey="field.explorer_compare_mode">
                              Explorer Compare Mode
                            </HelpText>
                          </label>
                          <select
                            id="scenarioExplorerCompareMode"
                            value={scenarioExplorerCompareMode}
                            onChange={(event) => {
                              const nextMode = event.target.value as ExplorerCompareMode;
                              setScenarioExplorerCompareMode(nextMode);
                              setScenarioExplorerSummary(null);
                              setScenarioExplorerComparison(null);
                              setScenarioExplorerDirty(true);
                            }}
                          >
                            {explorerAvailableCompareModes.map((mode) => (
                              <option key={mode} value={mode}>
                                {explorerCompareModeLabel(mode)}
                              </option>
                            ))}
                          </select>
                        </div>

                        <div className="field">
                          <label htmlFor="scenarioExplorerSector">
                            <HelpText helpKey="field.explorer_sector">Explorer Sector</HelpText>
                          </label>
                          <select
                            id="scenarioExplorerSector"
                            value={scenarioExplorerSelectedSector}
                            disabled={!explorerCatalogSectors.length}
                            onChange={(event) => {
                              setScenarioExplorerSelectedSector(event.target.value);
                              setScenarioExplorerSelectedGroups([]);
                              setScenarioExplorerSelectedVariables([]);
                              setScenarioExplorerGroupSelectionMode("all");
                              setScenarioExplorerVariableSelectionMode("all");
                              setScenarioExplorerHiddenSeries([]);
                              setScenarioExplorerSummary(null);
                              setScenarioExplorerComparison(null);
                              setScenarioExplorerDirty(true);
                            }}
                          >
                            {!explorerCatalogSectors.length ? (
                              <option value="">No explorer sectors available</option>
                            ) : null}
                            {explorerCatalogSectors.map((entry) => (
                              <option key={entry.id} value={entry.id}>
                                {entry.label}
                              </option>
                            ))}
                          </select>
                        </div>

                        <div className="field">
                          <label htmlFor="scenarioExplorerChartMode">
                            <HelpText helpKey="field.explorer_chart_mode">
                              Explorer Chart Mode
                            </HelpText>
                          </label>
                          <select
                            id="scenarioExplorerChartMode"
                            value={scenarioExplorerChartMode}
                            onChange={(event) => {
                              markExplorerRenderRequested();
                              setScenarioExplorerChartMode(
                                event.target.value as ExplorerChartMode,
                              );
                            }}
                          >
                            {(
                              ["line", "stacked_area", "bar", "stacked_bar"] as ExplorerChartMode[]
                            ).map((entry) => (
                              <option key={entry} value={entry}>
                                {explorerChartModeLabel(entry)}
                              </option>
                            ))}
                          </select>
                        </div>

                        <div className="field">
                          <label htmlFor="scenarioExplorerPlotLayout">
                            <HelpText helpKey="field.explorer_plot_layout">
                              Explorer Plot Layout
                            </HelpText>
                          </label>
                          <select
                            id="scenarioExplorerPlotLayout"
                            value={scenarioExplorerPlotLayout}
                            onChange={(event) => {
                              markExplorerRenderRequested();
                              setScenarioExplorerPlotLayout(
                                event.target.value as ExplorerPlotLayout,
                              );
                            }}
                          >
                            {(["combined", "per_scenario"] as ExplorerPlotLayout[]).map(
                              (entry) => (
                                <option key={entry} value={entry}>
                                  {explorerPlotLayoutLabel(entry)}
                                </option>
                              ),
                            )}
                          </select>
                        </div>

                        <div className="field">
                          <label htmlFor="scenarioExplorerYearStart">
                            <HelpText helpKey="field.explorer_year_start">
                              Explorer Year Start
                            </HelpText>
                          </label>
                          <input
                            id="scenarioExplorerYearStart"
                            type="number"
                            min={2005}
                            max={2100}
                            value={scenarioExplorerYearStart}
                            placeholder="2005"
                            onChange={(event) => {
                              markExplorerFilterRequested();
                              setScenarioExplorerYearStart(event.target.value);
                              setScenarioExplorerDirty(true);
                            }}
                          />
                        </div>

                        <div className="field">
                          <label htmlFor="scenarioExplorerYearEnd">
                            <HelpText helpKey="field.explorer_year_end">
                              Explorer Year End
                            </HelpText>
                          </label>
                          <input
                            id="scenarioExplorerYearEnd"
                            type="number"
                            min={2005}
                            max={2100}
                            value={scenarioExplorerYearEnd}
                            placeholder="2100"
                            onChange={(event) => {
                              markExplorerFilterRequested();
                              setScenarioExplorerYearEnd(event.target.value);
                              setScenarioExplorerDirty(true);
                            }}
                          />
                        </div>

                        {scenarioExplorerCompareMode === "indexed" ? (
                          <div className="field">
                            <label htmlFor="scenarioExplorerBaselineYear">
                              <HelpText helpKey="field.explorer_baseline_year">
                                Explorer Baseline Year
                              </HelpText>
                            </label>
                            <input
                              id="scenarioExplorerBaselineYear"
                              type="number"
                              min={2005}
                              max={2100}
                            value={scenarioExplorerBaselineYear}
                            placeholder="2030"
                            onChange={(event) => {
                              setScenarioExplorerBaselineYear(event.target.value);
                              setScenarioExplorerDirty(true);
                              }}
                            />
                          </div>
                        ) : null}

                        {scenarioExplorerCompareMode === "delta" ||
                        scenarioExplorerCompareMode === "percent_change" ? (
                          <div className="field">
                            <label htmlFor="scenarioExplorerBaselineScenario">
                              <HelpText helpKey="field.explorer_baseline_scenario">
                                Explorer Baseline Scenario
                              </HelpText>
                            </label>
                            <select
                              id="scenarioExplorerBaselineScenario"
                              value={scenarioExplorerBaselineScenarioId}
                              disabled={!explorerBaselineChoices.length}
                              onChange={(event) => {
                                setScenarioExplorerBaselineScenarioId(event.target.value);
                                setScenarioExplorerDirty(true);
                              }}
                            >
                              {!explorerBaselineChoices.length ? (
                                <option value="">Select at least two scenarios</option>
                              ) : null}
                              {explorerBaselineChoices.map((entry) => (
                                <option key={entry.scenario_id} value={entry.scenario_id}>
                                  {entry.label}
                                </option>
                              ))}
                            </select>
                          </div>
                        ) : null}
                      </div>

                      <div className="explorer-year-slider">
                        <div className="explorer-year-slider-head">
                          <span className="subtle">Year window slider</span>
                          <span className="subtle">
                            {scenarioExplorerYearStart || explorerYearBounds.min}-
                            {scenarioExplorerYearEnd || explorerYearBounds.max}
                          </span>
                        </div>
                        <div className="explorer-year-slider-track">
                          <input
                            type="range"
                            min={explorerYearBounds.min}
                            max={explorerYearBounds.max}
                            value={scenarioExplorerYearStart || explorerYearBounds.min}
                            onChange={(event) => {
                              markExplorerFilterRequested();
                              const nextValue = Number(event.target.value);
                              const currentEnd = Number(
                                scenarioExplorerYearEnd || explorerYearBounds.max,
                              );
                              setScenarioExplorerYearStart(
                                String(Math.min(nextValue, currentEnd)),
                              );
                              if (nextValue > currentEnd) {
                                setScenarioExplorerYearEnd(String(nextValue));
                              }
                              setScenarioExplorerDirty(true);
                            }}
                          />
                          <input
                            type="range"
                            min={explorerYearBounds.min}
                            max={explorerYearBounds.max}
                            value={scenarioExplorerYearEnd || explorerYearBounds.max}
                            onChange={(event) => {
                              markExplorerFilterRequested();
                              const nextValue = Number(event.target.value);
                              const currentStart = Number(
                                scenarioExplorerYearStart || explorerYearBounds.min,
                              );
                              setScenarioExplorerYearEnd(
                                String(Math.max(nextValue, currentStart)),
                              );
                              if (nextValue < currentStart) {
                                setScenarioExplorerYearStart(String(nextValue));
                              }
                              setScenarioExplorerDirty(true);
                            }}
                          />
                        </div>
                      </div>
                    </CollapsiblePanel>

                    <CollapsiblePanel
                      title={
                        <HelpText helpKey="field.explorer_group">
                          {explorerGroupLabel(scenarioExplorerSummary)}
                        </HelpText>
                      }
                      subtitle="Compact region or sub-scenario selector"
                    >
                      <ExplorerSelectionBoard
                        title={
                          <HelpText helpKey="field.explorer_group">
                            {explorerGroupLabel(scenarioExplorerSummary)}
                          </HelpText>
                        }
                        emptyMessage={`Load a summary to choose ${explorerGroupLabel(
                          scenarioExplorerSummary,
                        ).toLowerCase()} filters.`}
                        options={explorerGroupChoices}
                        selectionMode={scenarioExplorerGroupSelectionMode}
                        selectedValues={scenarioExplorerSelectedGroups}
                        onSetAll={() => setExplorerGroupMode("all")}
                        onSetNone={() => setExplorerGroupMode("none")}
                        onSetSelectedValues={(values) => setExplorerGroupSelection(values)}
                      />
                    </CollapsiblePanel>

                    <CollapsiblePanel
                      title={
                        <HelpText helpKey="field.explorer_variable">
                          Explorer Variables
                        </HelpText>
                      }
                      subtitle="Compact variable selector with drag-and-drop ordering"
                    >
                      <ExplorerSelectionBoard
                        title={
                          <HelpText helpKey="field.explorer_variable">
                            Explorer Variables
                          </HelpText>
                        }
                        emptyMessage="Load a summary to choose one or more variable filters."
                        options={scenarioExplorerSummary?.variables || []}
                        selectionMode={scenarioExplorerVariableSelectionMode}
                        selectedValues={scenarioExplorerSelectedVariables}
                        onSetAll={() => setExplorerVariableMode("all")}
                        onSetNone={() => setExplorerVariableMode("none")}
                        onSetSelectedValues={(values) => setExplorerVariableSelection(values)}
                      />
                    </CollapsiblePanel>
                  </div>

                  <div className="explorer-main">
                    <article className="card explorer-main-card">
                      <div className="card-head">
                        <div>
                          <h2>Explorer summary</h2>
                          <p className="subtle">
                            Compare scenarios, then use the legend to hide noisy series or the
                            selector panels to reorder what is plotted first.
                          </p>
                        </div>
                        <div className="metric-strip">
                          <span className="metric-chip">
                            Selected <strong>{scenarioExplorerSelectedPaths.length}</strong>
                          </span>
                          <span className="metric-chip">
                            Loaded <strong>{scenarioExplorerSummary?.scenarios.length || 0}</strong>
                          </span>
                          <span className="metric-chip">
                            Mode{" "}
                            <strong>{explorerCompareModeLabel(scenarioExplorerCompareMode)}</strong>
                          </span>
                          <span className="metric-chip">
                            Chart <strong>{explorerChartModeLabel(scenarioExplorerChartMode)}</strong>
                          </span>
                          <span className="metric-chip">
                            Layout{" "}
                            <strong>{explorerPlotLayoutLabel(scenarioExplorerPlotLayout)}</strong>
                          </span>
                          {explorerPerformanceSummary ? (
                            <span className="metric-chip">
                              Performance <strong>{explorerPerformanceSummary}</strong>
                            </span>
                          ) : null}
                        </div>
                      </div>

                      {scenarioExplorerSummary ? (
                        <>
                          <div className="inline-note">
                            <strong>{scenarioExplorerSummary.sector}</strong>
                            {scenarioExplorerSummary.label ? (
                              <>
                                {" "}
                                / <span>{scenarioExplorerSummary.label}</span>
                              </>
                            ) : null}
                            {scenarioExplorerSummary.explanation ? (
                              <>
                                <br />
                                {scenarioExplorerSummary.explanation}
                              </>
                            ) : null}
                            {scenarioExplorerCompareMode === "share_100" ? (
                              <>
                                <br />
                                Each year is normalized so the selected series sum to{" "}
                                <strong>100%</strong> within each scenario.
                              </>
                            ) : null}
                            {scenarioExplorerComparison?.compare_mode === "indexed" &&
                            scenarioExplorerComparison.baseline_year != null ? (
                              <>
                                <br />
                                Indexed to 100 at{" "}
                                <strong>{scenarioExplorerComparison.baseline_year}</strong>.
                              </>
                            ) : null}
                            {(scenarioExplorerComparison?.compare_mode === "delta" ||
                              scenarioExplorerComparison?.compare_mode === "percent_change") &&
                            scenarioExplorerComparison.baseline_scenario_label ? (
                              <>
                                <br />
                                Baseline scenario:{" "}
                                <strong>
                                  {scenarioExplorerComparison.baseline_scenario_label}
                                </strong>
                                .
                              </>
                            ) : null}
                          </div>

                          <div className="metric-strip">
                            <span className="metric-chip">
                              {explorerGroupLabel(scenarioExplorerSummary)}{" "}
                              <strong>{explorerSelectedGroupLabels.length}</strong>
                            </span>
                            <span className="metric-chip">
                              Variables <strong>{explorerSelectedVariableLabels.length}</strong>
                            </span>
                            <span className="metric-chip">
                              Visible series <strong>{explorerVisiblePlotSeries.length}</strong>
                            </span>
                            <span className="metric-chip">
                              Years{" "}
                              <strong>
                                {explorerRenderedYearBounds
                                  ? `${explorerRenderedYearBounds.min}-${explorerRenderedYearBounds.max}`
                                  : "-"}
                              </strong>
                            </span>
                          </div>

                          {explorerUpdatePending ? (
                            <div className="inline-note">Updating charts for the latest filter changes...</div>
                          ) : null}

                          {scenarioExplorerPlotLayout === "per_scenario" &&
                          explorerScenarioPlotPanels.length ? (
                            <div className="explorer-plot-grid">
                              {explorerScenarioPlotPanels.map((panel) => (
                                <ExplorerFullscreenPlotCard
                                  key={panel.scenarioLabel}
                                  title={panel.scenarioLabel}
                                  subtitle={`${panel.visibleSeries.length} visible series`}
                                >
                                  <ExplorerSeriesPlot
                                    series={panel.series}
                                    emptyMessage={`No numeric series matched the selected filters for ${panel.scenarioLabel}.`}
                                    ariaLabel={`Scenario Explorer chart for ${panel.scenarioLabel}`}
                                    chartMode={scenarioExplorerChartMode}
                                    hiddenLabels={scenarioExplorerHiddenSeries}
                                    yDomain={explorerPlotDomain}
                                    legendInitiallyOpen={false}
                                    onToggleSeries={(label) => toggleExplorerHiddenSeries(label)}
                                    onResetHiddenSeries={() => clearExplorerHiddenSeries()}
                                    baselineYear={
                                      scenarioExplorerComparison?.compare_mode === "indexed" &&
                                      scenarioExplorerComparison.baseline_year != null
                                        ? scenarioExplorerComparison.baseline_year
                                        : null
                                    }
                                    showZeroLine={
                                      scenarioExplorerCompareMode === "delta" ||
                                      scenarioExplorerCompareMode === "percent_change"
                                    }
                                  />
                                </ExplorerFullscreenPlotCard>
                              ))}
                            </div>
                          ) : (
                            <ExplorerFullscreenPlotCard
                              title="Combined plot"
                              subtitle={`${explorerVisiblePlotSeries.length} visible series`}
                            >
                              <ExplorerSeriesPlot
                                series={explorerPlotSeries}
                                emptyMessage="No numeric series matched the selected Scenario Explorer filters."
                                ariaLabel="Scenario Explorer chart"
                                chartMode={scenarioExplorerChartMode}
                                hiddenLabels={scenarioExplorerHiddenSeries}
                                yDomain={explorerPlotDomain}
                                legendInitiallyOpen={scenarioExplorerPlotLayout !== "per_scenario"}
                                onToggleSeries={(label) => toggleExplorerHiddenSeries(label)}
                                onResetHiddenSeries={() => clearExplorerHiddenSeries()}
                                baselineYear={
                                  scenarioExplorerComparison?.compare_mode === "indexed" &&
                                  scenarioExplorerComparison.baseline_year != null
                                    ? scenarioExplorerComparison.baseline_year
                                    : null
                                }
                                showZeroLine={
                                  scenarioExplorerCompareMode === "delta" ||
                                  scenarioExplorerCompareMode === "percent_change"
                                }
                              />
                            </ExplorerFullscreenPlotCard>
                          )}

                          <CollapsiblePanel
                            title="Values table"
                            subtitle={`${explorerVisiblePlotSeries.length} visible series`}
                            defaultOpen={false}
                            lazyMount
                          >
                            <ExplorerValuesTableWidget series={explorerVisiblePlotSeries} />
                          </CollapsiblePanel>

                          <CollapsiblePanel
                            title="Loaded scenarios"
                            subtitle={`${explorerSelectedScenarioEntries.length} scenario${explorerSelectedScenarioEntries.length === 1 ? "" : "s"}`}
                            defaultOpen={false}
                            lazyMount
                          >
                            <div className="tag-cloud">
                              {explorerSelectedScenarioEntries.map((entry) => (
                                <span className="mini-tag" key={entry.path}>
                                  {scenarioLabelFromPath(entry.path, explorerCatalogScenarios)}
                                </span>
                              ))}
                            </div>
                          </CollapsiblePanel>
                        </>
                      ) : (
                        <div className="empty-state">
                          Select one or more local scenarios and a sector, then load the Scenario
                          Explorer to inspect the backend-generated summary here.
                        </div>
                      )}
                    </article>
                  </div>
                </div>
              </article>
            </div>
          ) : null}

          {activeTab === "transformations" ? (
            <div className="card-stack">
              {workflow === "incremental_database" ? (
                <article className="card">
                  <div className="card-head">
                    <div>
                      <h2>
                        <HelpText
                          helpKey="section.incremental_sectors"
                          className="help-inline help-inline-heading"
                        >
                          Incremental sectors
                        </HelpText>
                      </h2>
                      <p className="subtle">
                        Leave the selection empty to run all incremental sectors. The adapter maps these UI selections to the internal Premise sector groups.
                      </p>
                    </div>
                    <button
                      className="button subtle-button"
                      type="button"
                      onClick={() => toggleAllIncrementalSectors(true)}
                    >
                      Reset to All Sectors
                    </button>
                  </div>

                  <div className="checkbox-grid">
                    {incrementalSectorCatalog.map((entry) => {
                      const isAll = selectedSectors.length === 0;
                      const active = isAll || selectedSectors.includes(entry.id);
                      return (
                        <label className="check-card" key={entry.id}>
                          <input
                            type="checkbox"
                            checked={active}
                            onChange={() => toggleIncrementalSector(entry.id)}
                          />
                          <div>
                            <strong>{entry.label}</strong>
                            <span>{entry.description || entry.id}</span>
                          </div>
                        </label>
                      );
                    })}
                  </div>
                </article>
              ) : (
                <article className="card">
                  <div className="card-head">
                    <div>
                      <h2>
                        <HelpText
                          helpKey="section.transformation_checklist"
                          className="help-inline help-inline-heading"
                        >
                          Transformation checklist
                        </HelpText>
                      </h2>
                      <p className="subtle">
                        Default behavior is “all sectors”. Selecting specific sectors keeps Premise’s built-in execution order and only changes inclusion.
                      </p>
                    </div>
                    <label className="switch">
                      <input
                        type="checkbox"
                        checked={selectedTransformations == null}
                        onChange={(event) => toggleAllTransformations(event.target.checked)}
                      />
                      <HelpText helpKey="field.run_all_transformations">
                        Run all transformations
                      </HelpText>
                    </label>
                  </div>

                  <div className="checkbox-grid">
                    {transformationCatalog.map((entry) => {
                      const active =
                        selectedTransformations == null ||
                        selectedTransformations.includes(entry.id);
                      return (
                        <label className="check-card" key={entry.id}>
                          <input
                            type="checkbox"
                            checked={active}
                            onChange={() => toggleTransformSelection(entry.id)}
                          />
                          <div>
                            <strong>{entry.label}</strong>
                            <span>{entry.description || entry.id}</span>
                          </div>
                        </label>
                      );
                    })}
                  </div>
                </article>
              )}

              <article className="card">
                <div className="card-head">
                  <div>
                    <h2>
                      <HelpText
                        helpKey="section.selection_summary"
                        className="help-inline help-inline-heading"
                      >
                        Selection summary
                      </HelpText>
                    </h2>
                    <p className="subtle">
                      External scenarios are still deferred from direct editing, but the architecture and run manifest already preserve their extension points.
                    </p>
                  </div>
                </div>

                <div className="metric-strip">
                  <span className="metric-chip">
                    Workflow <strong>{workflowLabel(workflow)}</strong>
                  </span>
                  <span className="metric-chip">
                    Scope{" "}
                    <strong>
                      {workflow === "incremental_database"
                        ? selectedSectors.length
                          ? `${selectedSectors.length} selected sector${selectedSectors.length === 1 ? "" : "s"}`
                          : "all sectors"
                        : selectedTransformations == null
                          ? "all transformations"
                          : `${selectedTransformations.length} selected`}
                    </strong>
                  </span>
                </div>
              </article>
            </div>
          ) : null}

          {activeTab === "export" ? (
            <div className="card-stack">
              <article className="card">
                <div className="card-head">
                  <div>
                    <h2>
                      <HelpText
                        helpKey="section.export_target"
                        className="help-inline help-inline-heading"
                      >
                        Export target
                      </HelpText>
                    </h2>
                    <p className="subtle">
                      One run produces one export target. The available targets depend on the active workflow and scenario count.
                    </p>
                  </div>
                  {workflow === "new_database" && totalScenarioCount < 2 ? (
                    <span className="pill" data-tone="warning">
                      Superstructure hidden until 2 scenarios
                    </span>
                  ) : null}
                </div>

                <div className="form-grid compact-grid">
                  <div className="field">
                    <label htmlFor="exportType">
                      <HelpText helpKey="field.export_type">Export Type</HelpText>
                    </label>
                    <select
                      id="exportType"
                      value={exportType}
                      onChange={(event) => updateExportType(event.target.value)}
                    >
                      {availableExportTypes.map((entry) => (
                        <option key={entry} value={entry}>
                          {exportTypeLabel(entry)}
                        </option>
                      ))}
                    </select>
                    <FieldFeedback
                      validation={validation}
                      fieldKey="export_type"
                      hint="Choose one export target for this run. Brightway stays the fastest path back into interactive analysis."
                    />
                  </div>

                  {NAME_BASED_EXPORT_TYPES.has(exportType) ? (
                    <div className="field">
                      <label htmlFor="exportName">
                        <HelpText helpKey="field.export_name">
                          {exportNameLabel(exportType)}
                        </HelpText>
                      </label>
                      <input
                        id="exportName"
                        type="text"
                        value={typeof exportOptions.name === "string" ? exportOptions.name : ""}
                        placeholder={exportNamePlaceholder(exportType)}
                        onChange={(event) => updateExportOption("name", event.target.value)}
                      />
                      <FieldFeedback
                        validation={validation}
                        fieldKey="export_name"
                        hint="Leave this blank when Premise can generate a stable default name for the selected export target."
                      />
                    </div>
                  ) : null}

                  {PATH_BASED_EXPORT_TYPES.has(exportType) &&
                  typeof exportOptions.filepath === "string" ? (
                    <div className="field field-span-2">
                      <label htmlFor="exportPath">
                        <HelpText helpKey="field.export_path">
                          {exportPathLabel(exportType)}
                        </HelpText>
                      </label>
                      <div className="path-control">
                        <input
                          id="exportPath"
                          type="text"
                          value={exportOptions.filepath}
                          placeholder={exportPathPlaceholder(workflow, exportType)}
                          onChange={(event) => updateExportOption("filepath", event.target.value)}
                        />
                        <button
                          className="button subtle-button"
                          type="button"
                          onClick={() => {
                            void browseExportDirectory().catch((error: Error) =>
                              setMessage({ text: error.message, tone: "error" }),
                            );
                          }}
                        >
                          Browse
                        </button>
                      </div>
                      <FieldFeedback
                        validation={validation}
                        fieldKey="export_path"
                        hint="Use a writable directory when exporting files or folders outside Brightway."
                      />
                    </div>
                  ) : null}

                  {exportType === "superstructure" ? (
                    <>
                      <div className="field">
                        <label htmlFor="superstructureFormat">
                          <HelpText helpKey="field.difference_file_format">
                            Difference File Format
                          </HelpText>
                        </label>
                        <select
                          id="superstructureFormat"
                          value={
                            typeof exportOptions.file_format === "string"
                              ? exportOptions.file_format
                              : "csv"
                          }
                          onChange={(event) =>
                            updateExportOption("file_format", event.target.value)
                          }
                        >
                          {SUPERSTRUCTURE_FILE_FORMATS.map((entry) => (
                            <option key={entry} value={entry}>
                              {entry}
                            </option>
                          ))}
                        </select>
                      </div>

                      <div className="field toggle-field">
                        <label className="switch" htmlFor="preserveOriginalColumn">
                          <input
                            id="preserveOriginalColumn"
                            type="checkbox"
                            checked={Boolean(exportOptions.preserve_original_column)}
                            onChange={(event) =>
                              updateExportOption(
                                "preserve_original_column",
                                event.target.checked,
                              )
                            }
                          />
                          <HelpText helpKey="field.preserve_original_column">
                            Preserve original column
                          </HelpText>
                        </label>
                      </div>
                    </>
                  ) : null}
                </div>

                <div className="inline-note">
                  {exportType === "datapackage"
                    ? "Datapackage exports are archive-style outputs handled directly by Premise."
                    : exportType === "brightway"
                      ? "Brightway exports can leave the name blank and let Premise generate names automatically."
                      : exportType === "superstructure"
                        ? "Superstructure also needs at least two scenarios and writes comparison artifacts."
                        : `Current export target: ${exportTypeLabel(exportType)}.`}
                </div>
              </article>

              {workflow === "pathways_datapackage" ? (
                <CollapsibleCard
                  title={
                    <HelpText
                      helpKey="section.contributors_and_metadata"
                      className="help-inline help-inline-heading"
                    >
                      Contributors and metadata
                    </HelpText>
                  }
                  subtitle="These entries are written into the generated pathways datapackage metadata."
                  defaultOpen={false}
                  actions={
                    <button className="button secondary" type="button" onClick={() => addContributor()}>
                      Add Contributor
                    </button>
                  }
                >
                  <FieldFeedback
                    validation={validation}
                    fieldKey="contributors"
                    hint="Keep contributor metadata compact; add a new row only when another person or institution should appear in the datapackage."
                  />
                  <div className="stack">
                    {(draftProject.config.contributors || []).map((entry, index) => (
                      <div className="item-card" key={`contributor-${index}`}>
                        <div className="form-grid compact-grid">
                          <div className="field">
                            <label htmlFor={`contributor-title-${index}`}>
                              <HelpText helpKey="field.contributor_title">Title</HelpText>
                            </label>
                            <input
                              id={`contributor-title-${index}`}
                              type="text"
                              value={entry.title || ""}
                              onChange={(event) =>
                                updateContributor(index, { title: event.target.value })
                              }
                            />
                          </div>

                          <div className="field">
                            <label htmlFor={`contributor-name-${index}`}>
                              <HelpText helpKey="field.contributor_name">Name</HelpText>
                            </label>
                            <input
                              id={`contributor-name-${index}`}
                              type="text"
                              value={entry.name}
                              onChange={(event) =>
                                updateContributor(index, { name: event.target.value })
                              }
                            />
                          </div>

                          <div className="field field-span-2">
                            <label htmlFor={`contributor-email-${index}`}>
                              <HelpText helpKey="field.contributor_email">Email</HelpText>
                            </label>
                            <div className="path-control">
                              <input
                                id={`contributor-email-${index}`}
                                type="email"
                                value={entry.email}
                                onKeyDown={(event) => {
                                  if (event.key !== "Enter") {
                                    return;
                                  }
                                  event.preventDefault();
                                  if (index === (draftProject.config.contributors || []).length - 1) {
                                    addContributor();
                                  }
                                }}
                                onChange={(event) =>
                                  updateContributor(index, { email: event.target.value })
                                }
                              />
                              <button
                                className="button subtle-button"
                                type="button"
                                onClick={() => removeContributor(index)}
                              >
                                Remove
                              </button>
                            </div>
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                </CollapsibleCard>
              ) : null}
            </div>
          ) : null}

          {activeTab === "monitor" ? (
            <div className="card-stack">
              <article className="card">
                <div className="card-head">
                  <div>
                    <h2>
                      <HelpText
                        helpKey="section.current_worker"
                        className="help-inline help-inline-heading"
                      >
                        Current worker
                      </HelpText>
                    </h2>
                    <p className="subtle">
                      Status, phase changes, queue position, and live artifact discovery for the currently selected run.
                    </p>
                  </div>
                  <div className="action-cluster compact-actions">
                    <span className="pill" data-tone={currentRunId ? "warning" : "neutral"}>
                      {jobStateLabel}
                    </span>
                    <button
                      className="button subtle-button"
                      type="button"
                      title={cancelRunDisabledReason || "Stop the active or queued run."}
                      disabled={cancelDisabled}
                      onClick={() => {
                        void cancelCurrentRun().catch((error: Error) =>
                          setMessage({ text: error.message, tone: "error" }),
                        );
                      }}
                    >
                      Cancel Run
                    </button>
                  </div>
                </div>

                {cancelRunDisabledReason ? (
                  <div className="field-feedback" data-tone="neutral">
                    {cancelRunDisabledReason}
                  </div>
                ) : null}

                <div className="metric-strip">
                  <span className="metric-chip">
                    Run <strong>{currentRunId || "none"}</strong>
                  </span>
                  <span className="metric-chip">
                    Queue <strong>{currentQueuePosition || "-"}</strong>
                  </span>
                  <span className="metric-chip">
                    Artifacts <strong>{artifacts.length}</strong>
                  </span>
                </div>

                {currentOutputLocation ? (
                  <div className="inline-note">
                    <strong>Output location.</strong> {currentOutputLocation}
                  </div>
                ) : null}

                {phaseSummaries.length ? (
                  <div className="phase-strip">
                    {phaseSummaries.map((entry) => (
                      <div className="phase-card" key={`${entry.phase}-${entry.timestamp}`}>
                        <strong>{JOB_PHASE_LABELS[entry.phase || ""] || entry.phase}</strong>
                        <span>
                          {entry.phase && entry.phase in JOB_PHASE_PROGRESS
                            ? `Phase ${Object.keys(JOB_PHASE_PROGRESS).indexOf(entry.phase) + 1} of ${Object.keys(JOB_PHASE_PROGRESS).length}`
                            : entry.event_type}
                        </span>
                        <span>{entry.message || ""}</span>
                      </div>
                    ))}
                  </div>
                ) : (
                  <div className="empty-state">No worker phases reported yet.</div>
                )}
              </article>

              <CollapsibleCard
                title={
                  <HelpText
                    helpKey="section.events"
                    className="help-inline help-inline-heading"
                  >
                    Events
                  </HelpText>
                }
                subtitle="Live event stream from the active or queued worker run."
                defaultOpen={false}
                actions={
                  <div className="field slim-field">
                    <label htmlFor="logFilter">
                      <HelpText helpKey="field.severity">Severity</HelpText>
                    </label>
                    <select
                      id="logFilter"
                      value={logFilter}
                      onChange={(event) => setLogFilter(event.target.value as LogFilter)}
                    >
                      <option value="all">All</option>
                      <option value="info">Info</option>
                      <option value="warning">Warnings + Errors</option>
                      <option value="error">Errors</option>
                    </select>
                  </div>
                }
              >
                <pre className="code-block tall-block">
                  {eventLines.length ? eventLines.join("\n") : "No worker events yet."}
                </pre>
              </CollapsibleCard>

              <CollapsibleCard
                title={
                  <HelpText
                    helpKey="section.artifacts"
                    className="help-inline help-inline-heading"
                  >
                    Artifacts
                  </HelpText>
                }
                subtitle="Files discovered under the current run directory."
                defaultOpen={false}
              >
                <ul className="artifact-list">
                  {artifacts.length ? (
                    artifacts.map((artifact) => {
                      const artifactUrl = buildArtifactUrl(currentRunTarget, artifact);
                      return (
                        <li key={artifact}>
                          {artifactUrl ? (
                            <a href={artifactUrl} target="_blank" rel="noreferrer">
                              {artifact}
                            </a>
                          ) : (
                            artifact
                          )}
                        </li>
                      );
                    })
                  ) : (
                    <li>No artifacts yet.</li>
                  )}
                </ul>
              </CollapsibleCard>
            </div>
          ) : null}

          {activeTab === "history" ? (
            <article className="card">
              <div className="card-head">
                <div>
                  <h2>
                    <HelpText
                      helpKey="section.run_history"
                      className="help-inline help-inline-heading"
                    >
                      Run history
                    </HelpText>
                  </h2>
                  <p className="subtle">
                    Saved configurations track prior queued and completed runs. Snapshots can be reloaded into the editor.
                  </p>
                </div>
              </div>

              <div className="stack">
                {projectHistory.length ? (
                  projectHistory.map((entry) => {
                    const finishedAt = entry.completed_at
                      ? formatTimestamp(entry.completed_at)
                      : "in progress";
                    const historyTarget = buildHistoryTarget(entry, projectPath);
                    return (
                      <div className="history-item" key={entry.run_id}>
                        <div className="history-head">
                          <div>
                            <strong>{entry.status || "unknown"}</strong>
                            <div className="subtle">{runSummaryLabel(entry)}</div>
                          </div>
                          <div className="subtle">{formatTimestamp(entry.created_at)}</div>
                        </div>
                        <div className="history-grid">
                          <span>Run ID: {entry.run_id}</span>
                          <span>Finished: {finishedAt}</span>
                          <span>Artifacts: {entry.artifact_count || 0}</span>
                          <span>{entry.run_dir ? `Run dir: ${entry.run_dir}` : "Run dir pending"}</span>
                        </div>
                        {entry.artifacts?.length ? (
                          <div className="history-artifacts">
                            <div className="metric-label">Saved artifacts</div>
                            <ul className="artifact-list history-artifact-list">
                              {entry.artifacts.map((artifact) => {
                                const artifactUrl = buildArtifactUrl(historyTarget, artifact);
                                return (
                                  <li key={`${entry.run_id}-${artifact}`}>
                                    {artifactUrl ? (
                                      <a href={artifactUrl} target="_blank" rel="noreferrer">
                                        {artifact}
                                      </a>
                                    ) : (
                                      artifact
                                    )}
                                  </li>
                                );
                              })}
                            </ul>
                          </div>
                        ) : null}
                        <div className="action-cluster compact-actions">
                          <button
                            className="button subtle-button"
                            type="button"
                            onClick={() => {
                              focusRunMonitor(entry.run_id, {
                                status: entry.status || "queued",
                                queuePosition: null,
                              });
                              setActiveTab("monitor");
                              setMessage({
                                text: `Monitoring run ${entry.run_id}.`,
                                tone: "success",
                              });
                            }}
                          >
                            Monitor
                          </button>
                          <button
                            className="button secondary"
                            type="button"
                            onClick={() => {
                              void loadHistorySnapshot(entry.run_id).catch((error: Error) =>
                                setMessage({ text: error.message, tone: "error" }),
                              );
                            }}
                          >
                            Use Config
                          </button>
                          <button
                            className="button subtle-button"
                            type="button"
                            onClick={() => {
                              void loadHistoryDetails(entry.run_id).catch((error: Error) =>
                                setMessage({ text: error.message, tone: "error" }),
                              );
                            }}
                          >
                            Details
                          </button>
                        </div>
                      </div>
                    );
                  })
                ) : (
                  <div className="empty-state">
                    {projectPath
                      ? "No runs recorded for this configuration yet."
                      : "Save and queue a configuration to build run history."}
                  </div>
                )}
              </div>
            </article>
          ) : null}

          {activeTab === "troubleshooting" ? (
            <div className="card-stack">
              <article className="card">
                <div className="card-head">
                  <div>
                    <h2>
                      <HelpText
                        helpKey="section.technical_details"
                        className="help-inline help-inline-heading"
                      >
                        Technical details
                      </HelpText>
                    </h2>
                    <p className="subtle">
                      Failed and completed runs remain inspectable with redacted diagnostics, logs, and support-bundle export.
                    </p>
                  </div>
                  <div className="action-cluster compact-actions">
                    <span className="pill" data-tone="neutral">
                      {diagnostics?.run_id || diagnosticsTarget?.run_id || "No run selected"}
                    </span>
                  </div>
                </div>

                <pre className="code-block">{diagnosticsSummaryText}</pre>
              </article>

              <CollapsibleCard
                title={
                  <HelpText
                    helpKey="section.redacted_logs"
                    className="help-inline help-inline-heading"
                  >
                    Redacted logs
                  </HelpText>
                }
                subtitle="Traceback excerpts, stderr, and stdout tails remain available for the selected run."
                defaultOpen={false}
              >
                <pre className="code-block tall-block">{diagnosticsLogsText}</pre>
              </CollapsibleCard>

              <article className="card">
                <div className="card-head">
                  <div>
                    <h2>
                      <HelpText
                        helpKey="section.support_workflow"
                        className="help-inline help-inline-heading"
                      >
                        Support workflow
                      </HelpText>
                    </h2>
                    <p className="subtle">
                      Use the support bundle when you need to share a run with maintainers without manually gathering manifest, logs, metadata, and diagnostics.
                    </p>
                  </div>
                </div>

                <div className="status-box" data-tone="neutral">
                  <strong>What the bundle includes</strong>
                  <ul>
                    <li>run manifest and metadata</li>
                    <li>diagnostics and structured event stream</li>
                    <li>redacted stdout and stderr tails</li>
                    <li>configuration snapshot when available</li>
                  </ul>
                </div>

                <div className="action-cluster">
                  <button
                    className="button subtle-button"
                    type="button"
                    disabled={!buildSupportBundleUrl(diagnosticsTarget)}
                    onClick={() => {
                      const url = buildSupportBundleUrl(diagnosticsTarget);
                      if (!url) {
                        setMessage({
                          text: "Select a run with diagnostics before exporting a support bundle.",
                          tone: "error",
                        });
                        return;
                      }
                      window.open(url, "_blank", "noopener");
                    }}
                  >
                    Download Bundle
                  </button>
                  <button
                    className="button primary"
                    type="button"
                    disabled={!buildSupportBundleUrl(diagnosticsTarget)}
                    onClick={() => startSupportWorkflow()}
                  >
                    Email Support
                  </button>
                </div>

                <div className="inline-note">
                  The app can open a prefilled email to <strong>romain.sacchi@psi.ch</strong>,
                  but browsers cannot attach files automatically. The bundle download starts at the
                  same time so you can attach it before sending.
                </div>
              </article>
            </div>
          ) : null}
        </div>

        <aside className="side-column">
          <article className="card side-card">
            <div className="card-head">
              <div>
                <h2>{activeHelp.title}</h2>
                <p className="subtle">{activeHelp.body}</p>
              </div>
            </div>
            <ul className="side-list">
              {activeHelp.bullets.map((entry) => (
                <li key={entry}>{entry}</li>
              ))}
            </ul>
          </article>

          <article className="card side-card">
            <div className="card-head">
              <div>
                <h2>
                  <HelpText
                    helpKey="section.iam_scenario_key"
                    className="help-inline help-inline-heading"
                  >
                    IAM Scenario Key
                  </HelpText>
                </h2>
                <p className="subtle">
                  Enter the IAM scenario decryption key used for encrypted scenario files. The UI can remember it across sessions on this machine.
                </p>
              </div>
            </div>

            <div className="field">
              <label htmlFor="iamKeyInput">
                <HelpText helpKey="field.iam_files_key">IAM_FILES_KEY</HelpText>
              </label>
              <div className="path-control">
                <input
                  id="iamKeyInput"
                  type={showIamKey ? "text" : "password"}
                  value={iamKeyInput}
                  placeholder="Paste IAM scenario decryption key"
                  onChange={(event) => setIamKeyInput(event.target.value)}
                />
                <button
                  className="button subtle-button"
                  type="button"
                  onClick={() => setShowIamKey((current) => !current)}
                >
                  {showIamKey ? "Hide" : "Show"}
                </button>
                <button
                  className="button secondary"
                  type="button"
                  disabled={!iamKeyInput.trim()}
                  onClick={() => {
                    void saveIamKey().catch((error: Error) =>
                      setMessage({ text: error.message, tone: "error" }),
                    );
                  }}
                >
                  Save
                </button>
              </div>
            </div>

            <div className="inline-actions">
              <span className="subtle">{iamKeySourceLabel}</span>
              <div className="action-cluster compact-actions">
                <button
                  className="button subtle-button"
                  type="button"
                  onClick={() => {
                    window.location.href = buildIamKeyRequestEmailUrl();
                  }}
                >
                  Ask for Key
                </button>
                <button
                  className="button subtle-button"
                  type="button"
                  disabled={
                    !iamKeyState?.has_value ||
                    (!iamKeyState.persisted && iamKeyState.backend === "environment")
                  }
                  onClick={() => {
                    void clearIamKeyState().catch((error: Error) =>
                      setMessage({ text: error.message, tone: "error" }),
                    );
                  }}
                >
                  Clear Stored Key
                </button>
              </div>
            </div>

            <div className="status-box" data-tone="neutral">
              <strong>Current behavior</strong>
              <ul>
                <li>The key is loaded into the current UI session for worker runs.</li>
                <li>If system keyring support is available, that backend is preferred.</li>
                <li>Otherwise the key is stored only in the local Premise UI data area for this user.</li>
              </ul>
            </div>
          </article>

          <article className="card side-card">
            <div className="card-head">
              <div>
                <h2>
                  <HelpText
                    helpKey="section.environment"
                    className="help-inline help-inline-heading"
                  >
                    Environment
                  </HelpText>
                </h2>
                <p className="subtle">
                  Local runtime and credential visibility. Secrets are not stored in configurations.
                </p>
              </div>
              <button
                className="button subtle-button"
                type="button"
                onClick={() => {
                  void retryStartupChecks().catch((error: Error) =>
                    setMessage({ text: error.message, tone: "error" }),
                  );
                }}
              >
                Retry startup checks
              </button>
            </div>
            <div className="metric-stack">
              <div className="metric-row">
                <span>Premise</span>
                <strong>{health?.premise_version || "-"}</strong>
              </div>
              <div className="metric-row">
                <span>Python</span>
                <strong>{environment?.python_version || "-"}</strong>
              </div>
              <div className="metric-row">
                <span>Platform</span>
                <strong>{environment?.platform || "-"}</strong>
              </div>
              <div className="metric-row">
                <span>Credentials</span>
                <strong>{credentials}</strong>
              </div>
              <div className="metric-row">
                <span>Dialogs</span>
                <strong>{dialogCapabilityLabel}</strong>
              </div>
            </div>
            {environment != null ? (
              <div className="status-box" data-tone={dialogCapability?.available ? "neutral" : "warning"}>
                <strong>Path selection</strong>
                <p>
                  {dialogCapability?.detail || "Dialog availability is unknown."}
                </p>
                {dialogCapability?.manual_path_entry ? (
                  <p>All file and directory paths can still be entered manually.</p>
                ) : null}
              </div>
            ) : null}
          </article>

          <article className="card side-card">
            <div className="card-head">
              <div>
                <h2>
                  <HelpText
                    helpKey="section.recents"
                    className="help-inline help-inline-heading"
                  >
                    Recents
                  </HelpText>
                </h2>
                <p className="subtle">
                  Global recent configurations and frequently used paths across the GUI.
                </p>
              </div>
              <button
                className="button subtle-button"
                type="button"
                onClick={() => {
                  void apiRequest<RecentsPayload>("/api/recents", {
                    method: "DELETE",
                  })
                    .then((payload) => {
                      setRecents(payload);
                      setMessage({
                        text: "Recent history cleared.",
                        tone: "success",
                      });
                    })
                    .catch((error: Error) =>
                      setMessage({ text: error.message, tone: "error" }),
                    );
                }}
              >
                Clear History
              </button>
            </div>

            <div className="recent-block">
              <div className="metric-label">Configurations</div>
              <div className="stack">
                {recents.recent_projects.length ? (
                  recents.recent_projects.map((entry) => (
                    <button
                      key={entry.path}
                      type="button"
                      className="recent-item"
                      onClick={() => {
                        void openProject(entry.path).catch((error: Error) =>
                          setMessage({ text: error.message, tone: "error" }),
                        );
                      }}
                    >
                      <strong>{entry.label || entry.path}</strong>
                      <span>{entry.path}</span>
                      <span>{formatTimestamp(entry.last_used_at)}</span>
                    </button>
                  ))
                ) : (
                  <div className="empty-state compact-empty">No recent configurations.</div>
                )}
              </div>
            </div>

            <div className="recent-block">
              <div className="metric-label">Paths</div>
              <div className="stack">
                {recents.recent_paths.length ? (
                  recents.recent_paths.map((entry) => (
                    <button
                      key={`${entry.kind || "path"}-${entry.path}`}
                      type="button"
                      className="recent-item"
                      onClick={() => applyRecentPath(entry)}
                    >
                      <strong>{entry.label || entry.path}</strong>
                      <span>{entry.kind || "path"}</span>
                      <span>{entry.path}</span>
                    </button>
                  ))
                ) : (
                  <div className="empty-state compact-empty">No recent paths.</div>
                )}
              </div>
            </div>
          </article>
        </aside>
      </section>
    </main>
  );
}
