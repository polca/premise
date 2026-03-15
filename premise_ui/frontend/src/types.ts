export interface RecentEntry {
  path: string;
  label?: string;
  kind?: string;
  last_used_at?: string;
}

export interface RecentsPayload {
  recent_projects: RecentEntry[];
  recent_paths: RecentEntry[];
}

export interface TransformationEntry {
  id: string;
  label: string;
  description?: string;
}

export interface IamScenarioCatalogEntry {
  id: string;
  model: string;
  pathway: string;
  label: string;
  description: string;
  default_source_path_group: string;
  availability: string;
  bundled: boolean;
  known_years: number[];
  year_notes?: string;
}

export interface LocalIamScenarioEntry {
  id: string;
  model: string;
  pathway: string;
  file_name: string;
  path: string;
}

export interface LocalIamScenariosPayload {
  scenarios: LocalIamScenarioEntry[];
}

export interface IamScenarioStoryline {
  id: string;
  model: string;
  pathway: string;
  label: string;
  source_heading?: string;
  description?: string;
  scope_note?: string;
  sections?: Array<{
    title: string;
    content: string;
  }>;
}

export interface IamScenarioStorylineCatalogPayload {
  schema_version: number;
  updated_at: string;
  source: string;
  storylines: IamScenarioStoryline[];
}

export interface ScenarioPreviewPoint {
  year: number;
  value: number;
}

export interface ScenarioPreviewSeries {
  label: string;
  unit?: string | null;
  points: ScenarioPreviewPoint[];
}

export interface ScenarioPreviewPayload {
  path: string;
  file_name: string;
  suffix: string;
  inferred_model?: string | null;
  inferred_pathway?: string | null;
  years: number[];
  columns: string[];
  series: ScenarioPreviewSeries[];
}

export interface ScenarioExplorerSectorCatalogEntry {
  id: string;
  label: string;
  unit?: string | null;
  explanation: string;
  offset?: number;
  group_by: string;
  variables: string[];
}

export interface ScenarioExplorerPoint {
  year: number;
  value: number;
}

export interface ScenarioExplorerSeries {
  variable: string;
  unit?: string | null;
  points: ScenarioExplorerPoint[];
}

export interface ScenarioExplorerGroupSummary {
  name: string;
  group_type: string;
  region?: string;
  subscenario?: string;
  variables: string[];
  years: number[];
  series: ScenarioExplorerSeries[];
}

export interface ScenarioExplorerScenarioSummary {
  scenario_id: string;
  model: string;
  pathway: string;
  label?: string | null;
  comparison_label?: string;
  group_by: string;
  regions: string[];
  subscenarios?: string[];
  variables: string[];
  years: number[];
  groups: ScenarioExplorerGroupSummary[];
}

export interface ScenarioExplorerSummaryPayload {
  sector: string;
  label?: string | null;
  explanation?: string;
  offset?: number;
  group_by: string;
  regions: string[];
  subscenarios?: string[];
  years: number[];
  variables: string[];
  scenarios: ScenarioExplorerScenarioSummary[];
}

export interface ScenarioExplorerComparePayload {
  compare_mode: string;
  baseline_year: number | null;
  baseline_scenario_id?: string | null;
  baseline_scenario_label?: string | null;
  summary: ScenarioExplorerSummaryPayload;
}

export interface ScenarioExplorerCatalogPayload {
  scenarios: LocalIamScenarioEntry[];
  sectors: ScenarioExplorerSectorCatalogEntry[];
}

export interface CapabilitiesPayload {
  workflows: string[];
  source_types: string[];
  export_types: string[];
  workflow_export_types: Record<string, string[]>;
  ecoinvent_versions: string[];
  iam_models: string[];
  iam_pathways: string[];
  iam_scenarios: LocalIamScenarioEntry[];
  iam_scenario_catalog: IamScenarioCatalogEntry[];
  transformation_catalog: TransformationEntry[];
  incremental_sector_catalog: TransformationEntry[];
  premise_version: string;
}

export interface DownloadAllScenariosPayload {
  job_id: string;
  status: string;
  directory: string;
  started_at: string;
  updated_at: string;
  total_count: number;
  processed_count: number;
  progress: number;
  current_file: string | null;
  current_file_progress: number | null;
  downloaded: string[];
  existing: string[];
  failed: Array<{ file_name: string; error: string }>;
  scenarios: LocalIamScenarioEntry[];
}

export interface ClearLocalScenariosPayload {
  directory: string;
  removed: string[];
  removed_count: number;
  scenarios: LocalIamScenarioEntry[];
}

export interface BrightwayDiscoveryPayload {
  available: boolean;
  current_project: string | null;
  projects: string[];
  databases: string[];
}

export interface EnvironmentPayload {
  python_version: string;
  platform: string;
  credentials: Record<string, boolean>;
  dialogs: {
    native_path_dialogs: {
      available: boolean;
      backend: string;
      detail: string;
      manual_path_entry: boolean;
    };
  };
}

export interface StoredCredentialPayload {
  name: string;
  has_value: boolean;
  value: string;
  source: string;
  persisted: boolean;
  backend: string;
}

export interface HealthPayload {
  status: string;
  service: string;
  ui_version: string;
  premise_version: string;
}

export interface Scenario {
  model: string;
  pathway: string;
  year: number;
  filepath?: string;
}

export interface ScenarioSet {
  name: string;
  scenarios: Scenario[];
}

export interface ExportOptions {
  filepath?: string;
  name?: string;
  file_format?: string;
  preserve_original_column?: boolean;
  [key: string]: unknown;
}

export interface ExportConfig {
  type: string;
  options: ExportOptions;
}

export interface AdditionalInventory {
  filepath: string;
  "ecoinvent version": string;
  region_duplicate?: boolean;
}

export interface Contributor {
  title?: string;
  name: string;
  email: string;
}

export interface GuiProjectManifest {
  schema_version: number;
  project_name: string;
  workflow: string;
  config: {
    source_type: string;
    source_version: string;
    source_project: string;
    source_db: string;
    source_file_path: string;
    system_model: string;
    system_args: Record<string, unknown>;
    use_cached_inventories: boolean;
    use_cached_database: boolean;
    quiet: boolean;
    keep_imports_uncertainty: boolean;
    keep_source_db_uncertainty: boolean;
    gains_scenario: string;
    use_absolute_efficiency: boolean;
    biosphere_name: string;
    generate_reports: boolean;
    additional_inventories: AdditionalInventory[];
    transformations: string[] | string | null;
    sectors?: string[];
    years?: number[];
    contributors?: Contributor[];
    export: ExportConfig;
    [key: string]: unknown;
  };
  scenario_sets: ScenarioSet[];
  run_history: RunHistoryEntry[];
  ui_state: Record<string, unknown>;
}

export interface RunManifest {
  schema_version: number;
  run_id: string;
  created_at: string;
  project_name: string;
  workflow: string;
  project_path?: string | null;
  working_directory: string;
  config: GuiProjectManifest["config"];
  scenarios: Scenario[];
  metadata: Record<string, unknown>;
}

export interface ValidationPayload {
  valid: boolean;
  errors: string[];
  warnings: string[];
}

export interface RunEvent {
  timestamp: string;
  run_id: string;
  event_type: string;
  level?: string;
  phase?: string | null;
  message?: string;
  details?: Record<string, unknown>;
}

export interface JobStatusPayload {
  run_id: string;
  status: string;
  queue_position: number | null;
  process_returncode: number | null;
  manifest: RunManifest;
  events: RunEvent[];
}

export interface RunHistoryEntry {
  run_id: string;
  created_at?: string;
  updated_at?: string;
  completed_at?: string;
  status?: string;
  workflow?: string;
  project_name?: string;
  dry_run?: boolean;
  export_type?: string;
  scenario_count?: number;
  run_dir?: string;
  artifact_count?: number;
  artifacts?: string[];
  warnings?: string[];
  process_returncode?: number | null;
  project_snapshot?: GuiProjectManifest;
}

export interface DiagnosticsPayload {
  run_id?: string;
  run_dir?: string;
  metadata?: Record<string, unknown>;
  diagnostics?: Record<string, unknown>;
  result?: Record<string, unknown>;
  events?: RunEvent[];
  stdout_tail?: string;
  stderr_tail?: string;
}

export interface PathDialogOptions {
  mode: "open_file" | "save_file" | "open_directory";
  title?: string;
  initial_path?: string | null;
  default_extension?: string;
  must_exist?: boolean;
  filters?: Array<{ label: string; pattern: string }>;
}

export interface RunDiagnosticsTarget {
  run_id?: string | null;
  project_path?: string | null;
  run_dir?: string | null;
}

export interface MessageState {
  text: string;
  tone: "success" | "error" | "info";
}
