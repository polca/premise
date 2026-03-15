import {
  buildExplorerBenchmarkSummary,
  explorerScenarioPlots,
  explorerSeriesForSelection,
  filterPlotSeriesByYearWindow,
  normalizePlotSeriesToShare,
  plotSeriesValueDomain,
  plotSeriesYearBounds,
} from "./explorerData.js";
import { EXPLORER_BENCHMARK_BASELINE } from "./explorerBenchmarkBaseline.js";
import type { PlotSeries } from "./explorerData.js";

type BenchmarkOptions = {
  scenarios: number;
  groups: number;
  variables: number;
  year_start: number;
  year_end: number;
  iterations: number;
  assert_baseline: boolean;
};

function parsePositiveInt(value: string | undefined, fallback: number): number {
  if (!value) {
    return fallback;
  }
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function parseArgs(argv: string[]): BenchmarkOptions {
  const values = new Map<string, string>();
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (!token.startsWith("--")) {
      continue;
    }
    const key = token.slice(2).replace(/-/g, "_");
    const next = argv[index + 1];
    if (next && !next.startsWith("--")) {
      values.set(key, next);
      index += 1;
    } else {
      values.set(key, "true");
    }
  }

  return {
    scenarios: parsePositiveInt(values.get("scenarios"), 6),
    groups: parsePositiveInt(values.get("groups"), 12),
    variables: parsePositiveInt(values.get("variables"), 8),
    year_start: parsePositiveInt(values.get("year_start"), 2020),
    year_end: parsePositiveInt(values.get("year_end"), 2100),
    iterations: parsePositiveInt(values.get("iterations"), 20),
    assert_baseline: values.has("assert_baseline"),
  };
}

function average(values: number[]): number {
  if (!values.length) {
    return 0;
  }
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function round(value: number): number {
  return Math.round(value * 1000) / 1000;
}

function sampleSelection<T>(values: T[]): T[] {
  if (values.length <= 1) {
    return values;
  }
  return values.filter((_, index) => index % 2 === 0);
}

function now(): number {
  return performance.now();
}

function runBenchmark(options: BenchmarkOptions) {
  const fixtureStartedAt = now();
  const summary = buildExplorerBenchmarkSummary(
    options.scenarios,
    options.groups,
    options.variables,
    options.year_start,
    options.year_end,
  );
  const fixtureMs = now() - fixtureStartedAt;

  const selectedGroups: string[] = sampleSelection<string>(summary.regions);
  const selectedVariables: string[] = sampleSelection<string>(summary.variables);
  const windowStart = summary.years[Math.max(1, Math.floor(summary.years.length * 0.2))] ?? options.year_start;
  const windowEnd =
    summary.years[Math.max(summary.years.length - Math.floor(summary.years.length * 0.2) - 1, 0)] ??
    options.year_end;

  const selectionRuns: number[] = [];
  const filterRuns: number[] = [];
  const normalizeRuns: number[] = [];
  const groupingRuns: number[] = [];
  const lineDomainRuns: number[] = [];
  const stackedDomainRuns: number[] = [];
  const boundsRuns: number[] = [];

  let lastSeries: PlotSeries[] = [];
  let lastFiltered: PlotSeries[] = [];
  let lastNormalized: PlotSeries[] = [];
  let lastGrouped: Array<{ scenarioLabel: string; series: PlotSeries[] }> = [];
  let lastBounds: { min: number; max: number } | null = null;
  let lastLineDomain: { min: number; max: number } | null = null;
  let lastStackedDomain: { min: number; max: number } | null = null;

  for (let index = 0; index < options.iterations; index += 1) {
    let startedAt = now();
    const selectedSeries = explorerSeriesForSelection(summary, selectedGroups, selectedVariables);
    selectionRuns.push(now() - startedAt);

    startedAt = now();
    const filteredSeries = filterPlotSeriesByYearWindow(selectedSeries, windowStart, windowEnd);
    filterRuns.push(now() - startedAt);

    startedAt = now();
    const normalizedSeries = normalizePlotSeriesToShare(filteredSeries);
    normalizeRuns.push(now() - startedAt);

    startedAt = now();
    const groupedSeries = explorerScenarioPlots(normalizedSeries);
    groupingRuns.push(now() - startedAt);

    startedAt = now();
    const lineDomain = plotSeriesValueDomain(normalizedSeries, "line");
    lineDomainRuns.push(now() - startedAt);

    startedAt = now();
    const stackedDomain = plotSeriesValueDomain(normalizedSeries, "stacked_area");
    stackedDomainRuns.push(now() - startedAt);

    startedAt = now();
    const bounds = plotSeriesYearBounds(normalizedSeries);
    boundsRuns.push(now() - startedAt);

    lastSeries = selectedSeries;
    lastFiltered = filteredSeries;
    lastNormalized = normalizedSeries;
    lastGrouped = groupedSeries;
    lastBounds = bounds;
    lastLineDomain = lineDomain;
    lastStackedDomain = stackedDomain;
  }

  return {
    options,
    fixture: {
      build_ms: round(fixtureMs),
      summary_scenarios: summary.scenarios.length,
      summary_groups: summary.regions.length,
      summary_variables: summary.variables.length,
      summary_years: summary.years.length,
    },
    selection: {
      selected_groups: selectedGroups.length,
      selected_variables: selectedVariables.length,
      year_window: [windowStart, windowEnd],
    },
    metrics_ms: {
      series_selection_avg: round(average(selectionRuns)),
      year_filter_avg: round(average(filterRuns)),
      normalize_share_avg: round(average(normalizeRuns)),
      scenario_grouping_avg: round(average(groupingRuns)),
      line_domain_avg: round(average(lineDomainRuns)),
      stacked_domain_avg: round(average(stackedDomainRuns)),
      year_bounds_avg: round(average(boundsRuns)),
    },
    output_shape: {
      selected_series: lastSeries.length,
      filtered_series: lastFiltered.length,
      normalized_series: lastNormalized.length,
      grouped_panels: lastGrouped.length,
      bounds: lastBounds,
      line_domain: lastLineDomain,
      stacked_domain: lastStackedDomain,
    },
  };
}

function assertExplorerBenchmarkBaseline(
  result: ReturnType<typeof runBenchmark>,
): string[] {
  const failures: string[] = [];
  const baseline = EXPLORER_BENCHMARK_BASELINE;

  for (const [key, value] of Object.entries(baseline.options)) {
    const actual = result.options[key as keyof BenchmarkOptions];
    if (actual !== value) {
      failures.push(`Option mismatch for ${key}: expected ${value}, got ${actual}`);
    }
  }

  if (result.selection.selected_groups !== baseline.expected.selected_groups) {
    failures.push(
      `Selected group count drifted: expected ${baseline.expected.selected_groups}, got ${result.selection.selected_groups}`,
    );
  }
  if (result.selection.selected_variables !== baseline.expected.selected_variables) {
    failures.push(
      `Selected variable count drifted: expected ${baseline.expected.selected_variables}, got ${result.selection.selected_variables}`,
    );
  }
  if (result.output_shape.selected_series !== baseline.expected.selected_series) {
    failures.push(
      `Selected series count drifted: expected ${baseline.expected.selected_series}, got ${result.output_shape.selected_series}`,
    );
  }
  if (result.output_shape.filtered_series !== baseline.expected.filtered_series) {
    failures.push(
      `Filtered series count drifted: expected ${baseline.expected.filtered_series}, got ${result.output_shape.filtered_series}`,
    );
  }
  if (result.output_shape.normalized_series !== baseline.expected.normalized_series) {
    failures.push(
      `Normalized series count drifted: expected ${baseline.expected.normalized_series}, got ${result.output_shape.normalized_series}`,
    );
  }
  if (result.output_shape.grouped_panels !== baseline.expected.grouped_panels) {
    failures.push(
      `Grouped panel count drifted: expected ${baseline.expected.grouped_panels}, got ${result.output_shape.grouped_panels}`,
    );
  }
  if (
    result.output_shape.bounds?.min !== baseline.expected.bounds.min ||
    result.output_shape.bounds?.max !== baseline.expected.bounds.max
  ) {
    failures.push(
      `Year bounds drifted: expected ${baseline.expected.bounds.min}-${baseline.expected.bounds.max}, got ${result.output_shape.bounds?.min ?? "?"}-${result.output_shape.bounds?.max ?? "?"}`,
    );
  }

  for (const [metric, ceiling] of Object.entries(baseline.max_avg_ms)) {
    const actual = result.metrics_ms[metric as keyof typeof result.metrics_ms];
    if (actual > ceiling) {
      failures.push(`Metric ${metric} exceeded ceiling ${ceiling} ms with ${actual} ms`);
    }
  }

  return failures;
}

const nodeProcess = (globalThis as { process?: { argv?: string[] } }).process;
const processArgs = Array.isArray(nodeProcess?.argv) ? nodeProcess.argv.slice(2) : [];
const options = parseArgs(processArgs);
const result = runBenchmark(options);
console.log(JSON.stringify(result, null, 2));

if (options.assert_baseline) {
  const failures = assertExplorerBenchmarkBaseline(result);
  if (failures.length) {
    throw new Error(`Scenario Explorer benchmark baseline check failed:\n${failures.join("\n")}`);
  }
}
