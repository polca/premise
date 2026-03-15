import type {
  ScenarioExplorerGroupSummary,
  ScenarioExplorerScenarioSummary,
  ScenarioExplorerSeries,
  ScenarioExplorerSummaryPayload,
} from "./types.js";

export type ExplorerChartMode = "line" | "stacked_area" | "bar" | "stacked_bar";

export type PlotPoint = {
  year: number;
  value: number;
};

export type PlotSeries = {
  label: string;
  scenarioLabel: string;
  unit?: string | null;
  points: PlotPoint[];
};

function uniqueStrings(values: string[]): string[] {
  return Array.from(new Set(values.filter((entry) => entry.trim())));
}

function explorerGroupOptions(summary: ScenarioExplorerSummaryPayload | null): string[] {
  if (!summary) {
    return [];
  }
  return summary.group_by === "subscenario"
    ? summary.subscenarios || []
    : summary.regions || [];
}

export function explorerSeriesForSelection(
  summary: ScenarioExplorerSummaryPayload | null,
  groupNames: string[],
  variables: string[],
): PlotSeries[] {
  if (!summary) {
    return [];
  }

  const activeGroups = groupNames.length ? groupNames : explorerGroupOptions(summary);
  const activeVariables = variables.length ? variables : summary.variables;

  return summary.scenarios
    .flatMap((scenarioSummary: ScenarioExplorerScenarioSummary) =>
      scenarioSummary.groups.flatMap((group: ScenarioExplorerGroupSummary) => {
        if (!activeGroups.includes(group.name)) {
          return [];
        }
        const scenarioLabel =
          scenarioSummary.comparison_label ||
          `${scenarioSummary.model.toUpperCase()} / ${scenarioSummary.pathway}`;
        return group.series.flatMap((series: ScenarioExplorerSeries) => {
          if (!activeVariables.includes(series.variable) || !series.points.length) {
            return [];
          }

          const labelParts = [scenarioLabel];
          if (activeGroups.length > 1) {
            labelParts.push(group.name);
          }
          if (activeVariables.length > 1) {
            labelParts.push(series.variable);
          }

          return {
            label: labelParts.join(" · "),
            scenarioLabel,
            unit: series.unit || summary.label || undefined,
            points: series.points,
          };
        });
      }),
    )
    .filter((entry: PlotSeries | null): entry is PlotSeries => entry !== null);
}

export function explorerScenarioPlots(series: PlotSeries[]): Array<{
  scenarioLabel: string;
  series: PlotSeries[];
}> {
  const grouped = new Map<string, PlotSeries[]>();
  for (const entry of series) {
    const existing = grouped.get(entry.scenarioLabel);
    if (existing) {
      existing.push(entry);
    } else {
      grouped.set(entry.scenarioLabel, [entry]);
    }
  }

  return Array.from(grouped.entries()).map(([scenarioLabel, scenarioSeries]) => ({
    scenarioLabel,
    series: scenarioSeries,
  }));
}

export function filterPlotSeriesByYearWindow(
  series: PlotSeries[],
  yearStart: number | null,
  yearEnd: number | null,
): PlotSeries[] {
  return series
    .map((entry) => ({
      ...entry,
      points: entry.points.filter((point) => {
        if (yearStart != null && point.year < yearStart) {
          return false;
        }
        if (yearEnd != null && point.year > yearEnd) {
          return false;
        }
        return true;
      }),
    }))
    .filter((entry) => entry.points.length);
}

export function normalizePlotSeriesToShare(series: PlotSeries[]): PlotSeries[] {
  const scenarioLabels = uniqueStrings(series.map((entry) => entry.scenarioLabel));
  const scenarioTotals = new Map<string, Map<number, number>>();

  for (const entry of series) {
    let totalsByYear = scenarioTotals.get(entry.scenarioLabel);
    if (!totalsByYear) {
      totalsByYear = new Map<number, number>();
      scenarioTotals.set(entry.scenarioLabel, totalsByYear);
    }

    for (const point of entry.points) {
      totalsByYear.set(point.year, (totalsByYear.get(point.year) ?? 0) + point.value);
    }
  }

  return series.map((entry) => {
    const totalsByYear = scenarioTotals.get(entry.scenarioLabel) ?? new Map<number, number>();

    return {
      ...entry,
      unit: "%",
      points: entry.points.map((point) => {
        const total = totalsByYear.get(point.year) ?? 0;
        return {
          year: point.year,
          value: total !== 0 ? (point.value / total) * 100 : 0,
        };
      }),
      label:
        scenarioLabels.length > 1 && !entry.label.startsWith(entry.scenarioLabel)
          ? `${entry.scenarioLabel} · ${entry.label}`
          : entry.label,
    };
  });
}

export function plotSeriesYearBounds(series: PlotSeries[]): { min: number; max: number } | null {
  const years = series.flatMap((entry) => entry.points.map((point) => point.year));
  if (!years.length) {
    return null;
  }
  return {
    min: Math.min(...years),
    max: Math.max(...years),
  };
}

export function plotSeriesValueDomain(
  series: PlotSeries[],
  chartMode: ExplorerChartMode,
): { min: number; max: number } | null {
  if (!series.length) {
    return null;
  }

  const allPoints = series.flatMap((entry) => entry.points);
  if (!allPoints.length) {
    return null;
  }

  const years = Array.from(new Set(allPoints.map((point) => point.year))).sort(
    (left, right) => left - right,
  );
  const seriesValueAtYear = (entry: PlotSeries, year: number) =>
    entry.points.find((point) => point.year === year)?.value ?? 0;
  const canStack = series.every((entry) => entry.points.every((point) => point.value >= 0));
  const effectiveChartMode =
    chartMode === "stacked_area"
      ? canStack
        ? "stacked_area"
        : "line"
      : chartMode === "stacked_bar"
        ? canStack
          ? "stacked_bar"
          : "bar"
        : chartMode;
  const yearlyTotals =
    effectiveChartMode === "stacked_area" || effectiveChartMode === "stacked_bar"
      ? years.map((year) =>
          series.reduce((sum, entry) => sum + seriesValueAtYear(entry, year), 0),
        )
      : [];
  const rawValues = allPoints.map((point) => point.value);

  if (effectiveChartMode === "stacked_area" || effectiveChartMode === "stacked_bar") {
    return {
      min: 0,
      max: Math.max(0, ...yearlyTotals),
    };
  }

  if (effectiveChartMode === "bar") {
    return {
      min: Math.min(0, ...rawValues),
      max: Math.max(0, ...rawValues),
    };
  }

  return {
    min: Math.min(...rawValues),
    max: Math.max(...rawValues),
  };
}

export function buildExplorerBenchmarkSummary(
  scenarioCount = 6,
  groupCount = 12,
  variableCount = 8,
  yearStart = 2020,
  yearEnd = 2100,
): ScenarioExplorerSummaryPayload {
  const years: number[] = [];
  for (let year = yearStart; year <= yearEnd; year += 5) {
    years.push(year);
  }
  const variables = Array.from({ length: variableCount }, (_, index) => `Variable ${index + 1}`);
  const regions = Array.from({ length: groupCount }, (_, index) => `Region ${index + 1}`);

  return {
    sector: "Electricity - generation",
    label: "Exajoules (EJ)",
    explanation: "Synthetic benchmark fixture for Scenario Explorer performance checks.",
    offset: 3,
    group_by: "region",
    regions,
    subscenarios: [],
    years,
    variables,
    scenarios: Array.from({ length: scenarioCount }, (_, scenarioIndex) => ({
      scenario_id: `scenario-${scenarioIndex + 1}`,
      model: scenarioIndex % 2 === 0 ? "remind" : "image",
      pathway: `Pathway ${scenarioIndex + 1}`,
      comparison_label: `Scenario ${scenarioIndex + 1}`,
      group_by: "region",
      regions,
      subscenarios: [],
      variables,
      years,
      groups: regions.map((region, regionIndex) => ({
        name: region,
        group_type: "region",
        region,
        variables,
        years,
        series: variables.map((variable, variableIndex) => ({
          variable,
          unit: "Exajoules (EJ)",
          points: years.map((year, yearIndex) => ({
            year,
            value:
              (scenarioIndex + 1) * 10 +
              regionIndex * 2 +
              variableIndex * 3 +
              yearIndex * 1.5,
          })),
        })),
      })),
    })),
  };
}
