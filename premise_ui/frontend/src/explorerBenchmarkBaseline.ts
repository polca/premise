export const EXPLORER_BENCHMARK_BASELINE = {
  options: {
    scenarios: 8,
    groups: 16,
    variables: 10,
    year_start: 2020,
    year_end: 2100,
    iterations: 10,
  },
  expected: {
    selected_groups: 8,
    selected_variables: 5,
    selected_series: 320,
    filtered_series: 320,
    normalized_series: 320,
    grouped_panels: 8,
    bounds: {
      min: 2035,
      max: 2085,
    },
  },
  max_avg_ms: {
    series_selection_avg: 10,
    year_filter_avg: 10,
    normalize_share_avg: 15,
    scenario_grouping_avg: 5,
    line_domain_avg: 10,
    stacked_domain_avg: 15,
    year_bounds_avg: 5,
  },
} as const;
