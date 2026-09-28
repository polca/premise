# Four-scenario update, validation, reporting, and SimaPro profile

Run `profile_simapro_workflow.py` from an environment containing this checkout's
premise and the source Brightway project. Set `PREMISE_KEY` or `IAM_FILES_KEY`
through the environment (or an optional local `.env`). The script requires an
explicit key so IAM inputs are decrypted rather than using unencrypted fallbacks.

```sh
export PYTHONHASHSEED=0
python benchmarks/profile_simapro_workflow.py \
  --output-dir export/profile-simapro-four-focused
```

Defaults are ecoinvent 3.12 cutoff, its `ecoinvent-3.12-biosphere` database,
IMAGE SSP2-M at 2030, 2040 and 2050 plus SSP2-L at 2030, all sectors, the `legacy` inventory
backend, automatic reports enabled, import uncertainty retained, source
uncertainty removed, and caches enabled. These match the current constructor
behavior except for the explicitly selected source and scenarios. Override
`--project`, `--source-db`, `--biosphere`, or `--inventory-backend` as needed.
Use four `--scenario MODEL:PATHWAY:YEAR` arguments to change the scenario set.

The script calls `NewDatabase(...)`, `ndb.update()`, then
`ndb.write_db_to_simapro(...)`. Reports are generated through the normal automatic
export path, with output paths redirected into the run directory. It does not
request extra validation or a second report. Timing wrappers are restored on
exit. Failed operations are persisted, including automatic report failures that
the exporter normally catches. Successful completion requires four nonempty CSVs
and automatic change-report artifacts.

The default focused cProfile collection covers validation, change reports, and
scenario reports. Wall timers also cover construction, each sector update,
inventory storage, export preparation (including normalization and geographical
linking), report activity indexing, and CSV writing. Export spans also record
unmatched SimaPro flow-category counts. `--profile all` profiles
the whole workflow. Both profiler modes add overhead, particularly in Python
loops. To measure normal elapsed time, repeat in a fresh process and a new output
directory with `--profile none`. Do not interpret the focused run's percentage of
time in validation/reporting as an unprofiled percentage. Cache state, backend,
uncertainty settings, source database, and hardware must be kept comparable.

Outputs:

- `simapro/`: one CSV per scenario.
- `reports/`: the automatic scenario workbook and structured change workbook/audit.
- `configuration.json`: scenarios, settings, Python/platform, Git revision and
  working-tree status; no decryption key.
- `events.jsonl`: completed spans, flushed incrementally, including failures.
- `metrics.json` and `summary.md`: elapsed times and parent process peak RSS;
  optional `psutil` also samples aggregate parent/worker RSS every 100 ms.
- `workflow.pstats` and `hotspots.txt`: cumulative/self-time profiles when enabled.
- `workers/`: separate report-worker profiles when profiling is enabled. Worker
  times overlap and must not be added to parent wall time.

Inclusive parent spans contain child work. Category totals use exclusive times
so validation nested inside sector updates or report generation is counted only
once. `sector_update` therefore means transformation time excluding separately
instrumented validation. `peak_rss_bytes` covers the parent, including imports;
`peak_process_tree_rss_bytes` includes workers and can double-count shared mapped
pages. Neither is a per-phase memory measurement. The latter is `null` when
process inspection is unavailable. Times include wrapper/profiler
instrumentation overhead, and JSON logging overhead is charged to enclosing
spans. The wrappers target coarse validator entry points rather than every
per-exchange check; detailed callees are visible in cProfile.

Keep CSVs, reports and logs local under ignored `export/`: they contain source
inventory data. The script refuses to reuse an existing output directory.

## Scenario failures found during the initial runs

On 2026-09-10, with source revision `1521a6a5` and the existing local electricity
mapping edits retained, two attempted scenarios prevented a complete benchmark:

- IMAGE SSP2-L 2050 stopped in `CarValidation.run_checks` with 27 unsuppressed
  `LEGACY.CO2_EMISSIONS_INCORRECT` errors. One gasoline EURO-6 car in Brazil had
  0.209883 kg CO2/km against a fuel-based expectation of 0.171469 kg CO2/km.
  Timings are preserved in `export/profile-simapro-four-focused/`.
- IMAGE SSP2-M 2020 stopped in the heat update because there was no matching
  district-heating supplier for buildings in Brazil. Timings are preserved in
  `export/profile-simapro-ssp2m-focused/`.

The default benchmark scenarios were revised to avoid these failures. Validation
was not disabled or suppressed, and production code was unchanged for these
initial measurements. These
failed runs are not included in complete-workflow timing comparisons.

## Completed focused profile (2026-09-10)

The revised four-scenario run passed and produced four SimaPro CSVs (about
353 MiB each), a 38-sheet scenario workbook, and a nine-sheet change workbook
with a 3,925,498-row Parquet audit. Peak process RSS was 6.17 GiB. Raw results
are in `export/profile-simapro-four-valid-focused/`, with console output in
`export/profile-simapro-four-valid-focused.log`.

The full focused run took 1,193 seconds. Change reporting accounted for
665 seconds; separately instrumented validation accounted for 32.5 seconds.
These are profiled measurements, not normal-runtime percentages.

The change-report profile identifies these **overlapping cumulative** costs:

| Function / operation | Calls | Profiled seconds |
|---|---:|---:|
| Detailed audit (`_generate_details`) | 1 | 537.0 |
| Activity comparisons (`_activity_records`) | 205,270 | 285.5 |
| Canonical JSON (`_canonical_json`) | 22,452,385 | 235.4 |
| Activity indexes (`_activity_index`) | 5 | 159.1 |
| Exchange signatures (`_exchange_signature`) | 9,147,284 | 143.5 |
| Reopening scenario stores (`_ensure_scenario_store`) | 4 | 94.9 |
| Audit sink writes (`_ParquetSink.write`) | 205,270 | 68.7 |

Do not sum this table: normalization and signature generation occur within
indexing and activity comparison, which themselves occur within the audit.
The focused collection recorded about 2.90 billion function calls overall.
All eight certificate checks during export and reporting reused certificates;
full graph recertification was not the observed bottleneck.

The exporter reported 1,998 unmatched flow-category occurrences per CSV.
The files were successfully written, but mapping completeness was not certified
by these timing checks. See the run log and the exporter's `unlinked.log`
diagnostics before treating the files as fully mapped SimaPro inventories.

## Normal-runtime measurement (cProfile disabled)

The identical scenario/source/settings combination completed in **799.35 seconds
(13 minutes 19 seconds)**, with peak RSS **7.80 GiB**. It produced four nonempty
CSVs, readable workbooks, and another **3,925,498-row** audit. Audit row counts
match the focused run; complete row-by-row equivalence was not checked.
Raw measurements are in `export/profile-simapro-four-valid-wall/`, with console
output in `export/profile-simapro-four-valid-wall.log`.

| Exclusive category | Seconds | Share of workflow |
|---|---:|---:|
| Change report, excluding certificate checks | 308.28 | 38.6% |
| Sector transformations, excluding instrumented validation | 147.84 | 18.5% |
| Geographical linking and export normalization | 112.43 | 14.1% |
| Other exporter work (including inventory loading) | 79.75 | 10.0% |
| Update inventory loading/storage | 73.12 | 9.1% |
| SimaPro CSV writing | 32.06 | 4.0% |
| Other update orchestration | 15.89 | 2.0% |
| Instrumented validation (updates + export + report certificate checks) | 12.16 | 1.5% |
| Construction | 9.40 | 1.2% |
| Scenario summary workbook | 8.42 | 1.1% |

Exclusive categories avoid counting nested validation twice. The inclusive
`ndb.update()` call took **243.40 seconds**. The inclusive SimaPro exporter
call took **546.55 seconds**, including both automatic reports. These inclusive
totals must not be added to the category table.

The **308.28-second change report** breaks down as follows:

| Operation | Seconds |
|---|---:|
| Reopen four scenario stores | 64.13 |
| Prepare source store | 6.23 |
| Detailed comparison and Parquet audit | 229.96 |
| Excel workbook | 7.95 |

Activity indexing accounts for **60.21 seconds within** the detailed audit.
The separately instrumented export preparation comprises **57.05 seconds**
of geographical linking and **55.11 seconds** of normalization across the four
scenarios. Actual schema validation and the other instrumented validation
entry points total **12.16 seconds**; all eight certificate checks reuse the
existing certificates.

The focused profile inflated report time from 308 to 665 seconds (2.16×),
and validation time from 12.16 to 32.50 seconds (2.67×). Use the normal-runtime
column for prioritization and cProfile for identifying callees. This is one
completed run of each mode on one machine, with caches enabled; it is not a
multi-run statistical benchmark or an exhaustive certification of all IAM
scenarios. The second run adds coarse timers for indexing, normalization,
and geographical linking, plus counts of unmatched flow categories; production
behavior and validation/report settings are unchanged.

## Optimization priorities suggested by the measurements

1. Reduce repeated exchange normalization, canonical JSON serialization, and
   signature construction in `premise/change_report.py`. The focused profile
   records 22.45 million canonical JSON calls and 9.15 million exchange-signature
   calls. Investigate reuse of source exchange descriptors across scenarios
   and avoiding repeated normalization within indexing/comparison, while
   preserving special-value, uncertainty, duplicate-matching and audit semantics.
2. Reduce report checkpoint reopening/materialization. `_report_scenarios`
   spends 64 seconds reopening four stores in the normal run. Investigate
   lightweight checkpoint readers or safe reuse at the export/report boundary;
   avoid retaining additional full inventories without measuring peak memory.
3. Investigate geographical linking and export normalization separately if
   optimizing the whole workflow: together they cost 112 seconds. The script
   supports `--profile all` for a future detailed profile of those paths.
4. Preserve validation coverage. Its measured cost is small relative to
   reporting and preparation, and the initial scenario failures demonstrate
   that the checks still catch material inventory problems.

The change-report workbook itself takes only eight seconds; replacing Excel
writing alone would address a small fraction of the report delay. These were
the measurements before the subsequent reporting optimization described below.

## Optimized complete workflow (2026-09-11)

The final implementation completed the same four-scenario `NewDatabase` build,
`update()`, and SimaPro export in **537.06 seconds**. Automatic change reporting
took **27.69 seconds**, including checkpoint reads, source normalization, detailed
comparison, the complete Parquet audit, and the Excel workbook. Against the
799.35-second reference workflow, this is a 32.8% overall reduction; reporting
alone improves **11.13x**, from 308.28 seconds. Both runs have cProfile disabled.

| Operation | Reference seconds | Optimized seconds |
|---|---:|---:|
| Complete workflow | 799.35 | 537.06 |
| `ndb.update()` (inclusive) | 243.40 | 253.46 |
| Change report (inclusive) | 308.28 | 27.69 |
| Instrumented validation, whole workflow | 12.16 | 11.63 |
| Reopen four scenario stores | 64.13 | 3.25 |
| Acquire source view | 6.23 | <0.01 |
| Detailed comparison and Parquet audit | 229.96 | 21.06 |
| Change workbook | 7.95 | 3.27 |

The source view reuses the inventory already loaded for SimaPro export. Its
normalization runs inside the detailed comparison; it has not been moved outside
the reporting timer. Exporters are released after each CSV. Other improvements
include read-only checkpoint readers, up to 12 independent report workers,
reused source exchange signatures and provenance, and ordered audit merging.
Validation rules and automatic reporting remain enabled.

All four CSVs and both workbooks were produced. The existing 1,998 unmatched
flow-category occurrences per CSV remain. The final full run's parent peak was
4.49 GiB; sampled parent-plus-worker RSS was **7.89 GiB**, versus 7.80 GiB for the
serial reference's parent. Aggregate RSS can double-count shared mapped pages,
and these are different peak measurement methods. Parallel reporting uses more
CPU cores; the 30-second result is specific to this macOS ARM64/Python 3.11
workload and machine, not a guarantee for smaller machines or other inventories.

Raw final results are in `export/profile-simapro-optimized-4/`; its log is
`export/profile-simapro-optimized-4.log`. Earlier intermediate implementations
measured 41.01, 30.71, and 31.11 seconds in complete workflows. These are successive
code versions, not repetitions of the final implementation. Fixed-input output
equivalence and its measurements are described in `change-report.md`. Later
runs of that separate checkpoint harness took 42.62 and 47.00 seconds including
loading, so the 27.69-second result does not establish a consistent upper bound.
