# Change-report performance and equivalence

`profile_change_report.py` captures one IMAGE SSP2-M 2050 all-sector build from
Brightway project/database `ecoinvent-3.12-cutoff` (`ecoinvent-3.12-biosphere`, compact backend,
default uncertainty settings). It does not write a Brightway scenario database.
The fixture includes inventory checkpoints and the exact validation/provenance
inputs, allowing old and new reporting code to process identical data.

Run in the environment containing premise and Brightway, from the repository root:

```sh
export PYTHONHASHSEED=0
# Supply PREMISE_KEY or IAM_FILES_KEY through the environment or the local .env.
python benchmarks/profile_change_report.py --capture --fixture export/report-inputs
python benchmarks/profile_change_report.py --fixture export/report-inputs \
  --output export/report-new --profile
```

Before editing the reporting code, preserve `premise/change_report.py` outside
the package. Use that file as the baseline implementation:

```sh
python benchmarks/profile_change_report.py --fixture export/report-inputs \
  --baseline-module /tmp/change_report_before.py \
  --output export/report-old --profile
python benchmarks/compare_change_reports.py export/report-old export/report-new
```

For ordinary runtime, omit `--profile`. Run baseline and candidate sequentially,
in fresh processes, alternating versions for three repetitions. Compare medians;
do not compare a profiled baseline to an unprofiled candidate. Each output
contains elapsed time, process peak RSS before the cached repeat, artifact paths,
and cached-repeat time in `metrics.json`. Profiled runs additionally save pstats
and cumulative/self-time tables. Inventory loading occurs before the timer;
process peak RSS includes loading. `input_seconds` measures loading separately,
and `total_seconds` includes loading plus the first complete report. With
`--read-only-checkpoints`, scenario checkpoints are read concurrently, as in
`NewDatabase`; baseline runs retain the ordinary checkpoint reader. The captured
checkpoint data is identical for both implementations.

The benchmark defaults to `PYTHONHASHSEED=0` in a fresh interpreter. This also
makes the reference implementation's floating-point sums over sets reproducible.
Use `--scenarios-file` with a JSON array of scenario dictionaries and
`--inventory-backend legacy` when capturing the four-scenario export workload.
`profile_simapro_workflow.py` measures the complete build and SimaPro workflow.

Worker profiles are saved separately under `workers/` when `--profile` is used.
Their cumulative times overlap; do not add them to the parent's wall time.
With optional `psutil`, `peak_process_tree_rss_bytes` samples aggregate parent
and child RSS every 100 ms (shared mapped pages can be counted more than once).
It is `null` when process inspection is unavailable. The original
`peak_rss_bytes` remains the parent process's peak and must not be presented as
the complete memory cost of parallel reporting.

The comparator streams the Parquet audit in batches and checks its schema,
row count, and ordered content, excluding only the generated report ID. It also
checks workbook sheets, dimensions, tables, freeze panes, cell styles, and values;
only the Overview report ID, generation timestamp, and audit filename may differ.

Keep captured inventories and generated audits local in ignored directories:
they contain proprietary data. Fixture metadata is a trusted-local pickle, so
only load fixtures created by this benchmark. No key is stored in the fixture.

The committed synthetic `tests/fixtures/change_report_v2.json` is the original
V2 report output from `_performance_regression_stores` in the report tests,
excluding its generated report ID. It contains no ecoinvent data and checks
compact, legacy, and reopened checkpoint implementations against the same rows.

## Verified full-inventory results (2026-09-09)

Baseline: report implementation from commit
`5138512834689d499665475f218f263501293b58`. Both implementations read the
same captured inventories on macOS with Python 3.11. The three final unprofiled
runs used the side-effect-free activity reader and bounded comparison caches.

| Run | Baseline seconds | Optimized seconds | Speedup | Peak RSS change |
|---|---:|---:|---:|---:|
| 1 | 319.55 | 93.30 | 3.42x | +6.0% |
| 2 | 315.96 | 96.59 | 3.27x | +3.5% |
| 3 | 316.97 | 95.07 | 3.33x | +4.7% |
| Median | 316.97 | 95.07 | 3.33x | +4.7% |

All 981,910 ordered audit rows, fingerprints, and workbook contents match the
baseline, apart from the explicitly excluded run-specific identifiers described
above. Each final run remains below the 10% peak-RSS increase limit. Median peak
RSS is 1,165,803,520 bytes before and 1,220,444,160 bytes after. Cached report
refresh remains approximately 2.2 seconds without profiling.

The report/inventory/validation/export regression selection passes 157 tests.
It covers exact synthetic output across backends, special-value normalization,
duplicate matching, cache reuse/invalidation, unchanged activities, and preservation
of both exchange overrides and lazy activity metadata during reporting.

The implementation avoids frozen public snapshots, decodes effective columnar
fields directly, and reuses pair-local exchange signatures. Signatures are released
after sorting or exact cancellation. Only conservatively eligible unchanged
activity hashes take the shortcut; special values retain the full comparison.
The report schema, validation diagnostics, and default automatic reporting behavior
remain unchanged.

Local raw measurements and audits are in `export/change-report-benchmark/`.

The final cProfile comparison is **948.88 → 216.59 seconds (4.38x)**, with total
calls reduced from 4.43 billion to 752 million. The profiled output also passes
the complete equivalence check. Remaining cumulative costs include activity
indexing (82.4 seconds), detailed activity comparisons (95.0 seconds), and audit
row preparation/writing (25.6 seconds). Nested cumulative timings overlap.

Memory qualification: the final **profiled** process peaked at 1,370,980,352 bytes
versus 1,142,931,456 bytes for its baseline (+20.0%). Thus the 10% RSS target
passes in all three unprofiled runs, but not in the cProfile run. The cause of
that difference was not isolated; do not infer profiled memory use from the
normal-run median. The implementation retains indexes and summaries plus
pair-local comparison data, not normalized copies of entire inventories.

## Four-scenario reporting implementation (2026-09-10)

The four-scenario workload uses IMAGE SSP2-M in 2030, 2040, and 2050, plus
IMAGE SSP2-L in 2030, with the legacy backend. Its fixed-input reference is
`export/report-optimization/baseline-seed0`: 298.38 seconds including loading,
with 3,925,498 audit rows. The corresponding complete update/SimaPro reference
is `export/profile-simapro-four-valid-wall`: change reporting takes 308.28
seconds and validation takes 12.16 seconds across the complete workflow.

Large reports use at most 12 independent Python workers. Each receives ordered
activity ranges and only the attribution entries relevant to those activities.
Ranges stay with the same worker across scenarios so prepared source exchanges
can be reused. Workers use private report snapshots and read-only mapped columns;
they never rerun the calling script or fork the user's interpreter. Smaller
reports use the serial path.

When the original source must be reconstructed, its existing normalization
routines run per activity inside the reporting workers. This work is included
in the first report's elapsed time. Ordinary access to that source view still
resolves the fully normalized inventory. Validation continues to run through
the existing certification path; no rules or findings are suppressed.

SimaPro reporting reuses the original inventory already loaded for export and
releases each exporter after its CSV is written. Source normalization still
occurs during report generation. Provenance events are parsed once for both
attribution and workbook summaries; reporting keeps private shallow views of
nested values that it only reads, avoiding repeated deep copies.

The parent preserves activity, scenario, audit-row, and summary ordering. It
reconstructs complete fingerprints before publishing the audit and merges
Parquet fragments while later scenarios are compared. Worker failures leave
existing reports intact and clean up temporary files. Full additions/removals
are assembled by column, and finite scalar JSON values use the native encoder;
special values retain the exact normalization behavior covered by the golden
fixture and randomized differential tests.

Fixed-input comparisons cover every ordered audit row, fingerprint, workbook
cell, sheet, and style. Baseline and candidate must use the same hash seed;
normal application workers also preserve the caller's floating-point sum order
for market summaries. The current full-inventory measurements and comparisons
are kept in `export/report-optimization/`; use the complete SimaPro benchmark
to qualify source reconstruction as well as checkpoint loading.

## Final four-scenario verification (2026-09-11)

The complete `NewDatabase.update()` and SimaPro workflow meets the 30-second
report target in the final measured run: **308.28 → 27.69 seconds (11.13x)**.
That timer includes reopening the four scenario checkpoints, normalizing the
source, comparing inventories, writing all audit rows, and saving the workbook.
The whole workflow takes **537.06 seconds**, versus 799.35 seconds initially.
See `simapro-workflow.md` for the full breakdown and memory qualification.

The final code also passes a separate comparison on identical captured inputs:
`comparison-final-3.json` reports equality for **all 3,925,498 ordered audit rows**,
the Parquet schema and fingerprints, and workbook values, sheets, tables, freeze
panes, and styles. Only the generated identifiers documented above are excluded.
This comparison uses `baseline-seed0` and `final-3` in
`export/report-optimization/`.

Later fresh-process runs using these saved checkpoints measured the following
with cProfile disabled. They are a separate harness from automatic SimaPro
reporting and must not be represented as repetitions of its 27.69-second result.

| Fixed-input run | Loading seconds | Report seconds | Total seconds | Aggregate peak RSS |
|---|---:|---:|---:|---:|
| Reference (`baseline-seed0`) | 70.34 | 228.04 | 298.38 | 7.81 GiB (serial parent) |
| Final code (`final-3`) | 5.99 | 36.63 | 42.62 | 9.14 GiB |
| Final code repeat (`final-4`) | 3.95 | 43.04 | 47.00 | 7.53 GiB |

Aggregate worker RSS is sampled and can double-count mapped pages; the serial
reference uses the parent's recorded peak. The slower fixed-input timings were
not isolated to a specific cause. The complete workflow reached the requested
30-second report time, but these results do not establish a consistent
30-second upper bound across runs and input-loading paths. Raw metrics for both
later runs are retained alongside the exact-output comparison.

The report, checkpoint, inventory, validation, export, and public-import regression
selection passes **183 tests**. Cases include special-value serialization,
parallel versus serial output, identical-score summary ordering, deferred source
normalization, provenance and fallback attribution, immutable checkpoint reads,
worker failure cleanup, and release of exported inventories before reporting.
Formatting and `git diff --check` also pass.
