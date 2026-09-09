# Change-report performance and equivalence

`profile_change_report.py` captures one IMAGE SSP2-M 2050 all-sector build from
Brightway project/database `ecoinvent-3.12-cutoff` (`biosphere`, compact backend,
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
process peak RSS includes loading. The captured checkpoint data is identical for
both implementations.

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
