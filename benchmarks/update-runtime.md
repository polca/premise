# All-sector update runtime

The target is a 50% reduction in time per scenario-year for a complete
`NewDatabase.update()`, with equivalent outputs and no transfer of work to
construction, export, or reporting.

## Measurement

`profile_update_workflow.py` uses the legacy backend and these four scenarios:

- IMAGE SSP2-M, 2030
- IMAGE SSP2-M, 2040
- IMAGE SSP2-M, 2050
- IMAGE SSP2-L, 2030

The source is `ecoinvent-3.12-cutoff`, in the Brightway project with the same
name, with `ecoinvent-3.12-biosphere`. Imported uncertainty is retained; source
database uncertainty is not. All default sectors run, persistence and reports
remain enabled, and the Python hash seed is fixed at zero.

Construction, the complete update call, and SimaPro export with both reports
are timed separately. Update timing includes scenario loading, sector checks,
final certification, checkpoint writing, and cleanup. The timing recorder does
not force garbage collection between recorded stages. Optional inventory
capture happens after all timed work and memory measurements.

The baseline implementation is commit `6f96dde2`. Measurements use preserved
copies of the Python sources with the same data paths and existing source/IAM
caches. Each preserved implementation has a SHA-256 source manifest. No
decryption key is stored in benchmark configuration or results.

Unprofiled results, in seconds:

| Measurement | Baseline | Update-only repeat | Full repeat | Final implementation |
| --- | ---: | ---: | ---: | ---: |
| Four complete updates | 258.791 | 125.259 | 128.050 | 134.822 |
| Mean per scenario-year | 64.698 | 31.315 | 32.013 | 33.705 |
| Update time reduction | — | 51.6% | 50.5% | 47.9% |
| Construction | 10.254 | 9.476 | 9.516 | 9.220 |
| SimaPro export, including both reports | 276.980 | — | 266.888 | 270.416 |
| Scenario report, included above | 9.288 | — | 9.568 | 9.405 |
| Change report, included above | 27.815 | — | 29.005 | 29.766 |

The measurements show approximately half the update time for this four-scenario
workload, with run-to-run variation. The final implementation measured a 47.9%
reduction; a consistent minimum reduction of 50% is not established. The two
earlier repeats preceded a transaction-snapshot compatibility fix for legacy
stores, which has separate mutation-isolation coverage. These measurements do
not establish the same speedup for every IAM, scenario year, or inventory backend.

Construction plus export/reporting decreased from 287.234 to 279.636 seconds in
the final run. Report timings alone varied upward, while their calculation code
remained unchanged. Lookup indexes skipped during update are discarded by
checkpoint serialization; reopened stores already rebuilt these indexes in the
baseline. Source reuse ends inside `update()`, including when an update raises.
The optimization therefore removes repeated work rather than deferring it.

Peak process-tree RSS for the full workflow was 7.72 GB in the baseline and
7.13 GB in the final run; root-process peaks were both approximately 4.20 GB.

Raw measurements and captures are under the ignored directory
`export/update-optimization/`: `baseline-wall-1`, `candidate-wall-5`,
`candidate-full-5`, and `candidate-full-6`, respectively. Each directory contains
`metrics.json`; full runs include SimaPro files, both workbooks, the detailed
audit, and captured inventories. The final source manifest matches all 77
captured Python files in the working tree.

## Changes

- Materialize a scenario directly from the pristine source, avoiding a fork
  followed by another complete copy. Retain one private source during update
  and release it before update returns or raises.
- Copy mutable inventory containers directly, preserving aliases, cycles,
  NumPy metadata, and custom copy protocols. A module-level recursive helper
  releases its memo immediately instead of retaining it in a closure cycle.
- Ingest exchange IDs in one pass and deduplicate reverse consumer links using
  their traversal order, avoiding repeated scans or copies of growing lists.
- Prepare emissions from store metadata, copy activity metadata and potentially
  modified pollutant exchanges, and reuse unchanged exchange payloads. The
  same pollutant scaling routine produces the replacement legacy store. Legacy
  transaction snapshots support dictionary exchange storage, and tests verify
  that later mutations remain isolated between the original and replacement.
- Avoid lookup indexes on stores immediately written to checkpoints. Those
  indexes are not serialized, and reopened stores construct their own indexes
  as before. A diagnostic query still constructs any index it needs.
- Keep cache field-presence semantics while avoiding repeated type-set
  construction and NumPy calls for ordinary Python floats.

## Equivalence checks

`compare_update_workflows.py` checks every captured activity and exchange,
including numeric values, uncertainty, metadata, activity order, and exchange
order. Random storage identifiers are excluded from the inventory digest;
semantic supplier identities remain included. It also compares validation
rules, checked-object counts, issues, coverage, certificates, provenance, heat
diagnostics, and validation intents. Only build IDs, generated activity codes
in provenance, and validation elapsed times are normalized.

For complete workflows it additionally compares:

- Every SimaPro CSV byte except generated IDs, whose one-to-one mapping must
  remain consistent across activity definitions and exchange references.
- Every detailed audit row except report/build IDs and a consistent one-to-one
  mapping of activity codes. Each run ID must be consistent throughout its own
  report; all semantic activity identities remain compared.
- Both workbooks' cells, sheet order, dimensions, styles, tables, and freeze
  panes, except the change report's run ID, generation timestamp, and audit
  filename in its Overview sheet.

Both full runs passed all checks: four inventories and their validation and
provenance metadata, four SimaPro CSVs, both workbooks, and all 3,924,295 detailed
audit rows. Final-run evidence is in
`export/update-optimization/candidate-full-6/equivalence.json` and
`equivalence.log`. Only the generated identifiers and timing fields described
above differ between runs; numeric results and validation certificates match.

The final implementation passed 255 focused tests covering stores, copying,
emissions, scenario orchestration, validation, reports, exports, cache behavior,
and the comparison harness. The JUnit result is
`export/update-optimization/final-tests.xml`.

## Running

Set `PREMISE_KEY` or `IAM_FILES_KEY` in the environment, then run each
implementation in a fresh process with its own output directory:

```sh
python benchmarks/profile_update_workflow.py \
  --capture --export --output-dir export/update-baseline
python benchmarks/profile_update_workflow.py \
  --capture --export --output-dir export/update-candidate
python benchmarks/compare_update_workflows.py \
  export/update-baseline export/update-candidate
```

Use `--profile` to collect `update.pstats` and `hotspots.txt` for diagnosis.
Profiler-instrumented times must not be used to calculate the claimed runtime
reduction. `--scenario MODEL:PATHWAY:YEAR` can be repeated for smaller diagnostic
runs; these do not replace the complete four-scenario verification.

Run the focused regression suite with:

```sh
python -m pytest tests/test_inventory_store.py tests/test_utils.py \
  tests/test_emissions.py tests/test_transformation.py tests/test_inventory_copy.py \
  tests/test_new_database.py tests/test_output_equivalence.py \
  tests/test_update_workflow_comparison.py tests/test_validation_framework.py \
  tests/test_change_report.py tests/test_change_report_optimization.py \
  tests/test_export.py tests/test_runtime_cache.py -q
```
