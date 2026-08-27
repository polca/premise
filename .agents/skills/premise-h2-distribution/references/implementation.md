# H2 Distribution Implementation Guide

Read this reference for code, YAML, inventory, reporting, documentation, test,
debugging, or review work on the extension.

## Establish the Boundary

Inspect the current branch, commit, dirty state, remotes, and local merge base.
Remote names and URLs are runtime facts, not skill constants. Compare with an
available local upstream ref when the distinction matters; do not fetch without
a reason within the user's request.

The fork can contain upstream changes unrelated to H2 distribution. A path in
the fork delta is not proof that it belongs to this feature. Trace the H2 call
path and tests before assigning ownership.

Use this evidence order when sources disagree:

1. Current executable source and tests.
2. Runtime YAML and packaged inventories.
3. Version-matched H2 sections in `docs/transform.rst`, `docs/extract.rst`, and
   `docs/fuel_market_system_models.rst`.
4. Examples, changelog, and committed analysis outputs.

Report documentation/source inconsistencies instead of silently choosing one.

## End-to-End Flow

```text
IAMDataCollection.production_volumes
  -> model-specific direct-H2 hierarchy
  -> regional sector/subsector final energy
  -> mass demand and demand-node estimates
  -> distribution-rule selection
  -> regional production/support activities
  -> demand-weighted sector-market logistics
  -> configured consumer relinking
  -> scenario diagnostics, validation, and fuel change report
```

`premise/fuels/base.py::_update_fuels` is the orchestration boundary. Its order
is intentional: calculate logistics, write its audit rows, generate hydrogen
activities, relink eligible consumers, write relinking rows, continue the fuels
transformation, carry state into the scenario, and validate.

## Change Map

| Concern | Start here | Also inspect |
| --- | --- | --- |
| Direct-H2 IAM hierarchy | `premise/fuels/hydrogen.py` (`HYDROGEN_FINAL_ENERGY_RULES`) | normalized variables in `premise/iam_variables_mapping/`; H2 methodology docs |
| Demand nodes | `_get_hydrogen_final_energy_by_subsector()`, `set_hydrogen_logistics()` | plant/station helpers, `_empty_hydrogen_demand_nodes()`, plotting script |
| Distribution rules | `premise/fuels/h2_decision_tree/hydrogen_distribution_shares.yaml` | rule matcher, mode/activity/conversion constants, threshold tests |
| Consumer routing | `hydrogen_consumer_routing.yaml` | relinker helpers, market-availability helpers, routing tests |
| Sector-market exchanges | transport-share and supplier helpers in `hydrogen.py` | additional inventories, geography fallbacks, conversion constants |
| Fuel orchestration/state | `premise/fuels/base.py` | `premise/new_database.py`, `FuelsValidation`, fail-fast tests |
| Audit/change reports | `HYDROGEN_LOG_COLUMNS`, H2 log writers | `premise/data/utils/logging/reporting.yaml`, `premise/report.py` |
| Inventories | H2 distribution/transport workbooks in `premise/data/additional_inventories/` | constants and source-version hooks in `new_database.py`, inventory tests, `docs/extract.rst` |
| Public behavior | H2 sections in `docs/transform.rst` and `docs/extract.rst` | `CHANGELOG.md`, examples, tests |

## Data and Scientific Contracts

Use `_empty_hydrogen_demand_nodes()` as the canonical output-column source.
`set_hydrogen_logistics()` targets `self.year`, interpolates within IAM years or
clamps to the nearest boundary, validates regional demand against the temporary
`World` aggregate, then removes rows recognized by
`_is_world_hydrogen_region()` before storing the table.

Final energy comes from normalized `production_volumes`; do not apply another
PJ-to-EJ conversion. Convert EJ/year to tonnes H2/year with the existing
`H2_LHV_GJ_PER_TONNE`. Model-specific candidate groups are ordered. Use the
first represented group and reserve all candidates, including unused fallback
detail, so aggregate/detail coordinates cannot both enter the result or leak
into `Other`. Preserve explicit residual subtraction and model exclusions.

When node assumptions change, identify whether the estimate is production-
volume-, final-energy-, or transport-service-based. Test zero/missing values,
units, capacity/refueling assumptions, fractional counts, rounding, and
per-node denominators. Do not replace a missing production proxy with an
unrequested scientific assumption.

## Decision Trees and Markets

YAML files are runtime configuration. Keep keys synchronized with the Python
constants and reporting surface.

- Rule matches require exact row values. The selected basis must exist and be
  numeric for bounded conditions.
- Generic nonnegative-demand rules should cover the domain without gaps or
  ambiguous overlaps. More specific rules may override them through priority.
- Every share key must resolve through `HYDROGEN_TRANSPORT_ACTIVITIES`.
  Non-pipeline modes need a distance; conversion chains must resolve through
  both the conversion map and activity definitions.
- Validate weighted shares at the sector-market level, not only individual
  demand rows. Preserve an explicitly documented onsite remainder.
- Steel and cement market availability also requires valid plant-based
  logistics. Other current sectors use positive final-energy availability.
- Supplier selection prefers the IAM region, mapped ecoinvent locations,
  `RoW`, then `GLO`; preserve or deliberately test any change to that order.
- Truck/ship exchange amounts use tonne-kilometres per kilogram of market
  output. Pipeline amounts and conversion/reconversion activities follow their
  supplier reference units and existing stoichiometric factors.

Consumer routing first preserves configured general-market cases, then uses
sector name/ISIC rules. Do not silently relink ambiguous, unmatched, logistics,
or unavailable-market consumers. Ensure generated markets cannot relink to
themselves or create duplicate/circular suppliers.

## Reporting and Inventory Wiring

When a diagnostic field or distribution mode changes, inspect all four layers:
the demand/relink record, `HYDROGEN_LOG_COLUMNS`, the log writer, and
`reporting.yaml`. A test that only matches one log string does not prove the
change-report schema remains complete.

The market builder resolves imported activities from the in-memory database; it
does not read inventory workbooks directly. For inventory changes, verify the
workbook exists in package data, the `new_database.py` hook declares the correct
source ecoinvent version/order, activity name/reference product/unit match the
lookup constants, migration succeeds, and both supported system models remain
valid where applicable. Do not infer workbook contents from filenames or docs.

## Test Matrix

Start with:

```powershell
python -m pytest tests/test_hydrogen.py tests/test_fuels.py -q
```

Add the narrow checks implied by the change:

- IAM hierarchy: affected model, preferred and fallback groups, residuals,
  absent coordinates, year interpolation/bounds, and exclusions.
- Demand nodes: unit conversions, zero/non-finite inputs, World validation,
  plant/station assumptions, and canonical columns.
- YAML rules: exact boundaries, priority overrides, domain coverage, mode
  wiring, share totals/remainders, and malformed configuration.
- Markets: positive/zero demand, plant-prerequisite absence, geography
  fallback, weighted shares, conversions, units, and duplicate prevention.
- Relinking: name/ISIC matches, conflict handling, unavailable markets,
  general-market exceptions, and generated/logistics dataset exclusions.
- Orchestration/reporting: failure propagation at each mandatory stage,
  scenario state/diagnostics, log order, and reporting-schema coverage.
- Inventory wiring: `tests/test_new_database.py`, inventory import tests, and
  prepared process tests for relevant ecoinvent/system-model combinations.
- Shared mechanics: the closest transformation, validation, report, export, or
  Brightway tests when those modules genuinely changed.

Run the broader non-slow suite only when proportionate:

```powershell
python -m pytest -m "not slow" --instafail
```

For scientific changes, assert meaningful quantities: demand conservation,
nonnegative finite amounts, threshold behavior, share/remainder accounting,
regional coverage, unique suppliers, correct relinking, and validation/report
observability—not merely successful execution.
