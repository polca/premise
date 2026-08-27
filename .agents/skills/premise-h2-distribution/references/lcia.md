# H2 Distribution LCIA Guide

Read this reference for `examples/h2-distribution_LCIA/`, Brightway market
comparison, contribution analysis, plots, or committed result tables.

## Preconditions and Scope

Use the user's prepared Brightway environment and exact project/database names.
`run_analysis()` changes the active Brightway project, installs/checks the
Premise GWP method, and reads the selected database. Do not guess names, create
a full prospective database, or run licensed builds solely to exercise the
example.

Keep reproducibility metadata with conclusions: repository commit, Premise and
ecoinvent versions, system model, IAM model/pathway/year, update sectors,
Brightway project/database, method packages, and analysis configuration.

Use a separate `output_dir` for exploratory runs. Overwrite committed result
tables or plots only when the user is intentionally refreshing baselines.

## Selection Is Part of the Result

`config.py` defines the generic market, reference product, unit, target
location, sector-market names, location fallbacks, methods, output names,
functional unit, and reconciliation tolerance. Treat these as one configuration
contract with `mapping.py`, `run_analysis.py`, and the notebook.

`select_markets()` requires exactly one generic low-pressure market at the
configured target location. Sector markets may use configured location
fallbacks. Inspect the exported selection audit before comparing scores. A
missing sector market can be a valid scenario outcome when its sector-region
demand or plant prerequisite is absent; distinguish that from a selection or
generation defect.

Do not compare activities with different products, units, functional units, or
geographic meaning under the same label. The default functional unit is one
kilogram H2.

## Methods and Interpretation

Use the exact configured Premise GWP, CED, and selected EF 3.1 methods. Report
missing or ambiguous methods rather than silently substituting similarly named
ones. Preserve signed contributions: credits and avoided burdens can be
negative and should not be clipped merely for plotting.

Separate conclusions about:

- the regional hydrogen production mix;
- transport mode and distance;
- compression, liquefaction, regasification, ammonia production/cracking;
- direct leakage/emissions;
- background supply-chain effects; and
- differences caused by market location fallback.

The example is scenario analysis, not a forecast. Avoid attributing differences
to distribution alone until the production mix and selection audit support that
interpretation.

## Reconciliation and Failure Conditions

Treat the programmatic checks as required evidence:

- each analyzed activity has one reference production exchange;
- every direct market emission is classified;
- hotspot contributions reconcile to the LCIA score;
- Layer 1 reconstructs the total score;
- Layer 2 production inputs plus distribution processes reconstruct Layer 1;
- regional steel comparisons use the same method and functional unit.

Use `RECONCILIATION_RTOL` from `config.py`; do not loosen it merely to make an
unexpected mismatch pass. Diagnose activity traversal, reference output
normalization, classification, or numerical precision first.

## Verification

For pure selection/classification changes, add synthetic tests around the
functions in `mapping.py` or `run_analysis.py` where practical. For a live run:

1. Run with `export=False` first when only diagnostics are needed.
2. Inspect selection and method tables before score comparisons.
3. Inspect hotspot and stage reconciliation tables before plots.
4. Confirm every written output path is unique and belongs to the intended
   output directory.
5. Compare refreshed baselines by meaningful totals and classifications, not
   only file or image differences.

Record which checks required the live Brightway database and which were covered
with synthetic fixtures. Do not imply live integration coverage when only
committed CSVs were inspected.
