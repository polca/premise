# US-WECC CCGT pilot: observations, reconstruction and inventory boundary

Updated 9 October 2026. This bounded pilot is opt-in and is not a proposed
global default. It covers non-CHP natural-gas combined-cycle power plants,
with a 2022 observed reference and annual service profiles for 2022–2030.
The code supports reconstruction through 2050, but the longer horizon is not
validated by the current evidence. The other four pilots remain separate work.

## Observed boundary and service weights

Use EIA-860 final 2022 generator and plant records and EIA-923 final 2022 net
generation, checked against `acquisition-manifest.csv`. Join plant identifiers
to the `WECC` NERC region. Select operating (`OP`), natural-gas-primary,
non-CHP combined-cycle generators. A block is identified by plant plus nonempty
unit code. It must contain CT+CA components, or the integrated CS category,
with a shared initial commercial-operation year. This provides a whole-block
commissioning proxy compatible with the combined gas/steam plant inventory.

Check every member against **all three** generator sheets: Operable, Retired
and Canceled, and Proposed. Reject a candidate with extra, duplicate, retired,
nonoperating, incompatible or differently dated members. Also reject blocks
commissioned in the reference year, whose annual output would be partial-year
service. The current selection contains 91 complete blocks, none first operated
after 2020. Their capacity is 41,476.3 MW AC and their reported net generation is
159,002,044.49 MWh. These are a selected subset, not all WECC CCGT.

Generator-output joins retain zero component output and sum it at block level.
Numeric zero-padding aliases are permitted only after proving a one-to-one
identity within the plant. A zero-output steam component does not independently
remove the capital of a combined-cycle block. This is a block aggregation of
reported generator values, not a claim that EIA officially requires whole-block
reporting under one generator identifier.

Aggregate block capacity `K(c,2022)` and output `G(c,2022)` by commissioning
year `c`. Initial service weights are `G(c,2022) / sum(G)`. The observed mean
age is 16.2767 years with output weighting and 16.6549 years with capacity
weighting. Initial survivors are used directly; they are never multiplied by
unconditional survival again. EIA initial operation does not date later
refurbishment, individual components or embodied construction work beforehand.
Calendar-year output weights end-year operating stock; within-year availability
is not reconstructed.

The audit reports excluded capacity: 2,214 MW with missing unit code, 6,072.2 MW
with mixed component commissioning years, and 337.3 MW with incomplete
combined-cycle configuration. The full-sheet membership and first-year checks
reject no additional members of the selected 91 blocks. Exclusions are a
boundary choice, not missing mass silently renormalised over the entire region.

## Exact IAM inputs and their limits

Read the local `remind 3.5.2/REMIND_generic_SSP2-NPi2025.mif` and
`REMIND_generic_SSP2-PkBudg650.mif` files using their recorded SHA-256 values in
`iam-file-inventory.csv`. Validate Model, Scenario, Region, exact variable names,
units, uniqueness and finite values. The selected region is USA:

| Purpose | Exact leaf variable | Unit |
|---|---|---|
| Operating capacity trajectory | `Cap|Electricity|Gas|CC|+|w/o CC` | GW |
| Reported additions diagnostic | `New Cap|Electricity|Gas|CC|+|w/o CC` | GW/yr |
| Electricity service trajectory | `SE|Electricity|Gas|++|Combined Cycle w/o CC` | EJ/yr |
| Capacity depreciation input | `Tech|Electricity|Gas|Combined Cycle w/o CC|Lifetime` | years |

The selected lifetime is 35 years throughout the tested horizon. An aggregate
gas capacity-factor series is not substituted for the CCGT leaf. These exports
do not supply native vintage composition. The USA trajectory is a geographic
proxy for relative change of the observed WECC subset, not a claim that the
IAM models that subset or reproduces its absolute capacity.

Interpolate capacity and output levels linearly to annual years. Scale each
trajectory to the corresponding observed 2022 quantity. The capacity scale has
units MW per reported GW, and combines the unit conversion and subfleet ratio;
the output scale similarly has units MWh per reported EJ. Do not apply a second
GW-to-MW conversion after this scale.

Expand reported five-year additions **rates** over their centred reporting
periods: 2025 covers 2023–2027, 2030 covers 2028–2032. Report both each complete
period's integral and the years falling within the requested horizon. Do not
linearly interpolate rates and silently change their period totals. This
convention follows the audited `remind2` reporting transformation; the generator
does not support the later switch to ten-year reporting periods. See
[the IAM assessment](iam-scenario-assessment.md) for pinned source code and
the distinction between native model and reported time periods.

## Capacity evolution and service attribution

The primary law is an explicitly labelled continuous analogue of the REMIND
quartic remaining-capacity curve:

`S(a) = max(0, 1 - (a / (1.25 L))**4)`, with `L = 35 years`.

Evolve an observed cohort using `S(a+1)/S(a)`. The continuous curve's integral
is `L`; annual sampling and the native model's age/period indexing are distinct.
This implementation is **not** an exact translation of REMIND's discrete
`pm_omeg`, period accumulation and early-retirement equations.

Two observed cohorts, 1975 and 1977, already exceed the curve's 43.75-year finite
support. They contain 410.5 MW, about 0.99% of selected capacity. Preserve these
observed survivors, with an explicitly assumed exponential remaining life of
five years. This extension is not an estimated physical lifetime distribution.
Test remaining means of three and ten years. Cohorts inside the quartic support
follow that curve to zero; the extension only accommodates observed older stock.

In the primary capacity-constrained case, calculate natural survivors, then:

- Add new age-zero capacity if the target exceeds those survivors.
- Remove capacity proportionally across survivors if the target is lower.
- Record opening capacity, natural decline, additional operating-service exits,
  additions, closing capacity and a checked stock-balance residual each year.

Those additional exits mean removal from operating service, not physical
disposal. `retirement_record` rejects their use as evidence for disposal dates.
The original source does not resolve mothballing, physical decommissioning,
selective dispatch or early retirement at the individual-block level.

Carry the observed cohort-specific output/capacity ratio forward. Give new
cohorts the initial fleet-average ratio. Scale all annual cohort outputs by one
common factor to reconcile to the output target; it cancels when forming vintage
weights. Fail if any cohort's implied full-load hours exceed 8,760. An equal
output-per-capacity case tests the persistence of initial utilisation differences.
Future service describes the end-year operating portfolio; commissioning/exit
exposure and dispatch within the year are not reconstructed.

The stock trajectory, reported additions, observed initial composition and an
assumed survival law need not be mutually consistent. Report the discrepancy
between inferred and scaled reported additions. An alternative case uses the
reported additions directly and reports its discrepancy from the stock target.
Do not fit away either residual or claim simultaneous agreement.

The generated cases are primary, equal-capacity service weighting, short and
long observed old-stock tails, 25- and 40-year depreciation sensitivities, and
reported-additions-constrained evolution. The 25/40-year values are perturbation
tests, not newly validated empirical lifetime defaults.

## Existing inventory quantity and scoped construction date

The reviewed ecoinvent 3.12 cut-off boundary is:

| Role | Activity code | Unit |
|---|---|---|
| US-WECC CCGT electricity service | `c310abcc54726059b3bf8496e05ffb5d` | kWh |
| Combined-cycle plant market | `6fb21f72782c0b3fc6c6725dfb1dd7fb` | unit |
| RoW combined-cycle plant construction | `409bc961971c08001fda366891c86c18` | unit |

Preserve the original capital coefficient, approximately
`1.38888888888889e-11 unit/kWh`, corresponding to 400 MW and 180,000 operating
hours. This common amortisation is separate from the 35-year capacity law.
Changing the timing weights does not recalculate, divide or multiply it by a
second lifetime. No explicit direct end-of-life port was found in the reviewed
construction activity; the pilot invents none.

`scope_capital_chain` copies the service, market and constructor for this pilot,
preserving every coefficient and leaving other users unchanged. The service's
plant exchange gets the annual vintage profile. The copied market-to-constructor
exchange gets an explicit zero-shift profile covering all event years. Thus a
construction date is not shifted a second time by the legacy plant rule.
Other construction inputs retain their own background roles. A selected chain
with an extra internal edge fails rather than being partly copied.

## Reproduction and release evidence

Generate the annual data in the premise environment, using the implementation
worktree on `PYTHONPATH`:

```sh
python dev/stock_vintage/generate_ccgt_cohorts.py \
  --input-dir /path/to/downloads \
  --iam-file '/path/to/remind 3.5.2/REMIND_generic_SSP2-NPi2025.mif' \
  --scenario SSP2-NPi2025 --output /tmp/ccgt-cohorts.json
```

In the existing Brightway environment, run TRAILS'
`dev/stock_vintage/extract_local_inventory.py` to produce a restricted compressed
JSON input and its hash manifest. It reads the complete database, checks unique
export identities and closed technosphere links, omits descriptive comments,
and does not modify the source database. Keep the extract outside repositories.

Then use premise's `dev/stock_vintage/export_ccgt_pilot.py`, providing
`--inventory`, `--cohorts` and a fresh `--output-dir`. The script uses the actual
temporal-rule application, matrix writer, global-index reordering and package
builder. It fails before export if a source biosphere code cannot be represented.
It creates legacy and corrected packages with 2022/2025/2030 anchors and a source
and package hash record. The legacy private package's blanket licence metadata
is corrected without changing matrix bytes or public exporter defaults.

Both packages currently use **constant ecoinvent background technology** at all
anchors. This is a real-inventory timing/conservation comparison, not an
IAM-transformed background LCA. Event dates before 2022 cannot supply historical
inventory technology absent from this package.

Run TRAILS' `dev/stock_vintage/check_ccgt_pilot.py` on that directory. It checks
signed capital amounts, exact annual event years, the internal zero-shift link,
repeated/reversed service requests, graph dates and every biosphere-flow total
against an independent sparse solve. Additional modes cover annual interpolation
disabled, cache loading and the Brightway solver. Current results and unresolved
release requirements belong in [implementation status](implementation-status.md).

The 69 focused premise tests pass after these additions. Both local IAM
pathways produce all seven reconstruction cases for 2022–2030. The primary case
closes the stock target; the reported-additions case closes reported additions;
their other residuals remain visible. Real-package TRAILS checks are still
pending at this documentation checkpoint. No CCGT release approval is implied.

EIA observations use its government-data reuse policy with attribution. Local
ecoinvent and IAM inputs, generated inventory packages and derived scenario
series are restricted research artifacts and are not committed or redistributed.
