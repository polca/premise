# Proposed data contract and migration

Status: design specification. Names introduced here are proposed, not existing
public APIs or installed package resources. The existing CSV and type 6 fields
remain the compatibility baseline.

## 1. Separate evidence, rules, and generated outputs

The current supplier table mixes lifetime context, distribution parameters, and
notes. The replacement workflow should use the following logical resources.
Physical file names can be finalised in work package P1; their responsibilities
and validation rules are part of the design.

| Resource | Required content |
|---|---|
| `asset_groups` | Stable group identifiers, asset units, component boundaries, classification rules |
| `source_records` | Source ID, exact table/cell or series, version/date/hash, statistic definition, units, geography, reference years, access/licensing limits |
| `lifetime_models` | Distribution family, named parameters with units, cohort validity, support, source IDs, uncertainty model |
| `stock_observations` | Cohort stock or age bins, measured weighting basis, reference date, geography and technology |
| `additions_and_transfers` | Gross additions and transfers by year, original cohort where known, retirement definitions |
| `service_models` | Capacity per asset, utilisation/output by age and year, degradation, lifetime service, units and sources |
| `exchange_role_rules` | Caller and supplier selectors, service region, role, precedence, exceptions, source/rationale |
| `profile_specs` | Evidence tier, stock reconstruction, allocation convention, binning/tail policy, fallback and source IDs |
| `generated_profiles` | Annual event amounts/weights, profile IDs, model/scenario/year identity, provenance hash |
| `migration_audit` | Legacy-row disposition, exchange matches, before/after moments and amounts, warnings, review status |

Use text tables/JSON/YAML for maintained data and schemas. Workbooks can remain
source evidence. A generated workbook or dashboard must not become the only
record of a numerical assumption.

## 2. Identity and scope

Dataset identity must include source database/version, name, reference product,
unit, and supplier location. A generated match must additionally identify the
calling activity and the exchange role. Do not identify activities by mutable
matrix indices in maintained source records.

The **service geography** determines the stock being represented. It can differ
from the manufacturing supplier's geography: a global vehicle supplier does not
imply a global operating fleet. Preserve both. Used-asset transfers can require
more than one geographic history.

Profiles have explicit model, pathway, reference year, technology and geographic
scope. Missing scope is not synonymous with globally valid evidence. A declared
fallback can have broader scope and must record which narrower request used it.

### Matching rules

1. Apply a documented foreground/exchange override when it defines this role.
2. Prefer an exact caller/supplier/context rule within its stated scope.
3. Apply a reviewed technology/group rule with explicit geographic fallback.
4. Use a declared low-evidence fallback only for a resolved exchange role.
5. Report unresolved roles; do not silently assign an existing-fleet curve.

Explicit priority and specificity must yield one winner. Equal-priority
conflicting rules are errors. Record all candidate rules and the winning rule.
Avoid substring-only matching that conflates a building shell, a pump, and a
whole treatment plant. Market traversal must preserve provenance and ensure
the same service-to-asset transition is temporalised once.

## 3. Semantic fields

Keep the following concepts separate even where some are unavailable:

| Field concept | Meaning |
|---|---|
| `lifetime_mean_years` | Mean total service life, with physical vs economic/territorial definition |
| `lifetime_family` and named parameters | Survival model; never an overloaded generic `loc` |
| `observed_stock_mean_age_years` | Observed mean age in a specified stock and weighting basis |
| `stock_age_bins` | Measured/estimated cohort composition with bin boundaries |
| `construction_lead_time` | Time between upstream production/construction and commissioning |
| `allocation_basis` | Homogeneous fixed service, common amortisation, individual-lifetime service, or another explicit convention |
| `event_role` | Manufacture, commissioning, maintenance, replacement, disposal, or operating input |
| `confidence` and `fallback_reason` | Evidence assessment; distinct from physical lifetime variability |

Source notes that state a median, mode, log-space mean, GSD, or arithmetic
standard deviation must be parsed into their actual meanings. Invalid Weibull
parameters or ambiguous source metadata are review failures, not an invitation
to substitute another family silently.

### IAM source and period metadata

The [actual-scenario audit](iam-scenario-assessment.md) requires these additional
source concepts when importing IAM stock information. These are design
requirements, not fields already supported by the loader:

- Model and reporting-code versions, run/scenario identity, native region and
  versioned geography/technology crosswalk; keep overlapping aggregates apart.
- Exact source variable and original unit, plus whether a value is a stock,
  annual rate, period total or service flow. Record represented period bounds
  and the transformation from model periods to reporting years.
- Cohort origin: observed, native model output, reconstructed, or assumed
  initial stock. Identify historical calibration separately from future model
  output, including assumptions before the first reporting year.
- Original cohort date and its meaning: manufacture, construction,
  commissioning or first service; separate refurbishment and replacement dates.
- Survival kernel/parameter definition, initial-vintage source, transfers,
  early retirement and idle-capacity treatment, with reconciliation residuals.
- Service units and utilisation/load conversion; identify whether inventory
  efficiencies already represent the fleet to prevent a second adjustment.

Store unnormalised cohort stock and service quantities alongside generated
weights. Native service cohorts do not by themselves establish individual
lifetime-service denominators. Keep the allocation convention explicit.
Missing region/technology rows are not zero; restricted source exports must
not silently supply empty cohort profiles.

## 4. Example generated profile

This synthetic example corresponds to the worked allocation example in
[strategy](strategy.md). It is not a proposed empirical vehicle profile.

```json
{
  "profile_id": "example-two-populations-2030",
  "schema_version": "stock-temporal-2-draft",
  "reference_year": 2030,
  "exchange_role": "existing_asset_service",
  "allocation_basis": "individual_lifetime_service",
  "asset_unit": "car equivalent",
  "service_unit": "10000 vehicle-kilometres",
  "asset_amount_per_service_unit": 0.075,
  "temporal_distribution": 6,
  "temporal_offsets": [-2, -12],
  "temporal_weights": [0.6666666666666666, 0.3333333333333333],
  "temporal_amount_source": "port",
  "source_ids": ["synthetic-two-populations"],
  "evidence_tier": "synthetic",
  "tail_mass_omitted": 0.0
}
```

The profile's generated total is separate from the timing weights. A timing-only
migration would retain the existing exchange amount and explicitly record that
the `0.075` model total has not been adopted. This example's normalised weights
do not imply a total input of one car.

### Transport fields

Use existing exchange fields for simple marginal profiles:

- `temporal_distribution = 6`;
- integer `temporal_offsets` and nonnegative `temporal_weights`;
- `temporal_amount_source = "port"` for a conserved caller-year coefficient;
- unused parametric fields empty; support comes from explicit offsets.

The maintained legacy CSV accepts pipe-separated sequences. Matrix CSV exchange
fields use an unambiguous JSON-array serialisation. Test both paths with the
actual readers. Validate weights before writing; do not rely on the current
consumer's normalisation/fallback to repair malformed source data.

The proposed resource schema must distinguish calendar event years from offsets.
Convert calendar years to offsets only at export or lookup, using the actual
reference year. Preserve both representations in the audit.

## 5. Annual lookup and interpolation

Current TRAILS chooses the closer template if explicit offsets or weights
differ; it does not interpolate different discrete profiles. Existing methods
also use template/scenario-year mappings. Both behaviours need end-to-end
verification before claiming annual cohort evolution.

Preferred design: generate a compact, deduplicated profile resource for every
requested service year, independent of the spacing of inventory matrix anchors.
The loader resolves profiles at the actual service year. This avoids exporting
large duplicate matrices merely to carry annual profiles.

If annual generation is unavailable, any interpolation must have an explicit
model basis. Align unnormalised event contributions on absolute cohort/event
years, interpolate quantities consistently, and then normalise. Interpolating
normalised relative-offset arrays can move manufacture dates or invent cohorts.
Reject unspecified interpolation rather than silently presenting a nearest-year
profile as annual evolution.

Out-of-range profile requests need a declared extrapolation rule. Reusing the
endpoint profile at a later year is not the same as ageing the endpoint stock.
Record how new inflows, survival, and scenario assumptions are extended.

## 6. Cohort/lifecycle representation

Marginal manufacture profiles can use existing type 6 fields. A homogeneous
fixed-lifetime model can derive retirement from cohort year without a new
retirement-probability dimension. More general correlated lifecycle histories
require additional information. For that optional extension, evaluate:

1. Sparse virtual activities/wrappers keyed by cohort, lifetime/event group, and
   service context, with existing routing carrying the resolved branches.
2. Compact cohort state consumed by routing, retaining identity until lifecycle
   contributions are formed.

Prototype option 1 first because its semantics can be checked with ordinary
matrix and graph fixtures. Select it only if benchmarks show acceptable growth
in activities, matrix fill, routing work, and stored inventories. An alternative
must demonstrate equivalent allocation and event conservation.

Do not create one dense year-by-cohort-by-retirement tensor for every exchange.
Share group profiles, exploit sparse support, and aggregate populations only
when their inventories and lifecycle response are demonstrably equivalent.

## 7. Versioning, package metadata, and caches

Add proposed package metadata for schema version, generator version, source
hashes, scientific model convention, required consumer capabilities, and the
selected migration level. These are additions to implement, not fields the
present loader already enforces.

Support old packages in an explicit legacy mode. Corrected profiles must never
silently change the meaning of an old package's generic `loc` or `scale`.
Static marginal type 6 profiles may be readable by existing consumers; annual
profile resources and cohort lifecycle state require a coordinated consumer.
Older readers may ignore unknown metadata, so capability flags alone cannot
guarantee rejection. Establish a documented minimum version and a producer-side
compatibility check, and test actual old-reader behaviour.

Cache identity includes profile/source hashes, schema and generator versions,
matching rules, allocation convention, scenario/year, binning/tail policy,
amount mode, and lifecycle representation. Reimports or changed profiles must
invalidate relevant routing, inventory, and score state. Historical matrix
caches may be reused only where their own identity remains valid.

## 8. Migration procedure

1. Freeze and hash the old table, defaults workbook, review outputs, and source
   revisions. Assign stable audit IDs to all 987 rows, including duplicates.
2. Record each row's disposition: retained source evidence, replaced profile,
   split by context, reclassified component/event, duplicate, or unresolved.
3. Separate source claims from reviewer-generated assumptions; remove automatic
   lifetime-to-age rescaling from the generation path.
4. Review caller/supplier matches in representative built databases. A row
   review alone does not show where a parameter actually applies.
5. Generate candidate profiles and audit quantities/moments. Do not overwrite
   the packaged table as a side effect of a review/dashboard script.
6. Export candidate packages with distinct identifiers and run consumer checks.
7. Promote reviewed data by an explicit versioned change, preserving the legacy
   fixture and migration report for reproducibility.

Maintain a disposition for non-stock rows as unchanged controls. Do not merge
biomass carbon uptake or long-term emissions into the new stock conventions.

## 9. Required audit output

Per generated match, report source row/rule/profile IDs; caller/supplier
identity; requested and selected geography/year; role; evidence tier; old/new
support; source and binned mean/median/quantiles; discarded tail mass; old/new
signed amounts; amount source; allocation basis; event chronology; fallback;
and review outcome.

Summaries report both CSV rows and unique resolved identities, plus actual
exchange coverage in each database. Separate unmatched rules from unmatched
exchanges, and distinguish deliberate exclusions from missing evidence.
