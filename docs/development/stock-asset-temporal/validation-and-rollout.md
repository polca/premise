# Validation, comparison and rollout

Status: required future checks, not a report of tests already run. The targeted
audit findings in [evidence](evidence-and-references.md) are the current evidence.

## 1. Analytical fixtures

Keep fixtures small, public/synthetic and independent of licensed inventories.
Use mathematical or separately implemented expectations rather than assertions
that simply call the generator twice.

| Fixture | Required result |
|---|---|
| Fixed lifetime, constant additions, constant use | Uniform surviving ages under the declared annual convention; stock and production shares equal |
| Equal lifetime service, different current ages | No extra manufacturing reweighting solely because of age |
| Same lifetime probability law, different survivor ages | Conditional lifetime composition differs when variance is nonzero; individual-lifetime allocation can differ from stock shares |
| Two-population worked example | Equal 15-year lifetimes give 1/30 + 1/30 cars per 10,000 km; 10/20-year lifetimes give 0.05 + 0.025 |
| Increasing, steady and decreasing additions | Stock balances and age profiles agree with an independent cohort calculation |
| Observed initial stock, zero new additions | Cohorts age and retire; manufacture dates do not move forward with the service year |
| Gross additions and retirements with unchanged total capacity | Turnover occurs despite zero net capacity change |
| Transfer between regions | Original manufacture cohort retained; transfer is not counted as a new manufactured asset or automatic disposal |
| New asset containing a new component | Construction lead time only; no existing-fleet age shift on that component |
| Replacement in an existing asset | Component timing follows replacement; enclosing structure age is not reset |
| Market wrapper and nested supply | Exactly one role-appropriate vintage transition; no duplicate shift |
| Fixed-life disposal | Retirement equals installation plus lifetime under the chosen convention; survives through its attributed service |
| Variable-life disposal | Retirement conditional on survival; no positive-service population retired before the service |
| Zero service / invalid lifetime / conflicting rule | Explicit error or declared no-service result; no hidden uniform fallback |

For normal lifetime examples, define treatment of the nonphysical negative tail.
For shared stochastic laws, check the complete population calculation as well as
normalised curves. Equal means do not imply equal shapes or allocation totals.

## 2. Numerical distribution and source validation

- Check finite nonnegative timing weights, declared support, equal sequence
  lengths, duplicate-offset aggregation, and unit sum after intentional
  normalisation. Signed technosphere amounts remain separate from weights.
- Verify mirrored lognormal examples against named median/GSD parameters and
  standard formulas. Verify triangular examples against a true asymmetric
  triangular CDF, including boundary modes. Preserve legacy fixtures separately.
- Compare source, continuous and binned means/quantiles; declare accepted
  discretisation differences. A distribution with the intended median need not
  have the intended mean, especially after truncation.
- Record tail mass and moment changes before renormalisation. Test sensitivity
  to the support rather than assuming a small discarded probability is harmless.
- Check source weighting bases: count, capacity, floor area, mass and service
  statistics cannot be compared without conversion.
- Verify raw row counts, unique identities, matched exchanges, unresolved rules
  and exclusions independently. All 987 baseline stock rows need dispositions.

Initial numerical targets for normalised synthetic profiles: absolute mass error
at most `1e-12`, and independently computed coefficients within `rtol=1e-10`,
`atol=1e-12` in the declared units. These are proposed numerical tolerances,
not tolerances on empirical observations. Record justified adjustments for
large/ill-conditioned solves; do not loosen checks to accommodate unexplained
model changes. Empirical fit tolerances follow measurement uncertainty and bins.

## 3. Export-to-router contract checks

Use the real premise assignment/export path and the real TRAILS loader/router.

1. Read a source/profile specification and produce a matched exchange.
2. Export all sequence fields and profile resources into a temporary package.
3. Load with TRAILS and inspect the actual pulses for that service year.
4. Compare calendar event years, amounts and source mode with the generator.
5. Verify output after cache reuse and after a deliberate profile change.

Required cases include year zero, negative offsets, fractional-source binning,
explicit pulse support without min/max, nonconsecutive matrix anchors, changing
profiles between anchors, and requests outside the profile/inventory horizons.
The annual-profile test must fail if the reader selects the nearest profile
instead of the requested year's cohort model.

Check foreground Excel overrides and state invalidation. Preserve the existing
`port`/`matrix` distinction, biosphere profiles, and multiple scenario labels.
Test new-writer/new-reader, legacy-writer/new-reader and actual old-reader
behaviour; metadata ignored by an old reader is not a compatibility safeguard.

## 4. Conservation and static limiting cases

For timing-only `port` profiles, pulse amounts must sum to the original signed
exchange amount. Under constant inventory matrices, constant characterisation,
full event accounting and the same inventory amounts, time aggregation should
agree with an independent static solution. This is a controlled limiting case,
not an expectation that dynamic real-world scores remain unchanged.

For the heterogeneous extension, allocate an asset's manufacture over all of its
service: the total attributed asset production must equal the original asset
production. Check at population level before aggregation. Do the equivalent
checks for lifecycle flows and signed waste/recycling exchanges without changing
the database's allocation system.

Do not claim amount conservation in general `matrix` mode when coefficients
change by pulse year. Test its specified equation separately. Avoid comparing
newly changed amortisation amounts against old static totals as if they were
the same model.

Adaptive routing must conserve stopped branches through frontier matrix solves.
Compare appropriate fixed-depth/adaptive cases and document temporal-resolution
effects where a branch is no longer explicitly expanded. Finite routing depth
and finite event horizons require separate checks.

## 5. Real-database pilot matrix

Begin with one supported ecoinvent version and system model available locally,
then expand to the versions/system models supported by the release. Record
exact versions; do not assume name/product mappings transfer unchanged.

| Pilot | Question it resolves |
|---|---|
| Passenger transport and a new-car manufacturing demand | Existing service vs new production; stock and allocation assumptions |
| PV/wind electricity with historical and future additions | Nonstationary cohorts, annual lookup and utilisation |
| Long-lived infrastructure service | Old cohorts, inventory horizon reuse and refurbishment |
| HVAC/pump or treatment equipment | Component boundaries and nested roles |
| Short-lived replacement component | Event scheduling and annual resolution |
| Unchanged biomass/long-term-emission cases | Isolation from unrelated temporal models |

Use no-write premise builds before writing candidate Brightway databases. Use
encrypted IAM inputs and local credentials without recording secrets. Write
candidate databases/packages under new names; preserve source and publication
artifacts. After writing, inspect actual matched suppliers and exchange amounts.

## 6. Attribute differences to changes

Keep a comparison ladder with reproducible hashes:

| Case | Change relative to its predecessor |
|---|---|
| A | Legacy runtime and legacy parameters |
| B | Corrected interpretation of the same assumed parameters; diagnostic only |
| C | Corrected exchange roles and validated stock profiles, old signed totals |
| D | Annual stock/scenario evolution, same declared allocation basis |
| E, optional | Heterogeneous lifetime-service weights and reconciled quantities |

Cases B–D are not necessarily separate releases. Use factorial/paired comparisons
where interactions prevent a simple additive attribution of differences.

For each functional unit, report inventory totals, temporal profiles, manufacture
and lifecycle contributions, LCIA totals, and changes caused by amount vs timing.
Compare static methods and time-dependent climate outputs only where those are
part of the study. Preserve ecoinvent/method versions and root attribution; close
disk-backed inventories after use.

Report uncertainty and the effects of stationary versus historical inflows,
lifetime variability, utilisation, transfers, profile-year interpolation,
inventory horizon treatment and allocation convention. An observed mean age
matching to several decimals is not validation of the full age distribution.

## 7. Performance and storage

Benchmark generator time, package/profile size, loading, routing, solve time,
peak RSS, cache size and stored inventory size, for cold and warm runs. Compare
the same database/scenario/demand on the recorded baseline environment.

The homogeneous baseline should not require a retirement-probability tensor.
For the extension, measure the cost of cohort/lifetime wrappers before expanding
to all groups. Set resource budgets from these pilot measurements. Avoid eagerly
materialising sparse/factorised inventories for validation; use labelled
reductions and bounded diagnostic samples.

## 8. Relevant existing tests and commands

Run the smallest relevant suites when implementing their modules. These commands
refer to files already present at the audited revisions; new fixtures will be
added alongside them.

```sh
# In the premise checkout/environment
pytest tests/test_trails_temporal.py
git diff --check

# In the TRAILS checkout/environment
pytest tests/test_temporal_distributions.py tests/test_datapackage.py
pytest tests/test_trails.py tests/test_importer.py
pytest tests/test_cache.py tests/test_cache_interpolation.py
pytest tests/test_lca.py tests/test_signed_routing.py tests/test_mixed_frontier.py
```

Broaden to the repository's required checks when shared routing/LCI changes.
Run ecoinvent-marked integration tests only with the required local data. A
documentation-only change requires documentation/link/consistency checks, not
a claim that these implementation tests have passed.

## 9. Release stages and rollback

1. **Audit:** freeze legacy inputs and document defects; no scientific defaults
   change.
2. **Pilot opt-in:** versioned corrected profiles, explicit scope and compatible
   readers; unresolved groups retain visible status.
3. **Baseline release:** all promoted groups have completed role/source review,
   numerical/integration validation and result comparisons. Homogeneous or common
   amortisation assumptions are stated per group.
4. **Optional extension release:** enable individual-lifetime allocation only
   for validated groups, with quantity reconciliation and lifecycle conservation.
5. **Default promotion:** publish migration notes and affected result scope after
   full review. Do not silently overwrite historical packages or references.

Rollback selects the preserved legacy package/model version and its compatible
consumer/cache namespace. Never make rollback depend on reconstructing old
profiles from overwritten source files. Quantify any study/manuscript changes
separately; this plan does not assert that existing conclusions have changed.
