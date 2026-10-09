# Implementation plan

Status: planned work. Checklist items describe future deliverables unless
explicitly marked complete. This document does not authorise a claim that the
proposed APIs, schemas, or migrations already exist.

## Repository and branch arrangement

- This documentation branch is `plan/stock-asset-temporal-distributions` in
  premise, starting at `trals` revision `82f5c5d0`.
- Keep premise implementation based on the relevant `trals` descendant, using
  an isolated branch/worktree. Do not implement against an unrelated export
  branch solely because it is the main local checkout.
- Create a coordinated TRAILS implementation branch from a recorded revision
  when consumer changes begin. The reviewed consumer baseline is `b7d1bb78`.
- Keep the plan in premise as the canonical cross-repository specification;
  link it from TRAILS implementation PRs rather than maintaining divergent copies.
- Use small reviewable changes and explicitly record compatible producer and
  consumer versions. Public release is a later deliverable, not part of writing
  this plan.

## Ownership boundaries

| Responsibility | Repository / current entry points |
|---|---|
| Evidence curation, group rules, source migration | premise: `dev/trails/`, `premise/data/trails/` |
| Context matching and profile assignment | premise: `premise/trails.py` |
| Exchange/package serialisation | premise: `premise/export.py`, `TrailsDataPackage` export path |
| Profile parsing, annual lookup and interpolation | TRAILS: `trails/datapackage.py`, `trails/trails.py` |
| Distribution evaluation and validation | TRAILS: `trails/temporal_distributions.py` |
| Foreground overrides/imports | TRAILS: `trails/importer.py` |
| Cache identity, routing and inventory state | TRAILS: `trails/cache.py`, `trails/cache_interpolation.py`, `trails/trails.py`, `trails/lca.py` |
| Result comparisons and publication reproduction | TRAILS: relevant tests and `dev/publication/` workflows |

New modules may be appropriate, but choose their names and boundaries in P1.
Keep the numerical stock/profile generator independent of Brightway database
access so analytical fixtures can run without licensed inventory data.

## P0 — Freeze and reproduce the baseline

Dependency: none. Repository: premise, with a TRAILS comparison environment.

- [ ] Record commit IDs, Python/dependency versions, source hashes, ecoinvent
      version/system model, and scenario settings for each reproduction.
- [ ] Create a complete audit of the 987 stock rows, including duplicates,
      missing fields, conflicting notes, and group classification.
- [ ] Reproduce all 317 lognormal rows through the real consumer, including the
      two whose maximum is zero; audit all 670 triangular rows numerically.
- [ ] Compare original review parameters, reviewed table, packaged table,
      exported exchanges, and consumed weights as distinct stages.
- [ ] Count matches in representative built databases, including market wrappers
      and nested asset inputs. Identify which study activities traverse them.
- [ ] Preserve immutable legacy profiles and result fixtures.

Completion: a machine-readable row/exchange audit and reproducible baseline
figures exist. Numerical observations are separated from claims about full LCA
results. Earlier targeted reproductions are evidence for starting this work, not
a substitute for its complete coverage.

## P1 — Finalise model choices and schemas

Dependency: P0 findings. Repository: specification in premise; consumer review
in TRAILS.

- [ ] Finalise exchange roles and deterministic matching precedence.
- [ ] Define the homogeneous fixed-service baseline and `common_amortisation`
      approximation separately from individual-lifetime allocation.
- [ ] Define service and asset units, installation/manufacture conventions,
      annual binning, tail tolerances, and out-of-horizon policies.
- [ ] Specify source, lifetime, stock, utilisation, profile and audit schemas.
- [ ] Define annual profile lookup independently of inventory anchor spacing.
- [ ] Define the version/capability contract and legacy compatibility behaviour.
- [ ] Define the bounded prototype for lifecycle representation; do not commit
      to dense cohort-by-retirement expansion before benchmarking it.

Completion: schemas can represent the equal-lifetime example, a variable-lifetime
example, transfers, a new-asset component, and a replacement event without
overloading `loc`. Every open decision has a recommended choice and a test that
would reject it; record resolutions in this plan.

## P2 — Curate sources and exchange roles

Dependency: P1. Repository: premise. Can proceed alongside P3.

- [ ] Apply the [asset-group checklist](asset-review.md) to all 23 groups.
- [ ] Extract lifetime and stock-age claims separately, preserving exact source
      locations and uncertainty. Verify global/regional scope.
- [ ] Remove lifetime-to-age rescaling from the reviewed generation workflow.
- [ ] Define caller/supplier role rules and explicit foreground overrides.
- [ ] Identify embedded lifecycle inventories that must be reconciled before
      new manufacture, maintenance or disposal paths are introduced.
- [ ] Pilot cars, PV or wind, long-lived infrastructure, and short-lived
      replacement components; expand by measured coverage and contribution.
- [ ] Assign every row a reviewed disposition, including unresolved cases.

Completion: pilot evidence is traceable and all groups have a review status.
Promotion of defaults requires completion of the full coverage checklist, not
just completion of the pilots. Keep missing evidence visible.

## P3 — Implement a deterministic profile generator

Dependency: P1, using synthetic and pilot data from P2. Repository: premise or
a small shared dependency only if justified during design.

- [ ] Read observed cohorts or reconstruct them from additions and survival.
- [ ] Support fixed lifetime and documented stochastic survival families, with
      named parameters and valid domains.
- [ ] Implement stationary fallback, initial-stock evolution, and future gross
      additions/retirement logic. Add transfers when needed by a pilot.
- [ ] Generate baseline stock/service profiles and maintain their assumptions.
- [ ] Integrate continuous profiles into annual bins; preserve supplied annual
      cohorts. Record original/binned moments and omitted tail mass.
- [ ] Emit explicit profiles, provenance hashes and diagnostic quantities.
- [ ] Make dashboards and figures consume generated weights rather than a
      separate hand-coded probability density implementation.
- [ ] Reject malformed inputs, impossible stock/lifetime combinations, negative
      cohort stocks and unsupported unit conversions with actionable messages.

Completion: analytical fixtures pass; repeated runs from identical sources
produce identical profile IDs and weights; no database writes are required.

## P4 — Integrate corrected profiles with premise and TRAILS

Dependency: P1 and P3; pilot matching rules from P2. Repositories: both.

- [ ] Apply profiles by exchange role and context in premise, retaining a
      complete match audit and detecting double temporalisation.
- [ ] Preserve signed inventory totals and existing amount modes for the first
      correction; expose the modelled amortisation difference in the audit.
- [ ] Serialise explicit profiles and proposed version/provenance metadata.
- [ ] Implement actual-year profile lookup, foreground overrides and declared
      extrapolation in TRAILS; remove hidden nearest-template behaviour for
      new annual profile resources.
- [ ] Implement strict validation for corrected profiles. Keep old package
      semantics reproducible under explicit legacy handling.
- [ ] Align legacy lognormal/triangular evaluation with documented conventions
      only under a versioned migration. Do not silently change archived inputs.
- [ ] Include profile identity in caches and invalidate dependent state after
      source/profile/import changes.
- [ ] Verify routing with fixed depth and adaptive pruning, keeping all stopped
      branches in the subsequent matrix solve.

Completion: a premise-generated fixture arrives at the router with exactly the
reviewed weights and reference years. Roles, quantities, source mode, and caches
behave consistently. Biomass and long-term-emission controls remain unchanged.

## P5 — Validate and release the homogeneous baseline

Dependency: P0–P4. Repositories: both.

- [ ] Demonstrate equality of stock and production shares when utilisation and
      lifetime service are equal, and document deviations from those assumptions.
- [ ] Validate fixed-lifetime manufacture/retirement chronology without adding
      a retirement-probability dimension unnecessarily.
- [ ] Complete group review or declare limited pilot coverage in an opt-in
      release. Unresolved groups cannot silently receive corrected status.
- [ ] Run the static limiting cases, signed-exchange cases, dynamic pilot LCAs,
      cache checks and memory/runtime benchmarks in the validation plan.
- [ ] Publish a before/after report separating kernel, role, source-profile and
      quantity effects. Keep inventory quantities fixed for the timing comparison.
- [ ] Rebuild affected candidate packages/databases under new names.
- [ ] Document assumptions and compatibility, including common-amortisation
      cases where physical lifetime variability is not individually allocated.

Completion: baseline correction is reproducible and can be released independently
of P6. A group using valid homogeneous assumptions does not need reweighting
merely because its assets have different current ages.

## P6 — Optional heterogeneous service/lifecycle extension

Dependency: P1–P4 plus adequate population/service evidence from P2.
Repositories: both. This is not a prerequisite for completing P5.

- [ ] Implement population-specific current use, lifetime service and production
      quantities using the strategy equations.
- [ ] Show why a common stochastic lifetime law differs from a common realised
      lifetime; reproduce the conditional-survival effect with a synthetic test.
- [ ] Prototype sparse cohort/lifetime wrappers and compare with an independent
      population-accounting calculation.
- [ ] Reconcile existing amortised manufacture and embedded lifecycle exchanges;
      avoid duplicate disposal, maintenance, credits or lifetime division.
- [ ] Preserve population identity for construction, replacements and disposal,
      including transfers and conditional remaining life where applicable.
- [ ] Benchmark activity/matrix growth, routing, memory and inventory storage.
- [ ] Change total exchange coefficients only with an explained unit-consistent
      reconciliation. Preserve separate timing and quantity comparisons.

Completion: production allocated across complete lifetime service conserves
each population's production, lifecycle events remain possible, and population
aggregation matches an independent reference. Groups lacking evidence remain in
their declared baseline mode.

## P7 — Study reassessment and default promotion

Dependency: P5 for baseline; P6 only for groups using the extension.

- [ ] Identify study/package hashes and functional units actually affected.
- [ ] Recalculate representative results and contribution/timing decompositions.
- [ ] Assess uncertainty in lifetime, utilisation, cohort data, fallback choice,
      horizon treatment and allocation convention.
- [ ] Update scientific descriptions to match the implemented mode; do not claim
      recovery of observed vintages from mean-only source data.
- [ ] Promote defaults only with a complete disposition audit, compatibility
      record and explained result differences.
- [ ] Preserve legacy package/result references and a documented rollback path.

Completion: affected results and their scope are explicit. A successful unit
test suite alone is insufficient for promotion of scientific defaults.

## Suggested reviewable change sequence

1. Baseline audit utilities and fixtures, without default changes.
2. Source schemas, role rules and analytical profile generator.
3. Consumer validation/annual lookup/cache contract and compatibility fixtures.
4. Producer export, pilot profiles, shared plots and matching audit.
5. Full group migration, baseline result comparisons and documentation.
6. Optional heterogeneous allocation/lifecycle prototype, then validated uptake.

Each change should name its completed work-package criteria and compatible
revision in the other repository. Avoid combining a distribution bug fix, source
data replacement and changed inventory totals in one unexplained result update.

## Decisions to resolve during implementation

| Decision | Recommended starting point | Evidence needed to close it |
|---|---|---|
| Baseline allocation | Homogeneous fixed service where supported; otherwise explicit common amortisation | Unit/lifetime/utilisation audit and sensitivity |
| Annual profile representation | Compact annual sidecar independent of matrix anchors | Actual-year lookup tests and package-size benchmark |
| Lifecycle representation | Fixed-life cohort chronology first; sparse wrappers for heterogeneous pilot | Conservation and scaling benchmark |
| Probability tail cutoff | Configurable, recorded; no universal hard maximum age | Moment and impact sensitivity |
| Missing region/year evidence | Named fallback with scope and uncertainty | Group review and observed-stock comparison |
| Default promotion | Opt-in corrected packages before changing defaults | Full coverage and study reassessment |
