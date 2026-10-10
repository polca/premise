# Conditional retirement and scoped lifecycle separation

Experimental implementation, 9 October 2026. These methods supply numerical
and inventory machinery; they do not establish empirical lifetimes or approve
any pilot. The allocation convention remains common amortisation.

## Keep three quantities separate

For service in year `t`, let `N(c,t)` be surviving stock from cohort `c`,
`u(c,t)` service per stock unit, and `q(t)` the existing capital coefficient per
service unit. The manufacture timing weight is:

```text
w(c,t) = N(c,t) u(c,t) / sum_c N(c,t) u(c,t)
capital pulse at c = q(t) w(c,t)
```

The stock distribution is not a distribution of failures. Observed stocks are
already survivors. Weighting them again by survival would incorrectly remove
older cohorts. Equal utilisation is an explicit fallback, not a consequence of
having stock data. Neither normalisation nor the retirement calculation changes
`q(t)` or introduces another lifetime denominator.

Survival governs future transitions and disposal timing. Manufacture cohorts
can be observed even when lifetime is uncertain. A disposal date must be
conditional on the asset having survived to provide service in `t`; assigning
`c + mean_lifetime` can wrongly place disposal before current service.

## Annual convention and initial stocks

`premise.stock_cohorts` treats stock as an end-of-year observation. New annual
additions enter at age zero after that year's retirements. Natural transitions
of an observed survivor are:

```text
N(c,t+1) = N(c,t) S(t+1-c) / S(t-c)
```

The implementation evaluates conditional survival directly; Weibull log-survival
differences avoid division by underflowed absolute survival. A Weibull mean
parameter denotes total service life, with scale `mean / Gamma(1 + 1/shape)`;
it is not mean age in the stock. Fixed life rejects observed survivors at or
beyond its specified lifetime. It is used in analytical tests and, explicitly
as a design-life assumption, in the bounded PV primary case.

The optional `quartic_capacity` law supplies the continuous remaining-capacity
analogue `S(a) = max(0, 1 - (a/(1.25 L))**4)`. It is not the native REMIND
discrete vintage implementation. Observed cohorts beyond its finite support
require an explicitly supplied exponential residual-life extension. The
[CCGT pilot method](ccgt-pilot-method.md) explains the source, indexing limitation
and sensitivities; this is not a library default lifetime.

This annual convention suits the December observations in the vehicle source.
It makes the interval `(t,t+1]` a retirement event in year `t+1`; a surviving
asset is never disposed in an already elapsed year. It refines the general
planning discussion of nearest-year discretisation. For real pilots, document
the date of the stock observation and compare a subannual/alternative-bin case
where within-year entry or utilisation matters. End-year stock weighting is
not automatically annual service weighting, especially for fast-growing PV.

`evolve_stock` requires consecutive future years and one declared input mode:

- Gross additions: add the specified annual inflow after natural retirement.
- Closing-stock targets: infer additions above natural survivors. A target
  below survivors needs a declared excess-exit meaning. The current explicit
  policy distributes excess exits proportionally across natural survivors.

For each year the report retains opening stock, natural retirement, excess
exits, additions, closing stock and a numerical balance residual. A territorial
exit is not physical disposal: exports and geographic reclassification can
remove stock while an asset remains in use. `retirement_record` refuses to turn
future territorial excess exits into physical end-of-life events. The same
guard applies to `service_exit`, which denotes loss of operating service without
evidence of physical disposal (for example, mothballed capacity).

## Retirement conditional on service

For each service-year cohort, calculate the probability of remaining in stock
in future year `y`, conditional on survival to `t`. Use projected cohort declines
inside the explicit projection, then natural survival beyond its last year.
The continuation assumes no further forced exits and records that assumption.
The service-weighted decline between successive years gives disposal weights.

Weights are finite, nonnegative and conserve total mass. The maximum horizon
defaults to 500 years. Residual survival above the tolerance fails and requires
an explicit longer horizon. Residual mass below `1e-12` is folded into the last
event and reported; it is not silently discarded and renormalised. Stable
summation prevents artificial early pulses from roundoff in fixed-life cases.

These are expected cohort calculations, not individual lifetime sampling.
Territorial movement, uncertain age bins, varying utilisation and alternative
survival assumptions require separate empirical sensitivities.

## Date the component currently providing service

`active_component_records` handles regular replacement inside surviving parent
assets. For parent commissioning year `c`, service year `t` and replacement
interval `d`, the current component generation is `floor((t-c)/d)`. Manufacture
is the corresponding installation milestone, rounded up to an integer year.
Component end of life is the earlier of the next replacement milestone and
parent retirement conditional on service in `t`. The output retains paired
manufacture/end events as well as their two marginals; every pair satisfies
`manufacture <= t < end`. Parent service weights aggregate these records.

This does not apply the replacement count again. If a 30-year plant uses an
initial inverter and one replacement at year 15, the source coefficient already
contains two equivalent inverters divided by lifetime service. With thirty equal
annual services, dating that unchanged `2/30` coefficient to the active inverter
recovers one manufactured at commissioning and one at year 15. End-of-life
totals likewise recover one at year 15 and one at year 30. Halving the coefficient
again would undercount the two components. The analytical test integrates all
thirty services and checks these four event totals independently.

The rule assumes known commissioning and regular replacements; it does not
infer observed component ages from parent stock data. Interval and parent-life
sensitivities change dates while preserving the existing coefficient. They do
not constitute a new, internally re-amortised material-demand forecast. A
territorial or operating-service exit remains insufficient evidence for physical
component disposal. Irregular small maintenance replacements and installation
losses require separately documented roles, rather than a fictitious periodic
replacement of every component. See the [PV method](pv-pilot-method.md).

## Separate embedded disposal without changing the static inventory

The audited car-chassis and lorry manufacture activities include disposal
inputs. Shifting the complete manufacture activity to a past cohort date would
also shift this disposal backwards. `premise.stock_lifecycle.split_embedded_lifecycle`
separates an explicitly reviewed capital-to-disposal path:

1. Copy the service caller and the necessary capital market/manufacture nodes
   into a named pilot context. Original activities remain available to the
   background system; their timing is not changed by a local pilot binding.
2. Remove only the selected lifecycle exchanges from the copied capital nodes.
3. Add distinct lifecycle adapter activities to the copied service caller.
   Their coefficients equal the signed products along the removed paths,
   including reference-production scaling. A credit keeps its sign.
4. Bind service-to-capital timing to the manufacture marginal and service-to-
   lifecycle timing to the conditional retirement marginal. Internal capital
   wrappers and lifecycle adapter links receive explicit zero shifts.

The rewrite accepts shared or merging acyclic paths and non-unit reference
production, rejects ambiguous identities and does not mutate its inputs. The
audit lists copied identities, lifted coefficients, original units and required
zero-shift bindings. It supplies no automatic scientific classification.

Optional `component_cuts` identify exact internal edges to lift as well. Such
an adapter supplies the scoped component with its selected disposal ports already
removed. Pointing it back to the original component would reintroduce embedded
disposal and double count the separately lifted end-of-life flow. Tests cover
single and nested component cuts, non-unit production, signed disposal, full
static biosphere equivalence and zero disposal in the manufacture-only branch.
The audit retains the original graph's activity scales for each scoped node.

`lift_scoped_biosphere` uses those audited scales to separate an exact direct
biosphere exchange into a pure biosphere adapter. In the PV pilot this dates
lifetime land occupation at service, while both original land-transformation
flows stay at construction. The transferred amount retains its original units
and sign; it is not divided by lifetime again. The source inventory and original
audit are immutable inputs. Tests verify full static biosphere equivalence,
preserved transformation and rejection of varying per-capital biosphere amounts
across anchors. Direct occupation timing is also checked in the actual consumer.

For the current four-activity car/truck paths, four activities are added: one
scoped service, two capital nodes and one lifecycle adapter. Full yearly cohort
copies are unnecessary when lifecycle quantities per capital unit are invariant.
`validate_lifecycle_anchor_audits` checks that condition across inventory anchors.
Service-year amortisation may vary; the lifted quantity per capital unit may not.
If it does, fail and retain cohort-specific quantities or another representation
that preserves their correlation with manufacture vintage. The simple two-
marginal representation is not equivalent in that case.

Unselected exchanges remain intact. A separate review must still identify new
components, replacement components, maintenance, and genuine machinery capital.
For example, a new glider is assembled with the chassis, whereas a factory used
to make it has its own stock history. Do not apply one zero-shift rule to both.
Battery replacement dates cannot be inferred solely from a vehicle's first-use
year. These boundaries remain release requirements for the empirical pilots.

The rewrite requires `uncertainty_mode="deterministic"`. Products of uncertain
coefficients and lifted ports do not preserve the original Monte Carlo
correlations. The implementation makes no stochastic-equivalence claim.

For a reviewed capital chain with no embedded lifecycle exchange to move,
`scope_capital_chain` copies the caller and explicit market/manufacturer chain,
preserving all quantities and providing internal zero-shift bindings. It rejects
unlisted internal links and cycles. No disposal amount is invented. This is the
CCGT boundary, tested independently with non-unit production and signed inputs.

## Evidence and reproduction

The numerical tests include stationary uniform stock under fixed life and
constant additions, conditional survival of observed old cohorts, service
weighting without changed amortisation, target balance, explicit early exits,
memoryless exponential remaining life, excessive-tail rejection, signed
lifecycle coefficients, shared paths and anchor-invariance checks.

The real local ecoinvent 3.12 cut-off check uses the complete 26,533-activity
matrix and all 3,297 biosphere flows. It compares the original demand with the
scoped car-chassis and lorry rewrites. Maximum absolute summed-biosphere
differences are `2.22e-15` and `2.22e-16`, respectively. The independent
technosphere and biosphere substitution identities have zero residual for
these paths. Relative full-system residuals are below `3e-17`.

The comparison uses processed Brightway matrix coefficients consistently.
Processed values differ from dataset metadata by up to about `4.9e-8` relative
because of input precision. Mixing these coefficient sources or comparing
different sparse solver results creates numerical differences unrelated to the
rewrite. The report retains this distinction; no licensed raw inventory is
committed. This is a deterministic structural check, not a real corrected
temporal pilot or an empirical lifetime validation.

```sh
# In premise's environment, with the implementation worktree on PYTHONPATH:
python -m pytest tests/test_stock_cohorts.py tests/test_stock_lifecycle.py \
  tests/test_stock_lifecycle_export.py
PYTHONPATH="$PWD" python dev/stock_vintage/generate_lifecycle_fixture.py \
  /tmp/stock-lifecycle

# In TRAILS' environment:
python dev/stock_vintage/check_lifecycle_roundtrip.py /tmp/stock-lifecycle \
  --output /tmp/stock-lifecycle/checks.json

# In an environment with the existing local Brightway project, from TRAILS:
python dev/stock_vintage/check_lifecycle_inventory.py \
  --premise-root /path/to/premise-implementation \
  --output /tmp/lifecycle-static-equivalence.json
```

The synthetic fixture uses actual premise matrix export and package assembly.
Manufacture occurs in 2005/2015/2021/2022 and retirement in 2025/2035/2041/2042,
with background inventory anchors only in 2020/2022. Independent annual
biosphere expectations detect duplicate shifts and missing signed credits.
TRAILS tests run 32 cases across positive/negative functional units, repeated
and reversed years, root attribution, interpolation and direct/Brightway solves.

This test exposed two legacy boundary behaviours: graph nodes were mapped to
matrix years, and frontier vectors were clamped before inventory storage was
initialised. In stock-profile packages both now retain the physical year while
matrix selection uses the available endpoint. Inventory/score calendars include
the routed support plus biosphere offsets, including successive component
shifts. Legacy packages retain their previous behaviour. Endpoint inventories
remain a technology proxy; retaining a date does not supply historical LCI data.

None of this closes the empirical data gate. Public observation missingness,
service weighting, lifespan choice, IAM reconstruction and real temporal pilot
comparisons still need the evidence recorded in `implementation-status.md`.
