# Implementation status and release evidence

Reviewed 10 October 2026. All five bounded pilots meet the three methodological
and producer/consumer requirements. Final private evidence archiving is in
progress. Both implementation repositories use `feat/stock-vintage-pilots`.
The planning worktree and original checkouts are preserved. Public defaults,
legacy parameter meanings and source Brightway databases are unchanged.

## Scope and decisions

The deliverable is an opt-in timing correction under **common amortisation**.
Each original signed caller-year coefficient is distributed over explicit
calendar years. No additional lifetime division or replacement multiplier is
introduced. Observations identify initial stock; declared survival, utilisation,
component and future-scenario assumptions produce annual 2022–2030 profiles.
Native exact-run IAM cohorts were not available. The two pinned local REMIND
3.5.2 scenarios therefore support labelled regional reconstructions, not claims
of exact native vintages. Water uses a separate replacement-only scenario.

| Pilot and method | Reviewed stock and role boundary | Cohort evidence | Real export/consumer evidence |
|---|---|---|---|
| [Passenger BEV](vehicle-pilot-method.md) | 628,318 known post-2009 UK cars; chassis, battery, maintenance and 17 parent end ports | 14 sensitivities per scenario; first-use proxy, unknown dates, annual sales, survival, utilisation and battery intervals explicit | 31 added activities; 210 signed annual port checks, 30 zero shifts; full ± demand in 2023/2030; warm cache confirmed |
| [Heavy trucks](vehicle-pilot-method.md) | 14,231 UK articulated diesel vehicles, 32–40 tonnes; manufacture, disposal and maintenance | 12 sensitivities per scenario; EURO6 inventory is a timing proxy, not the observed fleet class | Six added activities; 30 signed annual checks, six zero shifts; full ± demand in 2023/2030; warm cache confirmed |
| [PV](pv-pilot-method.md) | 363 US-WECC plants, 1,294.8 MW AC / 1,629.2 MW DC; parent, inverter, panels and occupation | Eight sensitivities per scenario, observed output weighting, replacement assumptions and AC/DC distinction | 63 added activities; 510 signed annual checks, 58 zero shifts; full 2023/2030 plus two waste-timing endpoints; warm cache and direct occupation dates pass |
| [Gas CCGT](ccgt-pilot-method.md) | 91 complete US-WECC blocks, 41,476.3 MW AC, excluding CHP | Seven sensitivities per scenario; conditional treatment of observed old stock and exact USA leaves | Three added activities; exact annual manufacture dates; full ± demand in 2023/2030 and warm cache pass |
| [Quebec potable water](water-pilot-method.md) | 45,114 km pipes and 974 storage assets; separate pipe/tank manufacture and disposal; original distribution losses retained | 13 sensitivities for each asset; unknown dates, bins, service weights, survival and replacement-only stock balance explicit | 36 added activities; 340 signed annual checks, 34 zero shifts; full ± demand in 2023/2030; separate confirmed warm-cache pulse checks |

Each package uses the complete 26,533-activity ecoinvent 3.12 cut-off source at
2022/2025/2030 anchors. Technology is constant to isolate timing and conservation.
This is not historical technology reconstruction or an IAM-transformed
background LCA. All 9,847 exported biosphere rows are checked for both demand
signs against an independent year-wise reference, alongside a graph operator
identity and full A/B rewrite equivalence.

| Pilot | Maximum absolute flow discrepancy | Maximum componentwise operator residual |
|---|---:|---:|
| CCGT | 2.22e-16 | <1.2e-16 |
| PV primary and endpoints | 2.67e-14 | 2.57e-16 |
| Trucks | 4.00e-15 | 3.04e-16 |
| BEV | 2.85e-14 | 1.83e-16 |
| Water | 7.60e-16 | 6.45e-16 |

No numerical tolerance was loosened to mask a changed quantity. A separate
one-shot sparse solve illustrates numerical conditioning; the principal
reference uses the same backend while independently assembling the calculation.
Legacy signed-reference errors are retained as diagnostics and separated from
timing differences. For example, old BEV battery disposal differs in quantity,
whereas parent manufacture plus disposal retains its source subtotal.

## Annual profiles, lifecycle and compatibility

The [profile contract](profile-resource-v1.md) defines exact caller/supplier
identities, units, consecutive annual service years, calendar event years,
provenance and resource hashes. The producer validates actual matched exchanges
and inventory context. The consumer requires `stock_vintage=True`, rejects
missing annual years and checks profile hashes even on warm matrix caches.
Foreground overrides take precedence without overwriting the base package cache.

[Conditional retirement and scoped lifecycle separation](lifecycle-and-cohort-method.md)
retain initial survivors, separate territorial exits from assumed physical
retirement, and prevent independent repeated vintage shifts. Marginal lifecycle
attribution requires invariant lifted coefficients across anchors. The rewrites
are deterministic; preserving Monte Carlo correlations is outside this claim.

Use the paired implementation code including premise `edc89b46` and TRAILS
`a3ef279`, or their reviewed branch descendants; the private archive will pin
exact delivered revisions. Premise uses Python 3.12 and this TRAILS revision
Python 3.11. There is no assigned public minimum release version yet.
An actual baseline-reader check at TRAILS `b7d1bb78` loads a corrected synthetic
package but produces the wrong annual dates/weights. Unknown metadata is not a
compatibility barrier. Older readers are unsupported for corrected packages.

The final audit found an adaptive-only score lookup using physical years absent
from the static inventory score table. The fix maps only score lookup to the
inventory year; physical graph years remain. Twelve new adaptive cases exercise
both signs, interpolation off/on, past/future events and strong frontier pruning.
The real full-LCI runs used fixed depth and their executed path is unchanged.

## Tests, sources and reproducibility

Final regression reports contain **123 passing premise tests** and **276 passing
TRAILS tests** (194 routing/lifecycle/static-score checks plus 82 loader, LCI,
cache, importer and temporal-distribution checks). Synthetic packages generated
by the actual premise writer cover annual non-anchor lookup, signed production,
lifecycle dates outside the inventory horizon, repeated/reversed requests,
multiple solvers, exact biosphere calendars and legacy controls. JUnit XML and
logs are retained locally for the final archive.

Acquisition/curation and annual generation live in `dev/stock_vintage/`, with
[hash-pinned sources](acquisition-manifest.csv), exact IAM leaves, source units,
region mappings, exclusions and rights. Four method documents describe every
pilot-specific command and assumption. The real exporters are
`export_bev_pilot.py`, `export_truck_pilot.py`, `export_pv_pilot.py`,
`export_ccgt_pilot.py` and `export_water_pilot.py`. TRAILS supplies corresponding
consumer checks and sensitivity/comparison reporters.

Local sensitivity outputs contain 126 CCGT, 144 PV, 468 vehicle and 234 water
case/year records. Figures were inspected. A separate real parent-calendar
comparison distinguishes the raw legacy distribution, actual legacy clamping
and corrected dates. PV endpoints verify unchanged role totals and unaffected
calendars. Illustrative emission plots select Carbon dioxide, fossil / air /
unspecified; they are not GWP or dynamic climate results.

The complete numerical legacy-table audit covers 987 rows / 964 unique
identities, including 23 duplicates. All 317 lognormal rows use uniform fallback,
including the two with maximum zero; all 670 triangular rows have independent
asymmetric-CDF diagnostics. Original table and runtime bytes remain unchanged.
Unscoped exchanges keep legacy status; only exact pilot contexts are revised.
This is not scientific approval or a full-database exchange audit of all groups.

A 20-case subprocess benchmark measures cold/warm loading and routing of every
legacy/corrected package, with confirmed cache reload and peak RSS through
routing. End-to-end full verifier wall times are retained separately and include
independent verification and concurrent load. Full-LCI peak RSS and isolated
native solve time were not instrumented. Bounded native streaming completes the
water comparison without materialising roughly 9.6 billion entries per demand.

## Relation to the original plan and remaining work

This is the bounded P0–P5 opt-in route; P5 explicitly permits limited pilot
coverage. The 23-group strategy and data search campaign remain the expansion
roadmap. Full scientific migration of every row, external provider requests,
heterogeneous individual-life allocation (P6), study/manuscript reassessment and
default promotion (P7) are separate tasks. No provider contact, purchase, merge
or publication has occurred.

The remaining completion step is durable private evidence preservation and its
verification. TRAILS `archive_private_evidence.py` requires all twelve corrected
full-LCI reports and all three regression suites to pass, checks source hashes,
and saves source/data/package artifacts, environment versions and clean code
revisions outside Git. Restricted raw inventories, IAM files and derived series
must remain local. The archive itself does not replace this methodological
review. The final evidence location and sign-off will be recorded after copying.
