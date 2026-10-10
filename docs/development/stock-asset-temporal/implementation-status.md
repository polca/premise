# Implementation and pilot evidence

Updated 10 October 2026. Work is in progress on `feat/stock-vintage-pilots` in
both repositories. No pilot is promoted and public defaults remain unchanged.
The planning branch remains an unchanged reference.

## CCGT real-inventory validation milestone

The [CCGT method](ccgt-pilot-method.md) documents a complete-block EIA reference
and exact REMIND leaf-variable reconstruction. Membership checks now examine all
three EIA-860 generator sheets. The accepted 91 blocks have 41,476.3 MW AC and
159,002,044.49 MWh of 2022 output; all began operation before the reference year.
Service weighting is observed output by commissioning cohort, with equal-capacity
weighting as a sensitivity. The 410.5 MW already older than the quartic curve's
support are preserved with an explicit, sensitivity-tested remaining-life proxy.

Both local SSP2-NPi2025 and SSP2-PkBudg650 files produce seven cases for every
service year in 2022–2030. Annual balance, output totals, full-load-hour limits
and five-year additions integrals are checked. Capacity-target and
reported-additions cases retain their discrepancy from the other constraint;
neither is represented as native IAM vintage output. The focused premise suite
now has 69 passing tests.

The actual premise exporter has created legacy and corrected packages from the
complete 26,533-activity ecoinvent 3.12 cut-off inventory, using constant technology
at anchors 2022/2025/2030. Three scoped CCGT activities and an explicit internal
zero-shift binding prevent a duplicate construction shift. This is a real
inventory timing/conservation experiment, not an IAM-transformed background LCA.
The restricted inventory extract, packages and IAM-derived series remain local.

TRAILS commit `d5dd3af` fixes the opt-in routing of suppliers with negative
reference production. The real cooling-tower residue market exposed the old
absolute-denominator error. Legacy results retain the old convention and its
conservation discrepancy is reported explicitly.

The corrected real CCGT case now passes all three bounded release requirements:
annual capital dates and quantities, internal zero shift, repeated and reversed
requests, both signs, 2023 with annual interpolation and a warm cache, and 2030
without interpolation. The graph identity has componentwise residuals below
`1.2e-16`; all 9,847 exported biosphere rows match an independently assembled
year-wise calculation within `2.22e-16`. That reference uses the same sparse
backend; the one-shot SuperLU comparison remains a separate conditioning
diagnostic. The focused TRAILS regression suite has 161 passing tests.

For a 2030 kWh, capital-attributed fossil CO2 is about `3.83763e-4 kg` in both
variants. Corrected timing has mean age 23.4754 years. The old raw construction
profile has mean age 16.9338 years, but legacy calendar clamping reduces the
reported no-interpolation mean to 7.8052 years. These are distinct effects.
A reproducible script generates the 126-row scenario/sensitivity table and
PNG/SVG/PDF figure. Restricted artifacts remain local. Final promotion review
and the other four pilot comparisons remain unfinished.

## Water-network observation-to-export milestone

The [water method](water-pilot-method.md) now separates Quebec pipe length from
water-storage counts using the same pinned Statistics Canada source. Its 2022
stocks are 45,114 km and 974 storage assets; unknown dates comprise 5.16% and
25.56%, respectively. Thirteen explicit annualisation, missing-date, survival,
service and future-stock scenarios generate 234 asset/case/year records.
No IAM water forecast is implied. The primary case holds each stock total
constant with inferred replacement additions and conditional retirement.

The actual inventory export preserves the tap-water market's 0.191 own-product
loss, giving net output 0.809 kg per activity. Scoped copies separate nine tank
material ports, two tank-disposal ports and 22 pipe/network-disposal ports. All
amounts, signs and geographic supplier identities remain. Zero shifts prevent
a second age or disposal offset. Both real packages use the full source database
and constant technology at 2022/2025/2030 under an explicitly non-IAM label.

The real consumer has passed the complete matrix identity for the rewrite,
signed annual pulses and all internal zero-shift links. Full temporal LCI and
legacy comparison are running at this checkpoint; the third water requirement
is not yet claimed complete. There are 77 passing focused premise tests and two
independent analytical tests protecting the new full-matrix verifier.

## PV observed-parent and active-component milestone

The [PV method](pv-pilot-method.md) now documents 363 complete EIA plants with
1,294.8 MW AC, 1,629.2 MW DC and observed annual generation, including fourteen
zero-output plants in stock. The two pinned REMIND scenarios each generate eight
cases across 2022–2030. The reference is service weighted; future trends remain
an explicitly labelled USA-to-WECC reconstruction.

The new active-component algorithm dates inverter manufacture and end of life
conditional on the component providing service. A thirty-service analytical
test recovers one initial and one replacement inverter without applying a second
amortisation factor. All 46 focused cohort, PV and existing power tests pass.
Source review separately identified panel handling losses and occasional
replacement, factory waste versus disposal, and conflicting packaging comments.
The real component export now preserves the full source system while separating
47 waste ports, two component-manufacture ports and direct land occupation.
Source inconsistencies remain explicit waste-timing endpoint sensitivities.
Matrix equivalence, 510 signed annual port checks and 58 internal zero shifts
pass. Full LCI now passes for both demand signs in 2023 with annual interpolation
and a confirmed warm-cache reload, and in 2030 without interpolation. All 9,847
biosphere flows agree with the independent year-wise calculation to a maximum
absolute error of `1.78e-14`; operator componentwise relative residual is at most
`2.57e-16`. Direct land occupation is in the requested service year for both
signs. Capital-attributed fossil CO2 remains `0.00855912127 kg/kWh`; the corrected
2030 parent mean age is 5.69750 years, versus legacy raw 13.50179 years (clamped
7.31900). Legacy retains its separately reported negative-production sign error.
The waste-timing endpoint comparisons remain outstanding, so the three PV
requirements are not yet claimed complete. The previous focused producer and
consumer suites passed 82 and 89 tests.

## Vehicle cohort reconstruction milestone

The [vehicle method](vehicle-pilot-method.md) fixes the bounded observed UK
populations, source survival semantics, service-index interpretation and battery
assumptions. Primary cohorts contain 628,318 known post-2009 BEVs (99.8941% of
observed BEVs) and all 14,231 selected articulated diesel trucks of 32–40 tonnes.
Unknown dates and the single pre-1980 truck have explicit policies and bounds.

Pinned EDGE code establishes an annual vehicle kernel distinct from the
power-capacity law; pinned reporttransport code identifies Sales as calendar-year
entrants. Exact-run native cohorts and parameter versions remain unavailable.
The two exact local REMIND scenarios generate 468 annual case/year records for
2022–2030, with independent stock/sales residuals and no silent unit conversions.
Territorial exits are separated from assumed physical retirement. Active
battery timing preserves the original 1.5 equivalent packs per nominal car life
without becoming a forecast of pack counts. The 61 focused cohort, vehicle,
PV and CCGT tests pass. The truck exporter now separates manufacture, disposal and maintenance, with
scoped event markets preventing repeated delays. Its 32 focused lifecycle and
vehicle tests pass. The real export adds six scoped activities; complete matrix
equivalence, 30 signed annual pulses and six zero shifts pass. Cache reload is
confirmed. Full 2023/2030 truck LCI and legacy comparisons are running. The car
exporter and full vehicle consumer checks remain required. Neither vehicle
pilot is promoted.

## Implemented software foundation

TRAILS commit `7af0bd1` adds an opt-in annual profile resource. Profiles contain
absolute event years and exact caller/supplier bindings; they resolve at the
requested service year independently of matrix anchors. Common amortisation
preserves the signed caller-year exchange coefficient. Changed resource bytes,
missing years, invalid weights, inconsistent units and binding conflicts fail.
The resource is checked on cache loads, and foreground overrides take precedence.
The work also fixes importer handling of supplier index zero and replacement of
intermediate annual rows.

The companion premise implementation accepts `stock_vintage_profiles` on
`TrailsDataPackage`. It validates source version/system model, scenario identity,
annual records and exact matched inventory exchanges; explicit bindings precede
legacy supplier rules. Temporal amount mode survives compact/legacy inventory
stores, checkpoint reloads and matrix export. The package includes a deterministic
JSON resource and SHA-256. Corrected packages carry no blanket CC0 licence claim.

The new consumer requires `stock_vintage=True`. Older readers can ignore unknown
metadata and are unsupported for corrected packages; no released minimum version
has yet been assigned. The branch implementations must be used together.

Validation completed:

- 132 focused TRAILS tests, including actual routing/LCI and an independent
  static sparse solve with unchanged technology.
- 28 premise tests covering temporal rules and the producer contract, including
  both inventory stores with and without checkpoint persistence, plus numeric
  ODS curation and explicit missingness checks.
- A ZIP produced by the actual premise matrix writer and package builder passed
  eight signed annual routing/LCI cases in TRAILS (2020, 2021, 2022, repeated and
  reversed requests). Global index reordering and an interpolated non-anchor
  service year are included. This is a synthetic fixture, not an empirical LCA.

Premise requires Python 3.12; this TRAILS revision supports Python 3.11. The
cross-environment check therefore uses two interpreters:

```sh
# From premise, with this worktree importable in the premise environment:
python -m pytest tests/test_stock_vintage.py tests/test_trails_temporal.py
PYTHONPATH="$PWD" python tests/test_stock_vintage.py /tmp/stock-roundtrip

# From TRAILS, in its Python 3.11 environment:
python dev/stock_vintage/check_roundtrip.py \
  /tmp/stock-roundtrip/synthetic_stock.zip --output /tmp/stock-roundtrip/checks.json
```

## Conditional retirement and lifecycle integration

The new [method and evidence](lifecycle-and-cohort-method.md) documents
`stock_cohorts` (observed initial survivors, conditional survival, annual balance,
service weighting and conditional retirement) and `stock_lifecycle` (scoped
service/capital copies, signed embedded-disposal separation, zero-shift
wrappers and cross-anchor quantity checks). The annual convention is end-year
stock; retirements in `(t,t+1]` occur in year `t+1`. No empirical lifetime
default is introduced. The rewrite is deterministic and does not preserve
Monte Carlo coefficient correlations.

Additional validation:

- 163 focused TRAILS tests pass across stock/lifecycle routing, legacy routing,
  LCI, interpolation, caches and importer behaviour. The combined run also
  required isolating an existing test that overwrote a shared matrix fixture.

- 60 premise tests pass across cohort evolution, lifecycle structure, actual
  export, producer validation, curation and legacy temporal rules.
- An actual exported lifecycle fixture passes 32 signed annual calendar/LCI
  cases in TRAILS, with direct/Brightway solvers, root/non-root accounting,
  interpolation disabled/enabled, and repeated/reversed service years.
- Two real static rewrites preserve all 3,297 biosphere flows in the complete
  26,533-activity ecoinvent 3.12 cut-off system. Maximum absolute errors are
  `2.22e-15` for car chassis and `2.22e-16` for the lorry. The report distinguishes
  processed matrix precision from raw dataset metadata.
- The lifecycle fixture exposed graph and frontier year-clamping bugs. Stock
  packages now preserve physical event years outside the background horizon;
  only coefficient selection maps to available matrices. Storage also covers
  successive routed offsets. Legacy package behaviour is retained.

This is structural and synthetic evidence. Empirical survival selection, service
weighting, remaining component boundaries and real corrected timing still need
the pilot-specific checks below.

## Public observations curated

`dev/stock_vintage/curate_observations.py` verifies pinned input hashes and retains
unnormalised cohort stocks and unknown mass. It does not apply survival to
already-observed surviving stock, infer lifetimes, impute unknown years, or
generate approved exchange profiles.

```sh
python dev/stock_vintage/curate_observations.py \
  --input-dir /path/to/local/downloads --output /tmp/stock-observations.json
```

The current extraction yields 43 observations: UK cars/HGVs for 2014–2025,
a narrower articulated 32–40 tonne diesel subgroup for 2015–2025, Quebec pipes
for 2020/2022, and two US-WECC power technologies for 2020–2022.
Raw downloads stay outside the repository. The acquisition manifest records URLs
and hashes. The table below reports 2022 observations, not validated service
distributions.

| Pilot observation | Selected stock | Unknown cohort share | Remaining conversion |
|---|---:|---:|---|
| UK battery-electric passenger cars | 628,984 vehicles | 0.0568% | First-use year as manufacture proxy; mileage weighting; chassis/battery/replacement boundaries |
| UK heavy goods vehicles (broad comparison) | 536,519 vehicles | 2.3759% | Replaced as primary candidate by narrower subgroup below |
| UK articulated diesel goods vehicles, 32–40 tonnes | 14,231 vehicles | 2.6000% plus one pre-1980 vehicle | EURO-class correspondence and tonne-kilometre weighting remain |
| US-WECC operating PV | 27,649.8 MW AC | 0% | Technology/mounting correspondence, output weighting, AC/DC basis and replacements |
| US-WECC operating natural-gas CCGT excluding CHP | 50,099.8 MW AC | 0% | Generator-to-plant/component boundary and output weighting |
| Quebec public potable-water pipes | 45,114 km | 5.1603% | Annualisation of construction bins, pre-1940 tail, unknown dates, service weighting |

UK `VEH1111` contains full numeric ODS values underneath rounded displays,
including cells displayed as `[low]`. Multiplying the stored values by 1,000
gives integer counts. Known cohorts plus the explicit unknown category exactly
reconcile to the reported total in every selected observation. Imported and
pre-1973 re-registered vehicles can have unknown first use; they are retained.
The source is [DfT/DVLA VEH1111](https://www.gov.uk/government/statistical-data-sets/vehicle-licensing-statistics-data-tables).

For power, EIA-860 plant identifiers join generators to NERC region `WECC`.
Only status `OP` enters the observed operating stock; standby/out-of-service
capacity is reported separately. The survey covers plants of at least 1 MW
combined nameplate capacity, so these are utility-scale candidates, not a
residential rooftop PV distribution. Plant and generator files for all three
years have been downloaded and pinned. A 2022 EIA-923 generation file is also
local; output joins and their coverage remain to be assessed.
[EIA-860](https://www.eia.gov/electricity/data/eia860/),
[EIA-923](https://www.eia.gov/electricity/data/eia923/).

For pipes, the published CSV says `UOM=Number`, but questions 13 and 15 of the
[2022 survey](https://www.statcan.gc.ca/en/statistical-programs/instrument/5173_Q12_V3)
specify kilometres for linear potable-water assets. The selected aggregate is
used once, without adding its local/transmission/unknown-diameter children.
The 45,114 km is the sum of mutually exclusive construction bins, not yet an
independently reconciled total. The source flags, coordinates and unknown bin
are retained. The survey's inclusion of federal organisations changes in 2022;
the 2020–2022 difference must not be interpreted as pure additions/retirements.
[Table 34-10-0289-01](https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=3410028901).

See [pilot boundary refinements](pilot-boundary-findings.md) for reproducible
truck selection and the power-service audit. The audit narrows CCGT candidates
to 41,476.3 MW in complete single-cohort blocks and PV to 1,844.5 MW of fixed-tilt
crystalline-silicon capacity. These are candidates with explicit exclusions;
output weights, component boundaries and technology proxies still need validation.
The power audit resolves five numeric-padding joins and rejects ambiguous IDs.

## Real inventory boundaries found

Read-only inspection used the local `ecoinvent-3.12-cutoff` Brightway database.
The following are candidates, not blanket group mappings:

- Electric-car transport consumes chassis **kilograms**, battery kilograms and
  maintenance units separately. It also has a signed used-battery exchange.
  Chassis manufacture embeds a dismantling service. First-use cohorts do not
  establish replacement-battery dates.
- Heavy-lorry transport consumes a 40-tonne lorry through a market; manufacture
  embeds a signed used-lorry exchange. The HGV observation is broader than this
  lorry size and any EURO-specific transport activity.
- US-WECC 570 kWp PV and CCGT service activities have explicit capital inputs.
  PV construction includes initial/replacement inverter quantities, with disposal
  embedded in component inventories. CCGT's reviewed direct construction inputs
  have no explicit disposal port. Further component tracing remains.
- The Quebec **tap-water market**, rather than treatment-plant production,
  consumes the water-network input measured in kilometres. Its supplier market
  leads to network construction in RoW. This is a declared manufacturing proxy,
  distinct from the Quebec service geography. Network construction includes
  tanks as well as pipes and embeds material disposal; those boundaries now
  have explicit follow-up actions in the refinement document.

Markets cannot all be classified as transparent wrappers: the tap-water market
is itself a service caller with a genuine capital input. Conversely, the
capital market-to-manufacturer edge must not apply another fleet-age shift.
Manufacturer inputs representing new components need construction-time rules;
capital machinery used to manufacture those components is a separate stock.

## Release gates and next work

All five pilots still require evidence for all three gates:

1. **Roles and lifecycle boundaries.** Inventory audit is underway. Car/truck
   end-of-life is demonstrably embedded in manufacture. Scoped separation now
   passes real static equivalence, and synthetic disposal is conditional on
   survival to service. Remaining new/replacement-component boundaries and
   empirical timing still require review; do not duplicate the stock shift.
2. **Stock/service/future balance.** Initial public observations are curated.
   Unknown dates, bins and geography/technology proxies need explicit baseline
   assumptions and sensitivity cases. Service weighting and REMIND stock/
   additions/survival reconstruction remain to implement. Native exact-run
   cohorts have not been found; no exact IAM reproduction claim is made.
3. **Real empirical round trip.** Synthetic producer/consumer checks pass.
   Real-database legacy/corrected comparisons, amount reconciliation, annual
   cache/interpolation checks and relevant lifecycle chronology remain to run.

A validated parser or a normalised curve does not close a scientific gate. The
goal remains active until each selected pilot passes all three requirements.

## Source rights

Attribution for observations: Department for Transport/DVLA, VEH1111, accessed
9 October 2026; Statistics Canada, Table 34-10-0289-01, accessed 9 October 2026;
U.S. Energy Information Administration, Form EIA-860 final data for 2020–2022,
accessed 9 October 2026. Curation and modelling choices are ours and do not imply
provider endorsement.

DfT material is under the [Open Government Licence](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/)
except where stated otherwise. Statistics Canada observations remain under its
[Open Licence](https://www.statcan.gc.ca/en/terms-conditions/open-licence).
EIA's [reuse policy](https://www.eia.gov/about/copyrights_reuse.php) permits reuse
of its government data with acknowledgement; third-party material can differ.
These permissions do not apply to the ecoinvent inventory or restricted IAM
inputs. Those remain local and are not redistributed with code or reports.
