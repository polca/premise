# Review of TRAILS exchange timing and temporal data

The approach has a useful foundation, but it is not yet a reliable general method for assigning exchange times. The largest problems are the conflation of asset age with service life, supplier-only classification of exchanges whose timing depends on their use, and a mismatch between the CSV distribution conventions and the TRAILS reader. Improving individual lifetime estimates before resolving these issues would produce more precise inputs to an inconsistent model.

This review recommends retaining relative exchange timing, explicit normalized profiles, and the existing inventory-store/export integration. It recommends changing the assignment model to distinguish **what the exchange represents, which event its time is relative to, and whether the caller represents new construction or current operation**.

**Scope and evidence.** Reviewed `premise/trails.py`, all 9,672 CSV rows, the matrix export, the existing temporal tests, the stock-asset review/fill scripts and selected underlying lifetime records. Also inspected the local downstream TRAILS parser and distribution evaluator, because copying correct-looking metadata is insufficient if it produces a different distribution at calculation time. The premise revision is `82f5c5d063473fd3984e818d11a54962989b1130`; the downstream TRAILS revision is `b7d1bb78f0388ba3916270e6654353e01624807e`.

Inventory coverage was screened in the Brightway project `ecoinvent-3.12-cutoff`, using the source database `ecoinvent-3.12-cutoff` (26,533 activities) and the existing scenario database `premise-simapro-comparison-remind-SSP1-PkBudg1000-2050-ei312-20261007` (42,758 activities). These are existing local snapshots, not newly generated scenarios. Coverage counts classify their stored exchanges against the current rules; small probes additionally execute the actual assignment method. No source inventory, production module, or temporal CSV was modified. No LCIA change is claimed or quantified.

## 1. Findings to resolve first

| Priority | Finding | Evidence and consequence |
|---|---|---|
| Critical | All 317 stock-asset lognormal rows become uniform in the local downstream reader | The CSV gives negative age offsets; the reader evaluates lognormal density only at positive offsets and falls back to uniform when none exist. `loc` and `scale` then have no effect. |
| Critical | Exchange timing is inferred from the supplier, without distinguishing an incorporated component from capital used to manufacture the caller | An electrical installation incorporated into a newly assembled PV system is assigned another 1–30-year historical age distribution. A factory used to manufacture a bus can legitimately have its own age distribution. The present rule cannot distinguish these two relationships. |
| High | Current operating services inherit future maintenance schedules | Maintenance allocated to one kilometre of current passenger-car transport is spread across years 0–15. This treats current service provision as if it were commissioning a new vehicle. |
| High | Recycling products can be supplied from the future | `market for copper, cathode` receives its recovered-copper input from `treatment of used cable` at +2 years. This is a current recovered-material supply, not disposal of the market's asset. |
| High | Real end-of-life flows can remain at construction time | Solar-thermal collector construction sends reinforcement steel to treatment suppliers tagged `throughput_process`; those exchanges receive no delay. Tagging all treatment suppliers as end-of-life would introduce new errors for manufacturing waste and secondary-material production. |
| High | Perennial plant lifetime is used as carbon uptake duration | Pineapple gets uniform uptake over −20 to 0 years despite a source-inventory cropping cycle of 24 months. Almonds, grapes, bananas and other products also inherit 20-year profiles that need carbon-pool-specific justification. |
| High | Forestry geography and management are lost | Australian eucalyptus rotation data, 33 years, are applied by name/product to Brazilian and Thai datasets. A Brazilian source explicitly represents 14 years with two harvests. |
| High | Long-term emissions are treated inconsistently and compressed into an arbitrary horizon | All groundwater long-term flows can receive profiles, while long-term air emissions have no rule. The groundwater profiles redistribute the entire amount into 100–1,000 years without establishing that the source inventory amount belongs to that interval. |
| High | Duplicate rows make lifetimes order-dependent | 379 duplicate name/product groups; 134 groups differ in lifetime or tag. Reversing the file changes 131 loaded lifetimes. |
| Medium | The reader does not implement a conventional asymmetric triangular distribution | 653 stock rows and one biomass row are asymmetric. The reader uses a symmetric tent clipped to the support, changing the requested shape and mean. |
| Medium | Fractional-year export is not preserved | All 90 discrete forestry rows have fractional offsets. Reader rounding can merge alternating bins: the 30-bin azobé profile becomes 16 occupied year bins. |
| Medium | Invalid and stale metadata can silently survive | Real-loader probes accept negative/NaN lifetimes, reversed bounds, a mode outside support, and inconsistent biomass offset/weight lengths. Removing a stock rule and rerunning leaves the old profile on the exchange. |

The current 13 temporal tests pass. They verify useful behaviours, including metadata survival through inventory stores and matrix export, but do not test most of the semantic or downstream numerical failures above.

## 2. What the module actually does

The reference point is the caller activity's time. A technosphere profile moves the supplier and, through downstream traversal, the supplier's upstream requirements. A biosphere profile moves only the elementary exchange. This difference makes the placement of a profile as important as its parameters.

| CSV tag or exchange class | Current implementation | Assessment |
|---|---|---|
| `stock_asset` | Match supplier name and reference product; copy its profile to every matching technosphere exchange | Appropriate for the historical capital share of current output only if the profile represents the supplying capital stock. Inappropriate as a universal rule for buying or assembling new assets and their components. |
| `maintenance` | Match supplier; use a uniform distribution from 0 to caller lifetime | Potentially appropriate for an aggregated lifetime maintenance bundle relative to commissioning. Usually inappropriate for maintenance already allocated to a unit of current operation. |
| `end_of_life` | Match supplier; put all mass at caller lifetime +1 year | Potentially appropriate for asset disposal embedded in a construction inventory, provided the caller lifetime is that asset's life. Incorrect for many treatment-stage and recovered-product calls. The universal extra year is unexplained. |
| Nested maintenance/end-of-life calls | Put the nested event at offset 0 if both caller and supplier are recognised lifecycle services | A useful guard against repeated lifetime delays. It depends on both activities being tagged and does not propagate through ordinary market wrappers. |
| `biomass_growth` | Match caller name/product and only the biosphere name `Carbon dioxide, in air` | Good to isolate uptake rather than move every exchange in the process. The same flow name can represent harvested biomass, changing stocks, accounting corrections or direct capture; their timing differs. |
| `long_term_emission` | Match flow name, compartment, subcompartment and unit; resolve by priority then specificity | A useful selector mechanism. Current profile choices lack sufficient process-specific evidence and omit long-term air. |
| `throughput_process` | Load any lifetime for subsequent maintenance/disposal logic; ignore its distribution fields | Current operation often should remain at the reference time. However, 55 rows contain profiles that are never used, and a plant lifetime is not necessarily the right time scale for the caller's particular exchange. |
| `market`, blank, unknown supplier | No direct assignment | Markets generally should be instantaneous routing nodes; this must be explicit and preserve event context. Unknown must be distinguishable from deliberately instantaneous. |
| Production exchanges and other exchange types | Skip | Keeping the reference production at zero is normally correct. Multi-output products with genuinely different production dates need explicit modelling; supported nonstandard consumption/substitution types need a defined policy. |
| Other biosphere exchanges | Leave existing timing unchanged; otherwise no added timing | Correct for many immediate emissions. Insufficient for known release-over-time, stock changes, lifetime land occupation and lifetime leakage. |

Relevant implementation locations: [loader](/Users/romain/Github/premise/premise/trails.py:409), [dataset lifetimes and stock parameters](/Users/romain/Github/premise/premise/trails.py:518), [assignment pass](/Users/romain/Github/premise/premise/trails.py:1291), [stock/lifecycle decisions](/Users/romain/Github/premise/premise/trails.py:1586), and [matrix temporal fields](/Users/romain/Github/premise/premise/export.py:1431).

## 3. Lifetime, age and event time need separate meanings

An asset's service life, its age in the current fleet, its remaining life, and its construction lead time are different quantities. A single `lifetime` plus an “age distribution” cannot safely substitute for all four.

Let `I(t−a)` be commissioning in year `t−a`, `S(a)` the probability of surviving to age `a`, and `u(t,a)` its utilization. A defensible starting point for the age distribution of capital supplying current output is:

`p_age(a | t) ∝ I(t−a) × S(a) × u(t,a)`.

The normalization and weights must match the contribution being allocated: asset numbers, capacity, output or infrastructure burden are not interchangeable. For constant inflow, deterministic life `L`, and constant utilization, the age distribution is uniform on 0–L and has mean L/2. That is an explicit stationary-stock assumption. A growing stock has more young equipment; an old declining stock can look very different. Cohort/survival modelling provides a consistent link between inflows, stocks and retirements. [Lifetime modelling](https://link.springer.com/article/10.1111/jiec.13586), [dynamic stock reconstruction](https://doi.org/10.1016/j.resconrec.2025.108198).

For a surviving asset already aged `a`, future retirement should follow the conditional remaining-life distribution. With continuous lifetime density `f`, this is `f(a+r)/S(a)` for remaining time `r ≥ 0`. A deterministic life gives remaining life `L−a`, not `L`. Construction and retirement need to refer to the same cohort; independent profiles can break that relationship.

There are at least three valid reference contexts:

1. **New asset commissioned now:** construction and initial components at or shortly before zero; maintenance/replacements during use; disposal after service life.
2. **Current output from an existing fleet:** operational fuel and emissions now; historical capital burden according to the relevant vintage mix; future disposal only when the chosen accounting convention allocates that burden to current output, with consistent cohort timing.
3. **A lifecycle event occurring now:** maintenance, treatment or recycling substeps remain close to now. Their own process capital may have historical vintages, but the event does not restart the serviced asset's lifetime.

The current CSV does not declare which context applies. `mean_age` is optionally read into stock parameters but is absent from the CSV and is never used by the assignment logic. `loc` is a mode for triangular distributions and a median under the plotting code's lognormal convention; it is not generally an average age. For a triangular distribution, mean `(minimum + mode + maximum)/3` differs from its mode. For an untruncated lognormal using median `m` and log-space deviation `σ`, mean is `m × exp(σ²/2)`; truncation changes it again.

A survey mean age must therefore not be inserted into a service-life field, nor used as a median or mode without an explicit conversion. The stock review's vehicle group cites ACEA fleet age, which is measured age of vehicles still in use, not their lifetime at retirement. [ACEA age tables](https://www.acea.auto/files/ACEA_Report_-_Vehicles_on_European_roads_2025.pdf).

## 4. Numerical distribution contracts are currently incompatible

**Lognormal.** The development plotter interprets negative `loc` as minus the median age and `scale` as geometric standard deviation, so log-space deviation is `log(scale)`. The downstream reader evaluates positive time offsets, treats positive `loc` as a median and `scale` directly as log-space deviation. These are two separate incompatibilities. Fixing the sign alone would leave the scale mismatch.

For the passenger-car row at CSV line 3161, lifetime is 14.74, `loc = −12.28333333`, `scale = 1.5`, and bounds are −15.28333333 to −1. Under the plotting convention the truncated continuous mean offset is approximately −10.46 years. The local reader produces uniform integer offsets −15 to −1, with mean −8 years. Neither should be described as an empirically validated fleet age just because the lifetime has a cited source.

**Triangular.** The reader's `max_distance − abs(offset−loc)` formula has equal slopes on each side, then clips to the requested support. A standard asymmetric triangle needs different slopes between the two bounds and the mode. The sewer-grid example at line 8061 has bounds −100 and −2, mode −60: intended continuous mean −54 years; current discrete reader mean approximately −57.79. Some additional differences come from integer discretization. `scale` is ignored for type 5, although most such rows populate it.

**Time resolution.** Matrix export preserves fractional offsets, but the local reader rounds explicit pulses to integers and truncates support bounds toward zero. Half-year values use Python's round-to-even rule. Azobé's 30 annual midpoints become 16 occupied years. This preserves total mass after aggregation but changes annual timing. A 1.5-year UV-lamp profile also collapses to one year. Declare an annual-bin convention and integrate density over bins; retain fractional event dates or split between adjacent bins when the mean must be preserved. Avoid a mixture of midpoint values, inclusive endpoint sampling, truncation and arbitrary +1 offsets.

![Distribution conventions compared with the local reader](../../results/trails-temporal-review-20261008/temporal_profile_discrepancies.png)

These findings concern the inspected local TRAILS revision. The reader code is outside `premise/trails.py`, but its behaviour determines what this module's exports mean. See the [lognormal evaluator](/Users/romain/Github/trails/trails/temporal_distributions.py:161), [triangular evaluator](/Users/romain/Github/trails/trails/temporal_distributions.py:77), and [pulse parser](/Users/romain/Github/trails/trails/datapackage.py:363). A shared, versioned distribution schema or a canonical explicit-pulse export would remove this ambiguity.

## 5. Exchange placement and supply-chain consistency

**Components versus manufacturing capital.** The source PV installation consumes one electrical installation as an incorporated component. Both are tagged stock assets. Its internal component exchange receives an additional historical profile over −30 to −1 years. If the parent installation was already shifted to its construction year, this makes the component older again. Component incorporation generally belongs near assembly, with a manufacturing lead time if justified. Conversely, bus production's small input of road-vehicle factory legitimately represents use of pre-existing manufacturing capital. A blanket “never shift stock-to-stock exchanges” fix would therefore also be wrong.

**Markets are routing nodes, sometimes with their own infrastructure.** A PV equipment market's edge to its producer should not independently invent a fleet history when the demand is for newly commissioned equipment. But the oil-distribution infrastructure inside an oil market may reasonably receive a capital-vintage profile. Timing the purchased product and timing the market's own infrastructure require separate rules. The 984 baseline stock-supplier calls under markets include both kinds; the count is an exposure count, not 984 confirmed errors.

**Maintenance during current operation.** The real assignment probe applies uniform 0–15 years to the passenger-car maintenance input of current transport. The reference output is a kilometre, not one new car's lifetime service. As a default, maintenance already allocated per kilometre should occur near the service year. A maintenance bundle embedded in a new-asset inventory may instead need a future schedule. Scheduled replacement of a battery, inverter, filter or stack should use component-specific intervals or survival rather than a universal uniform curve.

**Waste and secondary products.** The same treatment activity family can consume end-of-life waste, consume manufacturing scrap, or supply a recovered coproduct. CSV lines 5689 and 5690 label both used-cable treatment and its copper output `end_of_life`. The copper market consequently gets recycled copper at +2 years, from the market's heuristic one-year lifetime plus the universal extra year. Treating this positive recovered-material supply like the future disposal of an owned asset violates the caller's product timing.

At the other extreme, reinforcement-steel recycling/disposal embedded in a solar-thermal collector construction inventory remains at the construction event. Its exchange comments describe treatment of the structure's steel. These are strong candidates for explicit retirement timing. Manufacturing scrap sent to the same treatment suppliers should remain at manufacturing. Negative exchange amounts are not enough to decide which case applies; waste direction, reference product, allocation and exchange role matter.

**Nested events.** Keeping substeps of an already dated disposal event at offset zero is sensible. The current exception only recognises direct calls where the caller is tagged maintenance/end-of-life. A market wrapper tagged throughput can reintroduce a new lifetime delay. Do not solve this by giving every treatment process zero lifetime: the role of the incoming edge must carry the event context through routing and processing.

**Hybrid activities.** Seven stock rows explicitly bundle construction with maintenance or operation, including aquaculture cages, fish-processing plants and a LiS battery factory. Shifting the entire supplier back to construction also shifts its bundled operating or maintenance inventory unless it is decomposed. Some database aggregation limits cannot be resolved by attaching one distribution to an incoming exchange.

## 6. Lifetime and age data review

The CSV contains 987 stock rows representing 964 unique name/product keys; 452 biomass rows represent 430 keys. There are 7,840 throughput rows, 170 end-of-life rows, 63 maintenance rows, 40 market rows, 105 blank-tag rows and 15 long-term selectors. Empty lifetime cells on event suppliers are not automatically defects: the event's time often belongs to the caller/edge context.

| Family or example | Current data | Review judgement |
|---|---|---|
| Passenger cars | 154 stock rows use 14.74 years; the inspected car row uses a negative lognormal profile | The bundled CIRCOMOD workbook, `Data!129`, explicitly records a global private-car lifetime of 14.74 years with Weibull lifetime metadata. This can inform service life. It does not establish a lognormal in-use age distribution, a common age across powertrains, or a common age in 2020 and 2050. |
| Fleet defaults | Vehicle group sources include ACEA average fleet age | Retain as geographically and temporally scoped age evidence. Do not treat the age of survivors as retirement age. Young EV/fuel-cell fleets should not automatically inherit conventional-fleet vintages predating their deployment. |
| Wind turbines | Often 20 years; a 2.3 MW precast-concrete-tower row says “~25 years” in notes but stores 20 | The separate review contains direct source-inventory evidence for 20 years in this example. The numeric choice is defensible for that inventory, but the runtime note is stale and does not explain the decision or geographic transfer. |
| PV systems | Usually 30-year systems, with some components separately tagged or left as throughput | A reasonable system-life baseline is not a reason to shift every component by a system-age profile. Distinguish original components, replacements and manufacturing plant capital. IEA PVPS uses separate assumptions for modules, residential inverters, utility inverters and structures. [PVPS guidelines](https://iea-pvps.org/wp-content/uploads/2020/07/IEA_Task12_LCA_Guidelines.pdf). |
| Nuclear power | Plant construction has 60 years; several electricity-production callers have 30 years | These may refer to different vintage/source assumptions. If both control events in the same asset chain, they must be reconciled. In the screened scenario, EPR electricity production lacks the lifetime required by the current disposal rule. |
| Chemical factory | Generic factory 80 years; organic-chemical factory 50 | The review assigns the generic factory to the building group and uses an 80-year group default without direct or IEDC evidence. A civil shell's life is not necessarily the life of process equipment. Split components or document a weighted proxy; do not present 80 years as measured factory service life. |
| Roads and civil structures | Typical road 40; long-lived structures up to 150–200 | Separate pavement renewal from base/substructure and major civil works. Long calendar lives may be defensible for civil structures, but not as interchangeable timing for maintenance, replacements and initial production. |
| Short-lived components | Filters around 2 years; UV lamp 1.5 | Plausible replacement-scale assumptions require operating-hours evidence. Annual integer handling currently prevents the precision implied by 1.5 years. Initial installation and later replacement are separate events. |
| Annual crops | 303 biomass rows have lifetime 1; mostly uniform −1 to 0 | A coarse annual default is defensible if its reference is harvest and its temporal resolution is stated. It does not capture seasonal uptake or a specific growing season. |
| Fruits and perennial crops | 24 biomass rows use 20 years, including pineapple, banana, almond, apple and grape | High-priority correction/review. Plant/plantation persistence is different from the age of carbon in harvested fruit. The pineapple source explicitly says a 24-month crop cycle. Separate harvested-product carbon, perennial woody stock and establishment effects. |
| Forestry | Species profiles include 27.5, 30, 33, 61, 83, 100.5, 115 and 123.5 years | Better traceability than most other families, but averages of heterogeneous regional rotations are not measurements at each activity location. Preserve the source range, species and management type. |
| Eucalyptus | 33 years from Australian records applied to planted Brazilian and sustainable Thai datasets | Confirmed source mismatch. Brazilian planted-forest documentation specifies a 14-year activity with two harvests; model the harvest/coppice sequence and output allocation rather than replacing it with one 33-year rotation. |
| Mixed forestry coproducts | Some sawlogs receive 40 years, while pulpwood/wood chips of the same named activity receive 15 | Requires evidence of thinning, different cohorts, or distinct carbon pools. If outputs share the same harvested stand, product identity alone does not justify different uptake histories. |
| Maintenance and disposal | Uniform service-life timing and deterministic lifetime +1 | These are modelling assumptions, not measured maintenance/retirement distributions. Retain only where the reference context makes them meaningful. |

There are 279 stock rows whose historical support extends beyond the scalar `lifetime`. That is not inherently invalid if lifetime means a mean with a survival tail. It is inconsistent to use that same scalar as a deterministic retirement date without explaining how older surviving assets are treated. A field must explicitly mean mean service life, maximum age, design life, observed age or rotation time.

**Provenance and precision.** The stock review contains direct inventory evidence, IEDC records and group defaults. Its 1,004 rows use preliminary lifetime bases of 313 direct-inventory, 343 IEDC-median, 337 group-default and 11 inferred-neighbour values. These counts describe that review artifact, not independently validated final runtime values. The fill script copies suggested numeric parameters but does not systematically copy their supporting sources or refresh `param_notes`/confidence. This explains why some current numbers and old notes disagree. The car note still mentions sigma 0.55 while `scale` is 1.5.

Keep source record IDs, geography, source period, parameter meaning, uncertainty and reviewer decisions with each released value. A model-generated plausibility judgement is a useful screening step, not an independent empirical source. Do not retain ten decimal places from a group-level proxy as if they were measurement precision. Confidence scores currently have no operational effect and should not be interpreted as calibrated probabilities.

## 7. Biomass and other delayed biosphere flows

**Harvested carbon versus vegetation stock.** The present biomass rule shifts only atmospheric CO₂ uptake, which is preferable to moving all inputs and emissions in a crop dataset. Its applicability still depends on what that uptake represents. IPCC guidance distinguishes annual harvested biomass from carbon accumulating in perennial woody stocks; one plantation age cannot date both pools. [IPCC cropland guidance](https://www.ipcc-nggip.iges.or.jp/public/2006gl/pdf/4_Volume4/V4_05_Ch5_Cropland.pdf). For fruit, a seasonal profile with explicit treatment of reserve-carbon contributions is a more defensible starting hypothesis than spreading all harvested carbon over 20 years. Establishing an orchard is a different reference event again: future tree growth cannot automatically be represented as past uptake before establishment.

**Forestry model transfer.** The nonlinear forestry profiles and their documented source ranges are a useful improvement over a generic uniform rotation. The supporting model was evaluated primarily against even-aged, single-species planted stands. That does not establish applicability to every sustainably managed, mixed, tropical or selectively logged forest. [Lancz, Ghose and Pizzol](https://link.springer.com/article/10.1007/s11367-026-02695-0). Match species, country and management, and record fallback scope explicitly. A rotation chosen from a regional median should remain a scenario assumption where the inventory describes different harvesting practices.

**Cutting cycle is not tree age.** The azobé rows cite Biwolé et al. through ALIGNED and use 30 years for the complete uptake profile. The underlying study models selective logging and timber-stock recovery over successive 30-year cutting cycles, with trees already present in the initial stock. It does not establish that all carbon in a harvested tree was fixed during the preceding 30 years. My inference is that applying this cutting interval as a seedling-to-harvest growth duration is unsupported without a separate stand/cohort argument. This needs a substantive data correction or justification, in addition to fixing its rounded pulses. [Biwolé et al. (2019)](https://orbi.uliege.be/bitstream/2268/243504/1/Biwole_et_al_2019.pdf).

**Initial carbon convention.** All 90 forestry profiles match a construction that puts the sigmoid's initial standing carbon into the first uptake bin. They do not exactly match simple normalized annual increments from `C(0)` to `C(T)` as the documentation states. With the documented sigmoid, this initial pool is 1% of the final stock. The maximum individual weight difference from the documented increment formula is about 0.01. Decide whether initial seedling carbon is inherited from a nursery, fixed before the rotation, or deliberately included in the first-year uptake; document and test that decision to avoid duplication.

**Missing biomass rules.** The baseline contains 933 `Carbon dioxide, in air` exchanges: 649 match biomass rules, while 284 do not. Unmatched examples include rape seed, sugarcane, palm fruit bunches, land-use change and coproduct inventories. The scenario contains 2,106 such exchanges, of which 1,141 match and 965 do not. These are screening counts, not counts of errors: direct-air capture should generally remain at capture time, and accounting corrections should follow the physical carbon pathway rather than being blindly assigned a growth profile. Investigate unmatched agricultural/product-carbon flows by carbon balance and exchange provenance.

**Release after storage.** Dating uptake alone is insufficient when carbon remains in wood products, landfill, compost, soil or biochar before release. The module has no general short-/medium-term release rule for these pools, no explicit concrete-carbonation timing, and no general geological-storage leakage model. Appropriate profiles must follow the pool-specific release mechanism and existing inventory scope. Do not add uptake or decay a second time where the inventory has already represented it through upstream/downstream processes.

**Other elementary flows.** Lifetime land occupation, operational refrigerant leakage and dismantling losses are additional candidates. Current operation should normally keep its operational emissions at the reference year; a construction inventory bundling thirty years of occupation or leakage needs a use-phase schedule. Land transformation is an event; land occupation is area-time; neither should simply be assigned the same normalized time profile without respecting its unit and accounting basis. The baseline scan found 7,148 land-occupation exchanges, but did not establish that all require redistribution.

## 8. Long-term emissions need a process-based horizon

The present CSV has groundwater and soil selectors, but no selector for `air | low population density, long-term`. The inventories contain 3,959 such air exchanges in the baseline and 21,152 in the scenario. These remain without an added temporal profile while 32,240 and 36,986 groundwater long-term exchanges, respectively, are eligible for the groundwater fallback or specific selectors. The official ecoinvent distinction explicitly covers both air and groundwater and denotes release later than 100 years. It is not a label for the persistence of impacts after a present emission. [ecoinvent explanation](https://support.ecoinvent.org/lcia-results).

The seven implemented profiles put all mass in 32 bins spanning 100–1,000 years. Their mean offsets range from approximately 297 years for `front_loaded_long_term` to 564 years for `sorbed_metal`; the uniform profile has mean 550 years. Names such as “mobile metal” sound mechanistic, but the coefficients and time constants have no cited calibration in the runtime table or implementation. Soil/landfill type, pH, redox conditions, cover, hydrology and waste composition can matter more than a generic flow-name classification.

More seriously, a long-term inventory amount is not automatically the mass released by year 1,000. Historical ecoinvent documentation describes integration to 60,000 years for landfills and 80,000 years for uranium tailings. This is evidence that a universal 1,000-year support cannot be inferred from the label; the exact horizon of each current source inventory must be checked. [ecoinvent methodological report, section 2.5](https://support.ecoinvent.org/hubfs/Knowledge%20Base/Database/Fundamentals/201007_hischier_weidema_implementation_of_lcia_methods.pdf).

Preserve the distinction between the physical release distribution, the modelled inventory horizon and the reporting/characterization horizon. Do not normalize a truncated release curve to one unless the intention is explicitly to move the omitted future mass into the retained interval. If source timing is unknown, expose alternative documented scenarios and unresolved tail mass rather than silently assigning an apparently precise chemical profile. A release-time model must also avoid duplicating fate/transport already handled by the chosen LCIA method.

## 9. Coverage and failure visibility

| Screened feature | Source ecoinvent | Existing 2050 scenario |
|---|---:|---:|
| Technosphere exchanges | 312,518 | 491,459 |
| Stock-supplier matches | 3,807 | 8,971 |
| Direct stock-to-stock matches | 181 | 937 |
| Maintenance assignments | 213 | 1,075 |
| Maintenance assignments under throughput callers | 203 | 953 |
| End-of-life assignments | 225 | 1,096 |
| Recognised nested lifecycle substeps kept at zero | 3 | 18 |
| Lifecycle matches missing caller lifetime | 0 | 4 |
| CO₂ uptake matching biomass rules | 649 | 1,141 |
| CO₂ uptake without a biomass rule | 284 | 965 |
| Long-term air exchanges without a selector | 3,959 | 21,152 |

Most ordinary technosphere exchanges should not be spread over years. A high percentage of unassigned exchanges is therefore not itself a problem, and 100% assignment is not an appropriate objective. Coverage should instead distinguish:

- deliberately instantaneous exchanges, with a reason;
- assigned profiles, including their role, source and reference event;
- unresolved timing candidates;
- conflicting/invalid rules;
- rows that match no activity and activities introduced by scenario transformations;
- emissions or capital contributions outside supported historical/future inventory years.

The current “faulty exchanges” file only catches certain known-tag failures and missing supplier products. Unmatched rows and many missing timing mechanisms remain silent. The long-term match report is useful but does not cover the other assignment classes. Success should be measured with role-specific coverage and contribution-weighted exposure, not just exchange counts or absence of an error CSV.

The name-based construction keyword scan produced many false positives, including electricity output whose name mentions a PV installation and markets whose producer carries a profile later in the chain. Its counts should not be interpreted as missing infrastructure assignments. Use such screens to select chains for review, not to automatically tag suppliers.

## 10. Software and data safeguards

**Validate before mutating inventories.** Introduce a typed schema with known tags/distribution codes; finite numbers; positive life/rotation where required; ordered bounds; a mode inside triangular support; documented lognormal parameter units; finite offsets and weights; equal list lengths; nonnegative weights; positive total mass. Apply it consistently to every tag. The present normalization helper is invoked for long-term selectors but not stock or biomass profiles, and it does not reject NaN/Inf robustly.

**Remove row-order dependence.** Match stable dataset identities where available. Otherwise include explicit optional location, unit, version, system model, scenario and vintage selectors. Identical duplicates can be collapsed with provenance retained; conflicting duplicates must fail or use explicit priorities. The current last-value behaviour is confirmed by 131 changed lifetimes after reversing the CSV. Do not silently resolve these by keeping whichever value happens to appear last.

**Separate missing from zero.** Unknown timing is different from a deliberate zero-offset pulse. Invalid parsing currently becomes `None` in several places; absent rules simply continue. Store a decision status and reason. A strict mode should fail on unresolved high-priority roles rather than exporting an apparently complete temporal inventory.

**Make reapplication reproducible.** Store a rule ID, schema version, source-row/record ID and an ownership marker with generated metadata. Recompute owned fields from a clean baseline; remove obsolete owned profiles when a rule is removed or becomes ambiguous. Preserve explicit user-supplied timing under a documented precedence rule. The current code leaves stale values behind when a rule no longer matches.

**Preserve auditability through export.** Numeric time fields survive the existing store/export tests, which is a strength. Add assignment reason, profile ID, timing reference, input-data checksum and software schema version to the package or an accompanying manifest. The current export does not retain enough information to reconstruct why a particular edge was shifted.

**Separate distribution variability from epistemic uncertainty.** A temporal spread can represent a real mix of ages/events; uncertainty about mean life is a different axis. A broad time profile must not stand in for lack of knowledge without an explicit interpretation. Likewise, value uncertainty (`uncertainty type`, `loc`, etc.) is distinct from temporal parameters despite the familiar numeric distribution codes.

**Check support against available scenario inventories.** The same profiles are attached in every scenario year and location. Long negative offsets can precede the available IAM history; long positive offsets extend far beyond it. The inspected downstream implementation clamps the inventory lookup year while retaining raw pulse years in demand expansion. Thus a dated historical/future exchange can still be evaluated using boundary-year technology. Report the affected mass/contribution and require an explicit extrapolation policy. Also validate missing-activity placeholders across slices so a historical or future supplier does not silently become an emission-free diagonal placeholder.

## 11. A coherent replacement design

Keep reusable parameter profiles, but assign them through exchange roles. An activity label is supporting information, not a complete timing decision.

| Exchange role | Reference event | Recommended default treatment |
|---|---|---|
| Initial incorporated component/material | Parent assembly/commissioning | Offset 0 or a documented short production lead time |
| Capital used to produce current output | Current production/service | Historical age profile derived from stock/cohort evidence |
| Current operating input/emission | Current operation | Offset 0 at the chosen time resolution |
| Maintenance allocated per unit of current service | Current operation | Near zero unless the source explicitly represents a deferred event |
| Lifetime maintenance bundle | Commissioning | Event schedule or intensity conditional on survival |
| Component replacement | Parent commissioning/use | Replacement intervals and survival, with allocation of repeated units checked |
| Retirement embedded in a new-asset inventory | Commissioning | Service-life distribution |
| Retirement allocated to output of an existing cohort | Current service | Conditional remaining-life distribution tied to that cohort |
| Manufacturing waste | Manufacturing | Near zero |
| Treatment/recycling substep or recovered-material supply | Waste handling/material supply event | Near zero; preserve event time through market routing |
| Harvested biomass uptake | Harvest | Growth/assimilation profile for the relevant carbon pool and harvest |
| Perennial stock growth/land-use change | Establishment or conversion | Explicit stock-change trajectory |
| Delayed release from a stock/waste body | Disposal/storage entry | Process-specific release curve with full mass/tail accounting |
| Lifetime land occupation/leakage | Commissioning/use | Use-phase schedule with correct dimensional treatment |

Use two linked data structures: **profiles** containing distribution parameters and provenance, and **assignment rules** containing caller/supplier/flow selectors, exchange role, temporal reference and precedence. Keep life/rotation, stock age, construction lead time, replacement interval, decay/release time and reporting horizon in distinct fields. Keep multiproduct allocation and waste sign conventions explicit.

A practical first release can use conservative, transparent approximations without reconstructing all global asset cohorts. It still needs an explicit mode: for example, a stationary-stock approximation for generic current output and a separate new-asset mode. Publish the assumptions and sensitivity cases. Where the source activity bundles incompatible stages, decompose it or label the unresolved aggregation rather than implying one profile solves it.

## 12. Recommended sequence and acceptance criteria

1. **Stabilize the numerical contract.** Agree on lognormal median/log-mean and sigma/GSD, correct triangular evaluation, define annual discretization, remove silent shape fallbacks and add round-trip tests using the real downstream reader. Canonical explicit pulses are a useful option.
2. **Define reference events and exchange roles.** Resolve new asset versus existing fleet versus dated lifecycle event. Add market pass-through and distinguish incorporated components, production capital, manufacturing waste, disposal and recovered materials.
3. **Correct the strongest data mismatches.** Address pineapple/perennial uptake, eucalyptus geography/harvest regimes, azobé cutting-cycle interpretation, duplicate lifetimes, stale notes and missing long-term air treatment. Revisit the 1,000-year horizon before adopting more chemical-specific curves.
4. **Rebuild a traceable parameter table.** Carry source record, geography, source year, parameter meaning, evidence type and uncertainty. Reconcile caller and asset lifetimes along representative chains. Review family defaults and then prioritize individual rows by material contribution.
5. **Expand representative validation.** Exercise PV with replacement inverter, passenger transport, chemical production capital, solar-thermal disposal, copper recycling, annual/perennial crops, tropical/European forestry, landfill emissions, and DAC. Include at least two scenario years and regions for rapidly changing technologies.

Acceptance tests should establish the following, rather than merely checking that metadata fields exist:

- Sum of pulse amounts equals the original exchange amount, including negative waste/substitution amounts, after parsing, export, import and discretization.
- The intended profile's mean, quantiles, support and shape survive the full pipeline within declared tolerances.
- Changing CSV order cannot change results; conflicting duplicate rules are explicit errors.
- Rule removal/reapplication removes obsolete generated metadata without erasing user-owned profiles.
- Equivalent supply chains with and without a pure market wrapper produce equivalent timing.
- A new PV installation does not add another fleet-age delay to its initial wiring; manufacturing capital can still have its own age.
- Current passenger transport does not push routine maintenance across another complete vehicle lifetime.
- Recycled copper used now is not supplied from future cable recycling; genuine future dismantling stays future.
- Harvested fruit does not inherit the plantation's entire age, and forestry rules respect documented location and harvest cycles.
- Long-term air and water obey the same declared temporal policy; total mass outside a chosen reporting horizon is reported rather than redistributed silently.
- With frozen inventories, time-independent characterization and a complete horizon, summed temporal results recover the corresponding static result within a numerical tolerance. With evolving inventories, differences must be explained by dated technology rather than parsing or mass loss.
- Historical/future inventory extrapolation, missing-slice suppliers and temporal cycles have explicit diagnostics and contribution-based tolerances.

## Audit artifacts and reproducibility

The [row audit](../../results/trails-temporal-review-20261008/csv_row_audit.csv) records every source row, effective reader mean/support and structural flags. [CSV statistics](../../results/trails-temporal-review-20261008/csv_summary.json), [assignment probes](../../results/trails-temporal-review-20261008/assignment_probes.json), [baseline coverage](../../results/trails-temporal-review-20261008/ecoinvent-3_12-cutoff_coverage.json) and [scenario coverage](../../results/trails-temporal-review-20261008/premise-simapro-comparison-remind-SSP1-PkBudg1000-2050-ei312-20261007_coverage.json) provide the counts and selected exchange metadata. These files contain diagnostics and small examples, not complete proprietary inventories.

Reproduce with the local premise environment using `dev/audit_trails_temporal.py`; its `--database` argument can be repeated for the two databases named above. `dev/plot_trails_temporal_review.py` generates the profile comparison. The CSV audit compares against the downstream checkout supplied with `--downstream` and records both revisions and file hashes. The source table and production code remained unchanged during this review.

The source-evidence checks establish several clear corrections and many priority review areas. They do not individually certify every lifetime or estimate the resulting LCIA bias. That remaining empirical validation should be performed after the timing contract and assignment semantics are coherent; otherwise the same source value can produce different physical interpretations in different parts of the chain.
