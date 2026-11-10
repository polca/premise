# Native heat development

Heat remains outside the public UVEK update gate. Biomass and electricity are
supported screening updates; native heat supplier preflight does not establish
a working heat transformation or prospective export.

The current batch adds 28 exact `uvek_aliases` alongside unchanged ecoinvent
aliases in `heat.yaml`. Each binds a complete, inspected native process with a
positive one-MJ delivered-heat output. Source identities, inventory/metadata
hashes, reference bases and scoped proxy limitations are recorded in
[`uvek2026_review.json`](../../premise/data/heat/uvek2026_review.json).
Licensed process coefficients remain in private evidence.

`plan_native_heat_suppliers` checks agreement between that review and the YAML,
exact name/product/location/unit, nonempty reference output and declared energy
routes before any scenario mutation. It refuses missing or ambiguous matches,
changed production references, unreviewed geography/product substitutions,
foreground/helper impersonation and unphysical conversion factors. There is no
name heuristic or automatic fallback to another family.

Sixteen roles convert IAM final energy to delivered heat using actual native
fuel-input services, electricity draw or the hydrogen LHV documented by the
specific native boiler. All original quantities, formulas and uncertainty stay
in the LCI. These are nominal conversion factors, without an invented upstream
uncertainty forecast. The other 12 bindings have heat-output IAM bases and use
factor one; in particular, an exergy-allocated CHP fuel draw is not a physical
heat efficiency. Fuel-cell roles still require that separate physical review.

The special-project gas-boiler variant whose wrapper and burner imply a factor
above 1.2 remains excluded. The LPG process has a copied natural-gas description,
but its actual fuel is propane/butane; its real inventory is retained. The
industrial heat-pump proxy has source prose saying 15 kW while both its linked
heat pump and disposal specify 50 kW. Capacity, water cycle, application and
uncertain temperature/grade differences remain explicit; no correction is
invented. All representative native technologies retain their source location,
allocation and process inventories, with geographic/vintage limitations.

Against the real encrypted REMIND SSP2-NPi 2050 data, 21 of 28 positive heat roles
have these native bindings. Seven remain: biomass and coal methanol boilers,
industrial biofuel/electric/hydrogen boilers, and the two purchased-district-heat
roles. Other IAMs require additional roles. Missing technologies, layered market
construction, native district distribution, consumer classification/relinking,
combined updates, certification and public export are still required. Whole
sector completion cannot be inferred from this supplier batch.

Validation: 101 focused/shared tests pass, including existing heat and CLIC role
requirements. Isolated installed resources pass 38 native tests and replay all
28 bindings against actual native inventories, with unchanged source records.
The [preflight record](uvek-heat-preflight.json) contains receipt hashes; the
[sector tracker](uvek-sector-progress.json) retains the complete expanded scope.


District delivery now has a planner that copies the complete inspected native
Swiss delivery service and changes only its plant-heat supplier to the IAM
secondary market. Actual heat/network coefficients, all other flows, signs,
formulas and supported uncertainty remain. No second loss multiplier is inferred
from prose. Original producer records remain unchanged.

A private stage with the real encrypted REMIND2050 data produces 13 secondary
mixes from seven positive technologies and 13 delivery copies. Its 11,973-activity
graph has complete links, three finite LCIs and unchanged original scientific
signatures. Isolated installed packages replay it. These are development checks,
not a public heat update or export. See the
[distribution verification](uvek-heat-distribution-verification.json).

Three supplemental heat roots resolve 76 foreground records, retaining all 55
original pins. The fossil-hydrogen dependency now uses an explicit native SMR
screening proxy instead of electrolysis, with mass/pressure, refinery-allocation,
geography and vintage limitations. Preparation then stops on 14 unreviewed
correspondences in the observed3.9cohort; later cohorts may expose more. The
[mapping frontier](uvek-heat-mapping-frontier.json) records primary references.
Correction to the initial frontier note: the wastewater correspondence already
has factor -1, consistent with source production -1 m³ and native +1 m³. Its
blocker was independent treatment-proxy qualification, not a missing sign flip.


The next batch qualifies eight exact 3.9 cut-off correspondences: seven complete
native chemical/water production proxies and the generic wastewater service.
Producer references and product boundaries were inspected; only caller mass or
volume and the reference-coordinate sign are preserved, without invented purity,
density, yield or COD-load factors. Source market producer mixes/final delivery
are not reconstructed by the plant proxies and their effects remain unquantified.
Wastewater keeps its existing -1 factor once, while the large COD, capacity,
sewer, sludge and emission-model differences remain explicit. Six correspondence
reviews remain in the observed cohort; later cohorts may reveal more work.

A further exact-profile batch qualifies vehicle-grade gas, SCR, liquid nitrogen,
chemical heat and liquid epoxy for ecoinvent 3.9 cut-off. The actual gas-station
inventories share the initial 0.1–1 bar boundary, with no added pressure or energy
conversion. SCR retains kg NOx removed without efficiency rescaling. Liquid
nitrogen remains a liquid mass product. Chemical heat uses its existing exact
3.9 recipe, retaining the checked make-up water and both water releases. Epoxy
retains the unresolved native CAS/technology conflict and the earlier 3.12
functional-proxy limitations.

The ledger now has 461 scopes: 455 earlier fingerprints stay unchanged, and the
one epoxy raw-rule fingerprint migration is documented after verifying identical
3.12 effective decisions and unchanged shared reviews/recipe closures. The real
12-root preparation now reports only compressed air as unapproved in the current
3.9 cohort. Its distribution and reference-volume boundary still needs review;
later cohorts are unproven. The public heat gate remains closed.

The delivered-air review completes the initial fourteen 3.9 correspondence
qualifications. A new helper retains the market's two equal shares through
complete native average/optimised supply networks, including piping and losses.
Native generation draws, compressor coefficients and the literal unexplained
`xx` label remain unchanged and explicitly limited. Shared report lineage supports
a volumetric screening basis; exact physical reference conditions are not claimed
and no pressure/density or loss multiplier is invented. Four native/mix LCIs are
finite and the original scientific source signatures remain unchanged.

The next preparation attempt clears that cohort and exposes 38 unapproved
correspondences in a later 3.10 cohort. Sixteen have reviews in other releases;
22 need initial correspondence review. All 38 source records, 89 source producers
and available native counterparts were read from reference inventories, with no
lookup failures. Their [new frontier](uvek-heat-310-mapping-frontier.json) separates
profile extensions, incorrect materials, reference bases, treatment signs,
metals, infrastructure, fuels, fleet and other product/service cases. These are
pending reviews, not newly approved mappings. In particular, source sodium metal
cannot map to sodium chloride, and KCl mass cannot silently inherit a K2O basis.
The public heat gate remains closed.

The sodium/KCl batch qualifies four cases in the later 3.10 cohort plus two
additional sodium-helper inputs. Sodium metal is now represented by its retained
allocated electrolysis model, with four native upstream screening services and
five exact canonical direct flows; salt is only its feedstock. KCl uses the actual
producer's documented 60% K2O basis at factor 0.60, with the generic market-text
inconsistency and native grade/delivery differences disclosed. All 462 earlier
fingerprints remain unchanged. There are 468 exact scopes in total, and real
preparation reports 34 pending correspondences in the current cohort. The public
heat gate stays closed.

Twelve further exact 3.10 qualifications cover three regional low-voltage grids,
stainless steel, cast iron, polypropylene, silicone product, platinum, carbon
black, deionised water, rail freight and ABS. Actual references and producer
inventories were inspected. Stainless source prose mentions pig iron, but actual
exchanges use scrap and match the native electric process's alloy inputs; the
inspected converter alternative is less suitable. Native complete boundaries,
market/delivery differences, excluded emissions, vintage and unquantified effects
remain warnings. Signed caller quantities, formulas and uncertainty stay unchanged.

Two raw profile lists gain 3.10 evidence. All earlier effective decisions and
shared reviews/recipes remain identical; two structural fingerprint migrations
are explicitly qualified, while 466 earlier fingerprints stay unchanged. Twelve
new permissions bring the ledger to 480 scopes. Real preparation now reports 22
pending cases in the later cohort, with 16 of its original 38 qualified. The
public heat gate remains closed; later cohorts and integration remain unproven.

Three exact 3.10 waste/polymer qualifications retain the two virgin HDPE shares,
two hazardous-treatment shares and all seven freight entries. Quantities match
the earlier helpers, but actual 3.10 freight uncertainty differs, so new helpers
retain the current source fields. Four positive trace recyclate supplies have
fresh 3.10 omission audits and an unquantified physical score effect; nonempty
recycling inventories are not replaced by empty cut-off handovers. No mass or
freight renormalisation is introduced.

The hazardous market and direct Swiss service keep the existing -1 factor once,
consistent with source negative waste references and native positive service
references. Wet composition matches; source/native internal treatment, energy,
residue and emission differences remain warnings. No energy credit is invented.
Earlier helpers, shared reviews and omission audits remain unchanged. There are
483 exact scopes and 19 pending cases in the current cohort, with 19 of its
original 38 qualified. The public heat gate remains closed.


Three exact 3.10 complete infrastructure proxies qualify the 10 kW oil boiler,
3000 litre oil tank and chimney. Boiler and tank have matching physical units,
capacities and documented twenty-year lifetimes. Chimney documentation differs:
eighty years in the source versus forty years and an added steel liner in native
UVEK. Caller physical metres remain unchanged; no lifetime or density multiplier
is inferred. Complete material, energy, paint-basis, freight, disposal, geography
and vintage differences remain explicit warnings with unquantified score effects.

No underlying migration rule, recipe, shared review or native inventory changes.
All 483 previous fingerprints remain unchanged; three new scopes bring the ledger
to 486. Nineteen Brightpath tests, seven Premise cache/drift tests, six installed
signed demands and three finite native LCIs passed. Real preparation now reports
16 pending cases in the current cohort, with 22 of 38 qualified. Later cohorts
and public heat integration remain unproven. Verification:
`docs/development/uvek-heat-310-infrastructure-verification.json`.


Ten exact 3.10 complete product/processing proxies qualify steel and aluminium
metal working, glass-filled polyamide, petrochemical AE7, film-on-glass PTFE, SAN,
EVA, generic sulfonylurea, thermoforming/calendering and anhydrous liquid ammonia.
Each review pins actual source market/producer and native reference evidence.
The native polyamide prose contains a stale melamine substitution statement; actual
links and inspected upstream data use glass-filled Nylon 66. AE7 water omissions,
EVA emission gaps, broad sulfonylurea chemistry and embedded solution bases,
processing scrap allocation, polymer reference yields and ammonia route differences
remain explicit screening warnings with unquantified score effects. Caller material,
processing-service amounts, formulas, signs and uncertainty remain unchanged.

No underlying migration rule, recipe, shared review or native inventory changes.
All 486 prior fingerprints remain unchanged; ten scopes bring the ledger to 496.
61 Brightpath tests, 14 Premise cache/drift tests, 20 installed signed demands
and ten finite native LCIs passed. Real preparation reports six pending cases,
with 32 of the current 38 qualified. Later cohorts and public heat integration
remain unproven. Verification: `docs/development/uvek-heat-310-products-verification.json`.


Three exact 3.10 native route corrections replace electric-steel-to-converter-mix
plus rolling with native electric cast steel, biomass-methanol storage/delivery
with the independently verified wood-syngas plant-gate process, and all-size Swiss
lorry freight with the native European 32–40 tonne diesel service. Caller physical
quantities, signs, formulas and supported uncertainty remain unchanged. Scrap,
energy, allocation, vintage and region differences remain warnings; the heavy-lorry
class and fleet standard remain an explicit proxy for >32 tonne EURO IV.

All 496 prior approval fingerprints and prior recipes/shared reviews are unchanged.
Three new scopes bring the ledger to 499; three portable reviews bring shared
reviews to 772, with 3671 recipes unchanged. 24 Brightpath tests, seven Premise
cache/drift tests, six installed signed demands, 41 original caller regressions,
three native finite LCIs and byte-identical replay of two generated resources pass.
Real preparation now reports three pending cases, with 35 of the current 38 qualified.
These are primary aluminium, copper anode and condensate. Later cohorts and public
heat integration remain unproven. Verification:
`docs/development/uvek-heat-310-routes-verification.json`.


The last three observed 3.10 cases are qualified: complete primary aluminium,
global refined-copper screening proxy with explicit anode/cathode and additional
refining differences, and a condensate market reconstruction retaining its original
treated share, two separate collection volumes and 17 direct untreated fates.
Native treatment retains all 26 non-sewer inputs and 50 canonical biosphere flows;
its sole embedded sewer link is moved to a volume-based collection helper using
the actual native allocation. No invented density, recovery credit, independent
negative uncertainty cancellation, duplicated sewer or new source omission is used.
The existing -1 waste-reference factor is retained exactly once.

All 499 earlier approval fingerprints and prior recipes/shared reviews are unchanged.
Three scopes bring the ledger to 502, with three new recipes (3674 total) and two
portable reviews (774 total). 27 Brightpath tests, seven Premise cache/drift tests,
six installed signed demands, 41 original caller regressions and six finite composed
LCIs pass; both generated resources replay byte-for-byte. The numerical comparison
uses all 4362 canonical biosphere rows. Native matrix coefficients exactly match
float32-rounded raw exchanges; the recipe preserves the full raw values. A strict
exploratory LCI bound failed (1.58e-8 relative; 4.19e-7 maximum absolute), and remains
recorded. Independent coefficient checks explain processing precision with less
than 1.24e-17 unexplained residual; no LCIA equivalence is asserted.

Real preparation now passes all 38 observed 3.10 cases and reaches a new 24-case
3.8 cohort. Primary evidence for those 24 references and 49 producers has been
collected without lookup errors and grouped by issue. Eight are exact-profile
reviews of previously inspected correspondences; others include wastewater,
infrastructure, lime chemistry, fuel-delivery and chemical-heat boundaries.
Further cohorts and public heat integration remain unproven. See
`docs/development/uvek-heat-310-closure-verification.json` and
`docs/development/uvek-heat-38-mapping-frontier.json`.


Eight independently inspected 3.8 counterparts are qualified: hard-coal-ash
residual landfill, RoW rail, global medium-voltage grid, cast aluminium alloy,
cast iron, Swiss normal concrete, European-without-Switzerland tap water and
European low-voltage grid. Source/native references and actual producer evidence
are pinned. Keep the already-correct ash factor -1 once, all other factors +1,
and caller signs, quantities, formulas, uncertainty and ownership intact. Complete
native inventories replace source portfolios with explicit rail steam-share,
recycling, binder/strength, delivery, water-network and geographic limitations;
no LHV, wet/dry, density, loss, purity or cement-content factor is invented.

Only concrete gains a raw 3.8 profile. Its earlier 3.6 effective rule and original
shared review remain identical; one structural fingerprint migration is qualified,
with 501 prior fingerprints unchanged. Eight permissions bring the ledger to 510.
All 3674 recipes and 774 shared reviews remain unchanged. The older authoring stage
accepts only this frozen concrete extension. A pre-existing epoxy stage mismatch
is addressed only in the authoring test fixture, using the immutable earlier rule
and preserving actual migration tests against final resources.

123 Brightpath tests including earlier material callers, 12 Premise cache/drift
tests, 16 signed finite LCIs, 16 installed signed demands, 41 original caller
regressions and byte-identical replay of two generated resources pass. Source
scientific signatures are unchanged. Real preparation now reports 16 pending
3.8 cases, down from 24. Further cohorts and public heat integration remain
unproven. Source/upstream candidate evidence for quicklime, delivered coal and
class-4 sewage treatment has been collected; those corrections are not yet
qualified. Verification: `docs/development/uvek-heat-38-profiles-verification.json`.


Four 3.8 cases are corrected: packed milled lime uses native packed limestone,
coal supply uses a complete regional-storage mix instead of mine-gate coal,
and both nominal class-4 wastewater services use native class 4 rather than
class 1 or class 2. Existing wastewater -1 reference factors remain unchanged;
no capacity, heating-value, density, purity or stoichiometric scaling is introduced.
Source/native geography, quality, treatment/fate, delivery and uncertainty differences
remain explicit screening limitations.

The lime investigation also found an earlier scientific mistake: five approved
Swiss loose-milled-lime profiles (3.6–3.10) had been linked to calcined quicklime.
Their actual inventories grind uncalcined limestone; source 3.12 explicitly names
this same inventory limestone and separately models quicklime. These five active
approvals are replaced with inventory-consistent native milled limestone. Original
approval/review bytes and fingerprints remain in inactive `superseded_operations`
history. This is a scientific supersession, not a structural fingerprint refresh.
Genuine quicklime rules, all native inventories, all 3674 recipes and all 774 prior
shared reviews remain unchanged. There are 514 active scopes (505 prior unchanged,
five superseded, nine new active) and 779 shared reviews in total.

206 Brightpath tests, 14 Premise cache/history/drift tests, 18 signed finite LCIs,
18 installed signed demands, 41 original caller regressions and byte-identical
replay of both generated resources pass. Because earlier pilot mappings changed,
a fresh installed-wheel public default biomass/electricity run was certified,
exported and read back: 12529 activities, 55 original source pins, nine roots,
13 biomass mixes, five pellet audits, complete canonical biosphere links and the
original omission/restoration history remain intact. Four foreground links change
from quicklime to limestone with bit-identical physical amounts. Source scientific
signatures remain unchanged. Scores can change; no LCIA equivalence or characterized
impact difference is claimed.

Strict heat preparation now has 12 pending cases in the observed 3.8 cohort,
down from 16. These counts do not count the five repaired earlier approvals as
additional cohort closures. Further cohorts and public heat integration remain
unproven. Verification: `docs/development/uvek-heat-38-corrections-verification.json`.


### Four complete service proxies in the 3.8 heat cohort

Independently inspected and qualified inland diesel barge transport, ion-exchange
water, an organic chemical plant and the complete four-tank liquid-storage unit.
All four source/native references are +1 in the same physical unit; signed caller
amounts, formulas and supported uncertainty remain unchanged. These are exact
3.8 cut-off approvals, with complete native inventories and no added helpers.

Warnings retain the different barge fleet/infrastructure/emissions and lack of a
separate native bilge-oil treatment exchange; the DI-water abstraction, tap-water,
regeneration and wastewater boundaries; the chemical plant's land-class/background
model; and the tank coating's known without-solvent versus 60% solvent-mixture
basis. The complete tank-unit proxy does not claim equal dry coating mass. No
invented fuel, payload, water-balance, purity, lifetime, capacity, tank-count or
paint multiplier is applied. Geography, vintage, internal uncertainty, native
biosphere exclusions and unquantified score differences remain explicit.

Validation: 26 Brightpath tests, nine Premise cache/history/drift checks, eight
signed finite LCIs, eight installed signed demands, 41 installed original caller
regressions, byte-identical replay of both generated resources and matching
packaged data. All 514 prior approval fingerprints and superseded lime history
remain unchanged; four new scopes bring the total to 518. All 3,674 recipes and
prior reviews are retained; four new shared reviews bring the total to 783.
Actual 12-root/76-record preparation reaches eight pending cases in this observed
3.8 cohort, down from 12. Further cohorts remain unproven. Public heat remains
closed pending the rest of mapping, transformation/relinking and export checks.
The last verified public export still covers biomass/electricity only.

Evidence: `export/uvek_heat_38_services_review.json` in Brightpath and
`docs/development/uvek-heat-38-services-verification.json` in Premise. Standing
autonomous modelling authorization applies; no LCIA equivalence is claimed.


### Last eight observed 3.8 heat cases qualified; three 3.12 cases reached

Independently reviewed gypsum disposal, uncoated float glass, the 300 kW compressor,
both delivered alumina markets, delivered zinc/copper oxide and the effective 3.8
chemical-heat recipe. Eight new helpers retain 53 original market/import exchanges,
including separate production shares, freight, numeric formulas and supported
uncertainty. The European alumina market retains all three import helpers with
their own freight, plus its separate market delivery. No shares are renormalized.

Complete native production services remain disclosed screening proxies. Warnings
retain zinc-oxide heat/electricity differences and the documented iron-scrap proxy
for zinc-scrap processing; compressor PCB kg/m² bases; glass soda/gas/waste bases;
and gypsum landfill leachate differences through 60,000 years. Source gypsum wet
water 0.194 versus native 0.193990 does not justify a wet/dry multiplier; the −1
reference conversion is applied once. The existing steam rule/recipes remain
unchanged: its exact 3.8 target uses 1 MJ native heat plus all seven source water
entries and two physical water releases. No rounded kg/MJ conversion is used.
No source/native database is edited and no market/import input is omitted.

Validation: 54 Brightpath tests, 13 Premise cache/drift checks, 16 signed finite
LCIs, 16 installed demands, 41 original caller regressions, both generator resources
replayed byte-for-byte and all packaged data matched. All 518 prior approval
fingerprints, old recipes/reviews and inactive superseded-lime history remain
unchanged. Eight scopes bring the total to 526; eight new helpers bring recipes
to 3,682, and seven new shared reviews bring reviews to 790.

Real strict 12-root/76-record preparation clears all 24 observed 3.8 cases and
reaches three additional exact 3.12 cases: amorphous PET granulate, plastic-film
extrusion and metal adhesive. Primary evidence for those three and seven producers
is collected without read errors; mapping decisions remain pending. Further
cohorts are unproven. Public heat remains closed pending the remaining preparation,
transformation/relinking and export checks. The last verified public default export
still covers biomass/electricity only; no LCIA equivalence is claimed.

Evidence: Brightpath `export/uvek_heat_38_closure_review.json`, Premise
`docs/development/uvek-heat-38-closure-verification.json` and the new
`docs/development/uvek-heat-312-mapping-frontier.json`. Standing autonomous
modelling authorization applies; scientific limitations remain visible.


### Three 3.12 markets qualified; full selected heat preparation succeeds

Qualified the exact 3.12 amorphous-PET, film-extrusion and metal-adhesive markets
using three new helpers retaining all 13 original market exchanges, producer
shares, delivery coefficients and supported uncertainty. Complete native production
or processing inventories remain disclosed proxies. Film extrusion retains the
shared 1 kg service / 0.976 kg film basis and excludes the converted polymer; no
extra polymer or yield factor is added. Adhesive retains the full compound and
its documented lack of production energy, without invented curing or dry-resin
conversion. Finished-product market freight stays separate from native upstream
production transport. Geography, vintage, uncertainty, native biosphere exclusions
and unquantified score effects remain explicit limitations.

Validation: 22 Brightpath tests, eight Premise cache/drift checks, six signed finite
LCIs, six installed demands, 41 original caller regressions, byte-identical replay
of both resources and matching packaged data. All 526 prior approval fingerprints,
old recipes/reviews and inactive superseded-lime history remain unchanged. Three
scopes bring approvals to 529; recipes total 3,685 and shared reviews 793.

The real selected 12-root supplement preparation now succeeds with 606 activities:
76 source records and 530 helpers. Every production/internal/native/biosphere
reference and supplier identity is checked, with canonical ecoinvent 3.10 biosphere.
All 76 versions, IDs and hashes match the captured full lock, and the original 55
pins remain unchanged. Provenance correctly records 58 explicitly requested pins
and 18 automatically resolved dependencies captured in that lock. An initial
checker incorrectly expected all 76 explicit-pin flags to be true; its failure is
retained, the flag interpretation is corrected, and no provenance is rewritten.

Six signed demands across the three new heat roots have finite prepared supply
and combined native/direct inventories, with unchanged native scientific contents.
No Brightway database is written by these preparation checks. Public heat remains
closed: missing-role integration, all three IAM heat layers, native consumer
relinking, combined/default certification and public export still need completion.
Other IAM selections/sectors may require additional roles. The mapping milestone
is not a public heat update or a claim of LCIA equivalence/impact completeness.

Evidence: Brightpath `export/uvek_heat_312_markets_review.json` and Premise
`docs/development/uvek-heat-312-markets-verification.json`. Standing autonomous
modelling authorization applies. Two obsolete isolated installations were removed
after wheel-hash verification, reclaiming 929 MB; wheels, archives, databases,
exports and evidence remain intact.


### Supplemental heat suppliers: source energy bases and positive IAM coverage

Added six exact UVEK aliases for four supplemental supplier records. Three are
pinned CLIC sources; the retained electric boiler is created as one owned record
on the native CH medium-voltage grid. Positive CLIC heat roles use the existing
automatic family-override root selection, without legacy workbook matching or
manual helper installation; explicit user lockfile roots keep their existing
meaning. All non-UVEK YAML fields remain unchanged.

Methanol conversion follows its explicit source 20 MJ/kg and original fuel draw,
giving nominal factor 0.90. Hydrogen follows the original 3.9 fuel reference's
120.21 MJ/kg and original 0.008947368421052631 kg/MJ, giving 0.9297454944044002.
The heat record's conflicting 95% HHV/LHV descriptions remain warnings; no generic
120 MJ/kg, forced 0.95 factor or extra loss is introduced. This is an inventory-
derived nominal proxy, not validated turbine/CHP efficiency or a reconstructed
heat/power balance. Industrial hydrogen/bio-liquid roles retain their disclosed
technology/scale proxies. Complete source inventories remain unchanged.

The retained electric boiler preserves its exact 0.2805836139169472 kWh/MJ draw,
nominal 99% conversion and explicit missing-infrastructure limitation. Its final-
energy role uses factor 0.99, while the secondary heat-output role uses factor 1.
Repeated planning creates no duplicate boiler; changed pins, quantities,
references, aliases, ownership, model roles or background axes fail read-only.

Validation: 119 focused supplier/basis/distribution/root-selection checks, 116
shared heat/IAM/activity-map/profile regressions and 12 CLIC hybrid/provider checks
pass. Isolated packages replay all six supplementary roles against the actual
11,947-native + 606-prepared graph without mutation. Packaged data/code match the
worktree, and the retained boiler's real native-grid LCI is finite. Actual
encrypted REMIND SSP2-NPi 2050 preflight now covers 28 positive roles: 21 native
and five supplemental physical suppliers, plus two purchased-heat roles awaiting
the district-delivery layer. Source inventories remain unchanged.

Public heat remains gated. Three-layer markets, native consumer classification/
relinking, callback/certification and combined/default installed Brightway export
still require completion. No public heat update or LCIA equivalence is claimed.
See `uvek-heat-supplements.md` and
`uvek-heat-supplement-suppliers-verification.json`. An obsolete isolated install
was removed after retained-wheel hash verification, reclaiming about 465 MB;
archives, databases, exports and evidence remain intact.


Supplemental heat matching also accepts the existing `[CLIC slug]` collision
suffix, scoped to the exact locked heat source or locked foreground fuel.
Unrelated plain-name suppliers cannot capture those roles, and native matching
remains exact. The coal-methanol foreground fuel has its own captured version/ID/
hash pin, including when resolved as an automatic dependency. Retained boiler
reuse survives JSON tuple/list round trips without duplicate creation or source
mutation. These cases are included in the 119 focused checks above.


### Three-layer heat markets: private IAM and installed-package verification

The read-only planner now covers secondary supply, buildings end use and
industrial end use: 39 markets, 13 complete native delivery copies and one retained
boiler for encrypted REMIND SSP2-NPi 2050. Final-energy conversion precedes share
normalization; supplier inventories are not rescaled. Purchased heat follows the
native delivery network. World fallbacks weight regional delivered volumes and
retain regional delivery paths, without counting the IAM World row twice.
Negative/nonfinite World values are validated before aggregation too.

All 158 focused tests pass. Installed wheels reproduce all 53 owned records and
all diagnostics exactly using the same archived encrypted input. Fourteen signed
LCIs pass in both checkout and installed checks; complete links, all 76 captured
source versions/IDs/hashes, the original 55 pins and native scientific contents
are preserved. The encrypted input hash differs from the last public baseline;
no equality of those scenario results is claimed. See `uvek-heat-markets.md` and
`uvek-heat-markets-verification.json` for evidence and limitations.

This completes market planning only. Consumer classification/relinking, public
callback/preparation/certification and combined export remain unfinished. Public
heat stays gated, and the other 16 sectors still require operationalization.


### Native consumer boundary discovery

Read-only discovery found 162 used MJ heat-provider identities and 1,191 incoming
exchanges, before protecting source heat/electricity generators and classifying
end-use consumers. These are candidates, not approved runtime substitutions.
Twenty-one network-only exchanges remain separate: the native transport service
explicitly excludes heat supply. The chemical steam process has 55 incoming kg
exchanges and consumes water plus MJ **fuel-combustion** services. Those inputs
cannot be relinked one-for-one to delivered heat; their useful-heat basis still
needs qualification. An initial interpretation as delivered-MJ inputs was
corrected after inspecting the actual suppliers. No source exchange was changed.

The exact candidate identities, positive production-reference checks and
signed/formula/uncertainty counts are retained in
`uvek-heat-consumer-frontier.json`; full licensed records remain private. Matching,
geographic routing, protected-generation boundaries, omission/refusal audits and
consumer LCI checks remain to implement. The prior three-layer stage is committed
as `405dcbf1`. Its verified installed package supersedes an obsolete install,
removed after retained-wheel hash and live-process checks, reclaiming about
465 MB. Source databases, encrypted IAM archives, evidence and exports remain.


### Exact native heat-consumer planning

The consumer planner now produces 962 supplier-only changes in 736 native
consumer copies: 870 industrial, 89 buildings and three delivered-district inputs.
All signed quantities, formulas, uncertainty and literal originals are retained;
one negative input remains negative and one CPA location has an explicit World
proxy. Exact reviewed identities replace name heuristics at runtime. Native
MJ/kWh energy-reference inventories, CLIC sources and mapping helpers stay intact.

Read-only checks exposed the MSWI heat handover's actual production-only
inventory. Its native documentation allocates all burdens to disposal and zero
to heat/electricity. This handover now has an explicit retained-empty-boundary
rule; it does not acquire a future IAM heat burden. Network-only, ambient heat
and kg steam remain explicit historical exceptions too. This corrects the
initial candidate classification of MSWI heat as ordinary industrial supply.

All 176 focused tests pass. The complete 12,606-activity virtual graph passes
canonical-biosphere/key/identity checks and 16 signed LCIs using actual production
references, including 12 non-unit native references. The checker initially
assumed unit native production and was corrected without normalizing source
inventories. Installed consumer plans, literal audits and diagnostics match the
checkout. Source database scientific contents are unchanged and no database is
written. See `uvek-heat-consumers.md` and its verification receipt.

Public callback/preparation, semantic certification, exported audits and combined
Brightway export still need integration. The public heat gate remains closed;
this stage does not complete heat or the remaining 16 sectors.


### Native heat callback, semantic contracts and metadata

The standard heat wrapper now dispatches UVEK to a dedicated native callback;
ecoinvent retains its existing path. The native callback plans supply and
consumer copies, independently checks literal originals and quantity fields,
protected inventories, unique keys, actual production references and full links,
and commits only after validation. It records exact addition/relinking scope,
clears stale provider caches, and emits proxy warnings. Absent/validated-zero
selected heat is explicitly non-applicable rather than a false success or a
common-contract target-coverage error. Positive unsupported roles still fail.

The real encrypted REMIND2050 callback equals the independently verified
12,606-activity virtual graph: 53 additions and 962 supplier changes in 736
consumer copies. Its common sector contract is valid, all 53 additions are
declared, and 789 provenance events explain the changes. Export metadata now
retains native heat screening limitations and historical retained-input audits;
literal changed-exchange originals remain on their consumer records. NumPy
scalar/JSON audit comparisons normalize representation without rounding values.

All 208 focused/sector checks and 110 shared metadata/reporting/validation checks
pass. Isolated wheels reproduce callback results, all export metadata and all
789 provenance events; only phase elapsed times differ. Package resources and
changed modules match the checkout. Source working inventories stay unchanged,
and no database is written. This is internal-callback verification, not a public
update or Brightway export. See `uvek-heat-callback-verification.json`.

Public heat remains gated pending complete normal CLIC preparation, combined/
default callback certification, cold/warm replay and installed Brightway export
readback. Historical steam, ambient/network-only services and zero-allocated
MSWI heat remain explicit limitations. The broader 16-sector work is unfinished.
