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
