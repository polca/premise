# UVEK 2026 CLIC electricity pilot

## Objective and scope

Enable and validate a CLIC-backed premise workflow using UVEK 2026 cut-off
technosphere and ecoinvent 3.10 biosphere through an electricity-sector pilot.
Prioritize the inventories actually required by that pilot. Exhaustive completion
of the earlier Brightpath mapping queue and support for every other sector are
outside this scope.

## User experience

Keep the familiar `NewDatabase`, `update`, reporting and export workflow. Any
background-selection arguments must be minimal and explicit. Users must not
need to run Brightpath, manage intermediate conversions or relink inventories
manually. Existing ecoinvent calls and defaults remain compatible.

## Required work

1. Import the pinned public Brightcon ecoSpold bundle into a dedicated Brightway
   project. Verify available checksums, complete linking and readback; preserve
   amounts, signs, uncertainty and declared aliases. Record its 2,991 upstream
   biosphere exclusions and missing audit artifacts without approving further
   omissions implicitly. Demonstrate a finite destination LCI calculation.
2. Carry background family, technosphere release, system model and biosphere
   release explicitly through ingestion, conversion, linking, caches and export
   provenance. UVEK releases must not enter ecoinvent version comparisons or
   biosphere fallbacks. Cross-system-model conversion remains unsupported.
3. Resolve the required CLIC roots, upstream activities, retained workbook records
   and user inventories. Preserve foreground ownership, formulas, signed
   quantities and uncertainty. Verify suppliers against the actual destination.
4. Introduce reviewed `uvek_aliases` beside `ecoinvent_aliases` in the relevant
   YAML technology mappings, plus UVEK fuel aliases where necessary. Select them
   from the explicit background family; retain shared IAM and CLIC roles. Missing
   required UVEK mappings must produce actionable errors. Biosphere identities
   remain those of ecoinvent 3.10.
5. Reuse scientific approvals only within their recorded scope. Preserve accepted
   proxy limitations and approved omissions in warnings, provenance and audits.
   Refuse ambiguous or unresolved suppliers, unreviewed losses and unsupported
   unit changes. Group genuinely new modelling decisions for user approval.
6. Adapt the electricity transformations, market construction, relinking and
   certification needed for the pilot. Run a reproducible encrypted-IAM scenario
   through the public API, export it to Brightway and obtain a finite LCI while
   preserving the original source database and caller-owned records.
7. Add regression coverage, run relevant offline and integration checks, preserve
   lockfiles/manifests/fingerprints and make focused commits at verified
   checkpoints. Remove superseded temporary caches. Classify remaining blockers
   as premise integration, Brightpath mapping, source-data limitations or new
   modelling decisions.

## Completion gates

The exact context and electricity pilot must work end to end through the public
premise API, with complete links, preserved provenance and passing required
checks. Other sectors must have explicit support boundaries. An imported UVEK
database, an inventory-provider test or a finite source-database LCI alone does
not establish a working prospective scenario.

## Current checkpoint

The destination resolver and CLIC provider accept the exact context. Regression
tests cover actual destination keys, canonical biosphere references, signed
uncertainty/formulas, cache replay and early cross-model refusals. The Brightcon
bundle has been imported locally with all 11,947 activities and 417,072 retained
exchanges, and a source electricity LCI is finite.

The UVEK source extractor reads complete Brightway records and resolves Wurst
labels from their actual linked suppliers. It checks the source's declared
family, release, system model and biosphere, and rejects exchanges outside the
selected databases. Ecoinvent cleanup strategies are not applied. Formulae,
signed uncertainty and native metadata survive extraction on owned copies.
Export normalization uses the explicit biosphere axis, and native database
metadata, labels and report overviews distinguish UVEK 2026 from biosphere 3.10.
Passing 2026 to an ecoinvent biosphere lookup now raises an explicit error.

The native alias layer uses `InventorySet(family="uvek")`, `uvek_aliases` and
`uvek_fuel_aliases` in the electricity and fuel YAML mappings. Its 53 nonempty
alias definitions were checked against both the installed source and the
packaged UVEK 2026 catalog. Matching includes exact names, reference products
and units. Shared IAM roles remain unchanged; CLIC generation requires both a
reviewed slug and an explicit role binding. Grid and pure-kWh PV aggregate nodes
are excluded from native generator roles. For REMIND's total-PV variable,
`model_native_aliases` also selects the complete country-level
`Electricity, production mix photovoltaic, at plant` inventory. It contains
installation inputs and direct biosphere flows per kWh, so it is a generation
provider rather than a pure supplier-share carrier. IAMs separating residential
and commercial PV retain their distinct native roles. These model-specific
filters never establish another CLIC foreground's role. Missing aliases are reported and never
inherit ecoinvent filters. This does not establish sector support by itself:
several non-pilot IAM technologies and native nuclear fuel specifications still
lack reviewed definitions.

The public constructor accepts `source_family="uvek"`, `source_version="2026"`,
`inventory_source="clic"` and the installed canonical `biosphere_name`. It
validates these axes independently, preserves bundle provenance and distinguishes
source-template selection (the reviewed ecoinvent 3.12 ledger) from conversion's
actual destination. Source caches include family, biosphere and source revision;
retained/user cohorts retain their policy and portable audit references. Other
sectors are refused before inventory preparation or scenario mutation. Existing
ecoinvent defaults and calls remain unchanged.

A real encrypted REMIND SSP2-NPi 2050 constructor run loaded all 11,947 source
activities. Production `update("electricity")` still refuses the packaged
unapproved Brightpath correspondences, now individually rather than through a
blanket resource refusal. The approval ledger pins the complete rule, shared
reviews and transitive helper recipes for each inspected source profile. It
admits the six approved material corrections, the existing packed stone-wool
and older regional heavy-road reviews, and four inspected wet-waste reference
correspondences under the standing same-function disposal approval. Changed evidence, rule bodies or helper
recipes require review; reference-sign evidence alone cannot approve a
functional correspondence. This covers the ecoinvent source cohorts used by
the CLIC graph and any required retained workbook records. A private,
explicitly unapproved diagnostic reached native electricity supplier selection
after preparing those graphs; its output was not exported or certified.

The current native-family electricity recipe contains 9 roots and 55 CLIC records,
including the approved complete 110 MW tower-CSP inventory with nine hours of
thermal storage. The exact unsupported used-nitrate-salt reuse credit is omitted
only from its pinned decommissioning record, with its original signed exchange,
source hash and unquantified score effect retained in the audit. Plant, virgin
salt and storage inputs remain. That omission does not approve the other salt
correspondences or any further omissions.

A graph audit found the earlier 250 retained PV workbook records and 104 CLIC
PV-branch records were disconnected from the native background and selected
generators. The UVEK PV aliases now declare `use_legacy_templates: false`; the
planner uses family-specific definitions and retains explicitly requested roots
and dependencies. The earlier 18-root recipe remains available. A private
before/after diagnostic found identical provider selections and scientific
hashes for all 12,013 reachable records after omitting those unused branches.
That graph-preservation result does not approve the unresolved correspondences
or prove a completed scenario. Native PV inventories without capacity/module
metadata retain their documented source-inventory limitation.

The approved native pilot preserves country voltage grids, losses,
infrastructure and direct emissions, with IAM generation supplied at busbar
including residential PV. Source mix coefficients provide within-technology
supplier weights. Signed contributions are aggregated by exact supplier before
positive net weights are selected; excluded negative net weighting evidence
must remain in warnings and provenance. The source import carriers are not
edited. In the installed source, nine negative Greek-partner entries in the
Bosnian import mix leave no negative net supplier weights after aggregation.
The source documentation does not explain why those entries are negative.

Two additional supplier choices have explicit screening approval. If a country
has no positive source weights for an IAM technology, use the existing UVEK
global portfolio for that same technology, or its sole mapped native generator.
Record the source portfolio or unique supplier and the geography limitation;
never create equal shares for multiple unweighted suppliers. For IGCC and IGCC
with CCS, the two reviewed CLIC hard-coal/lignite generators use the positive
global coal-generation split, calculated from the installed source. That split
is approximately 95.53% hard coal / 4.47% lignite in the pinned bundle. It is a
historical proxy for an unavailable IAM fuel split, not a new IAM projection.
Both complete generator inventories and their declared CCS chains remain intact.
Missing templates, changed CLIC role bindings, unknown variants or an incomplete
two-fuel basis are refused. These choices appear in warnings, validation and
per-mix supplier provenance without changing IAM technology shares or grid
quantities.

This native pilot does not apply ecoinvent-specific baseline coal/hydropower
corrections, synthetic voltage grids, regional plant cloning or industrial-market
rewriting. Native nuclear efficiency changes require reviewed fuel-energy
specifications; their absence is reported. These limitations must remain visible
in scenario warnings and provenance.

Complete native biomass CHP, coal CHP and gas steam-turbine screening proxies
retain their approved allocation, fleet and geographic limitations. No separate
heat credit or additional storage inventory is added. Their complete allocated
inventories are protected from role-dependent efficiency rescaling.

Electricity transformations remain under validation. The strict public probe
identifies 38 unapproved matching rules in its first ecoinvent 3.6 source
cohort, down from 126 after inspected processing, production, waste and utility
services were registered. The actual 55-record native selection spans five source
cohorts. Running each cohort first under the same public policy, with all
foreign foreground anchors retained, gives:

| Ecoinvent cut-off source | Selected CLIC records | Unapproved correspondences |
| --- | ---: | ---: |
| 3.6 | 27 | 16 |
| 3.7 | 2 | 0 |
| 3.8 | 1 | 0 |
| 3.9 | 17 | 28 |
| 3.12 | 8 | 14 |

These are 58 exact-profile cases involving 51 distinct rules, after reusing
six existing reviewed helper decisions and inspecting direct production
counterparts for the pilot's exact source profiles, including powder sodium
chloride and Swiss kerosene with inspected upstream combustion boundaries,
four matching Swiss construction-waste handling services, and four dedicated
incinerators whose full wet compositions and heating values match the source.
Existing bulk-cathode copper and hot-rolled stainless decisions also cover
three actual source profiles, with original source hashes independently matched.
Ten further complete polymer, chemical and forming services reuse the standing
whole-function supply approval after checking their identities and reference
bases; their source-market and native-inventory differences remain warnings.
Twelve commodity/component correspondences cover sixteen further exact
profiles, including complete electronics, liquid gases and chlorine, aggregated
CMC and the same unpacked stone-wool intermediate. Their physical boundaries
and source-market, allocation and data-quality differences remain explicit.
Each of these twelve selected native suppliers has a nonempty finite baseline
LCI. Before/after scientific-content hashes also match the previous native
database baseline. These supplier checks do not certify the prospective scenario.
Twenty water/metal-service and alloy-stock mappings cover twenty-two more
profiles. Their eight distinct native suppliers also have finite baseline LCIs
and unchanged scientific-content hashes. Water country/resource geography,
rolling/stock separation and the documented welding basis remain explicit in
the review and warnings.
Five active-chemical correspondences cover seven more profiles: calcium
chloride, hydrochloric acid, sodium hydroxide and industrial phosphoric acid.
Paired native coproduct inventories, allocation definitions, reactant quantities
and purification feed coordinates establish an active-product kg basis. Solution
percentages in names therefore do not introduce a concentration multiplier.
The complete native supply mixes retain their geographic, delivery, allocation
and vintage limitations. Four distinct native suppliers have finite baseline
LCIs with unchanged source hashes. Sodium hypochlorite remains unresolved:
its source producer buys chlorine despite a captured-emissions description,
whereas the native captured-chlorine process has no purchased chlorine input.
Acrylic dispersion remains outside this chemical approval.
The ecoinvent 3.12 generic alkyd-paint market purchases exactly the previously
approved water-based producer. Matching original source and native reference
hashes support extending the same water-based supplier and +1/0.60 scaling
to this exact market, correcting its solvent-paint match. Source market delivery
is not reconstructed. Signed quantities, lognormal uncertainty and formulas
scale together, and limitations survive cache reuse. No other market or release
inherits that correction.
Six further complete bulk-chemical/ceramic products cover eight profiles:
liquid ammonia, carbon black, oxidic ceramic tiles, monoethanolamine, liquid
sulfur trioxide and sulfuric acid. Producer inventories and native product
boundaries were inspected, including phase, allocated coproducts and active kg.
They retain historical data limitations, the native sulfuric-acid smelter-gas
allocation and differing monoethanolamine precursor quantities with warnings.
No allocation/yield correction or internal chemistry repair is applied. All six
native suppliers have finite baseline LCIs and unchanged scientific hashes.
Ten stock/finished-product/service correspondences cover eleven more
profiles. Primary aluminium includes casting, reinforcing steel includes hot
rolling, and coated glass includes its substrate. Zinc coating excludes its
steel substrate and retains the source area coordinate. Hardboard remains per
cubic metre with no density correction. Aggregated powder zeolite retains its
active biosphere inventory. Mined lignite, Swiss sand and metallic antimony
retain their historical data and geography limitations. Nine distinct native
suppliers have finite baseline LCIs and unchanged scientific-content hashes.
The user approved the scoped [light-soda screening proxy](uvek-dense-soda-proposal.md)
for the 3.9 dense-soda dependency at factor +1. Missing densification, physical
form, allocation and supply differences remain explicit warnings, including
the unquantified impact-score effect. No density multiplier or invented
densification inventory is added. The native supplier has a finite baseline
LCI and unchanged source hashes; warnings persist through cache reuse.
The inspected 3.12 silver market now uses the complete native regional stock,
replacing its narrower lead-derived-only match. Native lead, copper, gold-silver
and recovered-scrap supplies and internal freight remain included once. All
twelve source providers supply positive-reference silver, including those named
as waste treatments; no waste-reference sign reversal applies to their silver
product. Native historical shares, purity, allocation, geography and delivery
differences remain warnings. Original 223 approval fingerprints are unchanged;
the corrected supplier has a finite baseline LCI and unchanged source hashes.
Eleven further equipment, component and generic-chemical correspondences
cover thirteen profiles. Matching references retain per-unit chiller/400MWe-plant
construction, per-metre cable, the original hall mix, generic chemical averages,
organophosphorus active ingredients and silicone-product average. Control-unit
electronics retains the same component bundle with final assembly excluded;
EPDM already includes extrusion and vulcanization. No new mixture, omitted
assembly reconstruction, device-weight, lifetime, capacity or concentration
conversion is added. Ten distinct native suppliers have finite baseline LCIs
and unchanged source scientific-content hashes. Generic averages and historical
component data retain their explicit representativeness limitations.
Three more direct waste services cover average incineration residue and
hard-coal ash in residual-material landfill, and mixed-plastic municipal
incineration. All declared wet-composition coordinates and biogenic-carbon
fractions match, together with actual cement, hydrated-residue and landfill
reference quantities. The source ash solidification option is already enabled
at the same cement quantity as native; no extra binder is added. Signed waste
kg converts with factor -1 to positive-reference native treatment-service kg,
preserving both burdens and credits. Three native suppliers have finite baseline
LCIs with unchanged scientific-content hashes. Geographic labels, historical
models and native emission coverage remain explicit limitations.
Six traced construction-waste collection/sorting services cover six more
profiles: bulk iron and reinforcement sorting, and collection for final disposal
of concrete, wall emulsion paint, mineral wool and paint on metal. Operating
coordinates and final named waste/landfill functions were inspected. Complete
native freight and updated construction-landfill/leachate models remain included;
legacy source endpoints omit direct leachate as negligible. Moisture, model
horizon and emission differences remain explicit screening limits, without a
moisture correction or reconstructed source emissions. Six native suppliers have
finite baseline LCIs and unchanged source scientific-content hashes. A separate
[concrete-sorting proposal](uvek-concrete-sorting-fate-proposal.md) remains pending:
its source residue goes to zero-allocated clinker co-processing, whereas the
native sorting service landfills it. PE/PP collection with global mixed waste
fates also remains unresolved; neither fate change inherits this approval.
Two standalone ecoinvent 3.6 landfills reuse the exact inert-waste and mineral-wool
terminal pairs already inspected within those collection services. The
mineral-wool mapping is corrected from a collection wrapper to the bare native
landfill. Neither mapping adds collection freight or dismantling; native landfill
operations, infrastructure, internal logistics and long-term leachate remain
included once, with explicit screening limitations. Both native supplier LCIs
are finite and source scientific hashes are unchanged.
At the bare-landfill checkpoint, all 411 public correspondence regression cases
and 180 Brightpath material/endpoint/resource checks pass. The authoring generator
reproduces the resource byte for byte, and packaged review/resource/manifest
bytes match the source. These checks do not certify a prospective scenario.
Three delivered tap-water markets cover four further profiles, using the
complete native Swiss supply as a geographic screening proxy per kg delivered
at the user. Native treatment, abstraction, pumping, storage and network remain
included once; source distribution losses are not multiplied into the target
demand. Water sources, treatment shares, losses, vintage and regional water-stress
differences remain explicit limitations. The native supplier LCI is finite with
unchanged scientific hashes, and eighteen focused checks pass after the earlier
full 411-case run. The actual two-record ecoinvent 3.7 cohort now harmonizes with
verified cache payload/audit hashes; the diagnostic records the later 3.6
refusal separately rather than attributing its unresolved rules to 3.7.
Four single-fate ecoinvent 3.9 waste markets add matching ordinary municipal
incineration for European copper, industrial-electronics plastic and
European/rest-of-world rubber. Composition, carbon-origin parameters, ash and
energy quantities and documented recovery statements were checked against
the actual sole source providers. Preserve signed kg with factor -1, the complete
native allocation and separately listed caller freight. Source-market collection
freight is not reconstructed; delivery, auxiliary heat/fuel-input and background
differences remain warnings. Thirty-one focused checks pass, including refusals
for mixed-fate plastic/oil and Swiss copper with fly-ash extraction. Three unique
native supplier LCIs are finite with unchanged scientific hashes. No new full
453-case correspondence run is claimed after those focused additions.
Six further complete material/service correspondences cover charcoal, platinum,
raw softwood board dried to u=10%, hydraulic excavation, whole rail freight
and petroleum-tanker freight. Signed kg, m3 and net cargo tonne-km retain factor
+1. Complete material/drying, machinery and vehicle/port/track boundaries were
inspected, without duplicate processes or density, moisture, heating-value,
capacity or lifetime factors. Fleet size, historical supply allocation,
machinery year and sparse native wood documentation remain explicit limits.
Twenty-eight focused checks pass and six native supplier LCIs are finite with
unchanged scientific hashes. The current correspondence module has 480 cases;
no fresh full 480-case run is claimed at that checkpoint.
The explicitly approved [lignite-scrubber proxy](uvek-lignite-scrubber-residue-proposal.md)
adds the inspected 3.6 profile per signed kg SOx retained at factor +1. Native
90% removal is retained while warnings identify source gypsum disposal and
wastewater treatment absent from the native coproduct/ash-wetting boundary.
Six focused checks preserve separate caller stack emissions, signs, uncertainty,
formulas and profile gates; the native supplier LCI is finite with unchanged
scientific hashes. No additional recovery or avoided-emission credit is added.
Six further cases distinguish MJ of building-machine diesel combusted from
MJ of useful industrial heat, and retain kg for hard-coal stock. The source
heavy-fuel-oil reference formula already applies its inverse-efficiency factor.
Native heat wrappers keep burner factors internally; caller heat demand remains
at factor +1. Source/native technology, fuel, thermal operating conditions,
allocation and residue-management differences remain warnings. Thirty-one
focused checks pass and five native supplier LCIs are finite with unchanged
scientific hashes. Generator reference ambiguity and direct gas-volume
conventions remain excluded at this checkpoint.
The separately approved [small-generator fuel proxy](uvek-small-diesel-generator-proposal.md)
adds factor +2.853881278538814 for the inspected 3.6 case, preserving its audited
direct diesel draw without asserting an electrical-output basis. Native
10 MW/source 18.5 kW scale and reference ambiguities remain warnings. Seven
focused checks pass and all 270 earlier approval fingerprints are unchanged.
Relevant Brightpath checks pass (186), the generator replay is byte-identical,
and the normalized native supplier LCI is finite with unchanged scientific
hashes. Direct gas-volume cases remain refused. The current module contains
518 cases; no new full 518-case correspondence run is claimed.
Five legacy supplier matches add six inspected profiles: barite, cast iron,
10 MWe turbine construction, the four-tank/16000 m3 storage module and the same
40 W solar-circulation pump. Exact specifications and stage boundaries support
the standing whole-function approval, with factor +1 per signed kg/module unit
and no capacity/lifetime scaling. This group excludes the hot-rolled steel mix
and omitted oil-additive formulation. Thirty focused checks pass and five
native LCIs are finite with unchanged scientific hashes. Earlier 271 approval
fingerprints remain unchanged; no raw rule changes occur in this group.
Two closer existing suppliers replace incorrect targets: the 3.6 low-alloyed
converter steel uses the native converter/casting process without hot rolling,
and the 3.8 European medium-voltage market group uses the complete ENTSO-E grid
supply. Signed kg/kWh retain factor +1. Native losses, infrastructure and SF6
exchanges remain internal; country coverage, trade representation, delivery,
vintage and imported biosphere differences remain warnings. Standing inspected
same-function and geographic proxy approval applies; no helper is added by
these corrections. Fourteen focused Premise checks and 192 relevant Brightpath
checks pass, both native supplier LCIs are finite, all 277 earlier fingerprints
and scientific source hashes are unchanged, and full generator replay is
byte-identical. The actual 3.8 cohort now completes, yielding its one selected
CLIC record plus three existing approved helpers; the 3.7 cohort also completes.
Cache/audit hashes, exact source records, generated recipe provenance and full
target-link coverage are checked. Three other source cohorts remain refused.
The normal public constructor loads 11,947 activities; its electricity update
still refuses the pending mislabeled heat-pump correspondence
`bp-17d12aa909c440e8`. No prospective scenario or export is certified.
Three further 3.6 profiles use independently inspected same-product supply
proxies. Generic copper is corrected from primary-only RLA refining to the
existing complete RER primary/secondary supply; the packaged 3.6-to-3.7 rename
supports its cathode identity. The current source inventory differs from the
historical cathode-review row and was inspected afresh, including all 23
positive-reference producers. Methanol retains the complete native storage
chain, with the source's small coal-route share and native import/route
differences warned. The zinc-production-named activity supplies an allocated
indium-rich residue, not zinc metal or a treatment service; signed residue kg
retain factor +1 and the native allocated burdens. No helper or recovery credit
is added. Sixteen focused Premise and 198 relevant Brightpath checks pass,
three native LCIs are finite with unchanged scientific hashes, all 279 earlier
fingerprints remain valid and full generator replay is byte-identical.
The current correspondence module contains 571 cases; no full 571-case run is
claimed. Source 3.7 and 3.8 cohorts still complete; the full scenario remains
refused on unresolved mappings. Triethylene glycol remains unresolved because
the source includes a substantial recycled share. Zeolite also remains
unresolved: the native spent catalyst-carrier composition includes carbon and
metals absent from the source's stated carbon-free gas-process zeolite.
The exact 3.6 Swiss class-2 wastewater treatment now uses its native counterpart
without the additional residential sewer network in the old target. Both
describe 71,100 population-capacity equivalents, COD 0.1556 kg/m3 and three-stage
treatment with sludge digestion. The native plant, internal sewer grid and
all other direct technosphere inputs remain; the old and new native targets'
direct biosphere coordinates are identical within each release. Opposite
references (-1 m3 waste / +1 m3 service) require factor -1 without volume or
capacity scaling. Complete-process energy, chemical, sludge/residue and ash
recovery differences remain warnings; no internal recipe is reconstructed.
Ten focused Premise and 204 relevant Brightpath checks pass, the native LCI is
finite with unchanged scientific hashes, all 282 earlier fingerprints remain
valid and generator replay is byte-identical. The current correspondence
module contains 578 cases; no full 578-case run is claimed. The other wastewater
market, zeolite case and uninspected profiles remain refused.
Three closer existing suppliers resolve four further profiles. The 3.6/3.9
generic wet wood-chip market uses the complete RER forest/sawmill and
hardwood/softwood mix, with signed kg dry matter retained without moisture,
density or yield scaling. Native species and forest/sawmill shares differ and
remain explicit warnings. The 3.12 stainless market has only EAF/AOD Type-304
producers ending in cast slabs; it now uses the native EAF 18/8 process without
the old target's converter share and hot rolling. The 3.6 >32 t EURO4 diesel
freight service uses the complete native 32–40 t diesel fleet, with the
class/standard, load, mode and vintage approximations retained. All source
producers have inspected positive references; no helper or quantity multiplier
is introduced. Twenty-three focused Premise and 211 relevant Brightpath checks
pass, three native LCIs are finite with unchanged scientific hashes, all 283
earlier fingerprints remain valid and full generator replay is byte-identical.
The current correspondence module contains 600 cases; no full 600-case run is
claimed. The 3.7/3.8 cohorts still complete, while the full pilot remains refused.
The separate 3.9 wastewater market was inspected: it combines partial treatment
with direct releases and sewer infrastructure; its supplier uses a regional
one-/two-/three-stage technology mix. The class-2 treatment approval does not
authorize replacing that market by 100% treatment. Nickel purity and cement/
concrete strength or binder differences remain refused. The latter three
profiles have a separate [cement/concrete proposal](uvek-cement-concrete-grade-proposal.md)
with finite native candidate LCIs; no approval is inferred from those checks.
Two further inspected processing counterparts are admitted. The 3.6 refiner's
positive secondary-aluminium billet output uses native old-scrap secondary
production instead of the primary/secondary alloy mix, retaining factor +1.
The 3.9 PCB waste service uses its matching native weighing/shredding/sampling
stage with factor -1. The source and native PCB inventories assign all
preparation burdens to disposal; their electricity and facility quantities
match. Native freight and waste heat
remain explicit differences. The source's prepared-electronics co-product was
verified as a positive production-only cut-off stock; no downstream metallurgy
or new recovery credit is added. Thirteen focused Premise and 216 relevant
Brightpath checks pass, two native LCIs are finite with unchanged scientific
hashes, all 287 earlier fingerprints remain valid and generator replay is
byte-identical. The current module contains 612 cases; no full 612-case run is
claimed. The source grain-oriented electrical-steel producer has no technosphere
exchanges and 2,747 biosphere exchanges: its aggregated upstream inventory
cannot be relinked to UVEK by a primitive replacement. The reinforcing-steel
and generic-organic-chemical substitutions remain refused.
The correspondence counts describe the current review frontier, not separate
approval questions or
a claim that later validation cannot find additional blockers. The approval
ledger contains 289 exact-profile fingerprints, with review evidence and
limitations retained through audits, portable diagnostics and cache reuse.
The separately approved SCR screening proxy retains the source's signed kg of
NOx retained and separate stack emissions, with the 85%/about-80% performance
difference in warnings. Equipment and other same-function supply proposals
remain pending.

The private diagnostic also lacks a verified geothermal electricity
generator, leaving 162 allocation gaps across 81 native countries and two mix
types. A concrete [Bagnore candidate](uvek-geothermal-bagnore-proposal.md) now
covers 94 published rows, 43 background-product categories and two proposed
unrolled-steel helpers. Its provisional combined LCI is finite, with unchanged
source database hashes. The flash-plant proxy, allocation basis and waste
endpoints await scientific approval. No unreviewed reconstruction is installed
and no IAM geothermal share is removed.

The 45 differences between the installed destination and the packaged catalog
were traced to 30 European location labels, 14 US electricity-region prefixes
and one Finnish gas-electricity name. The inspected pilot purchases linked in
the diagnostic without rewriting source geography. These findings authorize no
global location substitution. Certification, report generation, transformed
Brightway export and a finite prospective LCI remain completion gates.
