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
identifies 43 unapproved matching rules in its first ecoinvent 3.6 source
cohort, down from 126 after inspected processing, production, waste and utility
services were registered. The actual 55-record native selection spans five source
cohorts. Running each cohort first under the same public policy, with all
foreign foreground anchors retained, gives:

| Ecoinvent cut-off source | Selected CLIC records | Unapproved correspondences |
| --- | ---: | ---: |
| 3.6 | 27 | 43 |
| 3.7 | 2 | 1 |
| 3.8 | 1 | 2 |
| 3.9 | 17 | 43 |
| 3.12 | 8 | 20 |

These are 109 exact-profile cases involving 99 distinct rules, after reusing
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
The correspondence counts describe the current review frontier, not separate
approval questions or
a claim that later validation cannot find additional blockers. The approval
ledger contains 238 exact-profile fingerprints, with review evidence and
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
