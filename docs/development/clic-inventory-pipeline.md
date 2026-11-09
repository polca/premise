# CLIC inventory pipeline — implementation and rollout gate

The CLIC provider replaces eligible public foreground inventories with native
CLIC records. Workbooks containing licensed ecoinvent-derived recipes remain
bundled and continue through premise's existing source profiles, migration,
gap filling and linking. User-supplied inventories are also preserved.

**Rollout is pending.** The target range now starts at ecoinvent **3.7**;
3.5 and 3.6 background builds are rejected before downloading inventories.
Older native inventory sources remain supported for migration into newer
backgrounds. CLIC mode does not fall back to public workbook replacements.

## Current interface

```python
from premise import NewDatabase

database = NewDatabase(
    # Existing scenario, source database, biosphere and explicit IAM key arguments
    **build_arguments,
    inventory_source="clic",
    clic_options={
        "cache_dir": "/path/to/private/premise-clic-cache",
        "migration_policy": "pragmatic",  # Default; all warnings are recorded.
    },
)
database.update(["electricity", "fuels", "steel"])
database.write_inventory_recipe("inventory-recipe.json")
```

`update` prepares inventories lazily, before the scenario is forked or certified.
`prepare_inventories(sectors)` can prepare the same scope explicitly. Multiple
calls expand the union of required sectors without changing canonical names.
The legacy `inventory_source="workbooks"` default remains in place during
development; it is not an automatic compatibility fallback inside CLIC mode.

Pragmatic migration accepts inferred reverse routes, their aggregation/proxy
losses, overlapping packaged rules using the engine's deterministic last-rule
precedence, and documented UVEK proxies. Source and target validation, complete
link coverage and valid unit conversions remain mandatory. All accepted issues
appear in conversion audits and provenance. Use `migration_policy="strict"` to
restore the previous gate; `allow_documented_proxies` can be set explicitly in
either mode and otherwise follows the mode's default.

Retained and user workbook preparation accepts `family="uvek"`, `version="2026"`
and the destination `biosphere_name`. This destination means UVEK 2026 cut-off
technosphere with ecoinvent 3.10 biosphere; workbook source versions remain
ecoinvent declarations. `plan_hybrid_inventories` uses the reviewed ecoinvent
3.12 role ledger through `source_version="3.12"`, recorded as `source_context`,
independently of those destination axes. It does not interpret 2026 as an
ecoinvent version. Retained workbook hashes and selected record identities must
still match the reviewed ledger, including after source-profile preparation.
An older native workbook requiring a migrated 3.12 source profile fails until
that intermediate preparation is supported; the profile is never applied to
unmigrated supplier identities.

These workbook graphs use the CLIC harmonizer and adapter with the provider's
`catalog_provider`, `migration_policy` and `allow_documented_proxies` settings.
Purchases of retained or public foregrounds remain foreground links during
migration. The adapter checks every resulting purchase against the actual
destination and resolves biosphere UUIDs from its exact catalog. Owned records
retain formulas, signs, uncertainty and workbook provenance; conversion audits
and policy decisions accompany them. Workbook diagnostics expose the harmonized
manifest and audit locations; workbook metadata references those audits by cohort
fingerprint and SHA256, without embedding cache paths. Generated helpers already
present in the destination are reused only when their complete content matches,
excluding provenance. Conflicting codes fail before records are appended.
Heuristic technosphere mapping remains a production error. The canonical
ecoinvent biosphere bridge now checks publisher identities, units and receiving
contexts for the declared source release rather than requiring consent for an
obsolete heuristic label. Unresolved physical flows still fail; this does not
authorize heuristic supplier mappings.
This preparation support does not certify the UVEK electricity scenario.

## Resolution and distribution

1. Discover the latest version of the daily Zenodo archive, initially record
   [23935622](https://zenodo.org/records/23935622). Verify its SHA256, payload
   hashes, record counts and archive format before accepting any inventory.
2. Resolve explicit, reviewed role-to-slug bindings in
   `premise/data/clic/requirements.json`. Restrict role roots using scenario IAM
   arrays at the requested year, excluding zero technologies. Respect reviewed
   target-specific source replacements so obsolete solar templates enter only
   when required upstream, without also becoming primary electricity templates.
   Include procedural requirements such as cement capture templates,
   steel templates, battery capacity markets and hydrogen distribution.
3. Traverse retained workbook dependencies, stopping at public CLIC replacements.
   Import retained records only from the selected dependency cone. Keep source
   companion workbooks only when a profile affects an actually retained record.
   Public records already replaced by CLIC do not trigger unused workbook guards.
4. Choose the latest published semantic version of each required CLIC root and
   upstream supplier, except where a recipe pins another public version.
   Qualified current supplier identities take precedence over stale uploader
   database/code identifiers. Ambiguous suppliers and unit changes fail.
5. Verify selected unpinned versions against the live CLIC API. Broad selections
   use catalog pages; small selections use per-dataset metadata. If a required
   version is newer than the archive, stop with `ClicSnapshotOutdated`. There is
   currently no native-inventory API delta endpoint integrated into this provider.
   A successful default build therefore cannot silently use yesterday's version.
6. Harmonize source cohorts with Brightpath, then match all exchanges against
   the actual destination inventory and biosphere. Keep migration audits.

The whole compressed archive is downloaded once, because Zenodo distributes a
single ZIP. Only the selected roots and upstream activities are imported and
harmonized. Live freshness checks transfer metadata, not inventory packages.

## Recipes and reproducibility

Pass `inventory_recipe` as a JSON/YAML path or mapping. A partial recipe can pin
individual roots or upstream suppliers without importing unrelated datasets:

```json
{
  "schema_version": 1,
  "versions": {
    "hydrogen-production-gaseous-30-bar-from-pem-electrolysis-from-grid-electricity": "1.0.0"
  }
}
```

Exported lockfiles additionally pin every selected version by numeric ID and
content SHA256, the complete root set, archive identity and resolved-graph
fingerprint, plus SHA256 checksums for retained workbooks. A complete lockfile
cannot acquire unrelated newly published foreground suppliers. Requesting a
broader sector scope than its locked graph fails rather than silently extending
the recipe. Its `namespace_slugs` also freezes which published activities needed
qualified names, so unrelated catalog additions cannot rename locked activities.
Workbook changes require an updated, reviewed registry and any
recipe that pins the old workbook hashes.

Source/target contexts, engine/resource fingerprints, proxy policy, uncertainty
choice, generated helpers and migration diagnostics remain in the manifest.
Recipes pin native dataset versions; changing the Brightpath release or target
context can change converted results, and is recorded separately.

User inventory purchases add their own required CLIC roots, including declared
replacement sources used for amount filling. Existing purchases from the user's
background database keep that background choice. Ambiguous public suppliers need
an explicit `clic slug` exchange field. User files stay local; manifests and
lockfiles record their hashes under `user_inventories`. Replaying a lockfile
requires the same files as `additional_inventories`.

## UVEK destination integration

The explicit inventory target resolver separates background family, release and
biosphere. The UVEK destination under development is **UVEK 2026 technosphere
with ecoinvent 3.10 biosphere**, using cut-off only. Its release must not be
interpreted as an ecoinvent version or select the legacy biosphere fallback.
Existing ecoinvent targets and their EN15804 bridge retain their current meaning.
`ClicInventoryProvider.prepare(..., family="uvek", version="2026")` converts
and links a selected graph to this context. It obtains biosphere identifiers
from the explicit ecoinvent 3.10 catalog, using the configured catalog provider.
Existing ecoinvent preparation keeps its current flow lookup. Cache keys and
manifests distinguish the two axes, and signed quantities, formulas and
uncertainty remain attached to their original exchanges.

When documented proxies are allowed, UVEK preparation also accepts Brightpath's
reviewed forward-helper `migration.technosphere_information_loss` findings and
retains them in audits. Omissions and biosphere losses retain separate gates;
pragmatic policy does not grant blanket permission to discard exchanges.

This preparation interface does not establish `NewDatabase` sector support;
source ingestion, retained workbooks and transformations have separate
integration checks. Technology-specific `uvek_aliases` and, where needed,
UVEK fuel aliases will share IAM roles with the existing `ecoinvent_aliases`.

An explicitly reviewed `uvek_aliases.clic_slugs` declaration can replace the
shared CLIC choice for that family alone. It must declare `clic_unit`, a positive
`clic_reference_amount`, and nonempty `clic_limitations`. Positive IAM roles add
these roots during automatic planning; explicit recipes still lock their roots.
Binding checks the prepared production reference and carries the limitations
into the manifest, native supplier provenance and electricity warnings.
The pilot's approved CSP proxy is the complete CLIC 110 MW tower plant with
nine hours of thermal storage, per 1 kWh. Its Spain-based design and CH record
label, storage configuration, capacity factor, scale, geography and vintage
remain explicit limitations. No separate storage burden is added. This choice
does not change ecoinvent role bindings or approve upstream heuristic mappings.

Three native complete-inventory screening proxies are also approved for this
pilot: the Swiss wood-fired ORC 1400 kWth cogenerator allocated by exergy for
Biomass CHP; the native hard-coal/lignite electricity fleet (documented with
exergy allocation for cogeneration) for Coal CHP; and the native regional
gas-power-plant fleet for Gas ST. Exact electricity aliases declare a positive
`native_reference_amount` and nonempty `native_limitations`. Their scale, cycle,
allocation, geography and vintage limitations accompany each supplier decision.
The allocated inventories are retained, with no additional heat credit and no
IAM efficiency rescaling. This protection also applies when a native supplier
serves another IAM role, avoiding role-order-dependent changes to shared plants.
Geothermal electricity still requires a separately verified generator; native
geothermal heat inventories cannot satisfy that electricity role.

The approved omissions ledger at `premise/data/clic/approved_omissions.json`
currently covers one exchange in the pinned CLIC CSP decommissioning record
(version 1.0.0, ID 1840): its negative 23,000,000 kg used-nitrate-salt reuse
credit, for the exact UVEK 2026 / ecoinvent 3.10 biosphere target. Its ecoinvent
market carries allocated storage-construction burdens and has no verified
native counterpart; the heuristic printer-toner match is invalid. Omitting this
credit leaves its score effect unquantified. Virgin salt production, plant and
storage construction, and other decommissioning exchanges are retained.
The record hash, source context and complete original exchange must match the
approval. Changed quantities, formulas, uncertainty, duplicate matches or
foreground ownership fail for review. Other owners and target contexts retain
their exchanges. The original signed exchange and pin remain in the cohort
audit, manifest and activity provenance; the warning is repeated on cache reuse.
This ledger does not authorize other omissions or relax Brightpath's policy.

For the exact UVEK pilot target, preparation with documented proxies enabled
validates the approval fingerprints pinned in
`premise/data/clic/approved_rule_scopes.json`. Only its inspected source profiles
and matching executable rules are included. This admits the six reviewed
material corrections without granting global heuristic consent. Every matching external compatibility operation must
have its own approval; unreviewed matches fail transactionally with rule-level
diagnostics. Source/target validation, reference coordinates, losses and
foreground ownership remain independent guards. Approval fingerprints enter
cohort cache keys and manifest policy provenance. This needs the current
Brightpath implementation with scoped compatibility approvals; existing
ecoinvent preparation and default Brightpath strict policy retain their behavior.

Matched approved correspondences retain their review path, SHA-256,
authorization and declared limitations in the cohort audit and portable
diagnostics. Limitations produce warnings on both initial preparation and cache
reuse. The review metadata also enters cache identity, so editing a warning
cannot reuse an older silent result. Refused transactions retain the reviewed
preflight findings alongside the unresolved-rule errors, without writing a
converted inventory or claiming that the conversion succeeded.

The ledger also reuses the existing standing review for the complete packed
stone-wool market. All approvals pin the review-file hash and complete
rule/helper fingerprints, including profile-specific recipe overrides. A
changed review, direct correspondence or recipe fails for review; adding a new
decision to Brightpath cannot implicitly approve it in Premise. Sign evidence
or a generic heuristic label alone cannot enter this ledger. This registers the
prior complete-market decision, preserving producer shares, packing and delivery freight without
requesting the same modelling approval again.

It also pins the prior complete heavy-lorry screening review for four older
regional road-service identities across 26 inspected cut-off profiles. Signed
tonne-kilometres select the complete RER fleet, with geographic, fleet, loading
and vintage limitations retained in diagnostics. Later uninspected releases
cannot inherit that approval.

Four CH waste services in the pilot's ecoinvent 3.6 cut-off cohort also reuse the
standing same-material/function/unit disposal approval: drilling-waste
landfarming and residual-material landfill, and hazardous-waste incineration
for average hazardous waste and used mineral oil. Inspection confirms the wet
composition and treatment route in native 2026, the named wet-waste definitions
in 2025, negative one-kg source references and positive one-kg target services.
The ledger records the inspection hashes. The factor is exactly −1, with no
moisture, dry-mass or heating-value adjustment and no new recovery-credit model.
This establishes functional correspondence rather than inventory or LCIA
equality, and covers only those four inspected source profiles.

The same standing disposal approval covers 21 additional inspected CH 3.6
services for municipal or hazardous incineration, sanitary landfill and wood-ash
landfarming. All 42 documented wet-composition coordinates match native 2026,
with the same treatment route and opposite one-kg reference signs. Native 2025
has the same named waste classes and coordinates but shorter composition
comments. The pinned inspection metadata and warnings retain that limitation,
native technology, energy recovery, allocation and emissions boundaries, and
the upstream biosphere exclusions. No water-content adjustment is inferred from
rounded or legacy percentages in dataset names. The two generic inert-waste
landfill mappings and the steel/glass inert-landfill mappings remain excluded:
their composition or treatment equivalence was not established.

Four inspected 3.6 Swiss construction-waste handling services also reuse the
standing matching-function approval: bulk-iron sorting, reinforcement-steel
dismantling/sorting, and concrete/mineral-wool dismantling for recycling.
The positive native kg reference replaces the negative source kg reference
with factor −1. Material definitions and all source operating coordinates
match the native counterparts; the complete native sorting services retain
their additional freight and waste heat. The recycling records cover only
dismantling/handling, with no invented recovered-product credit or extra
processing. Native `xx` prefixes remain part of their actual source names.
Inspection metadata is pinned in `uvek_pilot_building_handling_review.json`.
This does not admit the mineral-wool landfill → dismantling/final-disposal
mapping, which has a different physical boundary.

Four further 3.6 Swiss municipal-incineration links use dedicated glass, PE, PP
and PS services, replacing construction-waste transport wrappers. The former
PP wrapper actually supplied PE incineration. All 42 wet-composition entries
and both heating values match the dedicated native 2026 endpoints; the shorter
2025 comments retain a documentation limitation. The legacy glass name says
0% water, but its detailed composition matches the source and controls this
review. Signed wet-kg quantities use factor −1 without moisture scaling.
Caller transport remains unchanged and wrapper freight is not inserted.
The exact 3.6 profiles, evidence and limitations are pinned in
`uvek_pilot_incinerator_endpoint_review.json`; other source profiles remain
unreviewed. The original heuristic policy gate and independent reverse rules
remain intact.

Two existing material decisions are registered for their actual pilot profiles:
bulk cathode copper in source 3.9/3.12 and hot-rolled 18/8 stainless steel in
source 3.9. Original source inventory and description hashes and the native
2025 inventory hashes exactly match `uvek_copper_cathode_review.json` and
`uvek_stainless_stock_review.json`. Native 2026 retains the positive kg
references and all 13 copper/three stainless direct technosphere coordinates.
The complete storage/production mixes, copper scrap allocation and stainless
hot rolling remain included once. Existing geography, grade, vintage and
allocation warnings remain explicit. These registrations add no other source
profiles or inverse correspondences, and retain the imported native biosphere
exclusions.

The standing whole-material/function and geographic-supply approval also covers
ten inspected pilot services recorded in
`uvek_pilot_polymer_service_review.json`: ABS, Nylon 6, PVF, PP/LDPE
granulates, bisphenol-A polycarbonate, neat ethylene glycol, liquid dimethyl
sulphate, plastic-pipe extrusion and injection moulding. All source and native
references are positive kg, with factor +1 and no solution conversion. Native
2025/2026 direct technosphere coordinates match. The forming services exclude
bulk converted polymer; their stated 0.996/0.994 output yields do not change the
process reference or justify another polymer input. Caller polymer, transport
and direct emissions remain unchanged. Complete native inventories retain
their data-quality limits, internal logistics, allocation and methane updates.
Delivered-market mixes, freight and losses are not reconstructed from the
native at-plant proxy, and geographic/vintage differences remain warnings.
Epoxy, tetrafluoroethylene, methanol and recovered triethylene-glycol supply are
outside this review; it also authorizes no pending equipment or geothermal
decision.

Twelve further commodity/component correspondences are recorded in
`uvek_pilot_commodity_component_review.json`, covering sixteen exact source
profiles. They retain complete film capacitors, Pb-free surface-mounted boards
and SAC396 solder per kg, without converting the capacitor's 0.7 g example
device into the reference or adding a second assembly. Liquid chlorine retains
upstream electrolysis and liquefaction once; liquid nitrogen/oxygen retain the
native cryogenic separation and allocation without another liquefaction step.
Glass fibre, lead, tin and generic industrial SiC retain their native production
and supply inventories, with source-market, recycling, allocation, grade and
data-quality differences in warnings.

The CMC powder supplier has an aggregated biosphere inventory with more than
1,000 active flows, rather than a production-only stock or empty helper. Its
native biosphere coverage differences remain explicit. The unpacked stone-wool
correspondence preserves the same intermediate-only boundary in source and
target: packing, administration and infrastructure assigned to the packed
module remain excluded, and use as complete finished insulation is not
validated. No density scaling or silent substitution of packed wool is made.
Direct native technosphere coordinates match across 2025/2026 within relative
tolerance 1e-12; the tin difference is binary floating-point serialization.
No native coefficient is rounded or rewritten.

`uvek_pilot_water_metal_service_review.json` adds twenty inspected
correspondences across twenty-two source profiles: thirteen 3.6 country markets
for decarbonised water, four 3.9 metal sheet-rolling markets, the same documented
MAG welding service per metre, and generic cast-/wrought-aluminium stock mixes.
Water retains the complete Swiss cooling/scrubber-water treatment per kg;
source supplier shares, country water-resource geography and regionalized
water-stress results are not reconstructed. Neither deionised water nor a
kg-to-volume correction is substituted. Rolling retains caller bulk metal
separately from process-loss metal already inside the native service. Welding
retains the same stated shielding-gas composition and wire use per metre, with
no added wire/gas inventory or metre-to-kg conversion. Aluminium stock retains
the original European primary/new-/old-scrap blends without renormalization,
zero-burden stock assumptions or an additional forming step. Source-market,
grade, allocation, vintage and shorter native 2025 documentation remain warnings.

`uvek_pilot_active_chemical_review.json` adds five correspondences for seven
actual source profiles. Calcium chloride, hydrochloric acid, sodium hydroxide
and industrial phosphoric acid preserve active-product kg with factor +1.
Native coproduct allocation and common-feed ratios, synthesis reactants and
purification feed coordinates establish that the concentration labels do not
require solution-mass scaling. Source-market shares, logistics, technology,
allocation and geography remain explicit proxy limitations. Separate caller
water, signed demands, uncertainty and formulas remain unchanged. The evidence
excludes sodium hypochlorite's purchased-versus-captured chlorine discrepancy,
acrylic dispersion's unresolved binder basis and the incorrect solvent-paint
match. Native supplier LCI checks prove finite baseline inventories and unchanged
source contents; they do not certify a prospective scenario.

`uvek_pilot_paint_market_review.json` extends the previously approved
water-based alkyd producer and +1/0.60 active-to-mixture factor to its inspected
ecoinvent 3.12 generic market. This market buys only that producer, at one kg
per kg, and original source/native reference evidence hashes match. Correct the
solvent-paint target, preserve signs and scale uncertainty/formulas together;
retain source market delivery and inventory differences in warnings. The authoring
generator replays the correction with an exact 3.12 gate. Other releases and
source identities remain unapproved.

`uvek_pilot_bulk_chemical_ceramic_review.json` adds six complete products
across eight exact profiles: liquid ammonia, carbon black, oxidic ceramic tiles,
monoethanolamine, liquid sulfur trioxide and sulfuric acid. Actual producer
inventories establish phase, product/function and positive kg references. Retain
the native ammonia technology mix, allocated ethanolamine inventory, oleum-supply
approximation and already embedded sulfuric-acid smelter-gas allocation without
reapplying allocation, concentration or yield factors. Native feedstock and
precursor differences, estimated/incomplete historical emissions, original
omitted wastes and geographic/delivery differences remain screening warnings.
These correspondences do not repair native chemical inventories or claim LCIA
equality. All six suppliers have finite baseline LCIs with unchanged scientific
source contents.

`uvek_pilot_stock_finish_review.json` adds ten correspondences across eleven
profiles for primary aluminium, hard fibreboard, coated flat glass, mined
lignite, reinforcing steel, Swiss sand, powder zeolite, metallic antimony and
batch zinc coating. Actual producer and native inventories establish product
and service boundaries: casting, rolling and glass substrate remain inside
the complete native products, while zinc coating retains separately listed
caller steel. Hardboard stays in cubic metres and coating in square metres;
no density, thickness, moisture, heating-value, yield or repeated allocation
factor is applied. Native hardboard manufacturing parameters remain unspecified,
zeolite is an active aggregated biosphere inventory, and historical data,
internal coating-metal approximations, mine coverage, geography and delivery
differences remain explicit limitations. Nine native suppliers have finite
baseline LCIs with unchanged scientific-content hashes. These are exact pilot
cut-off decisions, not a general grade or inverse-route approval.

`uvek_pilot_silver_market_review.json` corrects the inspected ecoinvent 3.12
silver market from the single lead-derived supplier to UVEK's existing complete
regional stock. Its four native supply routes sum to one; retain that mix and
internal freight without a new blend, extra refining, repeated allocation or
recovery credit. All twelve source producer inventories have positive silver
references, including treatment-named allocated coproducts. Preserve signed
product kg without a waste sign flip or invented purity factor. Historical
shares, coproduct allocation, purity, geography and delivery remain explicit
limitations. The generator replays the correction with an exact 3.12 gate;
prior scope fingerprints remain unchanged. Native baseline LCI is finite and
source scientific contents are unchanged.

`uvek_pilot_dense_soda_review.json` records the user's explicit approval of
native light soda as a screening proxy for the inspected 3.9 dense-soda market.
Preserve signed active-chemical kg with factor +1 and no density correction or
invented densification inventory. Warnings retain the native exclusion of
densification, physical-form difference, allocation, geography, delivery,
vintage and unquantified impact-score effect. Native 2025 has shorter comments
but the same direct technosphere vector as the explicit 2026 light-soda process.
The source Hou-process coproduct and native Solvay allocation remain different;
no native allocation is applied twice. Only the exact inspected cut-off source
profile is registered. Cache reuse retains these limits and baseline LCI is
finite with unchanged source scientific contents.

`uvek_pilot_generic_chemical_equipment_review.json` adds eleven
correspondences across thirteen profiles. The inspected chiller includes its
cooler and piping, the combined-cycle plant is construction per equipment unit,
the three-conductor cable stays per metre, and the hall retains its 30/70
wood/steel average. Generic chemical inventories retain their existing proxy
averages without inventing a physical formulation or identified chemical grade;
organophosphorus active-ingredient manufacturing and the broad silicone-product
average retain their declared boundaries. Control-unit electronics excludes
final assembly in both source and native inventories, while EPDM already
includes extrusion and vulcanization. Match reference quantities, preserve
signed demands/uncertainty/formulas and add no extra manufacturing, operation,
weight, capacity, lifetime, concentration or allocation factor. Source delivery,
market shares, background updates, historical data limitations and native
biosphere exclusions remain warnings. Ten native suppliers have finite
baseline LCIs with unchanged scientific-content hashes.

`uvek_pilot_matching_wet_waste_review.json` adds three direct treatment
services: 3.6 average incineration residue and hard-coal ash in residual landfill,
and 3.9 mixed-plastic municipal incineration. The landfill cases match all 42
wet-composition coordinates and biogenic-carbon shares; mixed plastic also
matches both heating values. Actual cement, hydrated-residue, landfill-facility
and process-specific quantities match after aggregating regional source
suppliers, and incinerator fossil CO2/methane coordinates match. The ash cement
option is already enabled in the source, not an additional native treatment.
Convert signed waste kg by factor -1 because source production is -1 kg and
native service production is +1 kg. Preserve uncertainty/formulas, separately
listed caller freight/emissions and the full native treatment once without
new ash, cement, recovery or carbon-origin adjustments. Native geographic labels,
historical transfer models and biosphere exclusions remain warnings. Three
native suppliers have finite baseline LCIs and unchanged source contents.

`uvek_pilot_traced_building_waste_review.json` adds six independently
inspected construction-waste collection/sorting correspondences. Bulk-iron and
reinforcement sorting preserve matched operating demands without recovered
metal credits. Concrete, wall emulsion paint, mineral wool and metal-paint
collection retain their complete native landfill/freight chains once. Actual
source waste-market routes were traced to the same named inert-landfill
functions, rather than inferred from collection labels. Native landfill models
include explicit long-term leachate and operational updates where legacy source
endpoints omit direct leachate; moisture, model horizon, geography and emission
coverage remain screening limitations, not claims of inventory equality.
Preserve signed kg with factor -1, uncertainty/formulas and separate caller
materials/transport. No duplicate freight, disposal, density/moisture factor or
source-emission reconstruction is added. Concrete sorting routed to clinker
and PE/PP collection with global mixed fates are explicitly excluded. Six native
suppliers have finite baseline LCIs with unchanged source scientific contents.

`uvek_pilot_bare_landfill_review.json` reuses two of those exact terminal
source/native pairs for standalone ecoinvent 3.6 inert-waste and mineral-wool
landfills. The mineral-wool correction removes an erroneous collection wrapper;
neither standalone service acquires collection freight or dismantling. Keep the
bare landfill's own operations, infrastructure and internal logistics once, with
the same explicit long-term-leachate, composition and moisture limitations.
Preserve signed kg using factor -1, formulas, uncertainty and separate caller
transport. Both native supplier LCIs are finite and source scientific hashes
remain unchanged. Other source versions and changed disposal fates stay closed.

`uvek_pilot_tap_water_review.json` adds three delivered-water markets covering
four inspected source profiles. Both source and native references are kg at
the user. Retain factor +1 and the complete native Swiss treatment, pumping,
storage and distribution inventory; do not multiply source distribution losses
into its own water balance or add network/treatment inputs twice. Treatment
shares, water-resource geography, losses, infrastructure, vintage and regional
water-stress differences remain warnings. The native supplier LCI is finite
with unchanged source scientific hashes. Eighteen focused checks cover signed
quantities, uncertainty/formulas, version gates and cached loss warnings. The
actual two-record ecoinvent 3.7 source cohort now harmonizes, with cache/audit
hashes verified; other cohorts still refuse unresolved correspondences.

`uvek_pilot_single_fate_incineration_market_review.json` adds four inspected
ecoinvent 3.9 markets: European scrap copper, industrial-electronics plastic,
and European/rest-of-world unspecified rubber. Each sends one kg exclusively
to the matching ordinary incinerator. Actual wet composition, biogenic-carbon
parameters, technology, ash/cement/landfill quantities, auxiliary-energy
reference amounts and recovery statements match. Preserve signed kg with
factor -1 and the complete native inventory once. Source-market collection
freight is not reconstructed; native internal logistics and separate caller
freight remain included. Delivery, heat/fuel-input boundaries and background
differences remain explicit screening limitations. Thirty-one focused checks
pass, and all three unique native supplier LCIs are finite with unchanged
scientific hashes. Mixed-fate markets and Swiss copper with fly-ash extraction
remain refused; this approval introduces no extra recovery credit or helper.

`uvek_pilot_matching_supply_service_review.json` adds six complete native
material/service proxies for exact source profiles: wood-derived charcoal,
elemental platinum, raw softwood board dried to u=10%, hydraulic excavation,
whole rail freight and petroleum-tanker freight. Source and native references
are positive kg, m3 or net cargo tonne-km; preserve signed demand with factor +1.
Wood includes material and drying, excavation excludes the excavated material,
and freight includes its vehicle/port/track life cycle. Add no duplicate process,
carbon/recovery credit or density, heating-value, capacity or lifetime factor.
The source tanker description and native 150000dwt model differ; native
historical fleet, machinery year, allocation and sparse wood documentation
remain explicit screening limits. Twenty-eight focused checks pass, and six
native suppliers have finite LCIs with unchanged source scientific hashes.

`uvek_pilot_lignite_scrubber_review.json` implements the explicit approval for
the ecoinvent 3.6 lignite scrubber at factor +1 per signed kg SOx retained.
The complete native 90%-removal service does not represent source landfilling
of 4.1 kg gypsum or treatment of 0.0024 m3 wastewater per kg retained. Those
residue/allocation differences and unquantified score effects remain warnings;
separate caller stack emissions remain unchanged, with no recovery or avoided-SOx
credit. Six focused checks and the native supplier LCI pass, with unchanged
scientific hashes. Approval does not extend to other source profiles.

`uvek_pilot_fuel_heat_service_review.json` adds two building-machine diesel
cases, hard-coal stock and three industrial-furnace heat producers. Diesel
references are MJ of fuel combusted; heat references are MJ of useful heat.
The heavy-fuel-oil source already scales its reference by `0.95 * (1/0.95)`.
Native heat wrappers retain their own burner multipliers internally, so caller
heat demand remains at factor +1. Coal stock keeps kg without an LHV factor.
Complete native machinery, fuel supply, furnace and residue treatment remain
included once, with explicit technology, operating-condition and background
limitations. Thirty-one focused checks pass, including unresolved generator
and gas-volume refusals at this checkpoint. Five native supplier LCIs are finite
with unchanged scientific hashes. No raw migration rule changes in this group.

`uvek_pilot_generator_fuel_review.json` implements the explicit small-generator
fuel-reference choice. Only the inspected 3.6 case uses factor
+2.853881278538814, preserving direct diesel demand while retaining source
reference ambiguity and native 10 MW versus source 18.5 kW scale/application
differences as warnings. Signed quantities, lognormal uncertainty, formulas,
separate emissions and cache warnings pass seven focused checks. All 270 earlier
approval fingerprints remain unchanged. Relevant Brightpath checks pass (186),
the full generator replay is byte-identical and the normalized native LCI is
finite with unchanged source hashes. Uninspected profiles and direct gas-volume
conversions remain closed; no electrical efficiency or recovery model is inferred.

`uvek_pilot_matching_legacy_supplier_review.json` covers five independently
verified whole-function matches across six profiles: natural barite, cast iron
with the same 35/65 scrap/pig-iron EAF and casting basis, one 10 MWe turbine,
the identical four-tank/16000 m3 storage module, and the same Grundfos 40 W pump.
Manufacturing, disposal and common omission boundaries match their definitions;
signed kg/module units retain factor +1 without capacity or lifetime scaling.
Standing whole-material/function/unit approval applies to these specifications.
This group excludes the converter-steel target's hot rolling/mix and lubricating-oil
additive formulation. Thirty focused checks pass,
including those refusals; five native supplier LCIs are finite with unchanged
scientific hashes. All 271 earlier approval fingerprints are unchanged.

`uvek_pilot_grid_converter_review.json` corrects two exact profiles to closer
existing native suppliers: 3.6 converter low-alloyed steel retains steelmaking
and casting without rolling, and 3.8 European medium-voltage electricity uses
the complete ENTSO-E grid rather than Swiss-only supply. Signed kg/kWh keep
factor +1. Grid losses, infrastructure and SF6 remain internal; native geography,
trade, vintage and imported biosphere differences remain explicit warnings.
Fourteen focused Premise and 192 relevant Brightpath checks pass; two native
LCIs are finite with unchanged scientific hashes, and generator replay is
byte-identical. All 277 earlier fingerprints remain unchanged. The actual 3.8
cohort completes with its three existing approved helpers; 3.7 also completes,
while the full electricity pilot remains refused on unresolved correspondences.

`uvek_pilot_metal_chemical_supply_review.json` inspects three exact 3.6 profiles:
the generic copper market now uses complete native primary/secondary supply;
methanol retains its complete native storage chain; and the zinc-production
activity is mapped by its actual allocated indium-rich-residue product.
Positive kg references retain factor +1. The current copper inventory was
inspected afresh rather than equated with an older reviewed source hash.
Production/recovery shares, allocation, delivery and vintage remain warnings;
no helper, recovered-metal credit or empty residue stock is introduced.
Sixteen focused Premise and 198 relevant Brightpath checks pass, three native
LCIs are finite with unchanged scientific hashes, all 279 earlier fingerprints
remain valid and full generator replay is byte-identical. Actual 3.7/3.8 cohorts
complete; the full pilot remains refused. Recycled triethylene-glycol supply
and the spent-catalyst versus carbon-free-zeolite mismatch remain unresolved.

Six existing reviewed helper decisions are also pinned for their exact pilot
profiles: the 3.8 chemical-industry heat market and used-cooking-oil market,
and the 3.9 Swiss/European scrap-aluminium markets, global WEEE market and
used-cable treatment. Their existing profile-specific recipes and complete
dependency graphs remain unchanged. The cable helper retains the approved
Swiss FAE screening limitation; the heat helper retains source make-up water
and direct water releases once. Approved cut-off stocks, freight and signed
recovery quantities retain their existing boundaries. Other source profiles
and unresolved component decisions cannot inherit these registrations.

Four direct RER processing services also have inspected matching per-kg
coordinates: seamless steel-pipe drawing in source 3.6, and average steel,
aluminium and copper product manufacturing in source 3.12. The native pipe
service already contains its wire-drawing multiplier; the native average
manufacturing services already contain their metal-loss inputs and factory
operation. No extra multiplier or metal input is added to the caller's
processing demand. The evidence is pinned separately in Brightpath's
`uvek_pilot_processing_reference_review.json`. This admits no global rolling
market or different manufacturing technology.

Eighteen direct production, construction and processing counterparts are also
inspected for matching function, unit, geography and positive unit references.
These include activated bentonite, ceramic tiles delivered to Switzerland,
10 MW diesel-generator combustion per MJ fuel, hydraulic excavation, float
glass, the 400 MW combined-cycle plant, allocated quarry sand, three distinct
6400 kWth wood-cogenerator construction components, outdoor plywood, LDPE/PP/HIPS,
aluminium sheet rolling and three specified steel routes. Evidence is pinned in
Brightpath's `uvek_pilot_direct_production_review.json`. Runtime review warnings
retain each boundary, including the complete granulate supply,
plant/component rather than operation coordinates, absence of another base-metal
input in rolling, and the plywood documentation conflict. No factory demand is
rescaled by density, plant lifetime, coproduct share or the excavation label
`proj. 500`. Cement strength-class coverage remains a separate unresolved
decision.

Two further 3.6 counterparts have inspected output and upstream-service
boundaries, pinned in `uvek_pilot_salt_refinery_reference_review.json`. Powder
sodium chloride represents the same solution-mining, cleaning and drying
process per kg; its source representativeness percentages do not define an
explicit production-route blend to reconstruct. Native reagent forms and
utility, wastewater and freight differences remain part of the complete
native inventory. Swiss kerosene represents the same allocated product per kg.
Both source and native inventories purchase upstream refinery-gas and heavy-fuel
combustion services, rather than listing direct fossil CO2 in the refinery
product itself. No extra furnace, combustion emissions or yield multiplier is
added. Separately listed caller fuel-use emissions remain unchanged. Warnings
retain differences in refinery vintage, coverage, fuel quantities and
allocation; neither correspondence establishes inventory or LCIA equality.

The pilot also reuses its approved native electricity topology for eight
inspected grid correspondences across ten source profiles, preserving the same
declared territory, voltage and delivered-kWh coordinate. Native losses,
infrastructure, import/trade inventories and direct emissions remain part of
the complete grid service. Warnings retain differences in historical weights,
regional membership, grid assumptions and vintage. Different territories and
voltage levels cannot inherit these approvals. A ninth correspondence flattens
the 3.6 RER SOx-retention market: it has exactly one fixed unit share of its
local scrubber, with matching per-kg retained-pollutant and 90% removal bases.
No efficiency rescaling or gypsum credit is introduced. Evidence is pinned in
`uvek_pilot_utility_reference_review.json`. The separately approved NOx SCR proxy
uses the complete native service per kg of NOx retained, despite its documented
removal-rate difference (85% source versus about 80% native). Its warnings retain
that difference, reagent/catalyst, vintage and allocation limitations and the
unquantified score effect. The factor stays +1; no efficiency ratio or avoided-NOx
credit is added, and separately listed plant emissions are preserved. This
approval covers only the three inspected ecoinvent 3.6 cut-off pilot exchanges.

Native role definitions can set `use_legacy_templates: false` when their complete
suppliers already exist in the destination. The UVEK PV roles use this declaration:
the ecoinvent PV workbook generation and installation branches do not supply the
native UVEK PV portfolios. Planning uses the selected family's aliases, so those
unused branches are not imported. Explicit CLIC roots, user-required workbook
nodes and their dependencies remain eligible. Ecoinvent retains its existing
template selection. This declaration does not permit missing native suppliers;
the electricity supplier/allocation gates still apply.

The [chemical quantity-basis review](uvek-chemical-quantity-bases.md) records
why the native aqueous NaOH/HCl labels do not require extra concentration
scaling, and why CaCl2 already carries its Solvay co-product normalization.
These checks do not authorize the separate production and delivery proxies.

The public Brightcon `ecospold2-biosphere310` bundle provides the initial
destination database for these checks. Its manifest records exclusions made by
the upstream mapping workflow. Import provenance must retain that manifest and
the upstream exclusions; they are not new omissions approved by premise.

## Cache boundaries

- Archive caches are keyed by content SHA256. Freshness discovery still runs
  on each online build; `offline=True` explicitly accepts snapshot freshness.
  Successful latest discovery also caches the immutable record ID for exact
  offline lockfile replay. This ID can differ from the requested discovery
  record; an exact lookup never silently substitutes another record.
- Harmonized cohorts are keyed by their scientific records, external foreground
  dependencies, source/target catalog digests, Brightpath/resource versions and
  migration policy. An unrelated cohort remains reusable when another changes.
- Retained workbook preparation is keyed by workbook hashes, selected records,
  the source graph, CLIC resolution/linking and conversion settings. Hits skip
  Excel extraction and legacy inventory preparation for those records. JSON
  reference pairs are restored to Brightway tuples on cache reads; fast writer
  payloads also normalize list references from existing serialized inventories.
- Scenario certification includes the actual composed source graph and inventory
  semantics. Provider provenance survives trimmed source/scenario caches.

Cache payloads and migration audits are verified before reuse. Writes are atomic
and protected by process locks. User source data and licensed workbook records
must remain in private caches.

## Reports and exporters

Scenario and validation/change workbooks contain a `CLIC Inventories` sheet with
dataset slugs, native versions, role, source/target context and snapshot identity.
Retained workbooks appear with their content hashes. Adjacent JSON manifests and
directly reusable recipes preserve the full selection; migration audit files are
copied beside exports with verified hashes. Database metadata includes manifest
and recipe for Brightway, superstructure and scenario-array exports. Matrix,
SimaPro and openLCA exports also emit inventory sidecars.

## Current conversion gaps

The complete all-cohort audit is in
[`brightpath-clic-target-blockers.json`](brightpath-clic-target-blockers.json), with
the standalone reproducer in
[`brightpath_clic_backward_repro.py`](brightpath_clic_backward_repro.py).
Across eight selected source cohorts and all canonical target versions, it finds
38 distinct unresolved identities: 4 first appear at 3.12 → 3.11, 8 at
3.11 → 3.10, 2 at 3.10 → 3.9, 8 at 3.9 → 3.8 and 16 at 3.8 → 3.7.
All 38 reproduce individually with valid source links and synthetic quantities.
Consequential targets also lack UVEK and cutoff-to-consequential migration routes;
the exact 3.10.1 consequential technosphere catalog is missing. EN15804 shares
the cutoff findings at its migration bridge; native adaptation and destination
linking remain separate checks. The solar-specific trace below covers only
the UVEK cohort, rather than all selected inventories.

The actual 2026-11-04 archive contains UVEK 2025 solar inventories. Brightpath
1.0.0a14 has a reviewed UVEK-to-3.12-cutoff route. Earlier targets require inferred
reverse steps. Those steps were the first blocker under the original strict
policy; pragmatic mode now permits them and records their limitations.

The first broad 3.7 cutoff conversion with pragmatic policy reaches target
validation with 98.84% technosphere and 99.34% biosphere link coverage in its
UVEK cohort. Five distinct unresolved identities affect 45 technosphere and
6 biosphere exchanges:

- Average chlor-alkali production of sodium hydroxide at RER.
- Mercury-cell chlor-alkali production of sodium hydroxide at GLO.
- A specific recycled-aggregate lean-concrete recipe at CH.
- Steam-reforming production of gaseous hydrogen at RER.
- Nitric-acid emissions to urban air. Version 3.7 has nitric acid to soil, which
  must not be substituted for an air emission.

Stepwise execution of the same UVEK cohort pinpoints the first invalid links:

| Completed conversion | Newly unresolved exchanges |
| --- | --- |
| UVEK 2025 → 3.12, then 3.12 → 3.11 → 3.10 | None |
| 3.10 → 3.9 | 27 average-production and 12 mercury-cell sodium-hydroxide purchases |
| 3.9 → 3.8 | 5 lean-concrete purchases, 1 steam-reformed-hydrogen purchase, 6 nitric-acid air emissions |
| 3.8 → 3.7 | None additional; the same 51 remain unresolved |

The relevant backwards-applied resources are
`ecoinvent-3.9-cutoff-ecoinvent-3.10-cutoff.json` for the first pair, and
`ecoinvent-3.8-cutoff-ecoinvent-3.9-cutoff.json` plus
`ecoinvent-3.8-biosphere-ecoinvent-3.9-biosphere.json` for the second group.
The sodium-hydroxide identities remain unchanged across 3.10 → 3.9, although
they are absent from the 3.9 catalog. Existing mercury-cell location replacements
in that resource cover the chlorine product, not the sodium-hydroxide coproduct.
Hydrogen is renamed correctly in 3.10 → 3.9 and first becomes unresolved on
3.9 → 3.8. The diagnostic trace retained intermediate unresolved links solely
for inspection; production validation remains enabled.

The first four need explicit target-compatible supplier choices; the last needs
an emission-preserving biosphere strategy. Consequential targets also encounter
the engine's UVEK cut-off-only export boundary. Patch target aliases 3.7.1 and
3.9.1 now pass through the same canonical version handling as `NewDatabase`.
Missing catalogs raise an error naming the required context. These actual data
and engine gaps remain release checks; strict-policy failures are no longer
treated as evidence that an inferred conversion cannot run.

## Maintainer workflow and remaining checks

The maintainer generators `dev/build_clic_role_registry.py` and
`dev/build_clic_hybrid_registry.py` derive a reviewable identity/dependency ledger.
Run the role generator first, then the hybrid generator. Review new bindings,
renames, units, scientific profile boundaries and procedural consumers before
updating the shipped registry. The ledger contains hashes and qualified
identities, without copying licensed inventory coefficients.

Run `benchmarks/benchmark_clic_inventories.py` with an existing trusted local
source cache and archive. It reports extraction/preparation stages separately
from CLIC conversion and caching. These inputs contain different native inventory
versions, so timing comparisons are not scientific result-equivalence claims.

Release checks still include full scenario certification for both inventory
backends, sequential sector expansion, user inventories, locked replay, supported
background versions/system models, report/checkpoint restoration, and all export
formats. Exported licensed inventories and detailed local build results must not
be committed to the repository.

## Measured development results, 2026-11-04

On the local Python 3.12 environment with Brightpath 1.0.0a14:

| Stage | Seconds |
| --- | ---: |
| One complete compressed archive download, 9.19 MB | 0.75 |
| Legacy preparation, 95 workbooks / 3,298 imported activities | 11.69 |
| Hybrid preparation, cold conversions and retained-source cache | 19.33 |
| Hybrid preparation, repeated with conversion and retained-source caches | 4.48 |

The broad selection imported 1,430 CLIC activities and 509 retained workbook
activities, reading or caching 17 workbook sources instead of 95. The measured
legacy cleaning/linking stages fell from approximately 2.39 to 0.73 seconds
(about 1.66 seconds saved); nested gap-filling timing is already included in product
preparation and must not be added twice. Strict Brightpath conversion makes the
first hybrid preparation slower overall. A warm legacy inventory pickle can
load in about 0.1 seconds, so the hybrid cached figure is a comparison with
uncached workbook preparation, not a claim to beat every existing cache hit.
These preparation timings exclude source cleaning, IAM loading, transformations,
live freshness checks and downloads. They are individual development runs, not
cross-machine performance guarantees or inventory-result equivalence tests.

A REMIND SSP2-NPi 2050 scenario with 15 sectors completed on the legacy and
compact backends against a trusted local ecoinvent 3.12 cutoff source graph.
The final compact build took about 40 seconds before report generation; locked
replay took about 38 seconds. These runs include roughly 9 seconds each for
construction/IAM loading and preparation, and 21 seconds for updates/certification.
The follow-up 15-sector 3.12 build with the pragmatic policy also passed, taking
about 46 seconds with a newly keyed conversion cache.
The scenario and validation reports list dataset versions and 7 retained
workbook sources. Matrix export passed validation. An isolated Brightway export
contained 34,737 activities and 3,303 biosphere flows; a hydrogen-supply LCI
calculation was finite and inventory provenance survived database metadata.

An IMAGE SSP2-M 2035 build also passed; it skipped the unavailable two-wheeler
fleet data through the existing sector behavior.
A 15-sector EN15804 build passed against the existing native ecoinvent 3.12
EN15804 database, using the existing guarded scrap adaptation before destination
matching. Its signed quantities and uncertainties are preserved. A two-scenario
cache-reload run also passed, including the behavior-bearing CLIC metadata.

After restricting targets to 3.7 onward and adding pragmatic-policy checks, the
offline regression run passed 3,238 tests with 4 skips in 78 seconds. Historical
inventory-source versions remain tested; pre-3.7 target parameter cases are
excluded from the supported target matrix.
Existing licensed `test_process_*` integrations require credentials;
an attempted full run received ecoinvent HTTP 401 responses. The offline suite
covers recipe/cache ownership, corruption recovery, provenance restoration,
user purchases, battery boundaries and vehicle aliases. The all-target rollout
gate above remains open.

Additional integration checks passed for incremental scenarios and for expanding
sector requirements across successive `update` calls. A real user workbook
selected only its 19 public CLIC dependencies and reproduced identical exchanges
with an exported lockfile; a separate user workbook correctly selected a
retained primary-steel template outside the requested sector. Both user-file
paths preserved the purchase coefficient in locked replay. Twenty complete,
one-activity public foregrounds with matching pinned native versions reproduced
the workbook pipeline's central coefficients and supplier identities; this does
not establish uncertainty-distribution or LCIA equivalence for all inventories.
Full 15-sector recipe replay preserved all 784 native version pins, 9 namespace
choices and 34,737 activities. Supplier structure, exchange quantities, active
uncertainty parameters and production volumes matched at a relative tolerance
of 1e-12. Generated UUIDs and unused distribution fields are excluded from that
comparison; floating-point arithmetic is not claimed to be bitwise identical.
The replay audit exposed two existing production-volume problems: electricity
markets used only the first requested technology, and a shared regionalized
template used the first IAM role that encountered it. The pipeline now sums all
selected electricity technologies and all IAM roles sharing a physical template.
The complete SimaPro export on the corrected development graph also completed;
its coverage report identified 2,093 excluded waste inventory indicators
(`Hazardous waste disposed` and `Non-hazardous waste disposed`) and 3,193
classification fallbacks. Review that coverage when using SimaPro results.
The complete openLCA export used the local `ecoinvent 3.12 LCIA Methods
2025-12-01.zip`. It contains 34,737 processes and 1,061,448 exchanges, including
333,089 linked technosphere exchanges. Archive CRC and provider closure passed.
Every flow, flow-property and compatible-unit reference resolves against the
inventory archive plus the original method package, matching the documented
two-step openLCA import. Of 693,622 biosphere exchanges, 626,781 reference method
package definitions; 66,841 exchanges use 257 elementary-flow definitions absent
from that package and preserved in the inventory ZIP. The export took about
153 seconds. This verifies serialization and reference integrity; an openLCA
application import, numerical engine comparison and Monte Carlo equivalence are
separate checks and are not claimed here.
