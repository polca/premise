# UVEK electricity screening export

Latest checkpoint: two restoration batches bring back 22 inputs using eight
reviewed native material proxies and one concrete-sorting sign correction across
eleven exact source profiles. Twenty inputs remain deferred; screening limitations
still apply.

The first operational milestone uses UVEK 2026 cut-off with ecoinvent 3.10
biosphere through the ordinary public Premise API. The user requested deferring
remaining scientific refinements to prioritize a working scenario and Brightway
export. Existing ecoinvent behavior and the user-facing API remain unchanged.

## Scientific boundary

The initial checkpoint deferred 42 original CLIC exchanges covering 23 supplier
identities and 27 source profile cases. All nine roots and 55 source records remain selected,
with no retained workbooks. These omissions are **screening only**: their omitted
burdens or credits and score effects are unquantified. They are not approvals of
the former heuristic supplier candidates. No renormalization, invented recovery
credit, or zero-burden replacement is introduced. Earlier approved proxies,
source omissions inside helpers, and the bundle's 2,991 upstream biosphere
exclusions remain separate limitations.

`approved_omissions.json` pins each decision to the CLIC dataset version, content
hash, original exchange dictionary and exact destination context. Changed source
quantities, uncertainties, versions or foreground ownership still fail. Audits
retain the original source index even when several inputs of one record are
removed. Every export carries the full manifest; database metadata additionally
states `scientific_status=screening_only` and lists deferred decision IDs.

Strict units, complete retained links, unsupported-route refusals, provenance,
semantic certification and numerical LCI validation remain required. Semantic
certification validates graph correctness; it does not certify impact completeness.

## Geothermal supplement

The pilot needs geothermal generation although native UVEK has none. A packaged
complete published Bagnore hydrothermal flash inventory supplies this IAM role,
with two raw-metal helpers and explicit site, allocation, native supplier,
normalization and boundary warnings. It retains all 94 publication rows, 95%
electricity allocation and measured geogenic emissions. It introduces no avoided
natural-release credit, undocumented exploration/end-of-life inventory or IAM
share removal. The published 544.1004 GWh/year normalization and conflicting
536.8 GWh allocation-sheet basis remain documented in its review. It is a
screening proxy for generic regional IAM geothermal. Its complete inventory is
preserved without IAM efficiency rescaling. All supplemental technosphere
suppliers must resolve uniquely before records are appended; the native source
database is not edited. The resource includes publication provenance, workbook
and inventory hashes, full crosswalk and source-row coverage.

Publication: https://www.mdpi.com/1996-1073/13/11/2839

## Deferred scientific backlog

Restore each dependency with an inspected functional correspondence and verified
reference quantity, sign and uncertainty. Review solution concentration bases,
material chemistry and grade, wafer manufacturing boundaries, waste endpoints,
and gas-delivery boundaries. Then measure score sensitivity before presenting
results as complete. Reverse conversion, other sectors, exhaustive Brightpath
mapping and broad performance/packaging refinements are outside this milestone.

| Supplier | Source releases | Deferred exchanges |
| --- | --- | ---: |
| market for epoxy resin, liquid (`bp-08f2432b62895931`) | 3.12 | 1 |
| market for acrylic dispersion, with water, in 58% solution (`bp-0ad47155a89ab7bb`) | 3.9 | 1 |
| market for grain oriented electrical steel, 0.23 mm coil (`bp-0baf74e5f1f09452`) | 3.12 | 1 |
| market for nickel, class 1 (`bp-1104956dff1c701e`) | 3.12 | 1 |
| market for tetrafluoroethylene (`bp-18d0fd79bfe6791b`) | 3.9 | 1 |
| market for waste paperboard (`bp-3e43100f56430f0c`) | 3.12, 3.9 | 2 |
| market for waste concrete, not reinforced (`bp-4335e9c5e2d93a7f`) | 3.9 | 1 |
| market for titanium (`bp-49e229b952f6983c`) | 3.9 | 1 |
| market for lubricating oil (`bp-4f69327dbf9d3e42`) | 3.12, 3.6, 3.9 | 12 |
| market for polyether polyols, short chain (`bp-5968433ef239f628`) | 3.12 | 1 |
| market for sodium ethyl xanthate (`bp-6046387e041ea5c2`) | 3.12 | 1 |
| market for natural gas, high pressure (`bp-6cada1fa986bcf8b`) | 3.9 | 2 |
| market for kraft paper (`bp-7c580ffe9daa9693`) | 3.12 | 1 |
| market group for natural gas, high pressure (`bp-7fe8308839f735bb`) | 3.6 | 1 |
| market for inert waste (`bp-87f7d6834d9147fa`) | 3.9 | 1 |
| market for concrete, normal (`bp-953779a9d7354634`) | 3.6 | 3 |
| cement production, Portland (`bp-9c3675cc3a3f003c`) | 3.6 | 1 |
| market for waste concrete, not reinforced (`bp-b47c8547ff19ec86`) | 3.9 | 1 |
| market for sodium hypochlorite, without water, in 15% solution state (`bp-c7c88174c5f11ce8`) | 3.6, 3.9 | 5 |
| market for waste mineral oil (`bp-dc3ac2fe6b0e61bf`) | 3.9 | 1 |
| treatment of waste concrete, not reinforced, sorting plant (`bp-e6c831a29eb4d1fa`) | 3.6 | 1 |
| market for waste mineral oil (`bp-ef0d29b6fa71b9c8`) | 3.9 | 1 |
| market for wafer, fabricated, for integrated circuit (`bp-fb36b57c4751ac7d`) | 3.12 | 1 |

## Validation

Focused tests cover exact omission pins, original indices, source non-mutation,
exported screening metadata, complete supplement quantities, atomic supplier
resolution, native electricity topology and preserved complete proxy inventories.
The public encrypted REMIND / SSP2-NPi / 2050 run is recorded separately with its
actual preparation, update, certification/export and finite-LCI results.

Verified checkpoint: encrypted REMIND / SSP2-NPi / 2050 preparation, electricity
update and public Brightway export passed. The exported database is
`premise-uvek-2026-remind-SSP2-NPi-2050-screening` in the dedicated
`premise-uvek-2026-biosphere310` project. Readback contains 12,452 activities,
complete internal production/technosphere links, and only the declared external
ecoinvent 3.10 biosphere. Both the Swiss low-voltage grid and the geothermal
generator yield finite LCIs. Original native technosphere and biosphere SQL
signatures are unchanged. The 42 screening deferrals, previous CSP omission and
supplementary publication provenance survive export. Focused integration tests:
67 passed. Separate component tests: 29 Brightpath and 25 Premise cases passed.
Both distribution wheels include the new resources; the installed Brightpath
wheel exercises the three component migrations.

## First restoration batch

Eight supplier rules now restore 21 exact original CLIC exchanges: Portland
cement, normal concrete, lubricating oil, tetrafluoroethylene, polyether polyols,
kraft paper, liquid epoxy resin and Class I nickel. Caller quantities and
supported uncertainty retain factor +1. The review explicitly retains delivery,
grade, formulation, vintage and allocation differences, TFE/epoxy metadata
conflicts, and the native caution on nickel-dominated results. No new helper,
omission, purity/yield/concentration correction or reverse route is introduced.

The ten exact compatibility scopes are pinned to the packaged Brightpath review.
`restored_screening_deferrals.json` retains the original decisions, source pins,
signed exchange dictionaries and replacement-review hash. Only these 21 decisions
leave the active omission registry; all other refusals/omissions remain enforced.

Validation: 70 Brightpath migration/authoring cases and 33 Premise cold/warm and
omission cases pass. Full resource generation replays byte identically. The
public encrypted REMIND / SSP2-NPi / 2050 scenario exports again to
`premise-uvek-2026-remind-SSP2-NPi-2050-restored-materials`, with 12,452 activities
and finite LCI. The earlier screening database remains available for comparison.
The historical table above describes the original backlog, including restored
cases; the active omission registry is the current deferred-exchange list.

## Concrete-sorting sign restoration

The exact Swiss ecoinvent 3.6 cut-off sorting reference is -1 kg waste; the
matching complete UVEK sorting service references +1 kg. The registered factor
is therefore -1, preserving physical treatment demand rather than producing a
negative disposal score. Signed normal/lognormal uncertainty, formulas and
bounds follow the same one-time sign change. Both descriptions cover the same
inert concrete and demolition/sorting boundary; native transport and emissions
remain explicit limitations. One original pinned exchange leaves the omission
registry, with its original decision retained in the restoration ledger.

The remaining 20 deferred exchanges still require separate work. In particular,
the Swiss inert-waste market uses an empty allocated clinker-treatment handover
plus freight; a landfill would invent disposal burdens. Concrete markets mix
final disposal, sorting and nonempty recycling. Oil markets mix incineration and
empty allocated cement-kiln handovers. These need faithful boundary treatment;
existing generic landfill/incineration candidates are not automatically approved.
Solution-reference bases, gas delivery and specialty-material counterparts remain
unresolved. The workflow/export remains screening only.

The second public encrypted scenario update and export also passed, producing
`premise-uvek-2026-remind-SSP2-NPi-2050-restored-materials-sorting` with 12,452
activities and finite LCI. Seven Brightpath sign/uncertainty/release-isolation
cases and two Premise cold/warm cases pass. The installed wheel carries both
restoration reviews and their resource/manifest; nine representative public
facade migrations are checked there.

Restored material quantities and supported uncertainty were verified exactly in
the actual harmonized CLIC graph before IAM transformation. Normal subsequent
IAM generator-efficiency changes alter four oil quantities in the final scenario;
readback records their ratios separately. These are scenario changes, not
migration concentration/yield factors. Original source database signatures remain
unchanged in the exported-database readback checks.

## Inert-waste and waste-oil market restoration

Three ecoinvent 3.9 cut-off market inputs are restored with their original route
shares and freight. Inert waste's empty allocated clinker-treatment branch is
explicitly audited rather than mapped to landfill. Swiss and RoW oil markets use
native hazardous incineration only for their original incineration fractions
(0.873833181379156 and 0.631053321918934 kg/kg); their empty allocated kiln
fractions remain in five literal source-input audits. No renormalization,
landfill burden, added energy credit or unreviewed source-profile fallback occurs.
Four rail-country legs use complete native RER freight as explicit geographic,
fleet, propulsion and loading proxies. Existing complete Swiss rail is separately
qualified for this exact source profile after a fresh inventory-hash check.

The full encrypted electricity scenario exports to
`premise-uvek-2026-remind-SSP2-NPi-2050-restored-waste-markets`: 12,455 activities,
complete retained links and finite LCIs. Original background signatures are
unchanged. Migration/authoring tests: 28 passed; Premise tests: 26 passed. Readback
retains all eight uncertain RoW freight legs with exact amount/loc/scale metadata,
all three restored caller service signs and five empty-terminal audits.
Twenty-five of the original 42 CLIC exchanges are now restored; 17 remain deferred.
Detailed evidence and final decisions are in the autonomous mapping decision log.
