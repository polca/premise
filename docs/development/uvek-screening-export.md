# UVEK electricity screening export

Latest checkpoint: the first restoration batch brings back 21 inputs using eight
reviewed native material proxies across ten exact source profiles. Twenty-one
inputs remain deferred; screening limitations still apply.

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
