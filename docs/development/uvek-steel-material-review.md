# Steel: eight material prerequisites

Eight exact ecoinvent 3.9 cut-off correspondences have been reviewed against
native UVEK 2026. Existing migration rules remain unchanged; only their exact
source-profile approvals are added. No runtime Python modules or new helper
datasets are needed.

| Material | Reference basis and main limitation |
| --- | --- |
| Ferronickel | kg of 25% Ni alloy, not contained nickel; historical global native model with weak direct-emission proxies |
| Dolomite | kg raw milled mineral, not calcined dolime; native limestone-based approximation |
| Ferromanganese | kg high-carbon 74.5% Mn alloy, not contained Mn; historical European supply without secondary production |
| Metal-electrolysis anode | kg complete carbon anode; historical 85% prebaked/15% Soderberg manufacture, not use-phase oxidation |
| Li-ion graphite anode | kg complete graphite-based anode, not bare graphite; includes native additives and manufacturing infrastructure |
| Conveyor | metre of complete conveyor equipment, not kg rubber belt; native component and end-of-life boundaries retained |
| Limestone | kg quarried raw mineral, not quicklime; Swiss mine inventory, no extra calcination or final freight |
| Packed fireclay refractory | kg complete manufactured and packed product; native German plant replaces global market delivery |

All source and native production references are +1 in the same unit. Factor +1
preserves caller quantities, signs, formulas and uncertainty. No contained-metal,
yield, lifetime, capacity or mass-to-length correction is added. Complete native
inventories replace the source supply mix and delivery model where stated;
separate caller freight and emissions remain unchanged. Differences in grades,
allocation, geography, vintage, internal uncertainty and native biosphere
exclusions remain screening limitations with unquantified score effects.
Ferronickel and conveyor documentation explicitly recommend more specific data
when those inputs materially affect the result.

The frozen Brightpath review is `export/uvek_steel_39_material_review.json`,
SHA256 `b9f95c859aaaddffac17f3509ff3a9b1f5f91b82e38605abbae9d9e44e0077ae`.
It records identity/reference facts and source/producer/native inventory hashes;
licensed inventory vectors stay private. No reverse, other-release or
cross-system-model approval is implied.

The material and preceding equipment tests pass together (two tests, 20.31 s).
They verify exact scoped approval fingerprints, strict target catalogs, negative
amounts and uncertainty, formulas, immutable source records, fresh/cache replay,
limitations and refusal when approvals are withdrawn. Black and Ruff pass.
These checks qualify the mappings, not the complete steel sector. Real
preparation and native signed-LCI evidence are recorded separately below.

The eight native suppliers pass 16 signed finite LCI checks with exact sign
reversal. The source database remains at 11,947 activities with its modification
metadata unchanged. See `uvek-steel-material-native-lci-verification.json`.

The regular conda environment installs Brightpath GitHub commit
`fd58a72a09212701885a03664e0cc5c98c2fe007`; installed commit identity and all
772 exact approval scopes validate. Previous approval operations are unchanged.

## Real preparation rerun

Actual encrypted REMIND SSP2-NPi 2050 steel preparation now reports **38
unapproved correspondences, down from 46**. All eight newly approved material
cases cleared. The first remaining refusal is Swiss inert-waste final disposal,
rule `bp-8be1ae15c0ed8dc7`. Strict migration validation remained enabled; only
the still-closed steel support guard was bypassed for this diagnostic.
No steel transformation or export is claimed. The private audit receipt is
`66d2eb8b41d316457363d9a8c4d1537dcc5a37b5ffb1cc6462c309af6496742c.audit.json.gz`
in the regular Premise retained/workbook-harmonized cache. The original
52-case frontier is retained as historical review input.
