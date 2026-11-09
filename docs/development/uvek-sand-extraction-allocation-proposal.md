# Three sand markets: extraction and allocated-mining decisions

Status: both scientific decisions explicitly approved by the user and implemented
for the three exact ecoinvent 3.9 cut-off sand markets (BR, RoW and IN).
Destination is UVEK 2026 cut-off with ecoinvent 3.10 biosphere. Three market
recipes and one allocated-mining helper retain all six producer entries and
17 freight entries. The zinc helper represents allocated burdens, not physical
zinc addition or a metal-recovery credit. Warnings disclose the approved
extraction, allocation, moisture, geography and uncertainty differences.

Packaged evidence: Brightpath `export/uvek_pilot_nitrate_sand_review.json`,
SHA-256 `1044e1a46af4dc1e685a495a0f82409b8ab4fd4c449f9a22d0371f74e806a88e`.
Three exact Premise scopes are registered; all 362 previous fingerprints remain
unchanged (365 total). This component checkpoint does not establish full pilot
scenario, certification, export or LCIA equivalence.

## Original supply and delivery

| Market | Original producer quantities per kg of market supply | Delivery entries |
| --- | --- | ---: |
| BR | 0.300026314800195 kg open-pit sand; 0.699973685199805 kg river-bed sand | 4 |
| RoW | 0.292442375462464 kg river-bed sand; 0.707465674107445 kg quarry sand; 0.0000919504300903769 kg zinc-mine sand co-product | 12 |
| IN | 1 kg river-bed sand | 1 |

Both proposed decisions retain all six original producer entries and seventeen
delivery entries separately, with their original quantities and supported
uncertainty. No normalization, zero-stock replacement, market-entry omission,
density adjustment, additional recovery credit or cross-model conversion is
proposed.

## Decision 1: extraction and processing proxy

Use complete native `Sand, at mine` (CH) per original kg for the five ordinary
pit/quarry/river producer entries. The native process supplies construction sand
from joint sand/gravel mining. It retains its own processing, water, energy,
land use, recultivation, machinery, internal transport, waste treatment and
allocation once. It is an at-mine process; do not add the native regional-storage
market and its delivery on top of the original market delivery.

This requires explicit screening approval. The source river-bed inventories
include dredging, slurry handling and washing, with nonmarketed fines returned
to the river. Brazilian open-pit extraction uses hydraulic mining and cyclones;
its product is documented at 5% humidity after air drying. The Swiss native
inventory has different mining, processing, joint-production and recultivation
boundaries. Product grading, water content and allocation are not established
as equivalent. Retain the original kg quantities without inventing a wet/dry,
yield, grain-size or density factor. Native internal uncertainty replaces the
source production inventories; caller and market-entry uncertainty remain
unchanged. Source representativeness, geography, vintage, water/emission and
biosphere-exclusion limitations remain warnings. LCIA differences are unquantified.

Delivery stays separate. Preserve the four Brazilian EURO3 lorry-class entries
using complete native 3.5–7.5 t, 7.5–16 t, 16–32 t and 32–40 t Swiss services.
The source >32 t bracket is approximated by 32–40 t; EURO3 is not retained as a
specific native fleet standard. Preserve the other rail, barge, light-road,
heavy-road and dry-bulk-ship entries through previously reviewed same-mode
complete service families. Country/fleet, loading, technology, emissions and
vintage differences remain warnings. Do not infer payload, distance or gross-
vehicle-mass factors, use coal-only rail, substitute heavy road for light road,
or add infrastructure already present in the whole service. All endpoints have
positive unit references in both installed UVEK releases.

Decision 1 alone can resolve BR and IN. RoW also needs Decision 2.

## Decision 2: preserve the nonempty zinc-mine allocation boundary

The RoW co-product is positive, priced supply. Its source zinc-mine inventory
has 130 technosphere and 134 biosphere exchanges; it is not an empty cut-off
handover. Replacing it with generic quarry sand would substitute a different
production/allocation boundary, and omitting it because its share is small
would discard nonempty burdens. Neither is proposed here.

A read-only comparison of all 264 supplier/flow coordinates establishes that
this source sand inventory is the source zinc-concentrate inventory multiplied
by 1/95. The recorded economic reference prices confirm the ratio:
0.004 / 0.38 EUR2005 per kg. Supported uncertainty fields also match after this
positive scaling, normalizing only implicit lognormal sign flags for comparison.
This is an observed relation in the frozen source inventory, not a new market-
price estimate, allocation rule or assumption of zero impact.

The source concentrate's reference properties explicitly specify 0.59 kg Zn
per kg of dry concentrate. Native `Zinc, from combined metal production, at
beneficiation` (SE) explicitly references one kg of metal contained in an
unrefined product; further refining is excluded. These documented bases support
an **allocated-mining burden proxy** of:

`0.59 / 95 = 0.006210526315789474 kg native contained-Zn beneficiation per kg source sand co-product`.

Retain the original co-product market quantity separately. Its weighted native
Zn-burden equivalent is 5.71060565824446e-7 kg per kg of RoW sand market supply.
A dedicated positive one-kg sand helper would disclose this allocation-based
burden representation. It does not physically add zinc to sand, represent
refined-metal purchasing, introduce metal-recovery credits or claim a physical
mass yield. Do not add ordinary quarry production to this co-product branch.

This is a new screening choice requiring approval. The source global zinc-mine
production mix and the native Swedish combined-metal mine differ in geography,
ore, co-products, allocation, technology, tailings, land/water/resource/emission
profiles and vintage. The exact source vector ratio does not establish native
inventory or LCIA equivalence. Native internal uncertainty would be retained;
the source-vector comparison is not a full source uncertainty reconstruction.
Keep the documented source ratio and assay; infer no extra grade, price,
allocation, ore-yield or avoided-production factor. Score effects remain
unquantified. Scope is only this co-product inside the exact RoW market.

The alternative is to reconstruct the actual source allocated mining inventory
and its suppliers, retaining all 264 exchanges, rather than use this native
mining proxy.

## Candidate evidence and limits

Fresh inspection reads 45 market/producer/freight/native candidate records.
The proposed complete endpoints and all retained market inputs are bound by
47 fresh records in `sand_screening_fresh_primary.private.json`. Three candidate
LCI calculations are finite and nonzero with unchanged native scientific
signatures. Those original candidate checks preceded approval. Subsequent checks exercise
the actual public Brightpath conversions for all three markets and establish
finite native-background LCI with unchanged source database signatures. The
25 Premise regression cases verify caller preservation, cold/warm caches,
portable warnings and refusal of uninspected releases. Installed-wheel
conversions pass, and full generator replay is byte-identical. These checks
do not establish comparative LCIA, scenario certification or exports. No source
entry is omitted.

Private artifacts: `sand_screening_candidates.private.json`, SHA-256
`4354b9da053cf90c390074a135d1ffd450a80a49360a67a2dc6a0df81b06dee4`,
`sand_screening_candidate_lci.json`, `sand_zinc_allocation_vectors.json`,
`sand_allocation_basis.json` and `sand_zinc_allocation_primary.private.json`.
Reproduction scripts: `inspect_nitrate_sand_closures.py`,
`check_sand_zinc_allocation_vectors.py`, `check_sand_allocation_basis.py`,
`draft_sand_screening_candidates.py` and `check_sand_screening_candidate_lci.py`.

Neither decision admits standalone river dredging, zinc mining, individual
chemical/waste suppliers, other releases or backgrounds, unrelated co-products,
reverse routes or cross-system-model mappings.
