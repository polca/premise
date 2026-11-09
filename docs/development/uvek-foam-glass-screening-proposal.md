# Foam-glass market: scoped feedstock-proxy decision

Status: user decision pending. The private candidate has finite native-background
LCI; no foam-glass mapping, helper or approval scope is enabled. Scope is only
the required ecoinvent 3.9 cut-off global foam-glass market in the electricity
pilot's nitrate-supply closure, targeting UVEK 2026 cut-off and ecoinvent 3.10
biosphere.

## Proposed screening option

Keep both original producer entries separately: 0.99009900990099 kg from
cullet-based production and 0.0099009900990099 kg from production without
cullet. Keep all four original freight entries and every market-entry
quantity/uncertainty field. Do not normalize shares or omit either branch.

Use the complete native `Foam glass, at plant` process per original kg for
both entries. It matches the final product's stated density (110 kg/m3),
thermal conductivity (0.040 W/mK) and positive kg reference. For the first
producer it also matches the original cullet and melting-electricity amounts,
within binary rounding for cullet. For the second, approximately 0.9901% of
supply, this is explicitly a **feedstock and technology screening proxy**:
the native process contains cullet, whereas that source producer does not.
That correspondence is beyond the standing same-product/geography approval.

Retain the native process's complete processing, packaging, raw-material
logistics, infrastructure, energy and disposal inventory once. Keep source
final-market delivery separately through reviewed complete rail, barge,
light-road and heavy-road services. Do not use regional-storage wrappers,
introduce renewable-certificate adjustments, add separate material/energy
credits, or invent a density, yield, cullet or moisture multiplier.

## Differences that the approval would accept

The cullet-free source replaces cullet with lime and silica sand, uses more
soda and melting electricity, and reports more direct fossil CO2. Applying its
original 0.0099009900990099 market share to the source's documented changes
from its cullet-based counterpart gives:

| Source process difference per kg of market supply | Quantity |
| --- | ---: |
| Sorted cullet | −0.00805792079 kg |
| Lime | +0.00151086015 kg |
| Silica sand | +0.00452231759 kg |
| Light soda ash | +0.00179093788 kg |
| Melting electricity | +0.00303558392 kWh |
| Direct fossil CO2 | +0.00130693183 kg |

These are source inventory differences, not calculated differences between
complete native and source LCIA scores. The proposed proxy does **not** retain
those internal process differences. Their score effect remains unquantified;
the small supply share alone does not establish negligible impact.

Native geography, energy, allocation, chemical supply, waste handling,
background vintage and biosphere exclusions remain limitations. The source
cullet-free producer has nine parameter definitions versus two in the original
process. Its feldspar and potassium-nitrate uncertainty/pedigree fields also
differ despite unchanged nominal quantities. The proposal keeps the complete
native internal uncertainty model; it does not reproduce that source producer's
internal uncertainty or parameter model. All source parameter definitions,
producer inventory hashes and the separate source market-entry uncertainty
remain in the review/provenance. No active source-parameter propagation or
source/native LCIA equivalence would be claimed.

This would approve only the specified minor-branch proxy inside the exact
3.9 cut-off global market. It would not approve standalone cullet-free
manufacturing, other product grades or releases, omissions, reverse routes,
cross-system-model conversion or unrelated chemical/utility suppliers.

## Reconstruction alternative

Retain and reconstruct the actual cullet-free source producer instead. The
fresh readback contains all 48 technosphere entries, direct biosphere exchanges,
uncertainty and nine parameter definitions. This route requires correspondences
for its chemicals, heat/water, wastewater, packaging, infrastructure and waste
suppliers. A modification of the native baseline limited to cullet and energy
would not, by itself, preserve that source inventory or uncertainty model.
Those additional supplier decisions must be resolved explicitly. No such hybrid
or complete source reconstruction is assumed while the choice is pending.

## Candidate checks

The private screening candidate has six separate inputs: two producer entries
and four original freight entries, linked to five distinct native suppliers.
Fresh read-only checks bind source and native inventories, positive unit
references in both installed UVEK releases, and prior complete-freight evidence.
It produces finite supply and nonzero LCI on the unchanged installed native
background. This is a candidate-component check, not a completed public
conversion, scenario certification, export or a comparative LCIA study.

Private artifacts: `foam_glass_screening_candidate.private.json`, SHA-256
`69280167026be83f677de83535229219b7e2aea0cc50e37bc07091b752e5ec3a`,
`foam_glass_screening_fresh_primary.private.json`,
`foam_glass_screening_candidate_lci.json` and `foam_glass_feedstock_delta.json`.
Reproduction scripts: `draft_foam_glass_screening_candidate.py` and
`check_foam_glass_screening_candidate_lci.py`.
