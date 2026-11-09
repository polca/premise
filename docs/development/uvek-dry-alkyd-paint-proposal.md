# Proposed dry white alkyd-paint proxy for the UVEK electricity pilot

Status: private candidate checked; user approval pending. No migration rule,
helper or approval scope is enabled. This proposal covers only the two required
ecoinvent 3.9 cut-off solvent-free white alkyd-paint markets, RER and RoW,
inside the nitrate-supply closure of the existing nine-root electricity pilot.
The destination is UVEK 2026 cut-off with ecoinvent 3.10 biosphere. It does not
extend the earlier water-based-paint approval or approve standalone resin,
other paint types, releases, inverse routes or different system models.

## Proposed quantity and inventory boundary

The source reference is one kg of solvent-free paint. The complete native
paint represents a nominal 60% paint mixture. Its titanium-dioxide and resin
quantities support this nominal relationship; it is a screening basis rather
than an exact formulation or mass-balance equivalence.

Prepare copied foreground processes with the following explicit changes:

1. Scale the native paint inventory by 1/0.60 per source dry kg, including its
   remaining signed exchanges and uncertainty. Do not scale original market
   delivery or any separate caller solvent demand by this factor.
2. Remove the native paint's explicit bulk-white-spirit purchase, originally
   0.28 kg per native paint reference. This is removal of an input from a copy,
   not a negative solvent exchange or an avoided-production credit.
3. Replace its resin input with a copy of the same native resin process that
   removes only its explicit 0.429 kg white-spirit input per resin reference.
   Retain the other resin exchanges and use its original unit reference;
   introduce no additional 0.7 factor from the process title. The four principal
   resin precursor quantities match the source resin production exactly.
4. Add the source paint producer's original 0.01356 kg processing-white-spirit
   demand through the complete native white-spirit supplier. Preserve the
   separate original European and RoW uncertainty fields; their equal means
   do not establish identical uncertainty.
5. Retain every other native input and direct emission, including process
   wastes and infrastructure. Preserve all 14 source market freight entries
   and their uncertainty. Fresh evidence for these complete freight services is recorded below;
   their quantities are not part of the paint-concentration scaling.

This gives two geographic paint variants and one shared resin helper. Each
paint variant uses 0.49 native resin-reference units per dry paint kg, compared
with 0.4882 source resin-reference units. Titanium dioxide is 0.506667 rather
than 0.5051 kg. These small formulation differences remain explicit proxy
limitations; no balancing coefficient is invented to force equality.

## Limitations requiring approval

This is a modified native-product screening proxy, not a reconstruction of
all source production exchanges. Native electricity/heat, processing and
waste-management conventions, infrastructure intensity, raw-material logistics,
allocation, geography and vintage remain. The source nitrogen purge includes
0.019 kg of liquid-nitrogen supply and a matching direct nitrogen release;
the native process has neither. This proposal retains the native process
boundary for that purge, with an explicit difference/omission audit if approved.
It does not silently treat the source pair as impact-free.

The copied native paint retains its own processing emissions and waste inputs.
For example, the scaled aliphatic-hydrocarbon emission is about 0.006667 kg,
versus 0.00683 kg in the source. Removing explicit bulk carrier does not prove
full solvent balance, matching emission factors or matching LCIA scores.
The infrastructure and waste quantities also differ after nominal scaling.
Warnings must disclose these differences and their unquantified score effects.
Source and caller inventories remain unchanged; no separate caller solvent
purchase is removed, reduced or credited.

## Candidate verification and its limits

The private draft contains ten retained resin inputs and eleven inputs in each
paint variant. Only the two identified bulk-carrier exchanges are removed
from the copied native processes. The processing-solvent uncertainty variants
and all 14 original market freight entries are retained in the draft.

Both process variants produce finite, nonzero LCI on the installed native
background. All foreground technosphere inputs are nonnegative; there is no
avoided-solvent credit in either candidate graph. Native technosphere and
biosphere scientific signatures match the previous baseline before and after
the calculations. These are process-component checks, not completed market
conversions, scenario certification, exports or source/native LCIA equivalence.

An independent algebraic check compares the candidate against complete native
paint minus the two documented carrier supplies plus source processing solvent.
Its initial relative inventory difference is 2.03e-8. Every processed native
paint/resin coefficient was verified to match the float32 representation of
its source amount, whereas the copied recipes retain the source doubles.
Propagating the measured coefficient-representation differences accounts for
that discrepancy: relative inventory difference becomes 6.18e-16. No physical
coefficient or acceptance threshold was changed to obtain this comparison.
The actual foreground/background solve has mixed row error below 7.88e-17.

Private artifacts in the pilot evidence directory:
`nitrate_dry_paint_proxy_candidate.private.json`, SHA-256
`d86d9bc7f9eccc32a46a1f88efdc476c1451ff995a24b98b6bb422ead3b4c706`,
`native_dry_paint_candidate_lci.json`, `draft_nitrate_dry_paint_proxy.py` and
`check_native_dry_paint_candidate_lci.py`. Primary readback is retained in
`nitrate_paint_basis_primary.private.json`.

## Delivery evidence checkpoint

Fresh read-only inspection verifies all fourteen original freight suppliers and
ten native release-specific endpoints, for 24 complete records. Each source
identity and inventory hash has a structured match in existing reviewed
whole-service evidence. This includes the two light-commercial road services
and the European barge service; their prior witnesses explicitly identify
3.9 cut-off. The five modes use complete native rail, heavy-road, 3.5 t
light-road, barge and container-ship services. Native 2025/2026 positive unit
references and direct technosphere vectors match. Existing geography, fleet,
fuel, loading, infrastructure and vintage warnings remain applicable.

The original fourteen entries remain separate, with unchanged quantities and
uncertainty. This inspection adds no standalone freight approval and does not
resolve or relax the pending dry-paint scientific decision. Evidence:
`dry_paint_delivery_primary.private.json`, `dry_paint_delivery_evidence.json`
and `check_dry_paint_delivery_evidence.py`.
