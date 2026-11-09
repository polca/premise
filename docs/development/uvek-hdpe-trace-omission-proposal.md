# Proposed scoped omission of trace recyclate in one HDPE market

Status: user decision pending. No mapping, omission policy or approval scope is
enabled. Scope is only the ecoinvent 3.9 cut-off global market for high-density
polyethylene granulate required by the existing electricity pilot's nitrate
supply. Destination: UVEK 2026 cut-off with ecoinvent 3.10 biosphere.

## Source inspection

The market has two positive virgin-production entries, four positive
recycled-to-generic entries and three delivery entries. Each recycled entry is
2.93285310007683e-14 kg per source kg, for CH, US, RoW and Europe without
Switzerland. Together they supply 1.173141240030732e-13 kg per source kg.
These quantities are positive, not zeros. The source renaming providers each
have a positive unit reference and buy one kg of a recycled-HDPE market.
Those markets buy nonempty recycling inventories, with operating chemicals,
electricity/heat, sorted feedstock, waste treatment, infrastructure and delivery.
The source renaming providers declare production volumes of 0.001 kg; that
metadata does not authorize discarding their positive market coefficients.

Native UVEK's `xx Recycling PE` and `xx Recycling mixed plastics` are empty
cut-off handovers. Both have positive unit references and no nonproduction
exchanges; their documentation allocates recycling burdens to the recycled
product's production. They cannot supply the inspected nonempty recyclate.
The documentation's rough electricity/avoided-product suggestion does not
establish a complete attributed recyclate inventory. No such substitution
credit or invented 0.6-kWh process is proposed.

## Proposed decision

Approve omission of exactly the four listed source market entries, recording
for each its identity, original amount, uncertainty fields, source profile and
inventory hash, together with an explicit warning and omission audit. Do not
establish a general small-exchange threshold or authorize other omissions.
The omitted branches have nonempty burdens; their LCIA effect is unquantified
and is not asserted to be zero merely because their mass is small.

Retain the two virgin-producer quantities separately:
0.809364548494888 kg from RoW and 0.190635451504994 kg from RER. Use the complete
native European virgin-HDPE granulate inventory for the matching product/kg
function under the standing geographic/product screening proxy, retaining its
limitations. Their total remains 0.999999999999882 kg. Do not renormalize it,
aggregate the two entries, infer a recycled-content multiplier or scale the
original market freight. Preserve all three delivery entries, quantities and
uncertainty using the previously inspected complete rail, heavy-road and
container-ship services.

Native product vintage, geography, background allocation, cumulative-inventory
boundary and emission differences remain explicit warnings with unquantified
score effects. The cumulative native material inventory does not provide an
explicit internal supply chain through which the electricity pilot can update
all embodied energy. No caller quantity, formula, uncertainty, ownership or
separate purchase changes. This approval would not admit standalone recycled
HDPE, the native empty disposal handovers, other releases or reverse and
cross-system-model conversions.

The alternative is to retain all four nonempty recycling branches and prepare
their source-based reconstructions, including the additional treatment and
utility dependencies. No omission is assumed while this decision is pending.

## Private candidate and checks

The private five-input candidate keeps two separate virgin entries and three
original freight entries. All source quantities and uncertainty are copied
without normalization. The four proposed omissions have separate original
exchange positions and nonempty-producer witnesses. All native endpoints have
positive unit references in both installed UVEK releases; the original source
freight witnesses match existing reviewed complete services.

The candidate gives finite supply and nonzero LCI on the installed native
background, with unchanged native technosphere and biosphere scientific
signatures. This verifies only the proposed remaining inventory. It does not
quantify the omission effect, establish source/native LCIA equivalence or prove
a working migration, certified scenario or export.

Evidence in the private pilot directory:
`hdpe_trace_omission_candidate.private.json`, SHA-256
`e60c8bbc0e97402b987beabb34e63f556f860a2dac173947d7ca4b65333ca118`,
`hdpe_trace_omission_fresh_primary.private.json`, `hdpe_trace_candidate_lci.json`,
`nitrate_hdpe_foam_routes_primary.private.json` and
`native_pe_recycling_candidates.private.json`. Reproduction scripts:
`draft_hdpe_trace_omission.py` and `check_hdpe_trace_candidate_lci.py`.

Implementation, if approved, must expose the four omissions in warnings,
portable provenance and audit output through an exact reviewed scope. Existing
refusals for other omissions must remain; broadening pragmatic loss allowances
is not part of this decision.
