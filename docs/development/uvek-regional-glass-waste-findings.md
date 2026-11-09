# Regional waste-glass treatment reconstruction

Scope: seven exact ecoinvent 3.9 cut-off markets in India, RoW, Cyprus,
Colombia, Peru, Brazil and South Africa, required by the existing nine-root,
55-record electricity pilot. Destination is UVEK 2026 cut-off with the
canonical ecoinvent 3.10 biosphere. The European group is excluded because it
also contains Swiss fly-ash extraction, whose separate decision remains pending.

The source market and treatment references are negative one kg. Generated
helpers represent positive treatment demand; parent exchanges therefore use a
factor of minus one. This does not turn treatment burdens into recovery credits.
Caller signed quantities, formulas, supported uncertainty and ownership remain
preserved through the conversion.

## Retained treatment boundaries

All 30 original treatment-share entries and ten individual collection/freight
entries remain separate, in source order, without normalization or omission.
Eleven shared source-preserving helpers retain open burning, open dumps and
unsanitary landfill, including all five infiltration classes and the hyperarid
class. Their 146 original elementary exchanges and ten diesel-combustion or
skid-steer operating exchanges remain at their original signed quantities and
supported uncertainty. Uncontrolled disposal is not replaced by controlled
incineration, sanitary landfill or an empty recycling handover.

Forty-five unique elementary-flow definitions used by these helpers have exact
publisher-UUID, unit, full-compartment and emission/resource-role matches in the
installed ecoinvent 3.10 biosphere. Label or CAS differences remain explicit
canonical-identity warnings; no molecular, speciation or receiving-compartment
factor is inferred. The broader European inspection includes 75 definitions;
only the 45 actually required by the seven admitted reconstructions qualify here.

Operating services and complete same-mode freight reuse previously reviewed
function/unit and geographic approximations after fresh full-inventory hashes
match their authority. Generic rail includes the complete transport service;
it is not replaced by a coal-only or diesel-only traction process. No separate
machinery, fuel, loading, distance or infrastructure multiplier is added.

## Ordinary incineration and its limits

Only ordinary municipal incineration uses the complete native
`Disposal, glass, 0% water, to municipal incineration` process. Its 44 documented
wet-composition coordinates match the inspected ordinary source 3.9 process and
the previously reviewed 3.6 source, and both native-release metadata hashes
match the prior evidence. This reuses the existing complete-function approval;
it does not introduce a new incineration technology or fly-ash substitution.

The native name's 0%-water wording and the source reserved reference properties
must not override the documented wet-composition model, which contains 2%
water. The complete native process is used per original kg with no moisture,
dry-mass, energy or density multiplier. Native internal emissions, residue/ash
handling, energy allocation, infrastructure, uncertainty, geography and vintage
remain complete-service screening limitations. No LCIA equivalence or negligible
score difference is claimed.

The Swiss fly-ash-extraction process also has matching documented composition,
but that does not make its extraction/recovery technology equivalent to ordinary
incineration. It remains outside this review. The inspected European group's
Swiss extraction contribution is 0.009847212624043828 kg/kg; no additional
recovery credit or omitted extraction exchange is inferred.

## Evidence

The full read-only route inspection covers 150 source/native records across
all eight required glass markets and their European dependencies. The seven
independent compositions bind 37 fresh full inventories. Private artifacts:
`nitrate_glass_full_evidence.private.json`,
`nitrate_glass_biosphere_definitions.private.json`,
`nitrate_glass_documented_composition_preflight.json` and
`nitrate_glass_regional_draft_primary.private.json`.

Packaged review: Brightpath `export/uvek_pilot_nitrate_glass_review.json`,
SHA-256 `b18737d17178d3de6330c951d8fd2d3c0e0082c59941afb3c72090e9f9af0493`.
Only these exact source profiles and derived helper closures are admitted. No standalone component, other-release, reverse or cross-system-model
approval is inferred. Native upstream biosphere exclusions and all score
effects remain explicit, unquantified limitations. Full pilot scenario,
certification and export remain separate completion gates.

Verified checkpoint: 71 focused Brightpath cases and 57 Premise cases pass.
Seven actual migrated graphs produce finite native LCI, with unchanged native
technosphere/biosphere scientific hashes. Seven installed-wheel conversions
pass, full generator replay is byte-identical, and semantic comparison proves
source coefficients, supported uncertainty and ordering unchanged apart from
the explicit waste-reference sign conversion. All 365 previous fingerprints,
3,297 recipes and 719 shared reviews remain unchanged; seven exact scopes and
18 recipes are added. Total registered scopes: 372. Temporary wheel, installed
stage, verified build copies and nine superseded cache files were removed.
