# TIAM-UCL coke heat: existing industrial-furnace proxy

The native alias for `heat, secondary, from coke` now follows the existing
Premise convention: useful heat from a hard-coal industrial furnace represents
the coke role. The exact BAFU supplier is
`Heat, at hard coal industrial furnace 1-10MW`, RER, per positive one MJ of heat.
Its existing native witness is reused in the review ledger.

The alternative native coke stove is only 5–15 kW. It is not selected as an
industrial heat supplier. No new helper, fuel conversion, efficiency multiplier,
deallocation or recovery credit is introduced. Shared Premise regionalization,
market construction, consumer relinking and export remain unchanged.

**Screening warning:** this remains a hard-coal proxy for coke. It does not
reconstruct coke chemistry, coke production or coke-specific combustion.
Capacity, heat grade/pressure, technology, geography, allocation, composition
and vintage differences have unquantified score effects. Complete source
inventory quantities, signs and uncertainty are retained in regional furnace
copies. The standing approval for analogous proxies applies; the limitations
are recorded in YAML, the native review ledger and the verification receipt.

## Required geography declarations

The first controlled TIAM-UCL callback failed before selecting a heat supplier:
the shared mapper could not resolve three native labels. The existing geography
YAML now additionally declares:

| Native label | TIAM-UCL lookup | Interpretation |
| --- | --- | --- |
| CPA | CHI | Representative China-centred proxy for the broader legacy region |
| ERCOT | USA | Texas lies within the USA region |
| LBFAC32 | WEU | Same representative equivalence as Europe without Switzerland |

These declarations do not rename source records or establish identical regional
boundaries. All 122 source geography labels now resolve in the controlled run.
Mappings for other models are unchanged as declarations.

## Controlled qualification

The shared `_update_heat` callback was exercised on the complete native graph
with explicitly controlled postprocessed TIAM-UCL arrays: positive useful-heat
volumes of one part coal and two parts coke in every physical region. These
are synthetic volumes, not a decrypted real TIAM scenario or an unencrypted
IAM-file fallback.

Both roles remain in the diagnostics in all 16 physical regions, at one-third
and two-thirds of delivered heat. Both already represent heat output and retain
conversion factor one. Their common physical supplier is combined normally in
the market; no role volume is dropped and no second plant burden is added.

The full export contains 12,045 activities, including 17 regional/World
secondary markets and 16 regional furnace copies. Every furnace copy has an
actual technosphere consumer and preserves its source scientific exchange
fields. All 66 positive/negative market-and-furnace LCIs are finite and reverse
sign exactly. All 114,507 technosphere links are internal; all 290,756 biosphere
links use ecoinvent 3.10.

Export: `BAFU:2026-controlled-TIAM-coal-coke-heat-20261111-202851`.

Positive coke-with-CCS volume still raises a missing-supplier error. It is not
silently dropped or replaced with the newly reviewed non-CCS proxy.

## Validation and its limits

The heat phase reports no issues. Full legacy export validation reports no
errors and 13,927 warnings: 10,086 positive-waste, 3,839 orphan and two circular-
reference diagnostics. No warning rule or suppression was changed.

For comparison, the unchanged native source reports 10,087 positive-waste,
3,734 orphan and the same two circular-reference warnings. One positive-waste
diagnostic disappears because ordinary shared relinking combines two same-
supplier landfill inputs in a steel record, preserving their sum and sign.
Orphan warnings increase with standard regional-template expansion and legacy
consumer relinking in this controlled supply-only case. Independent readback
confirms consumers for every reviewed furnace copy. These diagnostics are
retained, not a claim that the complete background is warning-free.

The export comparison baseline uses the same declared electricity-product
normalization as the shared transformation. This accounts for 405 changed
product identities without changing their quantity vectors; no source identity
is missing after that normalization. A temporary harness initially stringified
lazy exchanges; it was corrected to materialize mappings before serialization.
Neither issue required product-code changes or skipped validation checks.

Seventy-five relevant heat, conversion, consumer and geography tests pass.
The real ecoinvent 3.12 REMIND SSP2-NPi 2050 five-sector regression also passes:
31,205 activities, 11 phases, no errors and 85 warnings. That regression is not
a real TIAM test. YAML/JSON parsing, whitespace and generated-reference checks
pass. No Python module or dependency changed and no database migration is needed.

## Remaining scope

This is controlled qualification of one additional heat binding, not a complete
real TIAM-UCL `NewDatabase` run or a new clean-checkout qualification. Ten heat
roles remain unqualified: five CCS roles, two solar roles, deep geothermal and
two frozen residual roles. Other heat/fuel limitations and registered sectors
remain in the active goal. See `uvek-secondary-coke-heat-verification.json` for
aggregate evidence; source inventory vectors and full diagnostics stay private.
