# Seven delivered-water markets: reconstruction or native screening proxy

Status: the user explicitly approved the screening option with warnings.
The seven exact source mappings and approval scopes are implemented and
verified.
The scope is only seven required
ecoinvent 3.9 cut-off water markets within the UVEK 2026 / ecoinvent 3.10 biosphere
electricity pilot. Caller quantities, signs, formulas, uncertainty, ownership and
separately listed water releases must remain unchanged under either approach.

## Complete source closure

A fresh read-only traversal includes the European market group as well as the
six national markets. It reads 48 water processes with positive one-kg
references, 67 internal supply links, eight self-links and 24 parameter
definitions. No exchange formula is present in these imported records. All
seven source systems solve with finite, nonnegative supply and maximum absolute
matrix-equation residual below 3e-16.

| Required source | Water-process operations in its closure | Market operation multiplier per delivered kg |
| --- | ---: | ---: |
| India | 5 | 1.7500562587 |
| European group | 17 | 1 (group reference; downstream markets retain their feedback) |
| Brazil | 4 | 2.4231880058 |
| Quebec | 10 | 1.2361186370 |
| Colombia | 6 | 1.8647147474 |
| Peru | 3 | 2.1667573671 |
| South Africa | 3 | 2.2858093022 |

These multipliers solve the recorded source equations. They are not proposed
engineering loss factors or scaling factors for a complete delivered-water
supplier. The European group contains two entries: 0.002087020385985745 kg from
Switzerland and 0.9979129796140143 kg from Europe without Switzerland.

Source-preserving reconstruction requires retaining this cyclic foreground
structure, its distribution infrastructure, water returns, treatment mix and
supported uncertainty. Brightpath's current recipe schema rejects dependency
cycles. Mean-only algebraic elimination would change uncertainty propagation
and correlation and has not been approved. The closure also has 76 external
supplier identities; only seven currently have exact scoped approvals, leaving
69 correspondences to review. Those include chemicals, treatment infrastructure,
electricity, diesel pumping and waste services. They are not automatically
approved by their presence in a water process.

## Proposed screening option

Use complete native `Tap water, at user` (RER) per original signed kg for the
six national markets. For the European group retain its two original entries,
amounts and supported uncertainty, using complete native CH and RER delivered
water respectively. Both native references are positive one kg in UVEK 2025
and the pinned UVEK 2026 database.

This proposal deliberately substitutes the **whole native utility model**.
It replaces the source's internal treatment shares, supply feedback, losses,
network quantities, direct water returns and internal uncertainty. It does
not retain or reconstruct those source exchanges. In particular:

- The six national markets become explicit European geographic proxies,
  including their electricity, groundwater/surface-water mix and treatment.
- Native treatment, chemicals, infrastructure, maintenance, delivery, disposal,
  elementary flows and internal uncertainty remain complete and occur once.
- Do not multiply a complete native delivered-water supplier by the source
  self-loop multiplier. Native delivery already includes its own model.
- Do not add the source network purchase or source market water release on top
  of the complete native utility. That would mix delivery boundaries.
- Do not invent an embodied-water, loss, density or water-balance correction.
  Native and source water-accounting differences remain explicitly disclosed.
- Separately listed caller water purchases, emissions and wastewater are
  preserved. This approval would not authorize deleting or changing them.

Native CH/RER ordinary processes are historical estimates based on Swiss water
works and, for RER, German energy data. The direct native RER water-resource
inputs total approximately 0.00112866 m³/kg; its two explicit water returns total
0.0008567565 m³/kg. The corresponding source European closure's direct water
inputs and returns are approximately 0.0011680891 and 0.0001476730 m³/kg.
Those are **water-subsystem elementary flows**, excluding other upstream
suppliers' water use; their difference is not an LCIA result or an equivalence
claim. The different embodied-water/return conventions can materially affect
water-related results. No negligible-impact claim follows from a kg match.

Warnings must retain geography, treatment technologies, loss and network
assumptions, water-resource and return compartments, embodied-water accounting,
allocation, background vintage, native internal uncertainty and the bundle's
upstream biosphere exclusions. All score differences remain unquantified.
Only these exact source profiles would qualify; no reverse, other-release or
cross-system-model correspondence follows.

## Why country-labelled native alternatives are not silently substituted

The original UVEK XML confirms that the MoeK-labelled processes require callers
to account for one kg of embodied water. That is an additional accounting
convention, not an automatic add-on for every existing source caller. Several
countries are available but neither Peru nor Quebec has that exact native
process. The Colombia-labelled process uses Peruvian electricity explicitly.
The native RER MoeK process also has a different recorded return-water value
from the country variants (1.072265 kg rather than 0.111 kg). These are inherited
native data, not repaired here. Selecting and modifying them would need its own
regional and water-accounting design. The proposed simpler ordinary CH/RER
proxy states its geographic and water-accounting limitations directly.

## Reviewable artifacts and alternatives

Private evidence includes `water_delivery_closure_primary.private.json`,
`water_delivery_closure_findings.json`,
`water_delivery_closure_mapping_coverage.json`,
`native_water_original_balance.private.json` and
`native_water_delivery_candidates_primary.private.json`.
The native balance audit reads the original publisher XML and exact elementary
flow compartments, including its kg-to-m³ water-return representation.

The seven original candidate compositions are in
`complete_native_water_proxy_candidates.private.json`, with fresh native
witnesses in `complete_native_water_proxy_primary.private.json`.
Numerical checks are separate from approval and public migration. They do not
establish source equivalence, comparative LCIA, scenario certification or export.

The alternative is source-preserving reconstruction with cycle-capable generated
inventories, retaining all source distribution and treatment boundaries and
reviewing the 69 currently unscoped external correspondences. The whole-native
option must not be enabled merely to make the pilot pass: it requires explicit
approval of the scientific substitutions above.

All seven private candidates produce finite, nonzero native-background LCI.
The native technosphere and biosphere signatures match the earlier baseline
before and after calculation. Candidate SHA-256: `1f5cf8e9a5a812826cd9ee3a1c2f4f4dbb3b4f53c7f72c6b1a27d565018c34a5`.
Readback: `complete_native_water_proxy_candidate_lci.json`.

## Existing 3.12 mapping remains separate

South Africa already had an approved ecoinvent 3.12 cut-off mapping to the
complete Swiss utility. That default supplier and factor +1 remain in place;
only the new exact 3.9 profile selects the European helper. The original 3.12
review and authoring method are retained. One whole-rule fingerprint must be
reissued because the parent now also declares a 3.9 profile; its 3.12 supplier,
quantity basis and generated-inventory graph are unchanged. The other 371
previous fingerprints remain unchanged; seven new exact scopes give 379 total.

Packaged review: `export/uvek_pilot_nitrate_water_review.json`, SHA-256
`a9db19e74455732385fcddaceae44a514807126ad5cf549e95253242fd5c8e8b`.
Registration receipt: `nitrate_water_scope_registration.json`; semantic review:
`nitrate_water_staged_review.json`. Seven recipes are added; all previous 3,315
recipes and 720 shared reviews remain unchanged. These water correspondences
replace the source's internal model as explicitly approved; they do not claim
to preserve or solve its uncertain cycles in the destination.

The South African parent carries neutral shared review text that distinguishes
3.9 from 3.12, while each exact Premise scope retains its own inspection and
limitations. Two shared reviews are added in total (722), including that
version distinction. Existing dataset/exchange values for 3.12 are checked
against the previous rule, allowing only a provenance-comment difference.

Verified implementation: 72 focused Brightpath tests and 60 Premise tests pass,
including the existing delivered-water warning/cache regressions. All seven
actual migrated graphs yield finite native LCI; native scientific signatures
remain unchanged. Seven installed-wheel conversions pass and full generator
replay is byte-identical. Build copies, installed stage, wheel and 18
superseded cache files were removed, retaining the current five audited cache
entries and all lockfiles. The full electricity scenario/certification/export
has not yet completed.
