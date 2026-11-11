# Secondary solar heat through the shared workflow

The REMIND solar-heat alias selects the dedicated CLIC `heat production, at 110 MW concentrating solar power plant, with thermal energy storage` record: one MJ of the source's declared heat output. Exact name, reference product and unit filters exclude electricity and native PVT hot-water outputs. Normal lazy preparation resolves 407 records, including complete construction, storage, operation, decommissioning and converted supplier helpers.

## Inherited screening boundary

Source inspection establishes that all six nonproduction coefficients in the heat record equal the corresponding one-kWh CSP electricity coefficients divided by 3.6. The mapping preserves Premise's existing electrical-energy-normalized screening convention. It does **not** establish a dedicated useful-thermal plant, delivery temperature, industrial heat grade, thermal efficiency or new allocation. No inferred receiver-power correction, heat recovery, extra storage burden or electricity coproduct credit is added.

The source describes a Spanish 110 MW tower design with nine hours of storage but labels the record CH. Geographic extrapolation, storage, vintage and upstream differences remain warnings. The previously approved omission of the unsupported used-nitrate-salt reuse credit remains audited; no new omission is introduced. Virgin salt production, plant and storage construction, and other decommissioning exchanges remain. Decommissioning retains its negative input and negative production reference.

## Reproduced failures and fixes

Exhaustive validation exposed twelve boiler clones whose valid zero-valued lognormal location was discarded during supplier relinking. The generic eight-condition correction in `uncertainty-zero-relinking.md` preserves zero-valued parameters; no validation rule was relaxed. Transformation lint still reports four pre-existing findings, verified unchanged against the prior source. Changed tests have no new lint findings.

A nitrate-storage recipe also retained two distinct source waste-rubber markets, Europe without Switzerland and RoW, which both resolve to one native disposal supplier. Only their original-source contributor comments were added. Quantities, signs, uncertainty and provider bindings are unchanged. Both stochastic exchanges remain separate; they are neither merged nor suppressed. Brightpath's integrity manifest was regenerated. Premise verified all old approval fingerprints and refreshed only the affected ecoinvent 3.9 cut-off nitrate-market scope after proving the complete resource differs only by those two comments. The corresponding packaged provenance review records the source identities and evidence hashes.

## Verification and remaining scope

A controlled REMIND heat case completed normal update, exhaustive graph validation and Brightway export: 12,633 activities, twelve regional solar suppliers and thirteen secondary markets. The two-thirds solar market share, signed direct-input vectors, treatment signs, complete links and separate rubber contributors were inspected. All 864 positive/negative-demand LCIs were finite. There are 419 CLIC-tagged records after regional cloning, compared with 407 prepared records. The final Premise regression group passed 157 tests; twenty nitrate tests and the separately rerun manifest test passed. The ecoinvent 3.12 five-sector regression completed with 31,205 activities, without export. See the adjacent verification JSON for scope and hashes.

This is a controlled workflow qualification of the inherited screening model, not a full real IAM scenario or thermal-performance validation. The industrial solar alias remains unadmitted: when secondary supply data is absent, the MESSAGE case needs Premise's frozen historical heat fallback, whose hardcoded ecoinvent market is unavailable in UVEK. Both residual-heat bindings remain unfinished too. No supply data was invented to bypass that missing fallback.

Three MESSAGE geography lookups are recorded in YAML: ERCOT to NAM, the documented BAFU Europe-without-Switzerland export label LBFAC32 to WEU, and legacy CPA to representative CHN. CPA includes non-Chinese areas; this is not an exact boundary equivalence or a reconstructed regional split. Supplier labels, quantities and inventories remain unchanged. Fourteen geography tests passed. Broader IMAGE legacy-label coverage remains to be resolved with its residual-heat case.
