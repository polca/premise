# Existing native PEM catalyst suppliers

The native one-MW PEM stack already purchases `Iridium production` (GLO) and
`Platinum, at regional storage` (RER), both per kg. Their original UVEK archive
records independently confirm one positive kg in both the reference function and
the product output. Three version-2026 provider aliases now resolve the canonical
material interfaces to those exact native reference products. The platinum rule
explicitly permits the existing-input fallback; its schema still requires the
canonical product to match before resolving the version alias. Supplier identity,
unit, positive-reference and existing-location guards are unchanged.

This uses the suppliers already present in the stack. It adds no market, mining
process, geographic fallback, recovered-metal credit or additional process
inventory. The source's complete native platinum mix and delivery remain once.
All ecoinvent-version provider interfaces remain unchanged, including when no
global provider can be found.

The source iridium inventory is an opaque aggregated inventory with 31 direct
elementary-flow entries. It describes European 2000–2004 supply, is labelled
GLO, excludes investment goods and has no explicit own-iridium resource flow.
Its narrative mentions a tonne, while the published quantitative reference and
output both explicitly say one kg. Keep that published reference without
inventing a 1000-fold adjustment. These inherited limitations and unquantified
score effects are retained in configuration and copied into the updated
exchange's matched-design comment. The native inventory is not claimed to be
transparent, complete for resource assessment, or equivalent to ecoinvent's
economic-allocation proxy.

The real REMIND SSP2-NPi2050 diagnostic applies the existing matched PEM design:
0.1052631578947368 kg Ir and 0.3081921922823021 kg Pt per one-MW stack; the
configured pure-Ir/Pt design sets direct Ru to zero. These values come from the
existing catalyst trajectory, stack capacity and coating calculation; no new
loading, efficiency, allocation or area assumption is introduced. The three
matched decisions apply together, with the original GLO/RER supplier locations.
Unsupported-year and unavailable-supplier decisions for other rules remain
reported source-preserving decisions.

398 tests pass across native supplier cases, existing matched-design tests,
iridium fallback tests and the metals suite. Checks include all-or-preserve
application, no unreviewed regional replacement, propagated source limitations,
and refusal to apply native interfaces in five ecoinvent versions.

The actual diagnostic update and exhaustive graph checks pass with 12,040
activities and zero errors. A separate in-memory LCI overlays all 15 native
columns changed by that run, including the native mining resource corrections.
Every resulting technosphere and biosphere column matches the final scenario
at rtol 1e-12 / atol 1e-14. Cancellation uses the original processed matrix;
raw exchange means differ slightly and cannot be assumed to reproduce that
matrix exactly. Positive and negative one-stack LCIs are finite and reverse
sign. Source/biosphere metadata, processed files and the database registry are
unchanged; no Brightway database is written.

This qualifies the PEM supplier-interface checkpoint, not the complete metals
sector. Other equipment filters, ceramic suppliers, the titanium product-form
issue, an eligible-year mining-market check, combined-sector checks and export
remain outstanding. Public UVEK sector gates are unchanged.
