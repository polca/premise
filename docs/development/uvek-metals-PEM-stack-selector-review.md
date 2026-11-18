# Native PEM stack selector and treatment exclusion

The native one-kilowatt PEM fuel-cell stack is now selected by exact name,
reference product and unit. Its original production reference is positive one
unit. The added capacity entry uses the same 0.001 MW per unit conversion as
Premise's existing one-kilowatt curated stack. No material intensity, efficiency,
allocation, operating inventory or unsupported-year rule changes.

The published CLIC stack remains a separate identity. Its original inspected metadata had no metals role binding; native aliases
cannot establish one. A subsequent classification-only mapping now supplies
that reviewed binding; see `uvek-metals-published-PEM-binding-review.md`.
Full metals integration remains unfinished. This change does not qualify metals.

A case-sensitive mask now excludes two capitalized treatment records that the
broad existing solid-oxide-stack filter previously matched. Disposal is not
manufacturing. Wrong-unit, wrong-product and treatment controls remain excluded.
The exact native references, conversion and combined-metadata census are in
`uvek-metals-PEM-stack-selector-verification.json`.

All 362 existing mapping/material tests pass in a temporary copy where the
conversion-count expectation is refreshed from 68 to 69. The repository test
remains unchanged pending the separate test-only approval. All other existing
test statements are unchanged. No Python module, test or new upstream process
was added by this mapping change.
