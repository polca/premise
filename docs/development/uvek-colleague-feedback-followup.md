# Colleague review follow-up — 11 November 2026

## Cache trimming failure

A real source-cache round trip of a synthetic biosphere exchange reproduces the
reported ambiguous-array error: `pandas.notna` returns an array for list-valued
compartments, which cannot be used as a single Boolean. Empty lists also fail.
Tuple-valued compartments do not trigger this particular pandas failure.

The minimal generic correction in `premise/core/utils.py` checks nullness only
for scalar values. Structured compartments are preserved unchanged; scalar
`None` and NaN are still discarded. The supported cache fields and numeric
conversion policy are unchanged. Mapping files cannot resolve this failure
because the cache must accept normal importer compartment metadata.

Regression coverage: `tests/test_source_cache_compartments.py` exercises list,
tuple and empty-list compartments through cache writing and loading, preserves
signed amounts and zero uncertainty types, and verifies scalar-null removal.
