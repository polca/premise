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

## Biomass consumer identifiers

The legacy ecoinvent source cache strips activity codes. A synthetic cached
source reproduces `KeyError: 'code'` in `replace_biomass_inputs`; this is not a
missing fuel mapping. The same consumer-code set is subsequently used by the
biomass validator, which expects codes on every candidate activity.

The minimal generic correction assigns a UUID only when an activity lacks a
nonempty code, before the existing biomass filtering/relinking loop. Existing
codes remain unchanged. This mirrors the established export/cache-hydration
fallback and does not change the source-cache format, biomass eligibility,
supplier selection, quantities, allocation or uncertainty. It also permits old
source caches to work without deletion. Mapping files cannot supply these
runtime identifiers.

Regression coverage extends the existing signed woody-fuel test across cached
and uncached ecoinvent and UVEK sources. It checks unique restored identifiers,
existing-code preservation, consumer tracking, unchanged material consumers,
and retained source quantities and available uncertainty/formula metadata.

## Electricity observations

Read-only inspection used the local four-sector export
`BAFU:2026-remind-SSP2-NPi-2050-four-sector-review-20261110-205617`, not the
colleague's unavailable Windows export.

- The `3d93cee3` low-voltage helper comes from the reviewed European mineral-wool
  country-grid recipe. Standard electricity relinking reduces it to one kWh
  from the EUR IAM market. A full exported-exchange scan finds no consumers of
  this helper in the inspected local export. Its source-preserving name
  describes conversion, not the later scenario transformation; it is misleading
  when read as a claim that the historical mix survives the electricity update.
- Zero medium/low self-inputs are intentional. Native upstream input factors
  carry those losses. EUR medium and low markets retain upstream electricity
  factors above one; no additional self-loss should be added.
- Special Swiss SBB, certified, residual, BFE-2005 and guarantee-of-origin mixes
  are outside the ordinary source-market mapping and retain their exchanges.
  In the inspected export, low-voltage certified electricity has 171 consuming
  exchanges and medium-voltage SBB has 12. These are scenario-coverage
  limitations, not negligible unused labels. Their future treatment requires
  explicit mapping decisions that respect their distinct products.
- Regional IAM generation shares do not regionalize the underlying European
  supplier inventories. Water/land-use representativeness remains a genuine
  limitation of the approved technology/geographic proxies; score effects have
  not been quantified.

## Missing packaged cement review

The reported Brightpath revision
`6473d980671ed2a2524819bd5765c0e50d5da61f` contains
`brightpath/data/export/uvek_cement_treatment_39_review.json`. A clean wheel
built from a fresh `git archive` of that revision also contains the file, with
SHA-256 `b6c424b3b8c378e24b6541b599cbf52619b3cd7e7489ee8f5fda61fa8482ad45`.
Thus the report demonstrates a missing installed file, not a missing Git file.
The Windows environment failure has not been reproduced. Notebook interpreter,
import location and installed artifact must be checked together before assigning
its cause; changing or bypassing the approval policy is not warranted.

The currently pinned Brightpath revision `d4968e1f6611fa50c8252fed58cdb1472c3e5ee7`
passes all 712 approval-scope checks in the regular conda environment.

## Verification scope

The focused cache, biomass, electricity-comment, transformation and NewDatabase
suite passes 143 tests. Ruff and Black pass on the four changed Python files.
Both reported source failures fail in new regression cases before the fixes.
These checks do not constitute qualification of the remaining sector updates
or reproduction of the colleague's Windows environment. A real ecoinvent
3.12 cut-off / matching biosphere run completed the three standard updates for
REMIND SSP2-NPi 2050 with 29,640 activities. See
`uvek-colleague-ecoinvent-312-regression.json` for validation counts and limits.
The run used the current inventory resolution, not a replay of the colleague's
older archive, so activity-count equality is not asserted.

The corrected branch also completed the real UVEK four-sector standard workflow
and Brightway export. `uvek-colleague-four-sector-regression.json` records
13,695 activities, 39 regional electricity markets, internal technosphere links,
14 warnings and no validation errors, seven finite representative LCIs and
five further heat/consumer LCIs. Independent heat-consumer checks retained 970
source heat exchanges and 940 uncertainty rows across 746 consumers. This is a
four-sector regression, not completion of the remaining sector goal.

User follow-up: retain the special Swiss electricity mixes as they are. Review
replacing the wool-specific grid helper with the complete native European grid
through mappings, and audit standard regional power-plant duplication before
adding any UVEK-specific behavior. These follow-ups are not implemented by the
two source-failure fixes above.
