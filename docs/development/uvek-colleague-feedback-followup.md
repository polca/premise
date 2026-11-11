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

At the initial investigation checkpoint, Brightpath revision
`d4968e1f6611fa50c8252fed58cdb1472c3e5ee7` passed all 712 approval-scope checks
in the regular conda environment. The wool follow-up below supersedes that pin.

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

## Regional electricity technology copies: agreed direction

The shared `create_region_specific_power_plants` mechanism already calls
`fetch_proxies` for a selected technology list, especially newer thermal/CCS
and storage technologies. It does not blanket-copy hydro, nuclear, wind and
solar generators. Consequently a regional IAM market may legitimately still
point directly to a European supplier, even though its technology shares are
regional.

The follow-up is to audit each mapped technology and create missing regional
copies through that existing shared mechanism, with mappings/configuration as
the first choice. Regional copies must retain the complete source inventory,
relink upstream suppliers through the normal Premise workflow, and use existing
IAM efficiency/emission updates where applicable. Any code adaptation must be
a minimal generic way to select additional technologies through configuration,
not a separate UVEK transformation.

A regional label alone does not establish regional water or land use. Where
independent regional factors are available, use the established correction
logic and document the source. Otherwise retain the source values and explicit
geographic-proxy limitations. Do not invent local hydrology, reservoir land,
capacity factors or emissions merely to make a regional copy appear local.
Check regional market supplier selection, links, validation and finite LCIs.
This plan is not yet implemented or a claim of regional inventory equivalence.

## Implemented direct European wool grid

Brightpath `778f9e443a67f7254ed63483828bbd0c275198ec` replaces the sole
country-grid recipe edge in European wool sorting with the complete native RER
low-voltage grid. The signed 0.0022-kWh input and all its uncertainty fields are
retained, as are every other treatment input and previous source-omission
boundary. The redundant country-grid helper is retired. This is an explicit
European composition/geography proxy before scenario updating; it does not
claim equivalence to the earlier 39-country historical mixture. Standard
Premise electricity updating subsequently relinks it to EUR without an extra
loss factor or an extra alias dataset.

Only the wool and dependent nitrate-salt parent approval operations are
refreshed after verifying their old fingerprints. Every other approval operation
is unchanged. The installed Git revision passes all 712 scopes. Requirements
and the colleague installation guides pin this revision together.

Verification: 55 Brightpath tests and five focused Premise approval/cache tests
pass. Full authoring regeneration reproduces both resource files byte for byte;
the canonical ecoinvent-3.10 biosphere resource is unchanged. A fresh wheel
contains every Premise approval review with its exact recorded SHA-256, plus the
matching migration resource and manifest. Historical replay tests now exercise
their actual generation stages: later independently reviewed leaf changes must
not be mistaken for the inputs to an earlier authoring stage. Their strict
change-detection behavior remains tested.

A fresh four-sector export contains 13,694 activities, exactly one fewer than
before. `uvek-wool-direct-grid-verification.json` records the missing retired
helper, the existing RER grid's deterministic one-kWh redirect to EUR, preserved
0.0022-kWh demand/uncertainty, and internal technosphere links. Comparing all
remaining exchange graphs by functional/recipe identity and following only that
unit redirect finds differences no larger than 1.2e-15 relative roundoff.
Before/after wool-treatment LCIs are finite and agree within recorded numerical
tolerances (relative L2 difference 4.5e-12). The full update reports 14 warnings
and no validation errors. No Premise runtime Python is changed for this mapping
simplification; the only production Python changes in this follow-up are the
two separately reproduced source-cache/biomass fixes.
