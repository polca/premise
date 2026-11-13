# Native tailings consumers reach the shared regional treatment mix

BAFU's sulfidic-tailings disposal service has a positive one-kg reference.
Premise's existing tailings markets and the three converted CLIC recovery
processes use a negative one-kg waste reference. Renaming the native supplier
alone would not create a market or connect its consumers to the scenario.

The impoundment entry in `data/mining/tailings_activities.yaml` now declares the
source, supplier and market identities, their reference signs and limitations.
The existing `Mining` transformation reads this optional template before its
ordinary mixer runs. A separate negative-reference supplier retains all 77
native biosphere exchanges, including uncertainty. It represents the same
physical treatment per kg; there is no additional credit or burden.

The ordinary mixer creates the regional markets with the original four treatment
shares. Positive-reference native service identities then forward their callers
to those negative-reference markets with a factor of -1. Thus native callers
retain their signed quantities, uncertainty and formulas. The installed source
database remains read only. The source's global identity forwards to the existing
World mix; configured IAM regions receive their corresponding regional mix.

## Why a Python adaptation is needed

The native source has no tailings-market template. The existing mixer only
regionalizes markets it can find, so YAML aliases alone produced treatment
clones but zero markets. Two calls around the unchanged mixer now prepare a
mapped template and connect the native service identities afterward. The helper
methods live in the existing `Mining` class. They are inactive when the family
mapping has no `market_template`, as for ecoinvent. Unexpected reference amounts,
uncertain references, unit differences and unowned identity collisions are
refused. This introduces no separate UVEK mixer or constructor option.

The mixer AST is unchanged; all original treatment shares and ecoinvent filters
are unchanged. The adjacent verification receipt records these comparisons,
focused regression results and actual Brightway checks.

## Scientific limits

Native impoundment is a generic mechanically stable model with land use and
short-/long-term groundwater emissions, including a 60,000-year horizon. The
three complete CLIC recovery processes concern generic copper tailings. Applying
that treatment scenario to native callers is a screening approximation: ore
composition, recoverable metal, geology, climate, allocation and receiving-water
geography can differ. No yield, emission or metal-recovery adjustment is
invented. These limits are retained in YAML and generated dataset comments.
Existing configured geography and World fallback remain in use; no new regional
shares are invented. This is not a demonstration of source LCIA equivalence.

## Numerical verification

Each exported market has four negative treatment inputs and a negative reference.
Each native forwarder has a positive reference, one negative market input and no
biosphere exchanges. Exported matrix columns independently establish the sign
relationship. All treatment, market and forwarder LCIs are finite for both signs.

The installed `bw_processing` iterator stores matrix amounts at float32 precision,
while Brightway exchange records retain their declared shares. Weighted LCI
identities therefore use the actual processed coefficients, after checking that
they equal the declared values or their float32 representations. A mixed-scale
SuperLU solve also produced small cancellation residuals: a EUR forwarder/market
comparison differed by about 3.2e-8 before independent iterative refinement and
about 2.3e-16 afterward. Two residual-correction steps verify the strict identities
without modifying Premise, Brightway, exported data or validation tolerances.

The earlier family-handoff receipt remains historical: its zero-market result
predates this work. Qualification of other sectors and scenarios remains separate.
