# Two shared battery and final-energy inputs

The exact ecoinvent 3.9 cut-off pitch and nickel-sulfate markets now convert
through reviewed helpers. Their original supply entries, quantities, marginal
uncertainty and freight stay separate. The actual battery preparation retry
moves past both inputs: its first unresolved source-3.9 group falls from eight
to six. The complete battery sector is still unfinished; fifteen of nineteen
sectors remain verified.

The source pitch comes from a European petroleum refinery. We use the complete
native European refinery-bitumen inventory per nominal kg as a production
screening proxy. The old paper-industry dispersant match is unsuitable. This
does not establish electrode-binder grade, softening point, sulfur, ash or
carbon yield, and does not authorize using bitumen physically in place of all
pitches. Native flow-specific refinery allocation and 2016 refinery data differ
from the source model. Caller baking, carbonization, energy and emissions stay
separate. There is no invented grade, yield or heating-value correction, and
this decision does not cover coal-tar or plant-derived pitch.

Nickel sulfate retains its two source shares: 91.80925% from an allocated
cobalt-production co-product and 8.19075% from separate chemical production.
Every one of the co-product's 290 nonproduction exchanges is exactly
0.11337491313401228 times the corresponding source cobalt-metal inventory.
That checked ratio scales the complete native cobalt inventory. This is an
allocation-scaled burden proxy; cobalt metal is not asserted to be physically
consumed to make nickel sulfate. The native cobalt model uses weak mining and
reduction estimates and explicitly excludes tailings and further-treatment
emissions. Those limitations remain and their score effects are unquantified.

The separate chemical-production branch retains all fifteen operations and
eight signed elementary flows. Existing reviewed native supplies represent
nickel feed, sulfuric acid, nitrogen, utilities, factory and wastewater demands;
canonical ecoinvent 3.10 flows retain the listed emissions and water exchanges.
The original pure-solid NiSO4 kg basis is unchanged. Native cobalt sulfate is a
different chemical and is excluded. No hydration, solution, molecular-mass or
stoichiometric correction is introduced. All four original freight legs remain.

Four on-demand helpers reuse fifteen existing helpers without altering them.
No production or generator Python module is changed. No caller, market or
separately listed chemical-production exchange is omitted. The normal generator reproduces all three resources, including the
unchanged canonical biosphere files. Twenty-three Brightpath tests and four
Premise tests cover signed quantities, uncertainty, exact scope, cached warnings
and approval withdrawal.

Eight signed finite LCIs and four independent demand comparisons pass without
registering a database or activity. The largest relative inventory difference
is 3.905e-13, below the 1e-10 tolerance. Source/biosphere metadata and processed
files remain unchanged. These checks verify the encoded native models, without
claiming source/native grade, allocation, uncertainty or impact equivalence.
Full battery/final-energy preparation, update coverage and export remain open.
