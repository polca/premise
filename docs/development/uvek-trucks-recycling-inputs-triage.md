# What remains behind the two truck polymer mappings

After the two waste-market fixes, the first failing source-3.8 truck cohort has
two unapproved market identities: recycled HDPE and amorphous PET with recycled
supply. This does **not** mean only two simple aliases remain to complete trucks.
Both markets require real sorting, collection and reprocessing inventories.

A read-only inspection followed only these two source chains and their required
recycling, sorting, collection-container and cut-off stock activities. An input
diagnostic of those 43 owned source activities finds 106 external correspondences
that still require caller approval or a correction:

| Function | Correspondences |
| --- | ---: |
| Waste treatment and related infrastructure | 31 |
| Collection and delivery transport | 31 |
| Materials and infrastructure | 18 |
| Heat and fuel | 10 |
| Water and wastewater | 9 |
| Electricity | 7 |

These counts refer to unique source identities in this particular diagnostic,
not new datasets that should all be reconstructed or separate decisions the user
must approve. Of these, 25 already have a verified sign review selected for source
3.8, 35 retain an unresolved sign review, and 46 have no sign-review metadata.
A verified sign alone does not establish a scientifically suitable supplier.
The counts can change as complete existing models replace parts of the chain.

The next work should reuse complete reviewed utilities, same-mode transport and
material supplies where their function and reference basis fit. Several global
legacy candidates need correction rather than approval: waste-preparation
infrastructure points to a data centre, corrugated packaging points to base
paper, and some non-gas heat points to gas heat. The effective source-profile
selection must be checked before judging a global candidate. Parameter formulas
in three source collection-container inventories also reference names absent
from the imported parameter lists; their evaluated amounts are available, but
formula retention or a documented frozen reconstruction needs resolution.

Genuine empty source cut-off stock handovers are distinct from nonempty paid
sorting and recycling. The diagnostic retains those handovers as owned source
nodes instead of matching them to unrelated disposal processes. This is a
diagnostic ownership decision, not a shipped recycling mapping. The empty native
PET/mixed-plastics recycling handovers cannot represent the paid processors.

No Brightway database was written for this inspection. No additional source
omission, unrestricted approval or production Python change follows from it.
The complete truck sector remains unqualified; further source cohorts or update
filters can still require work after these two chains are resolved.
