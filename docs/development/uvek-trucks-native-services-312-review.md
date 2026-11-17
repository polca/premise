# Nine current lorry-service inputs

Nine required ecoinvent 3.12 cut-off inputs now use the same complete native
services already reviewed for earlier source versions: maintenance and disposal
of 16t, 28t and 40t lorries, plus brake, road and tyre wear-emission treatment.
Both native releases contain the exact named services with positive unit
references and nonempty inventories. The new source references were inspected
locally; earlier permissions alone did not authorize this version.

Maintenance remains one service per unit with factor +1. Source disposal and
wear treatment reference negative one unit or kg; native services reference
positive one, so factor -1 turns a negative treatment input into positive demand.
Caller quantities, units, uncertainty and formulas retain that sign convention.
There is no vehicle-mass, distance, payload or lifetime scaling, duplicate wear
emission, added material-recovery credit or additional vehicle inventory.

These are complete-service screening proxies. Vehicle design, maintenance
lifetime, component mix, recovery/allocation, wear composition, release
compartments, geography and vintage differ. Quantified score and functional
equivalence are not established. Limitations remain in the review and cache
audit. No transformation rule, existing helper or production/generator Python
module changed; the authoring index replays to identical mapping-resource data.
Only exact new Premise permissions and their evidence were added.

Eighteen Brightpath tests pass: ten signed service/scope checks and eight review
replay checks. Five Premise tests pass, including signed demands, cache warnings,
withdrawn permissions and preserved source-3.8 approval. Eighteen signed native
LCIs are finite, using the existing source/biosphere datapackages without a new
database, activity or processed file. Source and biosphere metadata are unchanged.

The actual truck preparation retry removes exactly these nine refusals: its
current ecoinvent 3.12 group falls from 27 to 18. The remaining group contains
thirteen material/manufacturing interfaces, three waste markets, one sea-freight
service and one antimony-mining process. It is not the count for every remaining
sector. Complete truck preparation, filters/physical coverage, public admission
and export remain unfinished. Fourteen of nineteen sectors remain qualified.
