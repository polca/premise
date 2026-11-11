# Native natural-gas CHP heat with CCS

This is a controlled shared-workflow qualification, not qualification of a complete real TIAM scenario or the combined fuels update. Eight heat role bindings remain unqualified: four CCS, two solar and two residual-heat bindings.

## Model and decision

The published ecoinvent-based CHP heat inventory cannot retain its original capture quantity unchanged when its heat supplier is replaced by native UVEK. That quantity would capture only 56.5% of the native allocated stack emissions. The authored resource instead retains the source's 8% heat penalty and requests capture of 90% of the native allocated stack: 0.03877632 kg gross, 0.034898688 kg captured, and 0.003877632 kg remaining per MJ heat. The negative fossil-CO2 counter-exchange equals the capture service demand. These quantities describe the allocated heat inventory, not an inferred physical CHP efficiency.

A no-burden reference bridge holds the reviewed German native heat supplier fixed. Without it, ordinary regional supplier relinking selected a differently allocated UK supplier and the fixed capture credit exceeded that supplier's stack emissions. The bridge adds no inventory burden, recovery credit or loss multiplier. German allocation, geography and technology remain explicit screening limitations.

The mapping selects this resource only when its IAM role is active. Its declared CLIC dependency is the complete natural-gas capture, 200 km pipeline and 1,000 m storage service. Normal lazy preparation resolves its eleven records; capture energy, infrastructure, transport and storage remain included. Capture-chain emissions are not subtracted as though they were the base plant's capturable stack emissions. Shared fuel-origin checks retain Premise's recorded-positive-total convention; they do not qualify a full combined fuels scenario.

## Verification

The normal `NewDatabase.update(["heat"])` and Brightway writer produced 12,075 activities in a controlled 16-region TIAM-shaped heat case. All 16 CCS suppliers, 17 secondary heat markets and eleven CLIC capture-chain records were inspected. Ninety-two signed-demand LCIs were finite; 48 carbon-origin checks passed. Every technosphere link is internal to the exported database and biosphere links use the requested ecoinvent 3.10 database. Exhaustive graph validation completed with no errors. See the adjacent verification JSON for the exact scope and evidence hashes.

## Necessary generic Python changes

Mapping resources can declare CLIC dependencies and exact supplier-quantity witnesses; preparation handles these before scenario forks. This avoids a new heat-specific inventory loader. See `prepared-inventory-dependencies.md`.

Exhaustive validation now reloads the original source when its in-memory store has been released, and applies the same declared reference-product aliases to baseline comparisons. Existing source cycles must not become false new-cycle errors merely because the baseline was lost. New feedback requires complete transformation declarations and a positive-vector proof that the absolute, reference-normalized net requirements have spectral radius below one. The proof is recorded in the validation report; undeclared, unit-gain, negative unit-gain and divergent feedback remain refused. No heat or fuels transformation engine was replaced.

## Attribution and modifications

Native basis: **Ökobilanz Datenbank der Schweizerischen Bundesverwaltung, BAFU:2026**, [BAFU life-cycle assessment database](https://www.bafu.admin.ch/de/oekobilanzierung). The authored resource is a modified screening model, not an official BAFU dataset. Modifications are the fixed German CHP reference, capture scaling, paired stack counter-exchange and IAM-region replication described above. Native data reuse remains subject to the [BAFU terms dated 23 June 2026](https://www.bafu.admin.ch/dam/de/sd-web/-9ZObeMCZy8A/BAFU-20XY%20Terms%20of%20Use_DE.pdf); corresponding attribution and terms are retained in the resource and activities. No full proprietary ecoinvent inventory is published here.
