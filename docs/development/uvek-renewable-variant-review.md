# Native wind templates use the shared renewable update

The earlier UVEK renewable run failed target coverage because the shared update's
hardcoded ecoinvent names matched none of the seven reviewed native generators.
This was a real filter gap, not missing IAM data or an inapplicable sector.

`data/renewables/wind_turbines.yaml` now declares exact name, reference-product and
unit identities for those seven generators and their 21 directly connected
moving-part, tower, foundation and offshore-substructure suppliers. The existing
InventorySet selector also admits their regional copies. It excludes older `xx`
models, aggregate wind/grid portfolios, an unused concrete-tower alternative,
unrelated products, wrong units and unbound CLIC records. The selected prototypes
have deterministic positive one-unit references. Missing prototypes or changed
reference conventions fail before copies are appended.

## Existing Premise logic and necessary Python changes

The existing `WindTurbine` class still copies each selected dataset, assigns a new
code, appends `, direct drive` to its name and relinks its construction purchases.
It retains the complete reference product, coefficients, emissions, uncertainty,
formulas and source documentation. Original datasets remain unchanged.

YAML alone could not supply filters to a method that only read Python literals.
The optional family mapping now supplies that selection to the existing loop.
An exact supplier set limits relinking to the selected turbine components:
substations, grid connections, factories, cables, roads, transport, lubricants
and disposal stay with their original suppliers. Renamed purchases drop stale
source input keys so normal linking resolves the new supplier identity.

The wrapper/constructor also receive the same explicit family and biosphere
arguments already forwarded to other sectors. Defaults retain ordinary ecoinvent
calls. Ecoinvent keeps its original eight name terms, unit-based construction
relinking, connection exclusion and copy behavior. No separate native engine or
NewDatabase argument is introduced. The support gate is updated only with the
real workflow evidence in the accompanying verification receipt.

## What this step establishes

As in the existing ecoinvent workflow, the renewable step creates preparatory
variant inventories. It does not calculate a new direct-drive bill of materials,
remove a gearbox or introduce permanent-magnet inputs. The native designs differ
in capacity, rotor/tower/substructure boundaries, lifetime and geography. A copied
name does not establish a verified direct-drive technology. These limits appear
in the YAML, warnings and generated comments.

The separate metals update remains unfinished. Its mappings must respect the
native component boundaries; a whole-turbine material intensity cannot simply be
assigned to moving parts while counting the tower and foundation again. This
renewable change does not qualify those future material mappings or demonstrate
source/native LCIA equivalence. Existing signed coefficients and supported
uncertainty are retained without inventing joint statistical correlations.

## Verification

Synthetic tests exercise exact identity selection, wrong-product and unbound-CLIC
exclusion, unchanged source records, signed quantities and uncertainty, complete
component linking, retained substations, stale input-key removal, missing
prototypes, invalid references and the default ecoinvent workflow. The explicit
background handoff and native powerplant filters are also checked.

The actual source-only probe creates 28 variants and passes exhaustive graph
validation without changing the installed source. The combined seven-sector run
also checks every copied exchange vector against its source template after only
the declared supplier-name change. Normal export and signed-LCI results are
recorded separately in the qualification receipt; the historical frontier receipt
is evidence of the original failure, not the current result.

The normal seven-sector REMIND SSP2-NPi 2050 call now passes graph validation and
exports 14,902 activities with normal reports. It creates 119 variants: 98 wind
generator copies across the original RER and 13 IAM/World locations, plus 21
turbine-component copies. All 119 copied exchange vectors match their source
vectors after only the declared supplier-name change. The audit counts 294
component purchases relinked and 126 other unit purchases retained. All 11,947
native source codes remain, and source database metadata are unchanged.

Every one of the 119 variants has finite LCIs for both signs: 238 checks. Strict
source/variant central LCI comparisons also pass for all 119 pairs at relative
and absolute tolerances of 1e-10. Private residual correction handles ordinary
linear-solver rounding; dimensionless normwise backward error is at most
9.24e-27. Componentwise relative error on essentially zero supplies is not used
as an acceptance metric: observed residuals below 1e-33 there can give a ratio of
one despite negligible absolute error. No runtime solver, inventory coefficient
or validation tolerance changes. All internal links and biosphere codes resolve.
See [qualification](uvek-renewable-qualification.json).
