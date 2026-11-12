# Production heat mapping batch

This batch reviews eight correspondences across ten exact cut-off source
profiles, using installed ecoinvent source inventories and native BAFU 2026
records. No runtime Python module changes are needed.

| Source | Inspected versions | Decision |
| --- | --- | --- |
| European natural-gas industrial heat market group | 3.8, 3.9 | Preserve each profile's 57 producer contributions in a production-side native heat recipe. |
| European 1 MWe lean-burn gas CHP heat | 3.8, 3.9 | Use the existing native 1 MWe exergy-allocated heat process, correcting the old heat-only-allocation match. |
| European and rest-of-world gas industrial furnaces above 100 kW | 3.9 | Use the complete Swiss 1 MW heat process, with size and geography warnings. |
| Rest-of-world wood-chip CHP heat | 3.9 | Use the complete Swiss 1 MWth exergy-allocated heat process; the source's 6,667 kW rating is fuel input, not thermal output. |
| Rest-of-world heavy-fuel-oil industrial heat | 3.9 | Use the complete native European 1 MW heat process. |
| Rest-of-world light-fuel-oil industrial heat | 3.12 | Use the complete native Swiss 1 MW heat process. |
| Swiss 25 kW wood-pellet furnace heat | 3.12 | Use the complete native 15 kW heat process with explicit size, annual-efficiency and emission differences. |

The original European gas-heat market match added a district network with
11% heat losses. Its source market layers instead connect producers and contain
no network, direct emissions, other inputs, uncertain shares or formulas. The
two new recipes retain all original deterministic producer contributions and
CHP/boiler shares, without adding a delivery network or loss multiplier. Native
large CHP suppliers retain source geography where available; other locations
use the existing ENTSO-E process as a geographic proxy. The native 1 MWe and
500 kWe gas-engine processes retain exergy allocation. The 200 kWe source uses
the reviewed 300 kWth heat proxy with explicit capacity-basis limitations.

All references represent allocated or useful heat per MJ, not fuel input per
MJ. No efficiency correction, capacity ratio, co-product credit or extra
combustion inventory is introduced. Signed caller quantities, formulas and
supported uncertainty remain unchanged. Complete native inventories replace
source technology, allocation, equipment, emissions, internal uncertainty and
vintage assumptions; score effects remain unquantified. Original source
biosphere exclusions are inherited limitations. These decisions use the user's
standing autonomous approval and retain warnings for later review.

The packaged review is
`brightpath/data/export/uvek_remaining_production_heat_review.json`, in
Brightpath commit `8f933b62f0e19b3d178d97adaee163b81d04552a`. It contains
reference and inventory-hash witnesses; complete licensed source vectors remain
private. Existing reviewed recipes remain unchanged.

Verification covers 26 migration tests, 33 related regressions, three fresh/cache
Premise tests, all 1,227 existing/new approval fingerprints, and 18 signed finite
LCIs of the native processes and weighted native supply recipes. The native
database modification marker remains unchanged. These LCIs do not establish
source/native score equivalence or exact joint uncertainty propagation.

Ruff passes for the new replay script and tests. The maintainer generator retains
its two pre-existing import-order findings; this batch adds no new finding there.
The full unrelated approval suite and a complete licensed ecoinvent scenario
build were not run for this data batch. Real mining preparation passes the
heat-market approval and stops at another unreviewed correspondence; mining and
the other affected sectors remain unqualified for public use until their
remaining inventories, shared updates and exports pass.
