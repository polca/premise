# Native UVEK heat consumers

`plan_native_heat_consumers` returns replacement consumer copies and retained-input
records without changing its database or market-plan arguments. It is an internal
planning component; the public heat gate remains closed until callback,
certification and export integration pass.

The packaged review binds 165 exact native identities to their documented
functions. No substring search is used at runtime. Useful-heat inputs in MJ use
the industrial or buildings end-use layer according to the reviewed source
technology. Explicit delivered district heat uses complete district delivery.
This is a screening classification: the consumer's actual heat grade, pressure,
temperature and application may differ. Industrial furnaces and allocated plant
heat are not claimed equivalent to a future IAM end-use mix. Allocation,
technology, geography, vintage and score differences remain explicit limitations.

Only supplier fields change. Signed quantities, units, formulas, uncertainty,
bounds, output keys, other exchanges and reference production remain exact. Each
changed exchange has a literal original plus target, route, geography and
scenario in `premise heat consumer`. Negative heat inputs keep their sign without
adding another avoided-emission credit. All CLIC source inventories, migration
helpers, owned heat records and native MJ/kWh energy-reference inventories remain
intact. Upstream heat used to manufacture infrastructure or other materials can
change; this does not claim an invariant full supply-chain score for generators.

Network-only transport, ambient heat-extraction components and kg-based steam
are retained explicitly. Chemical steam consumes water and fuel-input MJ
combustion services; no useful-heat factor or kg/MJ conversion is introduced.
MSWI heat has an explicitly documented 100% allocation to waste disposal and 0%
to energy. Its actual production-only inventory and signed uses are retained,
rather than assigning a new IAM heat burden to a zero-allocated handover. This
specific empty inventory must stay empty to satisfy the review. Historical
retained services are not claimed updated or decarbonized.

Geography uses the usual Premise lookup for the consumer's location. Missing
geography or regional supply uses a World fallback with an explicit proxy audit.
Missing entire IAM layers are recorded as non-applicable. For supply-only IAM
inputs, industrial heat may use the secondary-supply layer, matching existing
heat behavior; buildings heat is not silently reclassified. Other lookup errors,
wrong links, ambiguous native sources, changed owned markets, unsupported
backgrounds and corrupted replay audits fail before any source mutation.

The real REMIND SSP2-NPi 2050 plan changes 962 inputs in 736 native consumers:
870 industrial, 89 buildings and three district-delivery inputs. One negative
input remains negative. One CPA mine input uses an explicit World geographic
proxy. Native and prepared uses of the retained network/steam/ambient/MSWI
boundaries remain auditable, including those inside protected inventories.

Full-graph checks use all 12,606 activities and canonical ecoinvent 3.10 biosphere
links, retaining the 12 native non-unit production references and all original
foreground references. Sixteen signed demands cover all consumer routes, the
geographic proxy, the negative input and the three World heat markets. Supply
and inventory vectors are finite, with complete keys/identities. Source database
scientific signatures remain unchanged; no database is written. Installed wheels
reproduce the consumer copies, literal audits, retained records and diagnostics.
This is not yet a public heat update/export or a claim of LCIA equivalence.
