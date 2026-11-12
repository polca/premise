# Steel: iron ore, solid coke and rail corrections

Three ecoinvent 3.9 cut-off heuristic suggestions matched physically different
products or transport modes. They now use directly inspected native UVEK
suppliers, without creating helper datasets or changing runtime Python modules.

| Source | Incorrect suggestion | Reviewed native supplier |
| --- | --- | --- |
| Global iron-ore concentrate market, kg | Chromite | Global 65%-Fe ore at beneficiation, kg |
| Global coke market, MJ | Coke-oven gas | Global hard-coal coke at plant, MJ |
| European freight-train market group, tonne-km | Air freight | Complete native RER freight rail, tonne-km |

All source and native production references are +1 in the same unit. Each
mapping uses factor +1 and only the independently inspected 3.9 cut-off profile.
No existing Premise approvals covered these rule IDs. Other source releases,
reverse routes and cross-system-model conversions remain unapproved.

Iron concentrate is measured per kg of ore, not contained iron. The source
market explicitly states 65% Fe on a dry basis; the native beneficiation product
also states 65% Fe but includes lump and finer feed. Moisture and particle-size
detail remain limitations. Native 84% process yield is already internal; no
ore-grade, drying, yield or extra beneficiation factor is added. The whole
native supply replaces source primary/co-product shares and market freight.

Coke is a solid fuel supply. Both references already use MJ, so source
28.6 MJ/kg does not justify an additional mass conversion. The native process
retains its 79.8% coke allocation. It replaces the source coking mix, its minor
petroleum-coke contribution and market freight. No co-product credit, end-use
combustion or heat production is added. Native documentation calls for a more
specific local assessment where coke production materially affects results.

The rail service includes electric/diesel traction, shunting, vehicles and rail
infrastructure. Its historical DE/AT/IT/FR-based fleet replaces the source
European market-of-markets. Fleet composition, geographic weights, loads,
energy and infrastructure allocation differ; no second rail service,
infrastructure or distance multiplier is added.

All three are documented screening proxies. Complete native technology,
geography, delivery, vintage, internal uncertainty and native biosphere
exclusions can differ, with unquantified score effects. Caller signs, quantities,
formulas, uncertainty and separately listed emissions/freight are preserved.

Frozen Brightpath review:
`export/uvek_steel_39_product_corrections_review.json`, SHA256
`ea268b87ea68e8b15ffc993150372b4bf01d8ff36256280145f31341fb45cecb`.
Brightpath commit `889187971229b4eab3f7faf6be0fdc0064ab7fcf` is pushed.
The replay script checks all three parent rules before changing any of them.
Only derived basis facts and hashes are public; licensed vectors remain private.

Twenty-six Brightpath product/inert checks pass together (24.92 seconds),
covering signed quantities, normal/lognormal uncertainty, formulas, immutable
input, exact release restrictions, replay and drift rejection. The Premise strict
fresh/cache/approval-withdrawal test passes (18.47 seconds). Black and Ruff pass.
All three native suppliers pass six finite signed LCIs with exact sign reversal;
the 11,947-activity source database and modification metadata remain unchanged.
See `uvek-steel-product-corrections-native-lci-verification.json`.

Ferrosilicon remains unresolved. No native ferrosilicon identity was found in
the packaged bridge catalog. Its source producers are complete inventories,
including electricity, reducing agents, silica, steel and direct emissions.
Neither a battery-retention process nor pure silicon is an acceptable direct
substitute. A separate complete reconstruction still needs review.

## Installed package and real preparation

The regular conda environment installs the recorded Brightpath commit from
GitHub; its commit identity and all 777 approval scopes validate. Actual
encrypted REMIND SSP2-NPi 2050 diagnostic preparation now reports **33 pending
correspondences, down from 36**. All three product/mode corrections cleared.
The first remaining refusal is mill scale (`bp-007ad58954d82851`). Only the
closed steel support guard was bypassed; strict migration validation remained
enabled. No successful steel transformation or export is claimed.
Private audit receipt:
`ebb7163f6f815c5211c970c5c8797d14f2ed0a81adeabb4d0333df3b0123e579.audit.json.gz`.
