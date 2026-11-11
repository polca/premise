# Native wood CHP heat with carbon capture

This authored screening model preserves complete Swiss native 1 MWth wood CHP heat with exergy allocation, replacing the retained heat-CCS workbook's 6,667 kW CHP basis. A fixed-reference bridge preserves the reviewed allocation during standard IAM regionalization. The technology, size, location, allocation, capture energy, vintage and score differences remain explicit limitations.

Native heat consumes 0.398 MJ of wood combustion service per MJ output. That service emits 0.092 kg non-fossil CO2 per MJ. Retaining the source model's 10% heat-input penalty gives 0.0402776 kg allocated stack CO2/MJ heat. Capturing 90% requires 0.03624984 kg of capture service and an equal negative **non-fossil** stack correction. Residual allocated stack emissions are 0.00402776 kg/MJ, excluding separate capture-chain emissions. The source 0.07124238 kg credit would exceed this native allocated stack and must not be transferred unchanged.

The complete CLIC wood post-combustion capture service includes its capture energy, plant infrastructure, 200 km pipeline, storage at 1,000 m and leakage. Its 20 MW capture-plant basis remains a screening limitation. Native wood-chip supply, auxiliary electricity, combustion infrastructure, ash disposal and upstream biomass uptake remain intact. No electricity coproduct credit, second carbon-removal credit or avoided-heat credit is added.

Native documentation states 35% electrical and 45% thermal efficiency and approximately 16% allocation to heat. Preserve the actual 0.398 inventory coefficient rather than recalculating it from rounded prose or inverting it into physical efficiency. One combustion description mentions natural gas, although its identity, actual wood-chip input and non-fossil stack emission identify wood combustion. This documentation inconsistency is retained in warnings; the inventory is not silently rewritten.

The role is prepared only when active in IAM heat data. It uses the existing shared heat transformation and normal lazy CLIC preparation. No alternative UVEK heat engine or Python runtime change is introduced. Controlled verification does not establish a complete real TIAM scenario, future biomass fuel portfolio, LCIA equivalence or combined-fuels qualification.

## Attribution and modifications

Native basis: **Ökobilanz Datenbank der Schweizerischen Bundesverwaltung, BAFU:2026**, [BAFU database](https://www.bafu.admin.ch/de/oekobilanzierung). This is an authored modified screening model, not an official BAFU dataset. Modifications are the fixed wood CHP reference, capture quantity, paired biogenic counter-exchange and standard regional replication described above. BAFU-derived elements retain the [BAFU terms](https://www.bafu.admin.ch/dam/de/sd-web/-9ZObeMCZy8A/BAFU-20XY%20Terms%20of%20Use_DE.pdf), attribution and modification reference in metadata. CLIC dependencies retain their own licenses and provenance. No full proprietary ecoinvent inventory is published.

## Controlled verification

The normal heat update, exhaustive validation and Brightway writer produced 12,075 activities in a controlled 16-region TIAM-shaped case: 16 wood CCS heat suppliers, 17 secondary heat markets and eleven complete CLIC capture-chain records. Ninety-two signed-demand LCIs were finite. All regional capture balances were 90% of the native allocated non-fossil stack. Native CHP and combustion nonproduction exchange quantities and provider codes matched the original database exactly, including complete upstream wood inputs. The shared heat/mapping/preparation suite passed 107 tests. See `uvek-native-wood-ccs-heat-verification.json` for exact scope and evidence hashes. No complete real TIAM or combined-fuels qualification is implied.
