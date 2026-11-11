# Native coal CHP heat with post-combustion capture

The authored useful-heat screening model uses complete German native exergy-allocated coal CHP heat. Its reference bridge fixes that allocation during normal Premise regionalization. This is a German historical fleet proxy, not a new regional CHP design.

Native heat requires 0.77 MJ of coal combustion service per MJ output. The combustion service reports 0.094634 kg fossil stack CO2/MJ. Retaining the original heat CCS model's 9% heat penalty gives 0.0794263162 kg allocated stack CO2/MJ heat. The new capture demand is 90% of that quantity: 0.07148368458 kg/MJ. Its negative fossil stack counter-exchange is identical. The remaining allocated stack is 0.00794263162 kg/MJ, excluding separate capture-chain emissions. Transferring the original 0.048181815 kg capture quantity unchanged would remove only 60.7% of the native stack.

The retained workbook labels the coal model post-combustion but links oxy-fuel capture. This reconstruction uses CLIC's available post-combustion coal capture service, including its complete energy requirements, infrastructure, 200 km pipeline, storage at 1,000 m and leakage. No second electricity/heat recovery credit or distribution loss is added. The original 9% penalty is retained as a screening assumption; it is not derived by inverting the native allocated fuel draw.

The coke-with-CCS role retains Premise's existing coal CHP proxy convention, with explicit coke chemistry, upstream fuel and technology limitations. Both aliases require active IAM data and use the shared heat transformation. Complete real TIAM and combined fuels scenarios remain subject to separate qualification.

Capture-chain prerequisites include independently inspected whole-plant coal construction, softened water and delivered light fuel oil. Only exact ecoinvent 3.6 cut-off correspondences are approved. Reviews preserve signed demands and uncertainty and explicitly record native capacity mix, treatment, freight, disposal, geography and vintage differences. No missing dependency is bypassed or silently omitted.

## Attribution and modifications

Native basis: **Ökobilanz Datenbank der Schweizerischen Bundesverwaltung, BAFU:2026**, [BAFU database](https://www.bafu.admin.ch/de/oekobilanzierung). This is an authored modified screening model, not an official BAFU inventory. Modifications are the fixed CHP reference, capture scaling, corrected capture technology and regional replication described above. Reused BAFU-derived elements retain the [BAFU terms](https://www.bafu.admin.ch/dam/de/sd-web/-9ZObeMCZy8A/BAFU-20XY%20Terms%20of%20Use_DE.pdf), attribution and modification reference in resource metadata. CLIC dependencies retain their own licenses and provenance. No full proprietary ecoinvent inventory is published.

## Controlled verification

Coal, coke-proxy and combined coal/coke cases each completed the normal heat update, exhaustive validation and Brightway export with 12,074 activities. Each passed 90 signed finite LCIs and 48 carbon-origin checks. Each contains 16 regional CCS suppliers, 17 heat markets and ten complete CLIC capture-chain records. In the combined case the shared supplier receives the complete unit market demand once; coal and coke contributions are not double-counted. The shared heat/mapping/preparation group passed 105 tests and the dependency scopes passed 11 signed/profile tests. An encrypted REMIND SSP2-NPi 2050 ecoinvent 3.12 five-sector regression completed with 31,205 activities (no export in that regression). These controlled inputs do not qualify a complete real TIAM scenario or combined fuels update. See `uvek-native-coal-ccs-heat-verification.json` for scope and evidence hashes.
