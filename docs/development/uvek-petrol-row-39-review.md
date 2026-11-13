# RoW delivered fossil petrol — 13 November 2026

The RoW unleaded-petrol market previously selected an older Swiss refinery-only process. It represents delivered fossil fuel, including distribution, storage, evaporation and effluent treatment. The mapping now selects the complete native Swiss **Petrol, fossil, at regional storage** as a documented geographic screening proxy, per signed kilogram. It adds no helper dataset.

The source declares zero non-fossil carbon. Ordinary native CH/RER petrol-storage mixes contain biomass ethanol, so they are a different blend. The selected fossil process includes native CH/RER refinery supplies and fossil MTBE. Their input mass totals 1.001194 kg per delivered kilogram, with native loss allowance. Keep the native model without renormalization, an added refinery/delivery stage or invented energy/carbon correction. Caller fuel mass, uncertainty, formulas and separate combustion emissions remain intact.

This is whole-model replacement: 122 source internal rows become the complete 28-row native model. Source regional utilities, waste routes, freight, infrastructure and evaporation are not individually reconstructed. Distribution electricity changes from 0.0067 to 0.050762 kWh/kg. All ten source evaporation flows have the same publisher identities and compartments but different native amounts; native also reports 0.18274 MJ waste heat. Refinery technology, additive/specification, geography, delivery network, allocation, loss, energy/carbon basis, vintage, internal uncertainty and unquantified score effects remain explicit warnings. No LCI, LCIA or stochastic equivalence is claimed. No caller exchange is omitted.

Standing autonomous approval covers only the independently inspected ecoinvent 3.9 cut-off profile. All earlier approval operations remain unchanged. Other releases, patches, system models and reverse routes remain unapproved. No runtime Python module changed.

Verification: 27 Brightpath mapping/storage tests and five Premise fresh/cache, sign, uncertainty and refusal tests passed. Two signed LCIs and an independent native-demand comparison passed entirely in memory; no new Brightway database, registered activity or processed file was written. These verify implementation consistency with the selected proxy, not equivalence to the original source inventory.

A fresh battery preparation retry clears this petrol market and reduces the inspected source-3.9 cohort from 165 to 164 unapproved rules. Next is ilmenite-mine recultivation. Eight of nineteen sectors remain qualified; battery preparation and the wider integration are unfinished.

Machine-readable evidence: `uvek-petrol-row-39-verification.json`. Packaged decision: Brightpath `export/uvek_petrol_row_39_review.json`. Queued for the next coordinated batch push.
