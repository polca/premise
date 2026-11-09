# Natural-gas heat markets without added district distribution

Status: pending explicit scientific approval. Neither exact source profile is
admitted by the public pilot. Finite candidate LCIs do not authorize a mapping.

The proposal covers `bp-f7dd94e542cba21d`, the RoW natural-gas district/industrial
heat market in ecoinvent 3.9 and 3.12 cut-off, targeting UVEK 2026 cut-off with
ecoinvent 3.10 biosphere. Both source markets explicitly exclude transport and
heat losses. Their current district-heat-at-consumer match instead purchases
1.11 MJ of producer heat and district-distribution infrastructure per MJ.

Replace that endpoint with a helper retaining all 43 original supplier
quantities and marginal uncertainties. Every source and selected native
reference supplies positive 1 MJ of heat. The candidate uses the following
complete native producer services without an added distribution network:

| Source technology | Source share | Native candidate |
| --- | ---: | --- |
| Conventional CHP, 100 MWe | 68.5261% | Country's allocated `Heat, natural gas, at CHP power plant` |
| Combined-cycle CHP, 400 MWe | 5.6459% | Same country's allocated CHP heat service |
| Lean-burn CHP, 1 MWe | 10.0083% | `Heat, at cogen 1MWth, natural gas, allocation exergy`, CH |
| Lean-burn CHP, 500 kWe | 4.2151% | Same complete 1 MWth service |
| Lean-burn CHP, 200 kWe | 5.1837% | `Heat, at cogen 300kWth, natural gas, allocation exergy`, CH |
| Industrial furnace, above 100 kW | 1.5947% | `Heat, natural gas, at industrial furnace 1MW`, CH |
| Industrial furnace, low NOx, above 100 kW | 1.6331% | Native RER low-NOx industrial-furnace heat service |
| Modulating boiler, above 100 kW | 1.6087% | Native RER modulating-boiler heat service |
| Condensing modulating boiler, above 100 kW | 1.5843% | Native RER condensing modulating-boiler heat service |

The table is rounded; neither candidate renormalizes or aggregates the original
43 coefficients. Multiple source suppliers can use the same native process,
but their original quantities and uncertainty remain separate.

The CHP approximations need a new scientific decision. The native large-plant
services represent historical country fleets and do not distinguish the
source's conventional and combined-cycle technologies. Native engine sizes
are thermal ratings, whereas the source specifies electrical ratings. Their
documented electrical/thermal efficiencies imply approximately 233 kWe for
the 300 kWth model and 872 kWe for the 1 MWth model. These figures describe the
size approximation only; they do not scale the caller's heat demand or
reconstruct a generator. The native exergy allocation and complete plant
inventories remain as recorded. The source allocation formulas and internal
operating efficiencies are not reconstructed.

Where the exact country is available, use its native large-CHP heat service.
US electricity-region labels use the native US country service, representing
4.9966% of total source heat. Unavailable countries and RoW use the complete
ENTSO-E service as an explicit geographic proxy, representing 29.6963%.
Engine and ordinary-furnace services also retain their stated CH/RER
geographic limitations. There are 11 distinct native suppliers in each
candidate. This authorizes no general location rewrite or country alias.

Warnings must retain differences in capacity, cycle, allocation, heat quality,
fuel supply, internal efficiency, emission coverage, geography, vintage and
the imported native biosphere exclusions. The legacy low-NOx boiler/furnace
models are historical approximations, not current best-performing equipment.
Effects on impact scores are unquantified.

The caller factor remains **+1 per signed MJ of useful heat**. Each native
wrapper already applies its own combustion/efficiency factor. Add no second
fuel input, burner multiplier, plant, distribution network, heat-loss uplift
or electricity/heat recovery credit. Do not append the source producers'
combustion inventories to the complete native processes. Separately listed
caller exchanges, ownership, formulas and uncertainty remain unchanged.

Both private candidate mixes link to the exact installed background and
produce finite, nonempty LCIs. Independent native and extended-system
solutions agree within 2.60 × 10⁻¹⁰ in relative supply norm, with equation
residual below 2.34 × 10⁻¹⁶. Native scientific-content hashes match the unchanged
pilot baseline. These checks establish numerical linking of the candidates;
they do not establish impact-score equivalence, a public migration, scenario
certification or export.

Approval would cover only the two enumerated cut-off profiles and this
specified mix. Other heat markets, direct gas-volume conversions, reverse
routes and cross-system-model conversions remain outside this decision.
The existing user experience and warning/provenance path would remain.

Private evidence in the pilot work area:
`natural_gas_heat_supply_full_evidence.private.json`,
`natural_gas_heat_candidate_review.json` and
`native_natural_gas_heat_candidate_lci_report.json`.
