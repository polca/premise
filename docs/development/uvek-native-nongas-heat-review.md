# Native non-gas industrial heat

The ecoinvent 3.9 cut-off global market group for district or industrial heat other than natural gas now uses the existing UVEK “Heat, at hard coal industrial furnace 1-10MW” (RER). Both references supply one MJ of useful heat; the multiplier is +1. The native boiler already includes its 80% efficiency. No heat helper, extra fuel, infrastructure, network loss or recovery credit is added.

This is an explicitly documented screening proxy. Hard coal supplies about 69.34% of the original global portfolio. The proxy replaces its remaining fuel mix, production technologies, allocation, geography, vintage and internal uncertainty. The score difference remains unquantified. Source exchange quantities, signs and supported uncertainty remain unchanged. Five pulp producers together represent about 0.032% of that original heat portfolio; reconstructing their entire chemical supply chains is unnecessary for this selected proxy.

Brightpath commit: `e3cb8d6d078ee49b80282cc41d91ae58f1b58c96`. The packaged review is `export/uvek_native_nongas_heat_proxy_review.json`. Premise adds exactly one independent approval scope; earlier scopes remain unchanged. No runtime Python module changes are needed for this mapping.

Sixteen focused mapping tests, the resource manifest check and two signed finite native LCIs passed. The original native database remained unchanged. Premise regression checks additionally exercise fresh imports, cache reuse and withdrawal of approval.

A private diagnostic using the real encrypted REMIND SSP2-NPi 2050 scenario successfully prepares the steel inventories: 138 imported records and 12,085 prepared base records. The diagnostic temporarily bypassed the public sector gate; it does not establish public steel support, successful transformation, certification or export.

The next shared steel transformation fails because no secondary-steel activity is found for CAZ. Native BAFU electric-steel suppliers exist, but the Steel class currently defaults to ecoinvent filters and the steel YAML has no native aliases. This is the next issue to address through native mappings and the smallest necessary propagation of the existing family setting. Other unfinished sectors and previously recorded full-suite failures remain unfinished.
