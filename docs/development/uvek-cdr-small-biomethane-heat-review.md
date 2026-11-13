# Correct small-boiler biomethane useful-heat supplier

The inspected ecoinvent 3.12 Europe-without-Switzerland service produces useful
heat in a condensing/modulating boiler below 100 kW. The previous heuristic
correspondence pointed to a 300 kW native boiler. UVEK 2025 and 2026 both contain
a complete 50 kW useful-heat service with a positive 1 MJ reference, which is
inside the source capacity class. Their direct vectors match.

Use that complete native service per signed MJ of useful heat, factor +1.
The native service buys 0.91324 MJ of its complete burner per MJ of heat. Its
documentation estimates 109.5% lower-heating-value efficiency at 30% partial
load and 2100 operating hours; those conditions are not established for the
source. Keep the native inventory whole and warn about efficiency, emissions,
fuel pressure/pathway, temperature, geography, vintage and infrastructure.
Do not add a boiler, burner, operating electricity or an extra heat multiplier.
This maps useful heat to useful heat; it does not preserve source fuel draw or
claim equal inventories/impact scores.

Only the inspected cut-off 3.12 profile receives a verified reference decision.
Other versions, patches and cross-system-model requests remain refused. The
single supplier rule and its integrity manifest change. The existing authoring
helper replays the JSON decision through an 11-line wrapper and three generator
import/call lines. No runtime Python module or Premise transformation changes.
All earlier scope fingerprints remain, and one new exact scope is registered.

See the [verification receipt](uvek-cdr-small-biomethane-heat-verification.json)
for completed test groups and real-run evidence. The remaining European water
and MDEA correspondences are still under review. Neither this correction nor
a passed inventory-preparation stage would itself qualify CDR or its export.
