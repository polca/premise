# Natural-gas heat markets in the UVEK electricity pilot

The required RoW natural-gas heat market has 43 supplier entries in each of
ecoinvent 3.9 and 3.12 cut-off. Each supplies one positive MJ of heat, without
distribution or transport. Preserve every original supplier coefficient, order
and marginal uncertainty separately. The two version-specific market vectors
remain distinct; neither is aggregated or renormalized.

Use complete native UVEK heat-output services per original MJ, with factor +1.
These services already include their fuel demand, efficiency, infrastructure,
emissions and allocation. In particular, the native industrial furnace's
1.0526 MJ fuel input per MJ of heat already represents its documented 95%
efficiency. No additional efficiency factor, consumer-network burden, loss
uplift, electricity credit or heat credit is added. Negative caller heat credits
remain negative; this positive heat product does not use the waste-service
reference-sign reversal.

## Technology and geography

Keep distinct native industrial-furnace, low-NOx furnace, modulating-boiler and
condensing-boiler services. The source 200 kWe and 500 kWe cogenerators use the
complete native 300 kWth and 1 MWth exergy-allocated heat services respectively,
with explicit capacity, operating and geography warnings. For the source 1 MWe
lean-burn engine, use the closer native 1 MWe lean-burn heat service in RER,
rather than the earlier private candidate's 1 MWth service.

Conventional and combined-cycle utility CHP use complete native heat services.
Use the country endpoint where available, US for US subregions, and ENTSO-E
where a country or RoW supplier is unavailable. These are explicit geographic,
cycle and fleet proxies. There is no native global CHP endpoint.

The native RU CHP identity and its upstream burner link both specify RU, while
its descriptive geography text says RO. Retain the actual database identity and
links and report the contradiction. Do not silently relabel it or claim exact
geographic equivalence.

Complete-service proxies replace source internal production models, parameters,
formulas and uncertainty with the native inventories. They preserve market and
caller quantities, not internal inventory equivalence. Heat quality, efficiency,
load, allocation, capacity, fuel sourcing, geography, technology, vintage and
internal uncertainty differences have unquantified impact effects. The pinned
native bundle's 2,991 upstream biosphere exclusions remain explicit.

## Scope and verification

Only the two encountered source profiles, 3.9 and 3.12 cut-off, receive exact
Premise scopes. Other versions, standalone heat production, reverse routes and
cross-system-model conversion do not inherit this review. User inventories,
foreground ownership, signed quantities, formulas and separately listed
exchanges remain unchanged. The user authorized subsequent pilot modelling
decisions with documented warnings.

All 22 Brightpath and 15 Premise focused tests pass, covering signed caller
amounts, normal/lognormal uncertainty, formulas, non-mutation, cold/warm caches,
portable review metadata, unsupported profiles and atomic authoring drift.
Independent comparisons against read-only source evidence verify all 86 source
market entries, their order and uncertainty, and positive native heat references.
The prior 3,516 recipes, 730 shared reviews and 411 Premise fingerprints are
unchanged. Two recipes, one shared review and two exact scopes are added.

Both actual migrated graphs link to twelve native endpoints and produce finite
inventories on the installed UVEK 2026 / ecoinvent 3.10 biosphere background.
Exact foreground equations and independent native/augmented matrix solves pass,
with residual and refinement diagnostics retained privately. Native database
scientific signatures are unchanged. These numerical checks do not establish
proxy LCIA equivalence.

A built wheel converts both profiles through its own packaged public facade and
verifies resource and authorization hashes. The full generator reproduces
the resource byte for byte. The mixed-fuel heat market and broader
pilot refusals remain; full encrypted-IAM transformation, certification and
Brightway export are still required for goal completion.
