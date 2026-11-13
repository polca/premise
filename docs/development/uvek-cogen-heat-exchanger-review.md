# Correct the mislabeled Jakobsberg cogeneration component

The real carbon-removal preparation reached rule `bp-3e3963da2137593c` after
the pump and spent-solvent correspondence reviews. Its former heuristic match
sent a source named “heat pump production, heat and power co-generation unit,
160kW electrical” to the shared components of a cogeneration installation.
Those are different inventory boundaries. That match is replaced.

The native “Heat exchanger of cogen unit 160kWe” describes the same Jakobsberg,
Basel installation, lambda=1 engine, 1987 technology and assumed 80,000-hour
operating lifetime as the source activity comment. Both use 590 kg reinforcing
steel and 590 kg low-alloyed steel per unit, 1.77 m³ water intake and a total
1.42 m³ pig-iron-effluent treatment. This supports interpreting the source's
simplified steel component as the native heat exchanger. Source name, reference
product and product-information text instead say heat pump (including a 60 kW
capacity). That inconsistency is explicitly retained as a review limitation;
this is not an approval to substitute heat exchangers for heat pumps generally.

Native 2025 and 2026 references are each +1 unit and their direct input summaries
match, independently of exchange ordering. Requested source cut-off profiles
3.8–3.12 were inspected with +1 unit references and the same principal materials
and effluent total. Use the complete native inventory at factor +1 per signed
unit. Do not add or scale operations, capacity, lifetime, efficiencies, shared
cogeneration components or recovered-energy credits. Older, patch, reverse and
cross-system-model scopes are not approved.

The source and native inventories are not equal: native road/rail freight is
explicit (59 and 708 tkm), source freight is absent at this level; water-return
coverage differs, as does the upstream background. Native effluent treatment
also replaces the source country split. Keep those differences as warnings,
with unquantified score effects. Neither direct-vector nor LCIA equality is
claimed. No native Brightway inventory is changed.

Only one compiled rule changes, with its reference-profile guard, review and
integrity manifest. Brightpath's existing mapping-authoring helper replays the
new JSON decision; its generator gains one call and imports. Those maintainer
scripts are not runtime transformation modules. Premise receives only the
approval ledger, dependency pin, tests and documentation; its Python runtime
and shared transformation logic are unchanged.

The [verification receipt](uvek-cogen-heat-exchanger-verification.json) records
the package revision, completed checks and real carbon-removal result. This
mapping does not itself qualify the sector or complete the wider integration.
