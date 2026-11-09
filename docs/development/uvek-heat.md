# Native heat development

Heat remains outside the public UVEK update gate. Biomass and electricity are
supported screening updates; native heat supplier preflight does not establish
a working heat transformation or prospective export.

The current batch adds 28 exact `uvek_aliases` alongside unchanged ecoinvent
aliases in `heat.yaml`. Each binds a complete, inspected native process with a
positive one-MJ delivered-heat output. Source identities, inventory/metadata
hashes, reference bases and scoped proxy limitations are recorded in
[`uvek2026_review.json`](../../premise/data/heat/uvek2026_review.json).
Licensed process coefficients remain in private evidence.

`plan_native_heat_suppliers` checks agreement between that review and the YAML,
exact name/product/location/unit, nonempty reference output and declared energy
routes before any scenario mutation. It refuses missing or ambiguous matches,
changed production references, unreviewed geography/product substitutions,
foreground/helper impersonation and unphysical conversion factors. There is no
name heuristic or automatic fallback to another family.

Sixteen roles convert IAM final energy to delivered heat using actual native
fuel-input services, electricity draw or the hydrogen LHV documented by the
specific native boiler. All original quantities, formulas and uncertainty stay
in the LCI. These are nominal conversion factors, without an invented upstream
uncertainty forecast. The other 12 bindings have heat-output IAM bases and use
factor one; in particular, an exergy-allocated CHP fuel draw is not a physical
heat efficiency. Fuel-cell roles still require that separate physical review.

The special-project gas-boiler variant whose wrapper and burner imply a factor
above 1.2 remains excluded. The LPG process has a copied natural-gas description,
but its actual fuel is propane/butane; its real inventory is retained. The
industrial heat-pump proxy has source prose saying 15 kW while both its linked
heat pump and disposal specify 50 kW. Capacity, water cycle, application and
uncertain temperature/grade differences remain explicit; no correction is
invented. All representative native technologies retain their source location,
allocation and process inventories, with geographic/vintage limitations.

Against the real encrypted REMIND SSP2-NPi 2050 data, 21 of 28 positive heat roles
have these native bindings. Seven remain: biomass and coal methanol boilers,
industrial biofuel/electric/hydrogen boilers, and the two purchased-district-heat
roles. Other IAMs require additional roles. Missing technologies, layered market
construction, native district distribution, consumer classification/relinking,
combined updates, certification and public export are still required. Whole
sector completion cannot be inferred from this supplier batch.

Validation: 101 focused/shared tests pass, including existing heat and CLIC role
requirements. Isolated installed resources pass 38 native tests and replay all
28 bindings against actual native inventories, with unchanged source records.
The [preflight record](uvek-heat-preflight.json) contains receipt hashes; the
[sector tracker](uvek-sector-progress.json) retains the complete expanded scope.
