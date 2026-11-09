# Sodium hypochlorite with source chemistry and a chemical-plant heat proxy

Status: pending explicit scientific approval. No packaged correspondence or
CLIC approval scope has changed. The two source profiles remain refused.

This proposal covers `bp-c7c88174c5f11ce8`, the RER sodium-hypochlorite market
in ecoinvent 3.6 and 3.9 cut-off, for UVEK 2026 cut-off with ecoinvent 3.10
biosphere. Both source references explicitly represent **1 kg of active NaOCl
without water**, supplied in a 15% solution. The concentration in the product
name does not justify multiplying the purchased amount by 1/0.15.

The original UVEK XML says its hypochlorite process excludes upstream chlorine
supply because the chlorine is captured process emissions. It has no
purchased-chlorine input, and its caustic-soda demand differs from the source.
The inspected source instead purchases both chlorine and active sodium
hydroxide. These boundaries differ; the native
hypochlorite reference also lacks an explicit active-mass property in the
inspected export. The proposal therefore builds source-based market and
production helpers, rather than assigning a concentration factor to that native
hypochlorite process.

Each helper preserves the source's direct production structure and delivery:

| Source exchange | Proposed supplier or flow |
| --- | --- |
| Purchased chlorine | Complete native liquid-chlorine production mix |
| Active sodium hydroxide | Complete native 50% caustic-soda production mix, on its previously verified active-kg basis |
| Electricity | Complete native ENTSO-E medium-voltage grid |
| Natural-gas heat and other heat | Complete native average chemical-plant steam service, keeping two separate exchanges |
| Chemical-plant infrastructure | Complete native organic-chemical plant |
| Chloride, sodium-ion and water releases | Exact source elementary flows and receiving compartments |
| Final delivery by road and rail | Reviewed complete native RER services, keeping separate quantities |

Detailed licensed coefficients remain in the private evidence. Candidates retain
every stored quantity and each exchange's signed uncertainty and pedigree.
The source market's link to its
producer remains separate. Neither source contains parameters or formulas
requiring reconstruction. Caller formulas and ownership would remain unchanged.

**The new modelling decision is the heat substitution.** UVEK's
`Heat, unspecific, in chemical plant` explicitly provides 1 MJ of steam energy
from cold water and represents the average European chemical/plastics industry.
It excludes steam water input, assuming closed systems. Its complete inventory
contains gas, coal and light-fuel-oil combustion, plus electricity. Use it once
for each original thermal exchange, keeping both MJ quantities and their
separate uncertainty unchanged.

This changes the source heat supplier mix and its distinction between gas and
other fuels. Steam quality, temperatures, pressures, heat-market delivery and
loss boundaries, generation efficiencies, cogeneration allocation, geography
and vintage can differ. Those differences need explicit warnings; effects on
scores remain unquantified. The proposal does not reconstruct the original
thermal supply portfolios or claim equivalent useful-energy quality. Add no
assumed thermal-efficiency correction, steam make-up water, network, avoided
heat credit or separate fuel supply to the complete native service.

The other components reuse the standing inspected material/function/unit and
geographic proxy decisions. Chlorine and active caustic soda already have exact
approval scopes for both source releases. The source and native chemical-plant
records describe the same average plant based on two German sites; their
building/facility amounts and aggregate land quantities agree. Keep one plant
unit as the reference, without capacity or lifetime scaling. Native plant,
electricity and upstream reagent inventories retain their own allocation,
emissions, geography, vintage and documented biosphere exclusions. They are
complete service proxies, not identical source supply chains.

All four source freight hashes match earlier reviewed evidence. Use native rail
for the rail exchange; the raw child's aircraft heuristic must not be applied.
The direct elementary flows match ecoinvent 3.10 by publisher UUID, unit and full
receiving compartment. The older sodium label changes without a speciation
factor. Preserve all three releases, their signs and uncertainty; add none of
the native hypochlorite process's own chloride, waste heat or delivery inventory.

Both private candidates contain three foreground records: consumer, market and
production. They link completely to seven existing native suppliers and yield
finite, nonempty LCIs. One residual-correction step reduces independent supply
solutions' relative difference from 1.34 × 10⁻⁹ to 1.01 × 10⁻¹⁵, with equation
residual below 2.20 × 10⁻¹⁵, without changing inventory coefficients. Native
scientific-content hashes match the unchanged pilot baseline. These checks do
not establish impact-score equivalence, public migration, scenario certification
or export.

Approval would cover only these two source-market profiles, their source-based
helpers and the stated steam substitution. Other chemical production, a direct
native-NaOCl correspondence, other releases, inverse routes and cross-system-model
conversion remain outside the proposal. The existing user experience and
warning/provenance path would remain unchanged.

Private evidence in the pilot work area:
`hypochlorite_original_xml_evidence.private.json`,
`hypochlorite_reconstruction_source_evidence.private.json`,
`hypochlorite_candidate_native_primitives.private.json`,
`hypochlorite_source_candidate_review.json` and
`native_hypochlorite_source_candidate_lci_report.json`.
