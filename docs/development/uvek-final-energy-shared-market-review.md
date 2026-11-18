# Preserve shared Premise market selection in final energy

All 424 existing filter-bearing roles now declare UVEK selectors. All 425
original role definitions, ecoinvent selectors, IAM labels, masks and LHVs are
unchanged. Common migrated foreground names and reviewed helpers retain their
original output basis; published CLIC suppliers keep explicit role bindings.

Electricity roles select the ordinary Premise regional low-voltage market,
which the preceding electricity update creates. They do not regionalize native
country grids again. Biomass and heat roles likewise retain the shared market
selectors: the appropriate earlier updates must create those markets. A missing
prerequisite is not replaced by a new physical proxy merely to fill a selector.
This preserves normal sector sequencing and avoids redundant electricity rows.

Selector checks preserve the same role vocabulary for seven model settings,
select the regional low-voltage market and reject native country grids, heat
units and battery-capacity counterexamples. Seventy-nine existing heat and
mapping checks pass. These filter checks do not establish complete final-energy
coverage or export; the full combined workflow is being checked separately.
