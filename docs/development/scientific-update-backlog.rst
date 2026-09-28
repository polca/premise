Scientific update backlog
===========================

These proposals are not implemented changes. They require separate numerical
validation and review before modifying inventories or transformation rules.
P1 means a risk to the modelling method or the traceability of its data; P2 means a narrower follow-up.
No ranking here implies that the current dataset is unusable for every study.

Photovoltaics
---------------

P1: establish Atlas retrieval date and whether the source yields already include degradation; rerun yield/lifetime sensitivities before changing coefficients.

Other electricity generation and storage
------------------------------------------

P1: compare wind size, material, lifetime, capacity-factor and end-of-life boundaries against the current renewable update; assess CCS/geothermal source age separately.

Heat and end-use heating
--------------------------

P1: compare heat-pump refrigerants, leakage and useful-heat functional units; distinguish residential and industrial temperatures. Do not replace IAM conversion rules with a single literature COP.

Biomass
---------

P1: document residue counterfactuals and test allocation and biogenic-carbon timing; do not add substitution credits to cut-off datasets.

Hydrogen
----------

P1: compare production and delivered-hydrogen inventories route by route, with leakage, compression and background electricity held consistent.

Biofuels
----------

P1: audit land-use change and co-product conventions, then compare crop and residue pathways separately against current GREET/JEC inputs.

Synthetic fuels
-----------------

P1: compare electricity-to-fuel efficiency, CO2 origin and co-product allocation using harmonized reference output and plant boundaries.

Ammonia
---------

P2: compare electrolyser type, oxygen treatment and electricity sourcing before considering regional ammonia proxies.

Mobile and stationary batteries
---------------------------------

P1: compare current ecoinvent battery data before adding GREET updates; test assembly energy, chemistry shares, cycle life and capacity/throughput functional units separately.

Transport
-----------

P1: record exporter/model versions for each vehicle workbook; compare payload, range, powertrain and exhaust corrections. Review rail and shipping independently of road vehicles.

Steel
-------

P2: inspect supplementary route coefficients and the shared compression/transport/storage chain before claiming complete scientific validation.

Cement
--------

P1: trace each capture coefficient to the supplement and document the legacy BREF reference year; compare process-only and combined capture consistently.

Metals
--------

P1: update statistics only after reconciling commodity definitions and mining versus refining production; separately review processing inventories and co-mining allocation.

Mining and tailings
---------------------

P1: review no-leaching assumptions, regional uptake evidence and recovered-product allocation; test cut-off compatibility before transferring substitution benefits.

Carbon capture and removal
----------------------------

P1: audit net-removal balance, heat source, sorbent life, storage permanence and biomass counterfactuals; compare newer DAC routes with matched capture/storage boundaries.

Air emissions
---------------

P1: record factor release and policy cutoff, audit pollutant mapping, and assess historical/increasing-factor behaviour as a separate modelling decision.

Consequential modelling
-------------------------

P1: compare interval and constraint sensitivities by sector; never substitute average mixes as an unlabelled validation reference.

Acceptance for a modelling update
-----------------------------------

Record the old/new data versions, harmonize functional units and allocation,
check supplier availability across supported configurations, and compare both
activity coefficients and LCIA contributions. Separate changed foreground
assumptions from background changes. Retain the original scenario and an
explicit uncertainty/sensitivity analysis. See :doc:`/reference/source-review`
for evidence and access limitations.

Hydrogen exchange-floor discrepancy
-------------------------------------

P1: In ``HydrogenMixin._adjust_hydrogen_efficiency``, the IAM branch calculates
``max(scaling_factor * initial_energy_use, floor_value)`` but scales exchanges
with the preceding ``scaling_factor``. Compare the intended floor against the
actual exchange amounts and logged energy value. Add a regression case where
the IAM factor would cross the floor before deciding on a numerical fix.

Biofuel conversion-efficiency scope
-------------------------------------

P2: reconcile intended biofuel conversion improvements with the current fuels
implementation. ``generate_biofuel_activities`` passes land-use and
land-use-change adjustments, but no blanket conversion-efficiency callback.
The methodology now describes that behavior. Before adding numerical changes,
identify the relevant IAM variables, reference year, selected process exchanges
and interaction with crop yields and upstream energy updates.
