Configuring sector updates
============================

Run ``ndb.update()`` for the normal dependency-ordered update of all sectors.
A single-sector call is supported but does not create the prospective suppliers
normally provided by earlier updates. See :doc:`/getting_started/first-scenario`
for the complete workflow.

The current default order in ``NewDatabase.update`` is biomass, electricity,
cement, steel, fuels, renewable, metals, mining, heat, cdr, battery, cars,
two_wheelers, trucks, ships, buses, trains, final energy, external, emissions.
A requested list follows its supplied order. Dependencies therefore matter
when selecting only part of this workflow.



The updates below are applied when calling ``ndb.update()`` with the corresponding sector name.
They change selected parts of ecoinvent; sectors not listed here are left unchanged unless
explicitly mapped. The exact updates depend on the IAM model, scenario, year, and the ecoinvent
version used.

* **electricity**: updates electricity generation mixes, technology efficiencies, and regional
  markets; applies corrections such as hydropower water emissions and PV/wind regionalization
  where available. Mappings: ``premise/iam_variables_mapping/electricity.yaml``. Data: ``premise/data/renewables/``,
  ``premise/data/electricity/``.
* **fuels**: updates fuel supply chains and regional markets, including hydrogen, biofuels,
  synthetic fuels, and fossil fuels; adjusts efficiencies and losses along the supply chain.
  Mappings: ``premise/iam_variables_mapping/fuels.yaml``. Data: ``premise/data/fuels/``.
* **heat**: updates residential/industrial heat markets and technology shares; calibrates heat
  efficiencies and fuel inputs by region. Mappings: ``premise/iam_variables_mapping/heat.yaml``.
* **cement**: updates clinker and cement production efficiencies, fuel use, and (where applicable)
  carbon capture integration; rebuilds regional markets from IAM production volumes.
  Mappings: ``premise/iam_variables_mapping/cement.yaml``.
* **steel**: updates primary/secondary steel routes (e.g., BF-BOF, DRI, EAF), efficiencies, and
  regional market shares from IAM outputs. Mappings: ``premise/iam_variables_mapping/steel.yaml``.
* **transport (split by mode in `NewDatabase`)**: updates vehicle markets and fleets (cars, buses, trucks, ships, rail),
  technology shares, and energy carriers; adjusts related emissions factors where relevant. Use:
  ``cars``, ``two_wheelers``, ``trucks``, ``buses``, ``trains``, ``ships``. Mappings:
  ``premise/iam_variables_mapping/transport_*.yaml``. Data: ``premise/data/transport/``.
* **battery**: scales battery pack mass by projected energy densities; creates technology-specific
  and scenario-average battery markets (mobile and stationary). Data:
  ``premise/data/battery/energy_density.yaml``, ``premise/data/battery/mobile_scenarios.csv``,
  ``premise/data/battery/stationary_scenarios.csv``.
* **metals**: updates material intensities and adds/updates mining/refining markets; includes
  post-allocation corrections for co-mined metals and regional supply shares. Data:
  ``premise/data/metals/`` (e.g., ``activities_mapping.yml``, ``metals_db.csv``, ``mining_shares_mapping.xlsx``).
* **mining**: updates waste/tailings handling and regional mining markets where mapped. Data:
  ``premise/data/mining/tailings_activities.yaml``, ``premise/data/mining/tailings_topology.yaml``.
* **biomass**: updates biomass supply chains and regional forestry activities. Mappings:
  ``premise/iam_variables_mapping/biomass.yaml``. Data: ``premise/data/biomass/``.
* **cdr**: introduces and updates carbon dioxide removal routes (DACCS, BECCS, enhanced weathering,
  ocean liming) and their regional deployment. Mappings:
  ``premise/iam_variables_mapping/carbon_dioxide_removal.yaml``. Data: ``premise/data/cdr/``.
* **emissions**: applies IAM- and GAINS-based emission factors to relevant processes. Data:
  ``premise/data/GAINS_emission_factors/``.
* **renewable**: updates wind turbine (and related) inventories and regional deployment. Data:
  ``premise/data/renewables/``.
* **final energy**: regionalizes mapped end-use heating datasets and relinks their suppliers when final-energy data are available; it does not construct general downstream carrier mixes. Mappings:
  ``premise/iam_variables_mapping/final_energy.yaml``. Data: ``premise/data/energy/``.
* **external**: applies user-provided external scenarios to override or extend IAM data.

Each sector has dedicated configuration files under ``premise/data`` and
``premise/iam_variables_mapping`` which define variable mappings, technology lists,
and default parameters.
