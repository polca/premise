Terms used in this guide
=========================

These terms describe the data Premise reads and the changes it makes.

.. glossary::

   Activity
      A process in the inventory, such as producing steel or supplying
      electricity. It has a product, unit, location, inputs and outputs.

   Reference product
      The product or service supplied by an activity. Its quantity and unit
      define what the activity's inputs and emissions are measured against.

   Functional unit
      The amount of a product or service assessed in an LCA, such as 1 kWh of
      electricity. Compare results using the same service and system boundary.

   Exchange
      An input or output of an activity: a purchase from another process,
      production of its reference product, a resource withdrawal or an emission.

   Technosphere
      The network of production and treatment processes. A technosphere
      exchange connects one activity to another, such as a steel plant buying
      electricity.

   Biosphere
      The environmental side of the inventory: resources taken from nature
      and releases to air, water or soil. In Brightway, biosphere flows are
      registered in a separate database.

   LCI
      Life cycle inventory: the activities, their connections, and the amounts
      of materials, energy, resources and emissions.

   LCIA
      Life cycle impact assessment: applying characterization factors to an
      inventory to calculate impact indicators. Premise builds the inventory;
      use LCA software and an LCIA method to calculate its impacts.

   IAM
      Integrated assessment model: a model of possible changes in energy,
      production, land use or other systems under stated policy and social
      assumptions. Premise uses selected outputs from these models.

   Pathway
      A named set of assumptions and projections within an IAM. In a Premise
      scenario dictionary, ``model``, ``pathway`` and ``year`` identify the
      projection to use.

   Source database
      The existing ecoinvent database from which a build starts. The scenario
      inventory contains the changes made during that build.

   Market
      An activity that supplies a product from one or more producers. Its
      supplier shares describe the mix purchased by its consumers.

   Regionalization
      Creating a version of an activity for a location, usually an IAM region.
      This does not mean every input was measured or manufactured there.

   Relinking
      Changing which supplier an activity buys from. The product and unit
      must still match the intended input.

   Mapping
      Configuration that connects a scenario variable or technology to the
      activities Premise can change. A configured mapping still needs the
      corresponding data in the selected scenario file.

   Proxy
      A substitute dataset used when a more representative inventory is
      unavailable. Its limitations matter when interpreting results.

   Fallback
      A documented alternative used when the preferred data or supplier are
      unavailable, for example a supplier from a broader region.

   Allocation
      Dividing a process's inputs and emissions among its products. The rule
      depends on the database's system model and the process being represented.

   Checkpoint
      A saved inventory state that Premise can reopen. A cache is saved input
      or intermediate data reused to avoid repeating preparation work.

   Validation
      Checks of defined inventory rules, such as valid amounts, linked
      suppliers and market shares. Passing these checks does not verify every
      scientific assumption or predict which scenario will occur.

   PV
      Photovoltaics: producing electricity from sunlight with solar cells.

   CCS
      Carbon capture and storage: capturing CO2 and storing it. Capturing
      fossil CO2 avoids a release; it is different from removing CO2 from the
      atmosphere. CDR means carbon dioxide removal.

   LHV
      Lower heating value: the energy released by burning a fuel, excluding
      the heat recovered by condensing water vapour. Fuel-market conversions
      use the stated heating-value basis.

   Electrolyser
      Equipment that uses electricity to split water into hydrogen and oxygen.
      PEM means proton-exchange membrane; SOEC means solid oxide electrolysis
      cell. These technologies have different materials and operating needs.

   Vehicle powertrain
      The equipment that propels a vehicle. BEV means battery-electric vehicle;
      FCEV means fuel-cell electric vehicle; PHEV means plug-in hybrid electric
      vehicle. Their fuel, battery and replacement assumptions differ.
