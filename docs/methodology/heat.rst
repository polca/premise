.. raw:: html

   <span id="running-the-update"></span>

.. _heat-transformation:

Heat
======


.. contents:: On this page
   :local:
   :depth: 1

Scope and outputs
-------------------

The ``heat`` update creates regional markets for secondary heat supply,
buildings and industrial end use. Their reference products represent
delivered heat in megajoules. It converts final-energy inputs where needed,
regionalizes suppliers and redirects selected existing heat consumers.

.. figure:: /_static/process-diagrams/heat.svg
   :class: process-diagram
   :alt: Delivered-heat markets: Read heat layers; Prepare suppliers and convert volumes; Build available heat markets; Redirect heat consumers; Relink energy inputs and check.
   :align: center

Inputs and applicability
--------------------------

The source database supplies technology inventories; ``heat.yaml`` supplies
IAM aliases, inventory selectors and conversion assumptions. Scenario values
can represent either final energy or heat output. The model/layer coverage
and missing-data rules below determine whether a market can be built.
``ndb.update("heat")`` uses the upstream energy suppliers available at that
point in the update sequence.

Using this update
~~~~~~~~~~~~~~~~~~~

Running all sector transformations is recommended because heat suppliers can
consume regional electricity, biomass, and fuel markets created by earlier
updates.

After the setup in :doc:`/getting_started/first-scenario`:

.. code-block:: python

   ndb.update()

If only the heat-related supply chain is needed, preserve the normal dependency
order explicitly:

.. code-block:: python

    ndb.update(["biomass", "electricity", "fuels", "heat"])

Calling ``ndb.update("heat")`` is valid, but it does not first create prospective
electricity, biomass, or fuel markets. In that case, heat activities use the best
suppliers already present in the source database.

IAM data and market shares
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The heat mapping has 49 technology entries split between
``buildings_end_use``, ``industrial_end_use``, and ``secondary_supply``. Each
entry declares:

``layer``
    The market layer to which the IAM variable belongs.

``iam_aliases``
    The raw variable or expression used for each IAM. An alias can be one
    variable, a list of variables to sum, or a linear expression with signed
    coefficients.

``ecoinvent_aliases``
    The inventory activity used as the technology proxy.

``energy_basis``
    ``heat_output`` when the IAM value already represents delivered heat, or
    ``final_energy`` when the value represents an energy carrier consumed by a
    heating technology.

``conversion``
    ``none``, ``combustion``, ``electric_boiler``, or ``heat_pump``. This tells
    *premise* how to derive delivered heat from an inventory activity.

``residual`` and ``supplier_type``
    Optional metadata for calculated residuals, frozen source-mix suppliers, or a
    link to the generated secondary market.

Raw IAM volumes are retained until conversion. For final-energy variables,
*premise* calculates a region-specific delivered-heat factor from the selected
inventory supplier:

* combustion efficiency is derived from the supplier's fuel inputs and lower
  heating values;
* electric-boiler efficiency is derived from its electricity input; and
* heat-pump performance is derived from its electricity input and therefore
  represents the inventory's coefficient of performance.

Converted volumes are then normalized to market shares. Consequently, a unit of
electricity consumed by a heat pump and a unit of delivered district heat are
not incorrectly treated as equivalent contributions to useful heat.

The mapping loader is deliberately strict. If none of a layer's variables is
present, the layer is unavailable and can be skipped. If only part of a mapped
layer is present, the build raises an error instead of normalizing an incomplete
technology mix. Non-finite values and material negative values are also rejected.

Model coverage
~~~~~~~~~~~~~~~~

Coverage reflects the variables available in the current IAM scenario files and
the mappings in ``heat.yaml``. The numbers below are mapped technology categories,
not the number of generated regional datasets.

.. list-table:: Heat mapping coverage by IAM
   :header-rows: 1
   :widths: 18 18 18 18 28

   * - IAM
     - Buildings
     - Industrial
     - Secondary
     - Behavior
   * - REMIND
     - 12
     - 9
     - 8
     - All three layers; secondary carrier totals are separated into CHP and
       calculated non-CHP residuals where needed.
   * - REMIND-EU
     - 12
     - 9
     - 8
     - Same layered structure as REMIND, using REMIND-EU aliases and regions.
   * - IMAGE
     - 8
     - 7
     - 5
     - All three layers; residential and commercial space- and water-heating
       variables are aggregated. Unspecified secondary heat is a residual.
   * - GCAM
     - 7
     - 7
     - --
     - Buildings and industrial end use are explicit. A frozen source
       secondary-supply composition is created so district heat has a supplier.
   * - MESSAGE
     - 15
     - 12
     - 7
     - All three layers, including explicit resistance heat, heat pumps,
       hydrogen fuel cells, electricity, geothermal, and nuclear where reported.
   * - TIAM-UCL
     - --
     - --
     - 11
     - Supply-only. Purchased district/industrial heat consumers are relinked
       directly to the secondary market; on-site end-use fuel use is unchanged.

An individual scenario may still omit a whole layer or contain only zero values.
No market is created for a layer without positive scenario data. If all three
layers are absent, the heat transformation leaves the database unchanged.

The related end-use regionalization operation has its own chapter:

.. toctree::
   :maxdepth: 1
   :titlesonly:

   /methodology/final-energy

Transformation
----------------

Regionalization and supplier selection
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Mapped heat-production activities are copied to IAM regions before markets are
created. Supplier selection prefers, in order:

#. an activity already located in the IAM region;
#. activities in ecoinvent locations contained by that IAM region;
#. a ``RoW`` supplier; and
#. a global supplier where supported by the conversion lookup.

Where several inventory activities represent the same technology in an IAM
region, their ecoinvent production volumes distribute that technology's IAM
share. Duplicate supplier keys are removed before weighting.

The normal *premise* relinking pass then connects regional heat technologies to
the best available prospective electricity, biomass, and fuel suppliers. For
fuel-consuming heat activities, direct fossil and non-fossil carbon dioxide
emissions are recalculated from the carbon intensities of the linked regional
fuel markets. This is why applying the upstream sector updates before ``heat``
is preferable.

After supplier relinking, selected regional cogeneration heat activities
receive an additional efficiency adjustment. Premise estimates their energy
input per MJ of heat; when the resulting efficiency exceeds 3, it increases
all technosphere and biosphere exchanges by efficiency/3. Production stays
unchanged. Heat pumps, heat recovery/storage, treatment and market activities,
frozen mixes and nuclear cogeneration are excluded from this operation.

.. raw:: html

   <span id="legacy-datasets-are-retained"></span>

.. raw:: html

   <span id="legacy-ecoinvent-market-relinking"></span>

.. raw:: html

   <span id="residuals-and-frozen-legacy-proxies"></span>

Markets and downstream links
------------------------------

Market architecture
~~~~~~~~~~~~~~~~~~~~~

The three layers represent different physical quantities and have different
consumers::

    IAM secondary heat supply technologies
                      |
                      v
    market for heat, secondary, district or industrial
                      |
             +--------+--------+
             |                 |
             v                 v
    purchased heat in    purchased heat in
    buildings mix        industrial mix
             |                 |
    + on-site building   + on-site industrial
      technologies         technologies
             |                 |
             v                 v
    market for heat,     market for heat,
    for buildings        district or industrial
             |                 |
             v                 v
    building consumers   industrial consumers

The generated datasets are:

.. list-table:: Generated heat markets
   :header-rows: 1
   :widths: 20 34 28 18

   * - Layer
     - Dataset name
     - Reference product
     - Meaning
   * - Secondary supply
     - ``market for heat, secondary, district or industrial``
     - ``heat, district or industrial``
     - Heat supplied by district or industrial heat-production technologies.
   * - Buildings end use
     - ``market for heat, for buildings``
     - ``heat, central or small-scale``
     - Delivered heat from on-site building technologies and purchased secondary
       heat.
   * - Industrial end use
     - ``market for heat, district or industrial``
     - ``heat, district or industrial``
     - Delivered heat from on-site industrial technologies and purchased
       secondary heat.

For models with all three layers, the secondary market supplies both end-use
markets only through their respective IAM district-heat shares. It does not
replace the complete buildings or industrial market. Ordinary building and
industrial consumers are relinked to the corresponding end-use market, not
directly to secondary heat.

One market is created for every IAM region with a positive volume. A ``World``
market is also created and contains production-volume-weighted links to the
regional markets.

Existing ecoinvent market relinking
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The transformation redirects exact source activity-name and reference-product
pairs. It does not use a broad ``"market for heat"`` string replacement.

Building consumers are redirected from the following source market families to
``market for heat, for buildings``:

* central or small-scale heat other than natural gas, including its market
  group;
* central or small-scale biomethane, including its market group;
* central or small-scale natural gas, including its market group; and
* the Jakobsberg central/small-scale variants.

Industrial consumers are redirected from the following source market families to
``market for heat, district or industrial``:

* district or industrial heat from natural gas, including its market group;
* district or industrial heat other than natural gas, including its market
  group; and
* heat from steam in the chemical industry.

For supply-only IAMs such as TIAM-UCL, those industrial source inputs are instead
redirected to ``market for heat, secondary, district or industrial``.

The targeted relinking preserves each exchange amount and row. If one consumer
previously had several different source heat inputs, several rows can therefore
point to the same generated market after relinking. Brightway aggregates such
rows into the same technosphere matrix element; consumers that need a single
human-readable row can consolidate them during a later inventory-cleaning step.

Generated heat markets and frozen proxies are excluded from this targeted
consumer rewrite. This avoids circular dependencies and prevents the new market
hierarchy from rewriting itself. Frozen proxies are also excluded from the
subsequent general relinking pass.

Existing datasets are retained
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The old ecoinvent market activities are not deleted. Their presence in a
Brightway database does not mean that they still supply ordinary activities.
Consumer exchanges matching the targeted families are redirected, while source
activities may remain as unused activities or as part of an isolated source
subgraph.

Special-purpose markets are intentionally outside the generic rewrite. For
example, ``market for heat, for reuse in municipal waste incineration only``
keeps its specialized function and is not treated as a general buildings or
industrial heat supplier.

Assumptions and limitations
-----------------------------

A final-energy carrier does not uniquely identify a heating technology.
The explicit proxy choices below affect delivered-heat shares. A whole
missing layer can be skipped or use the specified source-market fallback; partial
mapped layers are errors. Frozen source compositions retain source-inventory
technology assumptions. The separate :doc:`final-energy` update does not
replace this market transformation.

Technology representation and assumptions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The buildings layer can represent district heat, hydrogen and natural-gas
boilers, LPG and methanol boilers, coal stoves, wood-log and pellet heaters, heat
pumps, resistance heaters, oil boilers, and a model-specific residual. The
industrial layer can represent biomass, hydrogen, electricity, heat pumps,
natural gas, biomethane, coal, oil, bio-liquids, solar thermal, and purchased
district heat. Secondary supply covers model-specific combinations of biomass,
coal, coke, natural gas, oil, electricity, heat pumps, geothermal, nuclear, solar
thermal, CHP, and CCS.

Some IAM variables are more aggregated than the available inventory
technologies. These cases are explicit in ``heat.yaml`` through an ``assumption``
field. Important examples include:

* unsplit IMAGE and GCAM building electricity is represented as resistance heat;
* IMAGE's unsplit industrial electricity retains the heat-pump proxy;
* REMIND industrial mechanical-work and low-temperature electricity mapped to
  heat is represented by an industrial electric boiler;
* TIAM-UCL coke heat uses coal-based inventory proxies;
* MESSAGE nuclear heat uses a nuclear-cogeneration inventory with exergy
  allocation at 140 degrees Celsius; and
* aggregated bio-liquids, gas-derived liquids, and industrial hydrogen fuel
  cells use the proxies documented beside their mappings.

The industrial electric boiler and nuclear-cogeneration pathways rely on the
additional inventory activities ``heat production, electric boiler, industrial``
and ``heat production, nuclear cogeneration``. Like other additional
inventories, they are imported and linked during ``NewDatabase`` initialization.

Residuals and frozen source mixes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Residuals are used only when an IAM total contains a component that cannot be
identified directly. Two different mechanisms are present:

Calculated residual
    REMIND and REMIND-EU secondary biomass, coal, and gas totals include CHP.
    The non-CHP residual is calculated as total carrier heat minus explicitly
    reported CHP heat. Small negative closure artefacts are clipped within an
    absolute tolerance of ``1e-8`` or a relative tolerance of ``1e-5``. Larger
    negative residuals stop the build.

Frozen source mix
    IMAGE's unspecified secondary heat and MESSAGE's ``Other`` building heat use
    copies of the relevant source ecoinvent market composition. The copies are
    made before consumer relinking, are regionalized, and are named
    ``heat supply, frozen legacy mix, buildings`` or
    ``heat supply, frozen legacy mix, district or industrial``. They are kept out
    of later relinking so their copied composition cannot become a recursive
    link to the new markets.

If buildings or industrial end-use data exists but the IAM has no mapped
secondary-supply layer, as in GCAM, *premise* creates
``heat, secondary, frozen legacy mix`` with a frozen source district-heat
composition. The fallback provides a consistent purchased-heat supplier without
inventing an IAM technology split.

Cutoff and consequential databases
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For cutoff databases, the normalized delivered-heat volumes form average
technology shares. For consequential databases, the same delivered-heat arrays
are passed through the *premise* marginal-mix calculation before the market is
written. The three-layer architecture, existing consumer routing, residual rules,
and cycle checks are otherwise shared by both system models.

Worked example and checks
---------------------------

Illustratively, 1 MJ of electricity supplied to a heat pump with an
inventory COP of 3 represents 3 MJ of delivered heat. Combined with 1 MJ of
purchased heat, the delivered shares are 75% and 25%, not 50% each.
Check ``Market Changes`` for shares and ``Fallbacks & Proxies`` for frozen
suppliers. Confirm buildings and industrial consumers link to the correct
layer and that purchased heat does not create a market cycle.

.. raw:: html

   <span id="maintaining-or-extending-the-mapping"></span>

Sources and inventory details
-------------------------------

Source provenance and currency
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. include:: /reference/generated/source-heat.inc
