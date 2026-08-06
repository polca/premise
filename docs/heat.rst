Heat transformation
===================

The heat transformation converts IAM heat projections into three distinct,
regionalized markets. This separation prevents heat production technologies,
purchased district heat, and on-site end-use technologies from being counted in
the same market.

The transformation:

* reads model-specific heat variables from
  ``premise/iam_variables_mapping/heat.yaml``;
* converts final-energy inputs to delivered heat where necessary;
* creates regional secondary-supply, buildings, and industrial heat markets;
* regionalizes the mapped heat-production activities and relinks their energy
  inputs;
* redirects consumers of selected legacy ecoinvent heat markets to the new
  markets; and
* validates market shares, IAM values, conversion efficiencies, carbon dioxide
  emissions, purchased-heat links, and generated-market cycles.

Running the update
------------------

Running all sector transformations is recommended because heat suppliers can
consume regional electricity, biomass, and fuel markets created by earlier
updates.

.. code-block:: python

    import bw2data as bd
    from premise import NewDatabase

    bd.projects.set_current("my_project")

    ndb = NewDatabase(
        scenarios=[
            {
                "model": "remind",
                "pathway": "SSP2-PkBudg650",
                "year": 2050,
            }
        ],
        source_db="ecoinvent-3.12-cutoff",
        source_version="3.12",
    )
    ndb.update()
    ndb.write_db_to_brightway(name="remind-ssp2-2050")

If only the heat-related supply chain is needed, preserve the normal dependency
order explicitly:

.. code-block:: python

    ndb.update(["biomass", "electricity", "fuels", "heat"])

Calling ``ndb.update("heat")`` is valid, but it does not first create prospective
electricity, biomass, or fuel markets. In that case, heat activities use the best
suppliers already present in the source database.

Market architecture
-------------------

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

IAM data and market shares
--------------------------

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
    Optional metadata for calculated residuals, frozen legacy suppliers, or a
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
--------------

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
     - Buildings and industrial end use are explicit. A frozen legacy
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

Technology representation and assumptions
-----------------------------------------

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

Regionalization and supplier selection
--------------------------------------

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

Residuals and frozen legacy proxies
-----------------------------------

Residuals are used only when an IAM total contains a component that cannot be
identified directly. Two different mechanisms are present:

Calculated residual
    REMIND and REMIND-EU secondary biomass, coal, and gas totals include CHP.
    The non-CHP residual is calculated as total carrier heat minus explicitly
    reported CHP heat. Small negative closure artefacts are clipped within an
    absolute tolerance of ``1e-8`` or a relative tolerance of ``1e-5``. Larger
    negative residuals stop the build.

Frozen legacy mix
    IMAGE's unspecified secondary heat and MESSAGE's ``Other`` building heat use
    copies of the relevant legacy ecoinvent market composition. The copies are
    made before any legacy-market relinking, are regionalized, and are named
    ``heat supply, frozen legacy mix, buildings`` or
    ``heat supply, frozen legacy mix, district or industrial``. They are kept out
    of later relinking so their copied composition cannot become a recursive
    link to the new markets.

If buildings or industrial end-use data exists but the IAM has no mapped
secondary-supply layer, as in GCAM, *premise* creates
``heat, secondary, frozen legacy mix`` with a frozen legacy district-heat
composition. The fallback provides a consistent purchased-heat supplier without
inventing an IAM technology split.

Legacy ecoinvent market relinking
---------------------------------

The transformation redirects exact legacy activity-name and reference-product
pairs. It does not use a broad ``"market for heat"`` string replacement.

Building consumers are redirected from the following legacy families to
``market for heat, for buildings``:

* central or small-scale heat other than natural gas, including its market
  group;
* central or small-scale biomethane, including its market group;
* central or small-scale natural gas, including its market group; and
* the Jakobsberg central/small-scale variants.

Industrial consumers are redirected from the following legacy families to
``market for heat, district or industrial``:

* district or industrial heat from natural gas, including its market group;
* district or industrial heat other than natural gas, including its market
  group; and
* heat from steam in the chemical industry.

For supply-only IAMs such as TIAM-UCL, those industrial legacy inputs are instead
redirected to ``market for heat, secondary, district or industrial``.

The targeted relinking preserves each exchange amount and row. If one consumer
previously had several different legacy heat inputs, several rows can therefore
point to the same generated market after relinking. Brightway aggregates such
rows into the same technosphere matrix element; consumers that need a single
human-readable row can consolidate them during a later inventory-cleaning step.

Generated heat markets and frozen proxies are excluded from this targeted
legacy rewrite. This avoids circular dependencies and prevents the new market
hierarchy from rewriting itself. Frozen proxies are also excluded from the
subsequent general relinking pass.

Legacy datasets are retained
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The old ecoinvent market activities are not deleted. Their presence in a
Brightway database does not mean that they still supply ordinary activities.
Consumer exchanges matching the targeted families are redirected, while legacy
activities may remain as unused activities or as part of an isolated legacy
subgraph.

Special-purpose markets are intentionally outside the generic rewrite. For
example, ``market for heat, for reuse in municipal waste incineration only``
keeps its specialized function and is not treated as a general buildings or
industrial heat supplier.

Cutoff and consequential databases
----------------------------------

For cutoff databases, the normalized delivered-heat volumes form average
technology shares. For consequential databases, the same delivered-heat arrays
are passed through the *premise* marginal-mix calculation before the market is
written. The three-layer architecture, legacy consumer routing, residual rules,
and cycle checks are otherwise shared by both system models.

Scenario report and diagnostics
-------------------------------

The Excel scenario report has three independent heat worksheets:

* ``Heat (buildings) - generation``;
* ``Heat (industrial) - generation``; and
* ``Heat (secondary) - generation``.

Generate it explicitly with:

.. code-block:: python

    ndb.generate_scenario_report(
        filepath="export/scenario_report",
        name="heat_scenarios.xlsx",
    )

The report contains the evaluated IAM series, by scenario, region, technology,
and year. A missing layer is omitted for that scenario; the report does not turn
a frozen fallback into IAM data.

Detailed transformation diagnostics are stored in each scenario under
``"heat diagnostics"``. They record layer availability, assumptions, residuals,
small-negative clips, raw IAM volumes, inventory-derived conversion factors,
delivered volumes, and normalized shares. For example:

.. code-block:: python

    diagnostics = ndb.scenarios[0]["heat diagnostics"]
    building_volumes = diagnostics["buildings_end_use"]["volumes"]

The regular change report includes created and modified heat datasets and any
validation anomalies logged by the transformation.

Validation and failure modes
----------------------------

The heat update performs the following checks before it returns the scenario:

* raw heat arrays must contain only finite, non-negative values;
* each generated regional heat market must contain exactly one megajoule of
  heat inputs per megajoule of output, within numerical tolerance;
* an end-use market must not contain duplicate links to the secondary market;
* combustion and electric conversion efficiencies must remain physically
  plausible;
* direct fossil and non-fossil carbon dioxide emissions must be consistent with
  linked fuels; and
* no direct or indirect cycle may exist among generated heat datasets.

A positive IAM technology volume with no usable inventory supplier raises an
error. A materially negative residual, a partially available IAM layer, or a
generated-market cycle also stops the build. Major efficiency, emissions, or
market-share anomalies are written to the validation log and announced at the
end of the heat update.

Inspecting a generated Brightway database
-----------------------------------------

Always identify a heat market by name, reference product, and location. The
buildings and secondary/industrial products are intentionally different where
needed, and names alone are not sufficient for a reliable audit.

.. code-block:: python

    import bw2data as bd

    bd.projects.set_current("my_project")
    db = bd.Database("remind-ssp2-2050")

    region = "EUR"  # Use a region from the selected IAM.
    secondary = next(
        activity
        for activity in db
        if activity["name"]
        == "market for heat, secondary, district or industrial"
        and activity["reference product"] == "heat, district or industrial"
        and activity["location"] == region
    )

    secondary_mix = [
        {
            "supplier": exchange.input["name"],
            "location": exchange.input["location"],
            "share": exchange["amount"],
        }
        for exchange in secondary.technosphere()
        if exchange["amount"] != 0
    ]

To find every direct consumer of that regional market:

.. code-block:: python

    consumers = []
    for activity in db:
        for exchange in activity.technosphere():
            if exchange.input == secondary and exchange["amount"] != 0:
                consumers.append(
                    {
                        "consumer": activity["name"],
                        "location": activity["location"],
                        "amount": exchange["amount"],
                    }
                )

In a full three-layer scenario, this list should normally contain the regional
buildings and industrial end-use markets rather than ordinary final consumers.
In a supply-only TIAM-UCL scenario, ordinary consumers of purchased industrial or
district heat can link directly to the secondary market.

Maintaining or extending the mapping
------------------------------------

``premise/iam_variables_mapping/heat.yaml`` is the authoritative mapping. When
adding an IAM or technology:

#. choose the physical layer before choosing an inventory proxy;
#. state whether the IAM series is final energy or heat output;
#. select the conversion rule and an exact ecoinvent activity filter;
#. add an explicit ``assumption`` for any proxy or aggregation choice;
#. use a signed ``terms`` expression only when the IAM relationship supports it;
#. add tests for complete, absent, and partially available layers; and
#. test both market shares and the absence of generated-market cycles.

The source files most relevant to this workflow are:

* ``premise/iam_variables_mapping/heat.yaml`` for IAM and inventory mappings;
* ``premise/heat_data.py`` for expression evaluation and layer availability;
* ``premise/heat.py`` for conversion, market creation, and relinking;
* ``premise/validation.py`` for heat checks; and
* ``premise/report.py`` for the three scenario-report worksheets.
