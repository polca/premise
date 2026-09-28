Steel production
==================


.. contents:: On this page
   :local:
   :depth: 1

.. raw:: html

   <span id="key-outputs"></span>

Scope and outputs
-------------------

The ``steel`` update creates IAM-region steel and pig-iron production
activities and steel supply markets, expressed per kilogram of product.
It changes mapped energy inputs, fossil CO2 emissions and technology shares.
Imported production routes are available candidates; the selected scenario
and mappings determine their participation in markets.

.. figure:: /_static/process-diagrams/steel.svg
   :class: process-diagram
   :alt: Steel production and supply: Prepare regional routes; Adjust mapped steel activities; Build steel markets; Relink consumers and check.
   :align: center

Inputs and applicability
--------------------------

The update is skipped when ``steel_technology_mix`` is absent. Supporting
pig-iron activities are regionalized before the mapped steel routes.

Inputs are the source database, additional steel inventories, and production
and efficiency variables mapped in ``iam_variables_mapping/steel.yaml``.
Inventories enter during initialization; ``ndb.update("steel")`` performs the
scenario transformation. Apply upstream energy updates in the default order
for prospective fuel and electricity suppliers. Model aliases describe
mapping coverage, not guaranteed availability in every scenario file.

Using this update
~~~~~~~~~~~~~~~~~~~

After the setup in :doc:`/getting_started/first-scenario`:

.. code-block:: python

   ndb.update("steel")

Transformation
----------------

The route-specific adjustment uses the inverse IAM efficiency change.
The implementation selects energy exchanges by fuel-name filters and scales
direct fossil CO2. For secondary steel, when a non-unit positive adjustment
is applied, electricity is bounded between 0.444 and 0.799 kWh/kg. Pig-iron
adjustments use a 9 MJ/kg accounted-energy floor. These bounds are applied
within the adjustment branch, not as a universal correction to every imported
inventory. See the route explanations below.

Dataset proxies
~~~~~~~~~~~~~~~~~

premise creates proxy datasets for each IAM region by duplicating
all available steel production routes selected from the inventory library
(see lci-steel.xlsx).
These include, for example:

Primary routes: BF–BOF with different coke/natural gas shares, NG-DRI + EAF, etc.

Secondary routes: Scrap-based EAF with different efficiencies.

These inventories are based on the work from `Harpprecht <https://doi.org/10.1039/D5EE01356A>`__ et al., 2025.

For each IAM region, premise changes the location of these duplicated datasets
to match the IAM region (falling back to RoW if no valid location exists),
and updates key metadata such as production volume.

Efficiency adjustment
~~~~~~~~~~~~~~~~~~~~~~~

The adjustment in ``premise/steel.py`` calculates the inverse mapped efficiency
change, applies the route-specific energy bounds, and scales technosphere
exchanges matching its configured fuel-name filters plus direct fossil CO2.
The source route structure is retained. The filters also include electricity;
secondary-steel handling is not a promise that only electricity can change.

.. note::

    You can check the efficiency gains assumed relative to 2020
    for steel production in your scenarios by generating a scenario
    summary report.

.. code-block:: python

    ndb.generate_scenario_report()


.. warning::

    If your system of interest relies heavily on the provision
    of steel, you should probably consider modelling steel production
    based on primary data. ecoinvent datasets for steel production rely
    on a few data points, which are then further process transformed
    by *premise*. Therefore, there is a large modelling uncertainty.

Carbon Capture and Storage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

CCS is represented by the capture-equipped production routes in the source
steel inventory library. The steel update regionalizes those activities and
uses mapped production volumes to select their market shares. Capture energy
and storage links follow the route inventories; there is no separate general
step here that adds capture to every steel activity from an aggregate
sector-wide sequestration fraction.

Markets and downstream links
------------------------------

Consequential steel markets exclude the ``steel - secondary`` supplier
category before selecting the remaining suppliers. Market construction is
limited to matching steel families; rolled, chromium and designated residue
or grain-related market names are excluded.

Steel markets
~~~~~~~~~~~~~~~

*premise* creates datasets "market for steel, low-alloyed" and "market for steel, unalloyed" for each IAM region.
Within each dataset, the supply shares of primary and secondary steel
are adjusted to reflect the projections from the IAM scenario, for a given region
and year, based on the variables described in the `steel <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/iam_variables_mapping/steel.yaml>`__ mapping file.

The table below shows an example of the market for India, with 90% across the listed primary routes and 10% electric steel.
These coefficients are illustrative; they do not specify an IAM scenario result.

 ============================================================================================================================== ============ ================ ===========
  Output                                                                                                                         _            _                _
 ============================================================================================================================== ============ ================ ===========
  producer                                                                                                                       amount       unit             location
  market for steel, low-alloyed                                                                                                  1            kilogram         IND
  Input
  supplier                                                                                                                       amount       unit             location
  market group for transport, freight, inland waterways, barge                                                                   0.5          ton kilometer    GLO
  market group for transport, freight train                                                                                      0.35         ton kilometer    GLO
  market for transport, freight, sea, bulk carrier for dry goods                                                                 0.38         ton kilometer    GLO
  transport, freight, lorry, unspecified, regional delivery                                                                      0.12         ton kilometer    IND
  steel production, blast furnace-basic oxygen furnace, unalloyed                                                                0.66         kilogram         IND
  steel production, blast furnace-basic oxygen furnace, with carbon capture and storage, unalloyed                               0.05         kilogram         IND
  steel production, natural gas-based direct reduction iron-electric arc furnace, unalloyed                                      0.04         kilogram         IND
  steel production, natural gas-based direct reduction iron-electric arc furnace, with carbon capture and storage, unalloyed     0.05         kilogram         IND
  steel production, blast furnace-basic oxygen furnace, with top gas recycling, unalloyed                                        0.08         kilogram         IND
  steel production, blast furnace-basic oxygen furnace, with top gas recycling, with carbon capture and storage, unalloyed       0.02         kilogram         IND
  steel production, electric, low-alloyed                                                                                        0.10         kilogram         IND
 ============================================================================================================================== ============ ================ ===========

Original market datasets
~~~~~~~~~~~~~~~~~~~~~~~~~~

Market datasets originally present in the ecoinvent LCI database are cleared
from any inputs. Instead, an input from the newly created regional market
is added, depending on the location of the dataset.

Relinking
~~~~~~~~~~~

Once steel production and market datasets are created, *premise*
re-links steel-consuming activities to the new regional markets for
steel. The regional market it re-links to depends on the location
of the consumer.

Assumptions and limitations
-----------------------------

Regionalization retains the source process structure. It does not supply
plant-specific operating data. Route names, energy bounds and CCS boundaries
must be checked together when comparing technologies. Market shares and
energy intensity are distinct scenario effects. The common consequential
supplier calculation is described in :doc:`system-models`.

Worked example and checks
---------------------------

The illustrative India market below contains 0.90 kg across primary routes
and 0.10 kg of electric steel per kilogram supplied. These illustrative coefficients specify the example mix. Check that steel
supplier amounts sum to one independently of transport exchanges, that the
intended routes supply the market, and that downstream consumers use it.
Review ``Market Changes`` and the energy/CO2 entries in ``Key Changes``.

Sources and inventory details
-------------------------------

Source inventories: steel
~~~~~~~~~~~~~~~~~~~~~~~~~~~

*premise* imports inventories for a wide range of steel production technologies.
These include conventional blast furnace-basic oxygen furnace (BF-BOF) routes,
as well as emerging processes such as direct reduction (DRI), hydrogen-based
production, electrowinning, and carbon capture (CCS) variants. They are from `Harpprecht <https://doi.org/10.1039/D5EE01356A>`__ et al. (2025).
They can be found here: `LCI_steel <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-steel.xlsx>`__.

The table below provides an overview of the included datasets, their key
input(s), and assumed regional scope.

==================================================================================================================  ==========
Steel production and related processes                                                                               location
==================================================================================================================  ==========
steel production, blast furnace-basic oxygen furnace, low-alloyed                                                    GLO
steel production, blast furnace-basic oxygen furnace, unalloyed                                                      GLO
alloys production, for low-alloyed steel                                                                             GLO
pig iron production, blast furnace, with carbon capture and storage                                                  GLO
carbon dioxide, captured at pig iron production plant, using monoethanolamine                                        GLO
steel production, blast furnace-basic oxygen furnace, with carbon capture and storage, low-alloyed                   GLO
steel production, blast furnace-basic oxygen furnace, with carbon capture and storage, unalloyed                     GLO
pig iron production, top gas recycling-blast furnace                                                                 GLO
steel production, blast furnace-basic oxygen furnace, with top gas recycling, low-alloyed                            GLO
steel production, blast furnace-basic oxygen furnace, with top gas recycling, unalloyed                              GLO
pig iron production, blast furnace, with top gas recycling, with carbon capture and storage                          GLO
carbon dioxide, captured at steel production plant, using vacuum pressure swing adsorption                           GLO
steel production, blast furnace-basic oxygen furnace, with top gas recycling, with CCS, low-alloyed                  GLO
steel production, blast furnace-basic oxygen furnace, with top gas recycling, with CCS, unalloyed                    GLO
pig iron production, with natural gas-based direct reduction                                                         GLO
steel production, natural gas-based direct reduction iron-electric arc furnace, low-alloyed                          GLO
steel production, natural gas-based direct reduction iron-electric arc furnace, unalloyed                            GLO
pig iron production, with natural gas-based direct reduction, with carbon capture and storage                        GLO
carbon dioxide, captured at steel production plant using DRI, using vacuum pressure swing adsorption                 GLO
steel production, natural gas-based DRI-EAF, with CCS, low-alloyed                                                   GLO
steel production, natural gas-based DRI-EAF, with CCS, unalloyed                                                     GLO
steel production, hydrogen-based DRI-EAF, low-alloyed                                                                GLO
steel production, hydrogen-based DRI-EAF, unalloyed                                                                  GLO
pig iron production, hydrogen-based direct reduction iron                                                            GLO
preheating of iron ore pellets                                                                                       GLO
preheating of hydrogen                                                                                               GLO
pig iron production, by electrowinning                                                                               GLO
leaching of iron ore                                                                                                 GLO
market for cathode, graphite                                                                                         GLO
nickel anode production, for electrolysis of iron ore                                                                GLO
production of alkaline solution from sodium hydroxide of 50 wt-%                                                     GLO
steel production, electrowinning-electric arc furnace, low-alloyed                                                   GLO
steel production, electrowinning-electric arc furnace, unalloyed                                                     GLO
ultrafine grinding of iron ore                                                                                       GLO
==================================================================================================================  ==========


These inventories provide a modular basis for modeling steel systems under various future-oriented scenarios and technological configurations.

.. raw:: html

   <span id="source-provenance-and-currency"></span>

Sources and data dates
~~~~~~~~~~~~~~~~~~~~~~~~

.. include:: /reference/generated/source-steel.inc
