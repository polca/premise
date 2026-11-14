Biofuels
==========

.. raw:: html

   <span id="fuels"></span>

.. contents:: On this page
   :local:
   :depth: 1

.. raw:: html

   <span id="key-outputs"></span>

Scope and outputs
-------------------

The biofuel routines within ``ndb.update("fuels")`` regionalize feedstock
supply chains and adjust mapped crop land occupation and land-use-change CO2.
Liquid-fuel markets determine their participation in petrol and diesel supply.
Use each source activity's reference product and unit when comparing stages.

.. figure:: /_static/process-diagrams/fuels-biofuels.svg
   :class: process-diagram
   :alt: Biofuel supply chains: Prepare residue and crop routes; Adjust eligible crop exchanges; Filter suppliers and construct fuel blends; Relink and update consumer carbon flows.
   :align: center

Inputs and applicability
--------------------------

Inputs include biofuel inventories, region-to-climate and feedstock mappings,
IAM fuel production, and crop land-use data where available. The fuels branch
requires at least one available petrol, diesel, natural-gas or hydrogen blend
array. Crop-data checks determine whether each land adjustment is applied;
missing crop information is not a measured zero burden.

Using this update
~~~~~~~~~~~~~~~~~~~

After the setup in :doc:`/getting_started/first-scenario`:

.. code-block:: python

   ndb.update("fuels")

Transformation
----------------

Premise prepares residue and used-cooking-oil routes, selects crop routes by
region and climate, and applies the land occupation and land-use-change
adjustments to eligible crop activities. The current biofuel routine does not
apply a blanket IAM conversion-efficiency multiplier to all production
inputs. Changes to upstream energy suppliers are separate from direct
crop-exchange changes.

Efficiency adjustment
~~~~~~~~~~~~~~~~~~~~~~~

The crop-specific operations adjust land occupation and land-use-change CO2.
Biofuel conversion-process inputs do not receive a blanket IAM efficiency
multiplier here. Their upstream fuel and electricity suppliers can change.

Feedstock regionalization
~~~~~~~~~~~~~~~~~~~~~~~~~~~

For bioethanol and oil-based biodiesel, *premise* selects region-appropriate
feedstocks before building fuel markets (e.g., "market for petrol" and
"market for diesel"). This prevents regions from drawing on feedstocks that are
not representative of their dominant crop choices (for example, sugarbeet
ethanol in Latin America).

The selection is driven by explicit IAM-region mappings:

* oil-based biodiesel feedstocks: `premise/iam_variables_mapping/iam_region_to_biodiesel_feedstock.yaml`
* bioethanol feedstocks (sugar, grass, wood, grain): `premise/iam_variables_mapping/iam_region_to_bioethanol_feedstock.yaml`

When a region has no suppliers in its allowed locations for a given feedstock,
*premise* falls back to any available supplier of that same feedstock to avoid
empty markets.

Land use and land use change
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When building a database using IMAGE, land use and land use change emissions
are available. Upon the import of crops farming datasets, *premise* adjusts
the land occupation as well as CO2 emissions associated to land use and land
use change, respectively.

 =========================================================== ========= ==================== ===========
  Output                                                      _         _                    _
 =========================================================== ========= ==================== ===========
  producer                                                    amount    unit                 location
  Farming and supply of corn                                  1         kilogram             CEU
  Input
  supplier                                                    amount    unit                 location
  market for diesel, burned in agricultural machinery         0.142     megajoule            GLO
  petrol, unleaded, burned in machinery                       0.042     megajoule            GLO
  market for natural gas, burned in gas motor, for storage    0.091     megajoule            GLO
  market group for electricity, low voltage                   0.004     kilowatt hour        CEU
  Energy, gross calorific value, in biomass                   15.910    megajoule            _
  **Occupation, annual crop**                                 1.584     square meter-year    _
  Carbon dioxide, in air                                      1.476     kilogram             _
  **Carbon dioxide, from soil or biomass stock**              1.140     kilogram             _
 =========================================================== ========= ==================== ===========

The land use value is given from the IAM scenario in Ha/GJ of primary crop energy.
The land occupation per kg of crop farmed is calculated as::

    land_use = land_use [Ha/GJ] * 10000 [m2/Ha] / 1000 [MJ/GJ] * LHV [MJ/kg]

Regarding land use change CO2 emissions, the principle is similar. The variable
is expressed in kg CO2/GJ of primary crop energy. The land use change
CO2 emissions per kg of crop farmed are calculated as::

    land_use_co2 = land_use_co2 [kg CO2/GJ] / 1000 [MJ/GJ] * LHV [MJ/kg]

Markets and downstream links
------------------------------

Feedstock-filtered routes enter the liquid-fuel markets described in
:doc:`markets`. Allowed geographic suppliers are preferred; the documented
fallback can use the same feedstock outside the preferred locations. Energy
shares are converted to supplier amounts using fuel heating values.

Regional supply chains
~~~~~~~~~~~~~~~~~~~~~~~~

Biofuel routes supply regional fuel markets where mapped production selects
them. See :doc:`synthetic` for the separate hydrogen/CO2 synthesis routes.

Assumptions and limitations
-----------------------------

Crop selection is a regional proxy. IMAGE land-use information does not
provide equivalent coverage for every IAM. Consequential constrained-supplier
rules and source allocation conventions affect residual and waste feedstocks.
The current lack of blanket conversion-efficiency scaling should be considered
when interpreting claims of future process improvements.

Worked example and checks
---------------------------

Using illustrative inputs of 0.01 ha/GJ and a crop LHV of 15 MJ/kg gives
0.01 × 10000 / 1000 × 15 = 1.5 m² of land-occupation coefficient per kg on the
source time basis. The inventory flow records its occupation-time unit.
Check the reported IAM unit and time convention before applying this conversion.
Inspect changed occupation and land-use-change CO2 exchanges separately from
fuel inputs, and confirm the selected feedstock appears in the fuel market.

Sources and inventory details
-------------------------------

Source inventories: biofuels
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The inventories cover crop farming, conversion of biomass to fuel, and
distribution. Examples include
`switchgrass supply <https://clic.psi.ch/datasets/farming-and-supply-of-switchgrass/>`__,
`ethanol from switchgrass <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-switchgrass-energy-allocation/>`__
and `biodiesel from used cooking oil <https://clic.psi.ch/datasets/biodiesel-production-via-transesterification-from-used-cooking-oil-energy-allocation/>`__.

Conversion can also produce outputs such as distiller's grains, electricity,
CO2 or bagasse. The catalogue includes energy allocation, economic allocation
and system-expansion variants. Match that choice when comparing routes.

These inventories originate from several different sources
(`Wu <http://greet.es.anl.gov/publication-2lli584z>`__ et al. 2006 (2020 update), `Cozzolino <https://www.psi.ch/sites/default/files/2019-09/Cozzolino_377125_%20Research%20Project%20Report.pdf>`__ 2018, `Pereira <http://task39.sites.olt.ubc.ca/files/2019/04/Task-39-GHS-models-Final-Report-Phase-1.pdf>`__ et al. 2017 and `Gonzalez-Garcia <https://doi.org/10.1016/j.scitotenv.2012.07.044>`__ et al. 2012),
`Cavalett <https://doi.org/10.1002/bbb.2395>`__ & Cherubini 2022, as indicated in the table below.

CLIC links distinguish the published allocation variants for each activity
and location.

The following datasets are introduced:

.. list-table::
   :header-rows: 1

   * - Activity
     - Location
     - Source
     - CLIC dataset page
   * - Farming and supply of switchgrass
     - US
     - Wu et al. 2006 (2020 update)
     - `View dataset <https://clic.psi.ch/datasets/farming-and-supply-of-switchgrass/>`__
   * - Farming and supply of poplar
     - US
     - Wu et al. 2006 (2020 update)
     - `View dataset <https://clic.psi.ch/datasets/farming-and-supply-of-poplar/>`__
   * - Farming and supply of willow
     - US
     - Wu et al. 2006 (2020 update)
     - `View dataset <https://clic.psi.ch/datasets/farming-and-supply-of-willow/>`__
   * - Supply of forest residue
     - US
     - Wu et al. 2006 (2020 update)
     - `View dataset <https://clic.psi.ch/datasets/supply-of-forest-residue/>`__
   * - Farming and supply of miscanthus
     - US
     - Wu et al. 2006 (2020 update)
     - `View dataset <https://clic.psi.ch/datasets/farming-and-supply-of-miscanthus/>`__
   * - Farming and supply of corn stover
     - US
     - Wu et al. 2006 (2020 update)
     - `View dataset <https://clic.psi.ch/datasets/farming-and-supply-of-corn-stover/>`__
   * - Farming and supply of sugarcane
     - US
     - Wu et al. 2006 (2020 update)
     - `View dataset <https://clic.psi.ch/datasets/farming-and-supply-of-sugarcane/>`__
   * - Farming and supply of Grain Sorghum
     - US
     - Wu et al. 2006 (2020 update)
     - `View dataset <https://clic.psi.ch/datasets/farming-and-supply-of-grain-sorghum/>`__
   * - Farming and supply of Sweet Sorghum
     - US
     - Wu et al. 2006 (2020 update)
     - `View dataset <https://clic.psi.ch/datasets/farming-and-supply-of-sweet-sorghum/>`__
   * - Farming and supply of Forage Sorghum
     - US
     - Wu et al. 2006 (2020 update)
     - `View dataset <https://clic.psi.ch/datasets/farming-and-supply-of-forage-sorghum/>`__
   * - Farming and supply of corn
     - US
     - Wu et al. 2006 (2020 update)
     - `View dataset <https://clic.psi.ch/datasets/farming-and-supply-of-corn/>`__
   * - Farming and supply of sugarcane
     - BR
     - Pereira et al. 2019/RED II
     - `View dataset <https://clic.psi.ch/datasets/farming-and-supply-of-sugarcane-2/>`__
   * - Farming and supply of sugarcane straw
     - BR
     - Pereira et al. 2019
     - `View dataset <https://clic.psi.ch/datasets/farming-and-supply-of-sugarcane-straw/>`__
   * - Farming and supply of eucalyptus
     - ES
     - Gonzalez-Garcia et al. 2012
     - `View dataset <https://clic.psi.ch/datasets/farming-and-supply-of-eucalyptus/>`__
   * - Farming and supply of wheat grains
     - RER
     - Cozzolino 2018
     - `View dataset <https://clic.psi.ch/datasets/farming-and-supply-of-wheat-grains/>`__
   * - Farming and supply of wheat straw
     - RER
     - Cozzolino 2018
     - `View dataset <https://clic.psi.ch/datasets/farming-and-supply-of-wheat-straw/>`__
   * - Farming and supply of corn
     - RER
     - Cozzolino 2018/RED II
     - `View dataset <https://clic.psi.ch/datasets/farming-and-supply-of-corn-2/>`__
   * - Farming and supply of sugarbeet
     - RER
     - Cozzolino 2018
     - `View dataset <https://clic.psi.ch/datasets/farming-and-supply-of-sugarbeet/>`__
   * - Supply of forest residue
     - RER
     - Cozzolino 2018
     - `View dataset <https://clic.psi.ch/datasets/supply-of-forest-residue-2/>`__
   * - Supply and refining of waste cooking oil
     - RER
     - Cozzolino 2018
     - `View dataset <https://clic.psi.ch/datasets/supply-and-refining-of-waste-cooking-oil/>`__
   * - Farming and supply of rapeseed
     - RER
     - Cozzolino 2018/RED II
     - `View dataset <https://clic.psi.ch/datasets/farming-and-supply-of-rapeseed/>`__
   * - Farming and supply of palm fresh fruit bunch
     - RER
     - Cozzolino 2018
     - —
   * - Farming and supply of dry algae
     - RER
     - Cozzolino 2018
     - `View dataset <https://clic.psi.ch/datasets/farming-and-supply-of-dry-algae/>`__
   * - Ethanol production, via fermentation, from switchgrass
     - US
     - Wu et al. 2006 (2020 update)
     - `economic allocation <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-switchgrass-economic-allocation/>`__; `energy allocation <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-switchgrass-energy-allocation/>`__; `system expansion <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-switchgrass-system-expansion/>`__
   * - Ethanol production, via fermentation, from poplar
     - US
     - Wu et al. 2006 (2020 update)
     - `economic allocation <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-poplar-economic-allocation/>`__; `energy allocation <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-poplar-energy-allocation/>`__; `system expansion <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-poplar-system-expansion/>`__
   * - Ethanol production, via fermentation, from willow
     - US
     - Wu et al. 2006 (2020 update)
     - `economic allocation <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-willow-economic-allocation/>`__; `energy allocation <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-willow-energy-allocation/>`__; `system expansion <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-willow-system-expansion/>`__
   * - Ethanol production, via fermentation, from forest residue
     - US
     - Wu et al. 2006 (2020 update)
     - `economic allocation <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-forest-residue-economic-allocation/>`__; `energy allocation <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-forest-residue-energy-allocation/>`__; `system expansion <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-forest-residue-system-expansion/>`__
   * - Ethanol production, via fermentation, from miscanthus
     - US
     - Wu et al. 2006 (2020 update)
     - `economic allocation <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-miscanthus-economic-allocation/>`__; `energy allocation <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-miscanthus-energy-allocation/>`__; `system expansion <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-miscanthus-system-expansion/>`__
   * - Ethanol production, via fermentation, from corn stover
     - US
     - Wu et al. 2006 (2020 update)
     - `economic allocation <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-corn-stover-economic-allocation/>`__; `energy allocation <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-corn-stover-energy-allocation/>`__; `system expansion <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-corn-stover-system-expansion/>`__
   * - Ethanol production, via fermentation, from sugarcane
     - US
     - Wu et al. 2006 (2020 update)
     - `economic allocation <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-sugarcane-economic-allocation/>`__; `energy allocation <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-sugarcane-energy-allocation/>`__; `system expansion <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-sugarcane-system-expansion/>`__
   * - Ethanol production, via fermentation, from grain sorghum
     - US
     - Wu et al. 2006 (2020 update)
     - `economic allocation <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-grain-sorghum-economic-allocation/>`__; `energy allocation <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-grain-sorghum-energy-allocation/>`__; `system expansion <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-grain-sorghum-system-expansion/>`__
   * - Ethanol production, via fermentation, from sweet sorghum
     - US
     - Wu et al. 2006 (2020 update)
     - `economic allocation <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-sweet-sorghum-economic-allocation/>`__; `energy allocation <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-sweet-sorghum-energy-allocation/>`__; `system expansion <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-sweet-sorghum-system-expansion/>`__
   * - Ethanol production, via fermentation, from forage sorghum
     - US
     - Wu et al. 2006 (2020 update)
     - `economic allocation <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-forage-sorghum-economic-allocation/>`__; `energy allocation <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-forage-sorghum-energy-allocation/>`__; `system expansion <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-forage-sorghum-system-expansion/>`__
   * - Ethanol production, via fermentation, from corn
     - US
     - Wu et al. 2006 (2020 update)
     - `economic allocation <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-corn-economic-allocation/>`__; `energy allocation <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-corn-energy-allocation/>`__; `system expansion <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-corn-system-expansion/>`__
   * - Ethanol production, via fermentation, from corn, with carbon capture
     - US
     - Wu et al. 2006 (2020 update)
     - `economic allocation <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-corn-with-carbon-capture-and-storage-economic-allocation/>`__; `energy allocation <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-corn-with-carbon-capture-and-storage-energy-allocation/>`__; `system expansion <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-corn-with-carbon-capture-and-storage-system-expansion/>`__
   * - Ethanol production, via fermentation, from sugarcane
     - BR
     - Pereira et al. 2019
     - —
   * - Ethanol production, via fermentation, from sugarcane straw
     - BR
     - Pereira et al. 2019
     - `economic allocation <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-sugarcane-straw-economic-allocation/>`__; `energy allocation <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-sugarcane-straw-energy-allocation/>`__; `system expansion <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-sugarcane-straw-system-expansion/>`__
   * - Ethanol production, via fermentation, from eucalyptus
     - ES
     - Gonzalez-Garcia et al. 2012
     - `View dataset <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-eucalyptus/>`__
   * - Ethanol production, via fermentation, from wheat grains
     - RER
     - Cozzolino 2018
     - `energy allocation <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-wheat-grains-energy-allocation/>`__
   * - Ethanol production, via fermentation, from wheat straw
     - RER
     - Cozzolino 2018
     - `energy allocation <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-wheat-straw-energy-allocation/>`__
   * - Ethanol production, via fermentation, from corn starch
     - RER
     - Cozzolino 2018
     - `energy allocation <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-corn-starch-energy-allocation/>`__
   * - Ethanol production, via fermentation, from sugarbeet
     - RER
     - Cozzolino 2018
     - `energy allocation <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-sugarbeet-energy-allocation/>`__
   * - Ethanol production, via fermentation, from forest residue
     - RER
     - Cozzolino 2018
     - `energy allocation <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-forest-residue-energy-allocation-2/>`__
   * - Ethanol production, via fermentation, from forest residues
     - RER
     - Cavalett & Cherubini 2022
     - `energy allocation <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-forest-residue-energy-allocation-2/>`__; `energy allocation <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-forest-residues-energy-allocation/>`__
   * - Ethanol production, via fermentation, from forest product (non-residual)
     - RER
     - Cavalett & Cherubini 2022
     - `energy allocation <https://clic.psi.ch/datasets/ethanol-production-via-fermentation-from-forest-product-non-residual-energy-allocation/>`__
   * - Biodiesel production, via transesterification, from used cooking oil
     - RER
     - Cozzolino 2018
     - `energy allocation <https://clic.psi.ch/datasets/biodiesel-production-via-transesterification-from-used-cooking-oil-energy-allocation/>`__
   * - Biodiesel production, via transesterification, from rapeseed oil
     - RER
     - Cozzolino 2018
     - `energy allocation <https://clic.psi.ch/datasets/biodiesel-production-via-transesterification-from-rapeseed-oil-energy-allocation/>`__
   * - Biodiesel production, via transesterification, from palm oil, energy allocation
     - RER
     - Cozzolino 2018
     - —
   * - Biodiesel production, via transesterification, from algae, energy allocation
     - RER
     - Cozzolino 2018
     - `energy allocation <https://clic.psi.ch/datasets/biodiesel-production-via-transesterification-from-algae-energy-allocation/>`__
   * - Biodiesel production, via Fischer-Tropsch, from forest residues
     - RER
     - Cavalett & Cherubini 2022
     - `energy allocation <https://clic.psi.ch/datasets/biodiesel-production-via-fischer-tropsch-from-forest-residues-energy-allocation/>`__
   * - Biodiesel production, via Fischer-Tropsch, from forest product (non-residual)
     - RER
     - Cavalett & Cherubini 2022
     - `energy allocation <https://clic.psi.ch/datasets/biodiesel-production-via-fischer-tropsch-from-forest-product-non-residual-energy-allocation/>`__
   * - Kerosene production, via Fischer-Tropsch, from forest residues
     - RER
     - Cavalett & Cherubini 2022
     - `energy allocation <https://clic.psi.ch/datasets/kerosene-production-via-fischer-tropsch-from-forest-residues-energy-allocation/>`__
   * - Kerosene production, via Fischer-Tropsch, from forest product (non-residual)
     - RER
     - Cavalett & Cherubini 2022
     - `energy allocation <https://clic.psi.ch/datasets/kerosene-production-via-fischer-tropsch-from-forest-product-non-residual-energy-allocation/>`__

.. raw:: html

   <span id="source-provenance-and-currency"></span>

Sources and data dates
~~~~~~~~~~~~~~~~~~~~~~~~

.. include:: /reference/generated/source-biofuels.inc
