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
Hence, the land occupation per kg of crop farmed is calculated as::

    land_use = land_use [Ha/GJ] * 10000 [m2/Ha] / 1000 [MJ/GJ] * LHV [MJ/kg]

Regarding land use change CO2 emissions, the principle is similar. The variable
is expressed in kg CO2/GJ of primary crop energy. Hence, the land use change
CO2 emissions per kg of crop farmed are calculated as::

    land_use_co2 = land_use_co2 [kg CO2/GJ] / 1000 [MJ/GJ] * LHV [MJ/kg]

Citation-date clarification
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The linked Task 39 phase-1 comparison report is dated February 2017; the 2019
URL directory is not its publication year. Its harmonization exercise shows
why allocation, fertilizer emissions and transport must be aligned before
comparing biofuel GWP results across tools.

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

Inventories for energy crops- and residues-based production of bioethanol and biodiesel
are imported, and can be accessed here: `LCI_biofuels <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-biofuels.xlsx>`__. They include the farming of the crop,
the conversion of the biomass to fuel, as well as its distribution. The conversion process
often leads to the production of co-products (dried distiller's grain, electricity, CO2, bagasse.).
Hence, energy, economic and system expansion partitioning approaches are available.
These inventories originate from several different sources
(`Wu <http://greet.es.anl.gov/publication-2lli584z>`__ et al. 2006 (2020 update), `Cozzolino <https://www.psi.ch/sites/default/files/2019-09/Cozzolino_377125_%20Research%20Project%20Report.pdf>`__ 2018, `Pereira <http://task39.sites.olt.ubc.ca/files/2019/04/Task-39-GHS-models-Final-Report-Phase-1.pdf>`__ et al. 2017 and `Gonzalez-Garcia <https://doi.org/10.1016/j.scitotenv.2012.07.044>`__ et al. 2012),
`Cavalett <https://doi.org/10.1002/bbb.2395>`__ & Cherubini 2022, as indicated in the table below.

The following datasets are introduced:

 ================================================================================== =========== =============================
  Activity                                                                           Location    Source
 ================================================================================== =========== =============================
  Farming and supply of switchgrass                                                  US          Wu et al. 2006 (2020 update)
  Farming and supply of poplar                                                       US          Wu et al. 2006 (2020 update)
  Farming and supply of willow                                                       US          Wu et al. 2006 (2020 update)
  Supply of forest residue                                                           US          Wu et al. 2006 (2020 update)
  Farming and supply of miscanthus                                                   US          Wu et al. 2006 (2020 update)
  Farming and supply of corn stover                                                  US          Wu et al. 2006 (2020 update)
  Farming and supply of sugarcane                                                    US          Wu et al. 2006 (2020 update)
  Farming and supply of Grain Sorghum                                                US          Wu et al. 2006 (2020 update)
  Farming and supply of Sweet Sorghum                                                US          Wu et al. 2006 (2020 update)
  Farming and supply of Forage Sorghum                                               US          Wu et al. 2006 (2020 update)
  Farming and supply of corn                                                         US          Wu et al. 2006 (2020 update)
  Farming and supply of sugarcane                                                    BR          Pereira et al. 2019/RED II
  Farming and supply of sugarcane straw                                              BR          Pereira et al. 2019
  Farming and supply of eucalyptus                                                   ES          Gonzalez-Garcia et al. 2012
  Farming and supply of wheat grains                                                 RER         Cozzolino 2018
  Farming and supply of wheat straw                                                  RER         Cozzolino 2018
  Farming and supply of corn                                                         RER         Cozzolino 2018/RED II
  Farming and supply of sugarbeet                                                    RER         Cozzolino 2018
  Supply of forest residue                                                           RER         Cozzolino 2018
  Supply and refining of waste cooking oil                                           RER         Cozzolino 2018
  Farming and supply of rapeseed                                                     RER         Cozzolino 2018/RED II
  Farming and supply of palm fresh fruit bunch                                       RER         Cozzolino 2018
  Farming and supply of dry algae                                                    RER         Cozzolino 2018
  Ethanol production, via fermentation, from switchgrass                             US          Wu et al. 2006 (2020 update)
  Ethanol production, via fermentation, from poplar                                  US          Wu et al. 2006 (2020 update)
  Ethanol production, via fermentation, from willow                                  US          Wu et al. 2006 (2020 update)
  Ethanol production, via fermentation, from forest residue                          US          Wu et al. 2006 (2020 update)
  Ethanol production, via fermentation, from miscanthus                              US          Wu et al. 2006 (2020 update)
  Ethanol production, via fermentation, from corn stover                             US          Wu et al. 2006 (2020 update)
  Ethanol production, via fermentation, from sugarcane                               US          Wu et al. 2006 (2020 update)
  Ethanol production, via fermentation, from grain sorghum                           US          Wu et al. 2006 (2020 update)
  Ethanol production, via fermentation, from sweet sorghum                           US          Wu et al. 2006 (2020 update)
  Ethanol production, via fermentation, from forage sorghum                          US          Wu et al. 2006 (2020 update)
  Ethanol production, via fermentation, from corn                                    US          Wu et al. 2006 (2020 update)
  Ethanol production, via fermentation, from corn, with carbon capture               US          Wu et al. 2006 (2020 update)
  Ethanol production, via fermentation, from sugarcane                               BR          Pereira et al. 2019
  Ethanol production, via fermentation, from sugarcane straw                         BR          Pereira et al. 2019
  Ethanol production, via fermentation, from eucalyptus                              ES          Gonzalez-Garcia et al. 2012
  Ethanol production, via fermentation, from wheat grains                            RER         Cozzolino 2018
  Ethanol production, via fermentation, from wheat straw                             RER         Cozzolino 2018
  Ethanol production, via fermentation, from corn starch                             RER         Cozzolino 2018
  Ethanol production, via fermentation, from sugarbeet                               RER         Cozzolino 2018
  Ethanol production, via fermentation, from forest residue                          RER         Cozzolino 2018
  Ethanol production, via fermentation, from forest residues                         RER         Cavalett & Cherubini 2022
  Ethanol production, via fermentation, from forest product (non-residual)           RER         Cavalett & Cherubini 2022
  Biodiesel production, via transesterification, from used cooking oil               RER         Cozzolino 2018
  Biodiesel production, via transesterification, from rapeseed oil                   RER         Cozzolino 2018
  Biodiesel production, via transesterification, from palm oil, energy allocation    RER         Cozzolino 2018
  Biodiesel production, via transesterification, from algae, energy allocation       RER         Cozzolino 2018
  Biodiesel production, via Fischer-Tropsch, from forest residues                    RER         Cavalett & Cherubini 2022
  Biodiesel production, via Fischer-Tropsch, from forest product (non-residual)      RER         Cavalett & Cherubini 2022
  Kerosene production, via Fischer-Tropsch, from forest residues                     RER         Cavalett & Cherubini 2022
  Kerosene production, via Fischer-Tropsch, from forest product (non-residual)       RER         Cavalett & Cherubini 2022
 ================================================================================== =========== =============================

Source provenance and currency
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. include:: /reference/generated/source-biofuels.inc
