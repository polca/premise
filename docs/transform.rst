TRANSFORM
=========

A series of transformation are operation on the LCI database to align process performance
and technology market shares with outputs from the IAM scenario.

Power generation
""""""""""""""""

Efficiency adjustment
+++++++++++++++++++++

The energy conversion efficiency of powerplant datasets for a given technology
is adjusted to align with the change in efficiency indicated by the IAM scenario.

Combustion-based powerplants
----------------------------

*premise* iterates through coal, lignite, natural gas, biogas and wood-fired powerplants
datasets in the LCI database to calculate their current efficiency (i.e., the ratio between
the primary fuel energy entering the process and the output energy produced, which is often 1 kWh).
If the IAM scenario foresees a change in efficiency for these processes, the input of the datasets
are scaled up or down by the *scaling factor* to effectively reflect a change in fuel input
per kWh produced.

The origin of this *scaling factor* is explained in XXX.

To calculate the old and new efficiency of the dataset, it is necessary to know
the net calorific content of the fuel. The table below shows the Lower Heating Value
for the different fuels used in combustion-based powerplants.

 ================================================================== ===========================
  name of fuel                                                       LHV [MJ/kg, as received]
 ================================================================== ===========================
  hard coal                                                          26.7
  lignite                                                            11.2
  petroleum coke                                                     31.3
  wood pellet                                                        16.2
  wood chips                                                         18.9
  natural gas                                                        45
  gas, natural, in ground                                            45
  refinery gas                                                       50.3
  propane                                                            46.46
  heavy fuel oil                                                     38.5
  oil, crude, in ground                                              38.5
  light fuel oil                                                     42.6
  biogas                                                             22.73
  biomethane                                                         47.5
  waste                                                              14
  methane, fossil                                                    47.5
  methane, biogenic                                                  47.5
  methane, synthetic                                                 47.5
  diesel                                                             43
  gasoline                                                           42.6
  petrol, 5% ethanol                                                 41.7
  petrol, synthetic, hydrogen                                        42.6
  petrol, synthetic, coal                                            42.6
  diesel, synthetic, hydrogen                                        43
  diesel, synthetic, coal                                            43
  diesel, synthetic, wood                                            43
  diesel, synthetic, wood, with CCS                                  43
  diesel, synthetic, grass                                           43
  diesel, synthetic, grass, with CCS                                 43
  hydrogen, petroleum                                                120
  hydrogen, electrolysis                                             120
  hydrogen, biomass                                                  120
  hydrogen, biomass, with CCS                                        120
  hydrogen, coal                                                     120
  hydrogen, nat. gas                                                 120
  hydrogen, nat. gas, with CCS                                       120
  hydrogen, biogas                                                   120
  hydrogen, biogas, with CCS                                         120
  hydrogen                                                           120
  biodiesel, oil                                                     38
  biodiesel, oil, with CCS                                           38
  bioethanol, wood                                                   26.5
  bioethanol, wood, with CCS                                         26.5
  bioethanol, grass                                                  26.5
  bioethanol, grass, with CCS                                        26.5
  bioethanol, grain                                                  26.5
  bioethanol, grain, with CCS                                        26.5
  bioethanol, sugar                                                  26.5
  bioethanol, sugar, with CCS                                        26.5
  ethanol                                                            26.5
  methanol, wood                                                     19.9
  methanol, grass                                                    19.9
  methanol, wood, with CCS                                           19.9
  methanol, grass, with CCS                                          19.9
  liquified petroleum gas, natural                                   45.5
  liquified petroleum gas, synthetic                                 45.5
  uranium, enriched 3.8%, in fuel element for light water reactor    4199040
  nuclear fuel element, for boiling water reactor, uo2 3.8%          4147200
  nuclear fuel element, for boiling water reactor, uo2 4.0%          4147200
  nuclear fuel element, for pressure water reactor, uo2 3.8%         4579200
  nuclear fuel element, for pressure water reactor, uo2 4.0%         4579200
  nuclear fuel element, for pressure water reactor, uo2 4.2%         4579200
  uranium hexafluoride                                               709166
  enriched uranium, 4.2%                                             4579200
  mox fuel element                                                   4579200
  heat, from hard coal                                               1
  heat, from lignite                                                 1
  heat, from petroleum coke                                          1
  heat, from wood pellet                                             1
  heat, from natural gas, high pressure                              1
  heat, from natural gas, low pressure                               1
  heat, from heavy fuel oil                                          1
  heat, from light fuel oil                                          1
  heat, from biogas                                                  1
  heat, from waste                                                   1
  heat, from methane, fossil                                         1
  heat, from methane, biogenic                                       1
  heat, from diesel                                                  1
  heat, from gasoline                                                1
  heat, from bioethanol                                              1
  heat, from biodiesel                                               1
  heat, from liquified petroleum gas, natural                        1
  heat, from liquified petroleum gas, synthetic                      1
  bagasse, from sugarcane                                            15.4
  bagasse, from sweet sorghum                                        13.8
  sweet sorghum stem                                                 4.45
  cottonseed                                                         21.97
  flax husks                                                         21.5
  coconut husk                                                       20
  sugar beet pulp                                                    5.11
  cleft timber                                                       14.46
  rape meal                                                          31.1
  molasse, from sugar beet                                           16.65
  sugar beet                                                         4.1
  barkey grain                                                       19.49
  rye grain                                                          12
  sugarcane                                                          5.3
  palm date                                                          10.8
  whey                                                               1.28
  straw                                                              15.5
  grass                                                              17
  manure, liquid                                                     0.875
  manure, solid                                                      3.6
  kerosene, from petroleum                                           43
  kerosene, synthetic, from electrolysis, energy allocation          43
  kerosene, synthetic, from electrolysis, economic allocation        43
  kerosene, synthetic, from coal, energy allocation                  43
  kerosene, synthetic, from coal, economic allocation                43
  kerosene, synthetic, from natural gas, energy allocation           43
  kerosene, synthetic, from natural gas, economic allocation         43
  kerosene, synthetic, from biomethane, energy allocation            43
  kerosene, synthetic, from biomethane, economic allocation          43
  kerosene, synthetic, from biomass, energy allocation               43
  kerosene, synthetic, from biomass, economic allocation             43
 ================================================================== ===========================

Additionally, the biogenic and fossil CO2 emissions of the datasets are also scaled up or down
by the same factor, as those are proportionate to the amount of fuel used.

Finally, another *scaling factor* is used to scale emissions of non-CO2 substances (CO, VOCs, etc.),
based on GAINS projections for the given technology, region and year.

We provide below an example of a natural gas powerplant, with a current (2020)
conversion efficiency of 77%. If the IAM scenario indicates a *scaling factor*
of 1.03 in 2030, this indicates tha the efficiency increases by 3% relative to current.
As shown in the table below, this would results in a new efficiency of 79%, where
all inputs, as well as CO2 emissions outputs are re-scaled by 1/1.03 (=0.97).
This excludes non-CO2 emissions, such as CO  in this example, which are re-scaled separately,
based on GAINS projections: such emissions, while partly correlated to fuel use,
are mostly mitigated via investments in electrostatic precipitators,
which is what GAINS scenarios model.

 =================================================== =========== =========== =======
  electricity production, natura gas, conventional    before      after       unit
 =================================================== =========== =========== =======
  electricity production                              1           1           kWh
  natural gas                                         0.1040      0.1010      m3
  water                                               0.0200      0.0194      m3
  powerplant construction                             1.00E-08    9.71E-09    unit
  CO2, fossil                                         0.0059      0.0057      kg
  CO, fossil                                          5.87E-06    5.42E-03    kg
  fuel-to-electricity efficiency                      77%         79%         %
 =================================================== =========== =========== =======

*premise* has a couple of rules regarding projected *scaling factors*:

* *scaling factors* inferior to 1 beyond 2020 are not accepted and are treated as 1.
* *scaling factors* superior to 1 before 2020 are not accepted and are treated as 1.

This is to prevent degrading the performance of a technology in the future, or
improving its performance in the past, relative to today.

Photovoltaics panels
--------------------

Photovoltaic panels are expected to improve over time. The following module efficiencies
are considered for the different types of PV panels:


 ====================== =========== ============ =========== ======= ====== =======
  % module efficiency    micro-Si    single-Si    multi-Si    CIGS    CIS    CdTe
 ====================== =========== ============ =========== ======= ====== =======
  2010                   10          15.1         14          11      11     10
  2020                   11.9        17.9         16.8        14      14     16.8
  2050                   12.5        26.7         24.4        23.4    23.4   21
 ====================== =========== ============ =========== ======= ====== =======

The sources for these efficiencies are given in XXX.

Given a scenario year, *premise* iterates through the different PV panel installation
datasets to update their efficiency accordingly.
To do so, the required surface of panel (in m2) per kW of capacity is
adjusted down (or up, if the efficiency is lower than current).

To calculate the current efficiency of a PV installation, *premise* assumes a solar
irradiation of 1000 W/m2. Hence, the current efficiency is calculated as::

    current_eff [%] = installation_power [W]  / (panel_surface [m2] * 1000 [W/m2])

The *scaling factor* is calculated as::

    scaling_factor = current_eff / new_eff

The required surface of PV panel in the dataset is then adjusted like so::

    new_surface = current_surface * (1 / scaling_factor)

For scenario years beyond 2050, 2050 efficiency values are used.


The table below provides such an example where a 450 kWp flat-roof installation
sees its current (2020) module efficiency improving from 20% to 26% by 2050.
THe are of PV panel (and mounting system) has been multiplied by 1 / (0.26/0.20),
all other inputs remaining unchanged.

 =================================================================== ========= ======== =======
  450kWp flat roof installation                                       before    after    unit
 =================================================================== ========= ======== =======
  photovoltaic flat-roof installation, 450 kWp, single-SI, on roof    1         1        unit
  inverter production, 500 kW                                         1.5       1.5      unit
  photovoltaic mounting system, …                                     2300      1731     m2
  photovoltaic panel, single-SI                                       2500      1881     m2
  treatment, single-SI PV module                                      30000     30000    kg
  electricity, low voltage                                            25        25       kWh
  module efficiency                                                   20%       26%      %
 =================================================================== ========= ======== =======


Markets
+++++++

*premise* creates additional datasets that represent the average supply and
production pathway for a given commodity for a given scenario, year and region.

Such datasets are called *regional markets*. Hence, a regional market for high voltage
electricity contains the different technologies that supply electricity at high voltage
in a given IAM region, in proportion to their respective production volumes.

Regional biomass markets
------------------------

*premise* creates regional markets for biomass which is meant to be used as fuel
in biomass-fired powerplants. Originally in ecoinvent, the biomass being supplied
to biomass-fired powerplants is "purpose grown" biomass that originate forestry
activities (called "market for wood chips" in ecoinvent). While this type of biomass
is suitable for such purpose, it is considered a co-product of the forestry activity,
and bears a share of the environmental burden of the process it originates from (notably
the land footprint, emissions, potential use of chemicals, etc.).

However, not all the biomass projected to be used in IAM scenarios is "purpose grown".
In fact, significant shares are expected to originate from forestry residues. In such
cases, the environmental burden of the forestry activity is entirely allocated to the
determining product (e.g., timber), not to the residue, which comes "free of burden".

Hence, *premise* creates average regional markets for biomass, which represents the
average shares of "purpose grown" and "residual" biomass being fed to biomass-fired powerplants.

The following market is created for each IAM region:

 =================================== ==================
  market name                         location
 =================================== ==================
  market for biomass, used as fuel    all IAM regions
 =================================== ==================

inside of which, the shares of "purpose grown" and "residual" biomass
is represented by the following activities:

========================== ===================================== ======================================= ===========================
  name in premise            name in REMIND                         name in IMAGE                         name in LCI database
========================== ===================================== ======================================= ===========================
  biomass - purpose grown    SE|Electricity|Biomass|Energy Crops   Primary Energy|Biomass|Energy Crops    market for wood chips
  biomass - residual         SE|Electricity|Biomass|Residues       Primary Energy|Biomass|Residues        Supply of forest residue
========================== ===================================== ======================================= ===========================

The sum of those shares equal 1. The activity "Supply of forest residue" includes
the energy, transport and associated emissions to chip the residual biomass
and transport it to the powerplant, but no other forestry-related burden is included.

Regional electricity markets
----------------------------

Long-term regional electricity markets
--------------------------------------


Cement production
"""""""""""""""""

Efficiency adjustment
+++++++++++++++++++++

Carbon Capture and Storage
++++++++++++++++++++++++++

Clinker-to-cement ratio
+++++++++++++++++++++++

Cement markets
++++++++++++++


Steel production
""""""""""""""""

Efficiency adjustment
+++++++++++++++++++++

Carbon Capture and Storage
++++++++++++++++++++++++++

Steel markets
+++++++++++++

Transport
"""""""""

Two-wheelers
++++++++++++

Passenger cars
++++++++++++++

Trucks
++++++

Fleet average trucks
--------------------

Driving cycles
--------------

Buses
+++++

Fuels
"""""

Efficiency adjustment
+++++++++++++++++++++

Land use and land use change
++++++++++++++++++++++++++++

Regional supply chains
++++++++++++++++++++++

Fuel markets
++++++++++++

CO2 emissions update
++++++++++++++++++++

