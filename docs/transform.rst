TRANSFORM
=========

A series of transformations are applied to the Life Cycle Inventory (LCI) database to align process performance
and technology market shares with the outputs from the Integrated Assessment Model (IAM) scenario.

Mobile batteries
++++++++++++++++

Inventories for several battery technologies for mobile applications are provided
in *premise*. See EXTRACT/Import of additional inventories/Li-ion batteries for
additional information.

Run

.. code-block:: python

    from premise import *
    import brightway2 as bw

    bw.projects.set_current("my_project)

    ndb = NewDatabase(
        scenarios=[
                {"model":"remind", "pathway":"SSP2-Base", "year":2028}
            ],
        source_db="ecoinvent 3.7 cutoff",
        source_version="3.7.1",
        key='xxxxxxxxxxxxxxxxxxxxxxxxx'
    )
    ndb.update("battery")


The table below shows the **current** specific energy density of
different battery technologies.

====================== ==================================== ==================== ==================
Type                   Specific energy density (current)    BoP mass share [%]   Battery energy
                       [kWh/kg cell]                                             density [kWh/kg
                                                                                 battery]
====================== ==================================== ==================== ==================
Li-ion, NMC111         0.18                                 73%                  0.13
Li-ion, NMC523         0.20                                 73%                  0.15
Li-ion, NMC622         0.24                                 73%                  0.18
Li-ion, NMC811         0.28                                 71%                  0.20
Li-ion, NMC955         0.34                                 71%                  0.24
Li-ion, NCA            0.28                                 71%                  0.20
Li-ion, LFP            0.16                                 80%                  0.13
Li-ion, LiMn2O4        0.11                                 80%                  0.09
Li-ion, LTO            0.05                                 64%                  0.03
Li-sulfur, Li-S        0.15                                 75%                  0.11
Li-oxygen, Li-O2       0.36                                 55%                  0.20
Sodium-ion, SiB        0.16                                 75%                  0.12
====================== ==================================== ==================== ==================

And the table below shows the **projected** (2050) specific energy density
of different battery technologies.

====================== ==================================== ==================== ==================
Type                   Specific energy density (2050)       BoP mass share [%]   Battery energy
                       [kWh/kg cell]                                             density [kWh/kg
                                                                                 battery]
====================== ==================================== ==================== ==================
Li-ion, NMC111         0.2                                  73%                  0.15
Li-ion, NMC523         0.22                                 73%                  0.16
Li-ion, NMC622         0.26                                 73%                  0.19
Li-ion, NMC811         0.34                                 71%                  0.24
Li-ion, NMC955         0.38                                 71%                  0.27
Li-ion, NCA            0.34                                 71%                  0.24
Li-ion, LFP            0.22                                 80%                  0.18
Li-ion, LiMn2O4        0.11                                 73%                  0.08
Li-ion, LTO            0.05                                 64%                  0.03
Li-sulfur, Li-S        0.34                                 75%                  0.26
Li-oxygen, Li-O2       0.93                                 55%                  0.51
Sodium-ion, SiB        0.20                                 75%                  0.15
====================== ==================================== ==================== ==================


*premise* adjusts the mass of battery packs throughout the database
to reflect progress in specific energy density (kWh/kg cell).

For example, in 2050, the mass of NMC811 batteries (cells and Balance of Plant) is expected to
be 0.5/0.22 = 2.3 times lower for a same energy capacity. The report of changes
shows the new mass of battery packs for each activity using them.

The target values used for scaling can be modified by the user.
The YAML file is located under premise/data/battery/energy_density.yaml.

For each battery technology *premise* creates a market dataset that represents the
supply of 1 kWh of electricity stored in a battery of the given technology.

The table below shows the market for battery capacity datasets created by *premise*.

=============================================== =========== ============================= ===============================
 Name                                            Location    Kg per kWh in 2020 (kg/kWh)   Kg per kWh in 2050 (kg/KWh)
=============================================== =========== ============================= ===============================
market for battery capacity, Li-ion, LFP	     GLO         8.6                           6.22
market for battery capacity, Li-ion, LTO	     GLO         18.4                          18.4
market for battery capacity, Li-ion, Li-O2	     GLO         5.05                          3.37
market for battery capacity, Li-ion, LiMn2O4     GLO         8.75                          8.75
market for battery capacity, Li-ion, NCA	     GLO         5.03                          4.14
market for battery capacity, Li-ion, NMC111	     GLO         7.61                          6.85
market for battery capacity, Li-ion, NMC523	     GLO         6.85                          6.23
market for battery capacity, Li-ion, NMC622	     GLO         5.71                          5.27
market for battery capacity, Li-ion, NMC811	     GLO         5.03                          4.14
market for battery capacity, Li-ion, NMC955	     GLO         4.14                          3.71
market for battery capacity, Li-sulfur, Li-S     GLO         8.89                          3.92
market for battery capacity, Sodium-Nickel-Cl	 GLO         8.62                          8.62
market for battery capacity, Sodium-ion, SiB     GLO         8.33                          6.54
=============================================== =========== ============================= ===============================

Changing the target values in the YAML file will change the scaling factors
and the mass of battery packs per kWh in the database.

Finally, *premise* also create a technology-average dataset for mobile batteries
according to four scenarios provided in Degen_ et al, 2023.:

============================================= =========== ===================================================================
 Name                                          Location    Description
============================================= =========== ===================================================================
market for battery capacity (LFP scenario)     GLO         LFP dominates the market for mobile batteries.
market for battery capacity (NCx scenario)     GLO         NCA and NCM dominate the market for mobile batteries.
market for battery capacity (PLiB scenario)    GLO         Post-lithium batteries dominate the market for mobile batteries.
market for battery capacity (MIX scenario)     GLO         A mix of lithium and post-lithium batteries dominates the market.
============================================= =========== ===================================================================

These  datasets provide 1 kWh of battery capacity, and the technology
shares are adjusted over time with values found
under  https://github.com/polca/premise/blob/master/premise/data/battery/scenario.csv.

.. _Degen: https://www.nature.com/articles/s41560-023-01355-z


Stationary batteries
++++++++++++++++++++

Inventories for several battery technologies for stationary applications are provided:

* Lithium-ion batteries (NMC-111, NMC-622, NMC-811, LFP)
* Lead-acid batteries
* Vanadium redox flow batteries (VRFB)

As for batteries for mobile applications, *premise* adjusts the mass of battery packs
throughout the database to reflect progress in specific energy density (kWh/kg cell).
The current specific energy densities are given in the table below.

====================== ==================================== ==================== ==================
Type                   Specific energy density (current)    BoP mass share [%]   Battery energy
                       [kWh/kg cell]                                             density [kWh/kg
                                                                                 battery]
====================== ==================================== ==================== ==================
Li-ion, NMC111         0.15                                 73%                  0.11
Li-ion, NMC622         0.20                                 73%                  0.15
Li-ion, NMC811         0.22                                 71%                  0.16
Li-ion, LFP            0.14                                 73%                  0.10
Sodium-ion, SiB        0.16                                 75%                  0.12
Lead-acid              0.03                                 80%                  0.02
VRFB                   0.02                                 75%                  0.02
====================== ==================================== ==================== ==================

The future specific energy densities are given in the table below.

====================== ==================================== ==================== ==================
Type                   Specific energy density (2050)       BoP mass share [%]   Battery energy
                       [kWh/kg cell]                                             density [kWh/kg
                                                                                 battery]
====================== ==================================== ==================== ==================
Li-ion, NMC111         0.2                                  73%                  0.15
Li-ion, NMC811         0.5                                  71%                  0.36
Li-ion, NCA            0.35                                 71%                  0.25
Li-ion, LFP            0.25                                 73%                  0.18
Sodium-ion, SiB        0.22                                 75%                  0.17
Lead-acid              0.04                                 80%                  0.03
VRFB                   0.04                                 75%                  0.03
====================== ==================================== ==================== ==================

The target values used for scaling can be modified by the user.
The YAML file is located under premise/data/battery/energy_density.yaml.

For each battery technology *premise* creates a market dataset that represents the
supply of 1 kWh of electricity stored in a battery of the given technology.

The table below shows the market for battery capacity datasets created by *premise*.

========================================================================== =========== ============================= ===============================
 Name                                                                       Location    Kg per kWh in 2020 (kg/kWh)   Kg per kWh in 2050 (kg/KWh)
========================================================================== =========== ============================= ===============================
market for battery capacity, Li-ion, LFP, stationary	                    GLO         8.6                           6.22
market for battery capacity, Li-ion, NMC111, stationary	                    GLO         7.61                          6.85
market for battery capacity, Li-ion, NMC523, stationary	                    GLO         6.85                          6.23
market for battery capacity, Li-ion, NMC622, stationary	                    GLO         5.71                          5.27
market for battery capacity, Li-ion, NMC811, stationary	                    GLO         5.03                          4.14
market for battery capacity, Li-ion, NMC955, stationary	                    GLO         4.14                          3.71
market for battery capacity, Sodium-Nickel-Chloride, Na-NiCl, stationary	GLO         8.62                          8.62
market for battery capacity, Sodium-ion, SiB, stationary	                GLO         8.33                          6.54
market for battery capacity, lead acid, rechargeable, stationary	        GLO         33.33                         28.60
market for battery capacity, redox-flow, Vanadium, stationary	            GLO         51.55                         25.00
========================================================================== =========== ============================= ===============================

Changing the target values in the YAML file will change the scaling factors
and the mass of battery packs per kWh in the database.

Finally, *premise* also create a technology-average dataset for stationary batteries
according to three scenarios provided in Schlichenmaier_ & Naegler, 2022:

======================================================== =========== =============================================================================
 Name                                                     Location    Description
======================================================== =========== =============================================================================
market for battery capacity, stationary (CONT scenario)   GLO         LFP and NMC dominate the market for stationary batteries.
market for battery capacity, stationary (TC scenario)     GLO         Vanadium Redox Flow batteries dominate the market for stationary batteries.
======================================================== =========== =============================================================================

.. _Schlichenmaier: https://doi.org/10.1016/j.egyr.2022.11.025


`market for battery capacity, stationary (CONT scenario)` supplies any storage
capacity needed in high voltage electricity markets.

Metals
++++++

*premise* updates the material intensities of energy and transport technologies,
with a particular focus on critical raw materials. The goal is to ensure that
both current and future datasets accurately reflect the evolving material
requirements of key technologies, such as wind turbines and batteries.
Key processes include collecting and processing material intensity data, adding
new metal production inventories, applying post-allocation corrections for
co-mined metals, and constructing global markets for mined and refined metals.

The workflow for updating material intensities in *premise* consists of the following steps:

* *Data collection*: Material intensity data is sourced from literature and stored in structured files.
* *Data processing*: The collected data is processed to align with the database, including unit conversions and mapping to relevant datasets.
* *Inventories*: Additional inventories for metals production (e.g., Cobalt, Lithium, Vanadium) are added to the database.
* *Post-allocation correction*: Multifunctional processes (e.g., co-mining) are adjusted to ensure proper mass balance.
* *Markets creation*: Global supply markets for mined and refined metals are built, reflecting current and future regional contributions.

To update the material intensities in the database, run the following code:

.. code-block:: python

    from premise import *
    import brightway2 as bw

    bw.projects.set_current("my_project)

    ndb = NewDatabase(
        scenarios=[
                {"model":"remind", "pathway":"SSP2-Base", "year":2028}
            ],
        source_db="ecoinvent 3.7 cutoff",
        source_version="3.7.1",
        key='xxxxxxxxxxxxxxxxxxxxxxxxx'
    )
    ndb.update("metals")


Data collection and processing
------------------------------

Distributions for material intensities, derived from a comprehensive literature
collection, are provided in `SI_2_Material_requirements.xlsx <https://github.com/polca/premise/blob/master/premise/data/metals/SI_2_Material_requirements.xlsx>`_.
From this database, `metals_db.csv <https://github.com/polca/premise/blob/master/premise/data/metals/metals_db.csv>`_ is created,
which *premise* uses to update the material intensities for each technology.

The mapping file that associate metal intensities to datasets to be
updated can be found in `activity_mapping.yml <https://github.com/polca/premise/blob/master/premise/data/metals/activities_mapping.yml>`_.

To convert the units in `metals_db.csv <https://github.com/polca/premise/blob/master/premise/data/metals/metals_db.csv>`_
to the units used in ecoinvent (e.g., converting [kg metal/kW] to [kg metal/kg battery]), *premise* uses
the conversion factors found in `conversion_factors.csv <https://github.com/polca/premise/blob/master/premise/data/metals/conversion_factors.xlsx>`_.

Finally, *premise* uses the data under `metal_products.csv <https://github.com/polca/premise/blob/master/premise/data/metals/metal_product.xlsx>`_
to refine the activity in ecoinvent to be updated, select the specific metal
product (e.g., boric oxide for boron used in wind turbine magnets)
and convert the intensities to the relevant compound (e.g., 1kg of Boron is converted to 86.19 kg of B2O3).


Inventories
-----------

*premise* provides inventories for the following metals:

* `Cobalt <https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-cobalt.xlsx>`_.
* `Germanium <https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-germanium.xlsx>`_, as a co-product from zinc mine operation, based on the unallocated dataset in ecoinvent.
* `Graphite <https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-graphite.xlsx>`_.
* `Iridium <https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-PGM.xlsx>`_, as a co-product from PGM mine operation, based on the unallocated dataset in ecoinvent.
* `Lithium <https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-lithium.xlsx>`_.
* `Rhenium <https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-rhenium.xlsx>`_, as a co-product from copper mine operation, based on the unallocated dataset in ecoinvent.
* `Ruthenium <https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-PGM.xlsx>`_, as a co-product from PGM mine operation, based on the unallocated dataset in ecoinvent.
* and `Vanadium <https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-batteries-vanadium.xlsx>`_.

The inventories are provided under `premise/data/additional_inventories <https://github.com/polca/premise/tree/master/premise/data/additional_inventories>`_

Post-allocation correction
--------------------------

Regarding the co-production of metals in multifunctional processes
(i.e., co-mining of metals), *premise* modifies the database to allocate
according to physical mass balance: extraction of individual elements in the
ore is fully attributed to the production of the respective metal; while other
elementary and intermediate flows follow an economic allocation, which is the
default option to deal with multi-functionality in ecoinvent. As discussed in
Berger_ et al. (2023), this approach ensures a correct mass balance.

For example, the amount of platinum resource included in the dataset representing
the mining of 1 kg of platinum is set to 1 kg, while the amount of other
metals (e.g., palladium, rhodium) is set to zero. The same approach is applied
to the datasets representing the mining of the other co-mined metals.

The file used to apply this correction is `corrections.yaml <https://github.com/polca/premise/blob/master/premise/data/metals/post-allocation_correction/corrections.yaml>`_.

The markets are relinked to metals-consuming activities throughout the database.

.. _Berger: https://doi.org/10.1007/s11367-020-01737-5


Mining and refining markets creation
------------------------------------

*premise* builds global supply markets for several mined and refined metals.
In these markets, the contribution of different mining and refining regions corresponds
to their *current* market shares. Following this approach, the supply from different
regions for a specific metal will be directly proportional to the country-level
contributions to the global market. These shares are derived from various sources,
mainly `BGS <https://www2.bgs.ac.uk/mineralsuk/statistics/worldStatistics.html>`_
and `USGS <https://doi.org/https://doi.org/10.3133/mcs2023>`_, in addition to data from
van den Brink_ et al. (2022) for Antimony refining.
For certain markets where data was available, *premise* incorporates projections
from `BNEF <https://about.bnef.com/>`_ regarding the development of future mining and refining projects to
forecast the market shares' evolution up to 2030.

The file used to build the global supply markets for mined and refined metals is `mining_shares_mapping.xlsx <https://github.com/polca/premise/blob/master/premise/data/metals/mining_shares_mapping.xlsx>`_.

.. _van den Brink: https://doi.org/10.1016/j.resconrec.2022.106586

Additionally, global metal supply markets modeled account for the average transport
distances and modes of transport for the different metals from producer
to consumer. These data are retrieved from `UNCTAD <https://unctad.org/system/files/official-document/ser-rp-2022d5_en.pdf>`_.

Average transport distance and modes of transport for each producer can be found under: `transport_markets_data <https://github.com/polca/premise/blob/master/premise/data/metals/transport_markets_data.csv>`_.

Mining
++++++

To update the mining practices in the database, run the following code:

.. code-block:: python

    from premise import *
    import brightway2 as bw

    bw.projects.set_current("my_project)

    ndb = NewDatabase(
        scenarios=[
                {"model":"remind", "pathway":"SSP2-Base", "year":2028}
            ],
        source_db="ecoinvent 3.7 cutoff",
        source_version="3.7.1",
        key='xxxxxxxxxxxxxxxxxxxxxxxxx'
    )
    ndb.update("mining")

Sulfidic tailings
-----------------

Mine tailings represent one of the most environmentally problematic waste streams generated by mining activities,
especially when they originate from the processing of sulfidic ores. If not properly managed, such tailings can
lead to acid mine drainage, causing long-term toxic contamination of surrounding ecosystems even decades after
mine closure [1].

Globally, most tailings generated through the beneficiation of hard rock metal ores and industrial minerals are
stored in dammed impoundments where they are often submerged to minimize dust and reduce sulfide oxidation [2].
However, a range of alternative tailings management strategies is increasingly being adopted.

To better reflect these evolving practices in life cycle modeling, we modified the ecoinvent database by introducing
multiple treatment pathways for sulfidic tailings. These include:

* **Surface impoundment**, which remains the default inventory in ecoinvent.
* **Backfilling into underground voids**, based on [1], which builds upon operational data
from [3]. The life cycle inventory for this process includes the consumption of materials such as
cement binders, slags, and fuel, and accounting for the associated energy demands. Backfilling is assumed to involve
cement stabilization of the residues, effectively preventing leaching emissions from the deposited material.
* **Flocculation-flotation**, based on [4], where the sulfur-rich fraction from the tailings
stream is separated using polyacrylamide and xanthate as flocculants and collector agents to improve pyrite separation.
The valorized output can potentially be used downstream in the cement and ceramic tiles industries.
* **Roasting and leaching**, also based on [4], involves first removing the sulfur content of tailings
through drying and roasting. Copper and zinc are then recovered using a combination of ammoniacal leaching, ion
flotation, and chemical precipitation.

In the default ecoinvent system, all sulfidic tailings are treated via impoundment. The table below presents regional
estimates for the uptake of the various alternatives, along with the references used to approximate the data points or
inform the underlying trends.

These newly introduced treatment pathways represent a more energy- and material-intensive treatment alternative
compared to impoundments. However, they also provide a means of significantly reducing, or potentially eliminating,
leachate-related emissions, which are critical environmental burden of tailings disposal. The modeled transition
thus captures the trade-offs between higher resource consumption and the mitigation of long-term pollution risks.
The assumed reduction in impoundments reflects broader trends in the industry toward more sustainable and circular
tailings management practices, supported by technological innovation and emerging environmental regulation [5].

+--------------+-------------------+-------------------+--------------------+--------------------+---------------------+----------------------------+----------------------------+-----------------------------+-----------------------------+---------------------+
| Region       | Backfilling 2020  | Backfilling 2050  | Impoundment 2020   | Impoundment 2050   | Ref. (BF/Imp)       | Floc-Flotation 2020        | Floc-Flotation 2050        | Roasting & Leaching 2020    | Roasting & Leaching 2050    | Ref. (Floc/R&L)      |
+==============+===================+===================+====================+====================+=====================+============================+============================+=============================+=============================+=====================+
| North America| 15%               | 30%               | 80%                | 60%                | [6], [7]            | 4%                         | 8%                         | 1%                          | 2%                          | [1], [8], [9], [10]  |
+--------------+-------------------+-------------------+--------------------+--------------------+---------------------+----------------------------+----------------------------+-----------------------------+-----------------------------+---------------------+
| LATAM        | 5%                | 25%               | 90%                | 65%                | [7], [11]           | 4%                         | 8%                         | 1%                          | 2%                          | [1], [8], [9], [10]  |
+--------------+-------------------+-------------------+--------------------+--------------------+---------------------+----------------------------+----------------------------+-----------------------------+-----------------------------+---------------------+
| Europe       | 15%               | 35%               | 80%                | 55%                | [1], [12]           | 4%                         | 8%                         | 1%                          | 2%                          | [1], [8], [9], [10]  |
+--------------+-------------------+-------------------+--------------------+--------------------+---------------------+----------------------------+----------------------------+-----------------------------+-----------------------------+---------------------+
| APAC         | 10%               | 20%               | 85%                | 70%                | [13], [14]          | 4%                         | 8%                         | 1%                          | 2%                          | [1], [8], [9], [10]  |
+--------------+-------------------+-------------------+--------------------+--------------------+---------------------+----------------------------+----------------------------+-----------------------------+-----------------------------+---------------------+
| Africa       | 5%                | 10%               | 90%                | 70%                | [8], [15], [16]     | 4%                         | 6%                         | 1%                          | 2%                          | [1], [8], [9], [10]  |
+--------------+-------------------+-------------------+--------------------+--------------------+---------------------+----------------------------+----------------------------+-----------------------------+-----------------------------+---------------------+
| Global       | 10%               | 25%               | 85%                | 65%                | [7], [8], [16]      | 4%                         | 6%                         | 1%                          | 2%                          | [1], [8], [9], [10]  |
+--------------+-------------------+-------------------+--------------------+--------------------+---------------------+----------------------------+----------------------------+-----------------------------+-----------------------------+---------------------+

.. [1] https://doi.org/10.1016/j.scitotenv.2023.162038
.. [2] https://doi.org/10.1016/j.jclepro.2017.03.129
.. [3] http://dx.doi.org/10.1016/j.jclepro.2008.08.014
.. [4] https://doi.org/10.1016/j.resconrec.2022.106567
.. [5] https://doi.org/10.1016/j.clet.2022.100499
.. [6] https://www.hecla.com/wp-content/uploads/Hecla-2021-TailingsMgmt_FactSheet.pdf
.. [7] https://globaltailingsreview.org/wp-content/uploads/2020/08/global-industry-standard-on-tailings-management.pdf
.. [8] https://globaltailingsreview.org/wpcontent/uploads/2020/09
.. [9] https://www.nature.com/articles/s41598-021-84897-0
.. [10] http://dx.doi.org/10.1016/j.jclepro.2019.01.312
.. [11] https://doi.org/10.3390/su141710901
.. [12] https://ec.europa.eu/environment/pdf/waste/mining/MWEI%20BREF.pdf.
.. [13] https://doi.org/10.3390/rs13040743
.. [14] https://doi.org/10.3389/feart.2021.726336
.. [15] https://sancold.org.za/wp-content/uploads/2024/05/SANCOLD-Your-Tailings-Dam-2020.pdf
.. [16] https://globaltailingsreview.org/wp-content/uploads/2020/09/Ch-II-Mine-Tailings-Facilities_Overview-and-Industry-Trends.pdf

Inventories
-----------

*premise* provides several inventories regarding updated mining practices:

* `Alternative treatment pathways for sulfidic tailings
 <https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-sulfidic-tailings.xlsx>`_.

The inventories are provided under `premise/data/additional_inventories <https://github.com/polca/premise/tree/master/premise/data/additional_inventories>`_

Biomass
+++++++

Run

.. code-block:: python

    from premise import *
    import brightway2 as bw

    bw.projects.set_current("my_project)

    ndb = NewDatabase(
        scenarios=[
                {"model":"remind", "pathway":"SSP2-Base", "year":2028}
            ],
        source_db="ecoinvent 3.7 cutoff",
        source_version="3.7.1",
        key='xxxxxxxxxxxxxxxxxxxxxxxxx'
    )
    ndb.update("biomass")


Regional biomass markets
------------------------

*premise* creates regional markets for biomass which is meant to be used as fuel
in biomass-fired powerplants or heat generators. Originally in ecoinvent, the biomass being supplied
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

* market for wood chips (for "purpose grown" biomass)
* market for wood chips (for "purpose grown" woody biomass)
* supply of forest residue (for "residual" biomass)

The sum of those shares equal 1. The activity "supply of forest residue" includes
the energy, embodied biogenic CO2, transport and associated emissions to chip the residual biomass
and transport it to the powerplant, but no other forestry-related burden is included.

.. note::

    You can check the share of residual biomass used for power generation
    assumed in your scenarios by generating a scenario summary report.

.. note::

    When running *premise* with the consequential method, the biomass market
    is only composed of purpose-grown biomass. This is because the residual biomass
    cannot be considered a marginal supplier for an increase in demand for biomass.


.. code-block:: python

    ndb.generate_scenario_report()


Power generation
++++++++++++++++

Run

.. code-block:: python

    from premise import *
    import brightway2 as bw

    bw.projects.set_current("my_project)

    ndb = NewDatabase(
        scenarios=[
                {"model":"remind", "pathway":"SSP2-Base", "year":2028}
            ],
        source_db="ecoinvent 3.7 cutoff",
        source_version="3.7.1",
        key='xxxxxxxxxxxxxxxxxxxxxxxxx',
        use_absolute_efficiency=False # default
    )
    ndb.update("electricity")


Efficiency adjustment
---------------------

The energy conversion efficiency of power plant datasets for specific technologies is adjusted
to align with the efficiency changes indicated by the IAM scenario.

Two approaches are possible (`use_absolute_efficiency`):

* application of a scaling factor to the inputs of the dataset relative to the current efficiency
* application of a scaling factor to the inputs of the dataset to match the absolute efficiency given by the IAM scenario

The first approach (default) preserves the relative share of inputs in the dataset, as reported in ecoinvent,
while the second approach adjusts the inputs to match the absolute efficiency given by the IAM scenario.


Combustion-based powerplants
----------------------------

First, *premise* adjust the efficiency of coal- and lignite-fired
power plants on the basis of the excellent work done by Oberschelp_ et al. (2019),
to update some datasets in ecoinvent, which are, for some of them, several decades
old. More specifically, the data provides plant-specific efficiency
and emissions factors. We average them by country and fuel type to obtain
volume-weighted factors. The efficiency of the following datasets is updated:

* electricity production, hard coal
* electricity production, lignite
* heat and power co-generation, hard coal
* heat and power co-generation, lignite

The data from Oberschelp_ et al. (2019) also allows us to update emissions of
SO2, NOx, CH4, and PMs.

.. _Oberschelp: https://www.nature.com/articles/s41893-019-0221-6

Second, *premise* iterates through coal, lignite, natural gas, biogas, and wood-fired power plant datasets
in the LCI database to calculate their current efficiency (i.e., the ratio between the primary fuel
energy entering the process and the output energy produced, which is often 1 kWh).
If the IAM scenario anticipates a change in efficiency for these processes, the inputs of the
datasets are scaled up or down by the scaling factor to effectively reflect a change in
fuel input per kWh produced.

The origin of this scaling factor is the IAM scenario selected.

To calculate the old and new efficiency of the dataset, it is necessary to know
the net calorific content of the fuel. The table below shows the Lower Heating Value for
the different fuels used in combustion-based power plants.

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
  hydrogen, from natural gas                                                 120
  hydrogen, from natural gas, with CCS                                       120
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

Additionally, the biogenic and fossil CO2 emissions of the datasets are also
scaled up or down by the same factor, as they are proportional to the amount of fuel used.

Below is an example of a natural gas power plant with a current (2020) conversion efficiency
of 77%. If the IAM scenario indicates a scaling factor of 1.03 in 2030, this suggests
that the efficiency increases by 3% relative to the current level. As shown in the table below,
this would result in a new efficiency of 79%, where all inputs, as well as CO2
emissions outputs, are re-scaled by 1/1.03 (=0.97).

While non-CO2 emissions (e.g., CO) are reduced because of the reduction in fuel consumption,
the emission factor per energy unit remains the same (i.e., gCO/MJ natural gas)).
It can be re-scaled using the `.update("emissions")` function, which updates emission factors according
to GAINS projections.


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
* efficiency can only improve over time.

This is to prevent degrading the performance of a technology in the future, or
improving its performance in the past, relative to today.

.. note::

    You can check the efficiencies assumed in your scenarios by generating
    a scenario summary report, or a report of changes. They are automatically
    generated after each database export, but you can also generate them manually:

.. code-block:: python

    ndb.generate_scenario_report()
    ndb.generate_change_report()

Photovoltaics panels
--------------------

Photovoltaic panels are expected to improve over time. The following module efficiencies (mena, minimum, maximum)
are considered for the different types of PV panels, applied as a triangular distribution on the panel surface
required to reach the peak power output of the dataset:

===================== ==================== ==================== =================== ================== ================== ================= ================== ================== =========================================
  module efficiency      micro-Si            single-Si           multi-Si            CIGS               CIS                CdTe              GaAs               perovskite         Source
===================== ==================== ==================== =================== ================== ================== ================= ================== ================== =========================================
  2010                   10.0 (7.5-12.5)     15.0 (11.3-18.9)    14.0 (10.5-17.5)    11.0 (8.3-13.8)    11.0 (8.3-13.8)    10.0 (8.8-12.0)   28.0 (21.0-35.0)   25.0 (19.0-31.0)   [1], [2], [3], [4], [5], [6], [7], [8]
  2020                   11.9 (9.0-15.0)     17.9 (13.0-22.0)    16.8 (12.0-21.0)    14.0 (10.5-18.0)   14.0 (10.5-18.0)   16.8 (13.0-21.0)  28.0 (21.0-35.0)   25.0 (19.0-31.0)   [1], [2], [3], [4], [5], [6], [7], [8]
  2023                   -                   22.0 (17.0-24.0)    -                   15.0 (11.3-19.0)   -                  19.0 (15.0-20.0)  -                  -                  [2], [4], [6]
  2050                   13.0 (9.0-16.0)     27.0 (20.0-34.0)    24.0 (16.0-30.0)    23.0 (17.3-29.0)   23.0 (17.3-29.0)   22.6 (22.0-25.0)  28.0 (25.0-28.0)   25.2 (22.0-31.3)   [1], [2], [3], [4], [5], [6], [7], [8]
===================== ==================== ==================== =================== ================== ================== ================= ================== ================== =========================================

.. [1] https://www.ise.fraunhofer.de/content/dam/ise/de/documents/publications/studies/Photovoltaics-Report.pdf
.. [2] https://www.ise.fraunhofer.de/content/dam/ise/de/documents/publications/studies/Photovoltaics-Report.pdf
.. [3] https://www.ise.fraunhofer.de/content/dam/ise/de/documents/publications/studies/Photovoltaics-Report.pdf
.. [4] https://www.ise.fraunhofer.de/content/dam/ise/de/documents/publications/studies/Photovoltaics-Report.pdf. For future efficiency: own assumption, -+25%.
.. [5] Future eff: Fraunhofer ISE Photovoltaics Report 2019; Uncertainty: Own assumption: -+25%.
.. [6] https://www.sciencedirect.com/science/article/pii/S0927024823001101
.. [7] https://link.springer.com/article/10.1007/s11367-020-01791-z
.. [8] https://pubs.rsc.org/en/content/articlelanding/2022/se/d2se00096b; https://www.csem.ch/en/news/photovoltaic-technology-breakthrough-achieving-31.25-efficiency/


The sources for these efficiencies are also given in the inventory file LCI_PV_:

.. _LCI_PV: https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-PV.xlsx

And the efficiency values are stored in the file premise/data/renewables/efficiency_solar_PV.csv.

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
-------

*premise* creates additional datasets that represent the average supply and
production pathway for a given commodity for a given scenario, year and region.

Such datasets are called *regional markets*. Hence, a regional market for high voltage
electricity contains the different technologies that supply electricity at high voltage
in a given IAM region, in proportion to their respective production volumes.


Regional electricity markets
----------------------------

High voltage regional markets
-----------------------------

*premise* creates high, medium and low-voltage electricity markets for each IAM region.
It starts by creating high-voltage markets and define the share of each supplying technology
by their respective production volumes in respect to the total volume produced.

High voltage supplying technologies are all technologies besides:

* residential (<=3kWp) photovoltaic power (low voltage)
* waste incineration co-generating powerplants (medium voltage)

Several datasets can qualify for a given technology, in a given IAM region.
To define to which extent a given dataset should be supplying in the market,
*premise* uses the current production volume of the dataset.

For example, if coal-fired powerplants are to supply 25% of the high voltage
electricity in the IAM region "Europe", *premise* fetches the production volumes
of all coal-fired powerplants which ecoinvent location is *included* in the
IAM region "Europe" (e.g., DE, PL, LT, etc.), and allocates to each of those
a supply share based on their respective production volume in respect to the
total production volume of coal-fired powerplants.

For example, the table below shows the contribution of biomass-fired CHP powerplants
in the regional high voltage electricity market for IMAGE's "WEU" region
(Western Europe). The biomass CHP technology represents 2.46% of the supply mix.
Biomass CHP datasets included in the region "WEU" are given a supply share
corresponding to their respective current production volumes.


 ============== =========================================== ==================== ================================== =====================
  energy type    Supplier name                               Supplier location    Contribution within energy type    Final contribution
 ============== =========================================== ==================== ================================== =====================
  Biomass CHP    heat and power co-generation, wood chips    FR                   3.80%                              0.09%
  Biomass CHP    heat and power co-generation, wood chips    AT                   2.87%                              0.07%
  Biomass CHP    heat and power co-generation, wood chips    NO                   0.06%                              0.00%
  Biomass CHP    heat and power co-generation, wood chips    FI                   7.65%                              0.19%
  Biomass CHP    heat and power co-generation, wood chips    SE                   9.04%                              0.22%
  Biomass CHP    heat and power co-generation, wood chips    IT                   8.27%                              0.20%
  Biomass CHP    heat and power co-generation, wood chips    BE                   4.59%                              0.11%
  Biomass CHP    heat and power co-generation, wood chips    DE                   12.53%                             0.31%
  Biomass CHP    heat and power co-generation, wood chips    LU                   0.05%                              0.00%
  Biomass CHP    heat and power co-generation, wood chips    DK                   6.60%                              0.16%
  Biomass CHP    heat and power co-generation, wood chips    GR                   0.01%                              0.00%
  Biomass CHP    heat and power co-generation, wood chips    CH                   1.81%                              0.04%
  Biomass CHP    heat and power co-generation, wood chips    ES                   5.10%                              0.13%
  Biomass CHP    heat and power co-generation, wood chips    PT                   1.34%                              0.03%
  Biomass CHP    heat and power co-generation, wood chips    IE                   0.77%                              0.02%
  Biomass CHP    heat and power co-generation, wood chips    NL                   2.32%                              0.06%
  Biomass CHP    heat and power co-generation, wood chips    GB                   33.18%                             0.81%
  _              _                                           Sum                  100.00%                            2.46%
 ============== =========================================== ==================== ================================== =====================


Transformation losses are added to the high-voltage market datasets.
Transformation losses are the result of weighting country-specific
high voltage losses (provided by ecoinvent) of countries included in the
IAM region with their respective current production volumes (also provided by
ecoinvent). This is not ideal as it supposes that future country-specific
production volumes will remain the same in respect to one another.

High voltage regional markets for aluminium smelters
----------------------------------------------------

Aluminium production is a significant consumer of electricity.
In the ecoinvent database, aluminium smelters are represented by
specific electricity markets. Conversely, Integrated Assessment Models
(IAM) scenarios aggregate the electricity consumption of aluminium
smelters with that of other electricity consumers.

To improve accuracy, it is necessary to align the electricity markets
of aluminium producers with regional electricity markets. However,
certain aluminium electricity markets have already achieved substantial
decarbonization, primarily due to the use of hydroelectric power in
some smelters.

Therefore, premise integrates aluminium smelters into regional electricity
markets only for those regions that have not yet undergone significant
decarbonization. The regions affected are:

* Rest of World (RoW)
* IAI Area, Africa
* China (CN)
* IAI Area, South America
* United Nations Oceania (UN-OCEANIA)
* IAI Area, Asia excluding China and Gulf Cooperation Council (GCC)
* IAI Area, Gulf Cooperation Council (GCC)

Meanwhile, *premise* maintains the current decarbonized electricity markets
for aluminium smelters in the following regions:

* IAI Area, Russia & Rest of Europe excluding EU27 & EFTA
* Canada (CA)
* IAI Area, EU27 & EFTA

Although the future development of aluminium-specific electricity markets
remains uncertain, it is reasonable to hypothesize that these markets
will follow the decarbonization trends of their respective regions.
Consequently, aligning the carbon-intensive electricity markets of
aluminium smelters with regional electricity markets is likely more
accurate than retaining the current setup.

In fact, such approach has been used by the International Aluminium Industry
association itself, in their Aluminium Sector Greenhouse Gas Pathways to 2050 Roadmap_, where
they connected the electricity consumption of aluminium smelters to future
regional mixes defined by the International Energy Agency (IEA).

.. _IAI Beyond 2 Degrees Aluminium Roadmap: https://international-aluminium.org/resource/aluminium-sector-greenhouse-gas-pathways-to-2050-2021/


Storage
-------

If the IAM scenario requires the use of storage, *premise* adds a storage
dataset to the high voltage market. *premise* can add two types of storage:

* storage via a large-scale flow battery (electricity supply, high voltage, from vanadium-redox flow battery system)
* storage via the conversion of electricity to hydrogen and subsequent use in a gas turbine (electricity production, from hydrogen-fired one gigawatt gas turbine)

The electricity storage via battery incurs a 33% loss. It is operated by a 8.3 MWh vanadium redox-based flow battery,
with a lifetime of 20 years or 8176 cycle-lifes (i.e., 49,000 MWh).

The storage of electricity via hydrogen is done in two steps: first, the electricity is converted to hydrogen
via a 1MW PEM electrolyser, with an efficiency of 62%. The hydrogen is then stored in a geological cavity
and used in a gas turbine, with an efficiency of 51%. Accounting for leakages and losses, the
overall efficiency of the process is about 37% (i.e., 2.7 kWh necessary to deliver 1 kWh to the grid).

The efficiency of the H2-fed gas turbine is based on the parameters of Ozawa_ et al. (2019).

.. _Ozawa: https://doi.org/10.1016/j.ijhydene.2019.02.230


Medium voltage regional markets
-------------------------------

The workflow is not too different from that of high voltage markets.
There are however only two possible providers of electricity in medium
voltage markets: the high voltage market, as well as waste incineration
powerplants.

High-to-medium transformation losses are added as an input of the medium voltage
market to itself. Distribution losses are modelled the same way as for
high voltage markets and are added to the input from high voltage market.

Low voltage regional markets
----------------------------

Low voltage regional markets receive an input from the medium voltage
market, as well as from residential photovoltaic power.

Medium-to-low transformation losses are added as an input from the low voltage
market to itself. Distribution losses are modelled the same way as
for high and medium voltage markets, and are added to the input
from the medium voltage market.

The table below shows the example of a low voltage market for the IAM IMAGE
regional "WEU".

 ============================================================== ============== ================ =========== ==================================================
  supplier                                                       amount         unit             location    description
 ============================================================== ============== ================ =========== ==================================================
  market group for electricity, medium voltage                   1.023880481    kilowatt hour    WEU         input from medium voltage + distribution losses
  market group for electricity, low voltage                      0.025538286    kilowatt hour    WEU         transformation losses (2.55%)
  electricity production, photovoltaic, residential              0.00035691     kilowatt hour    DE
  electricity production, photovoltaic, residential              0.000143875    kilowatt hour    IT
  electricity production, photovoltaic, residential              9.38E-05       kilowatt hour    ES
  electricity production, photovoltaic, residential              9.03E-05       kilowatt hour    GB
  electricity production, photovoltaic, residential              7.82E-05       kilowatt hour    FR
  electricity production, photovoltaic, residential              6.80E-05       kilowatt hour    NL
  electricity production, photovoltaic, residential              3.76E-05       kilowatt hour    BE
  electricity production, photovoltaic, residential              2.16E-05       kilowatt hour    GR
  electricity production, photovoltaic, residential              2.08E-05       kilowatt hour    CH
  electricity production, photovoltaic, residential              1.48E-05       kilowatt hour    AT
  electricity production, photovoltaic, residential              9.44E-06       kilowatt hour    SE
  electricity production, photovoltaic, residential              8.66E-06       kilowatt hour    DK
  electricity production, photovoltaic, residential              6.83E-06       kilowatt hour    PT
  electricity production, photovoltaic, residential              2.60E-06       kilowatt hour    FI
  electricity production, photovoltaic, residential              1.30E-06       kilowatt hour    LU
  electricity production, photovoltaic, residential              1.01E-06       kilowatt hour    NO
  electricity production, photovoltaic, residential              2.40E-07       kilowatt hour    IE
  distribution network construction, electricity, low voltage    8.74E-08       kilometer        RoW
  market for sulfur hexafluoride, liquid                         2.99E-09       kilogram         RoW
  sulfur hexafluoride                                            2.99E-09       kilogram                     transformer emissions
 ============================================================== ============== ================ =========== ==================================================

.. note::

    You can check the electricity supply mixes assumed
    in your scenarios by generating a scenario summary report.

.. code-block:: python

    ndb.generate_scenario_report()

Long-term regional electricity markets
--------------------------------------

Long-term (i.e., 20, 40 and 60 years) regional markets are created
for modelling the lifetime-weighted burden associated to electricity supply
for systems that have a long lifetime (e.g., battery electric vehicles, buildings).

These long-term markets contain a period-weighted electricity supply
mix. For example, if the scenario year is 2030 and the period considered
is 20 years, the supply mix represents the supply mixes between 2030 and 2050,
with an equal weight given to each year.

The rest of the modelling is similar to that of regular regional electricity
markets described above.

Original market datasets
------------------------

Market datasets originally present in the ecoinvent LCI database are cleared
from any inputs. Instead, an input from the newly created regional market
is added, depending on the location of the dataset.

The table below shows the example of the low voltage electricity market
for Great Britain, which now only includes an input from the "WEU"
regional market, which "includes" it in terms of geography.


 ============================================ =========== ================ ===========
  Output                                       _           _                _
 ============================================ =========== ================ ===========
  *producer*                                   *amount*    *unit*           *location*
  market for electricity, low voltage          1.00E+00    kilowatt hour    **GB**
  **Input**                                    _           _                _
  *supplier*                                   *amount*    *unit*           *location*
  market group for electricity, low voltage    1.00E+00    kilowatt hour    **WEU**
 ============================================ =========== ================ ===========

Relinking
---------

Once the new markets are created, *premise* re-links all electricity-consuming
activities to the new regional markets. The regional market it re-links to
depends on the location of the consumer.

Cement production
+++++++++++++++++

The modelling of future improvements in the cement sector is dependent on the IAM model chosen.

When choosing IMAGE, scenarios include the emergence of a new, more efficient kiln, as well
as kilns fitted with three types of carbon capture technologies:

* using monoethanolamine (MEA) as a solvent,
* using oxyfuel combustion,
* using Direct Separation (Leilac process).

The implementation of the corresponding datasets for these new kiln technologies are based on the work of
Muller_ et al., 2024.

.. _Muller: https://doi.org/10.1016/j.jclepro.2024.141884

We differ slightly from the implementation of Muller_ et al., 2024, in that:

* the heat necessary for the regeneration of the MEA solvent is assumed to be provided by a natural gas boiler (instead of a fuel mix resembling that of the kiln itself), with up to 30% coming from recovered heat from the kiln by 2050,
* the amount of heat needed for the regeneration of the MEA solvent goes from 3.76 GJ/ton CO2 in 2020, to 2.6 GJ/ton CO2 in 2050,
* the provision of oxygen for the Direct Separation option comes from an existing air separation dataset from ecoinvent,
* the fuel mix for the kiln is that of ecoinvent, further scaled down by the change of efficiency of the kiln (in Müller et al., 2024, they use directly the fuel mix provided by the IMAGE scenario, which we do not find representative, as it also includes the fuel used by other activities in the non-metallic minerals, notably a large share of natural gas).

In a nutshell, *premise*:

* makes copies of the `clinker production` dataset,
* adjusts the fuel consumption and related CO2 emissions,
* adjusts specific hot pollutant emissions removed by the carbon capture process (Mercury, NOx, SOx),
* adds an input from the carbon capture process, based on a capture efficiency share,
* and removes a corresponding amount from the outgoing CO2 emissions.

The Direct Separation process only captures calcination emissions, while the other two technologies capture
both combustion and calcination emissions.

When choosing another IAM (e.g., REMIND, TIAM-UCL), the current implementation is relatively
simpler at the moment, and does not involve the emergence of new
technologies. In these scenarios, the production volumes of kilns
equipped with CCS is not given. Instead, the share of CO2 emissions
that is sequestered is given. We use the ratio of the CO2 emissions
sequestered over the total CO2 emissions to determine the share of
the CO2 emissions that is sequestered in the clinker production dataset

Run

.. code-block:: python

    from premise import *
    import brightway2 as bw

    bw.projects.set_current("my_project)

    ndb = NewDatabase(
        scenarios=[
                {"model":"remind", "pathway":"SSP2-Base", "year":2028}
            ],
        source_db="ecoinvent 3.7 cutoff",
        source_version="3.7.1",
        key='xxxxxxxxxxxxxxxxxxxxxxxxx'
    )
    ndb.update("cement")

Dataset proxies
---------------

*premise* duplicates clinker production datasets in ecoinvent (called
"clinker production") so as to create a proxy dataset for each IAM region.
The location of the proxy datasets used for a given IAM region is a location
included in the IAM region. If no valid dataset is found, *premise* resorts
to using a rest-of-the-world (RoW) dataset to represent the IAM region.

*premise* changes the location of these duplicated datasets and fill
in different fields, such as that of *production volume*.


Efficiency adjustment
---------------------

*premise* then adjusts the thermal efficiency of the process.

It first calculates the energy input in teh current (original) dataset,
by looking up the fuel inputs and their respective lower heating values.

Once the energy required per ton clinker today (2020) is known, it is
multiplied by a *scaling factor* that represents a change in efficiency
between today and the scenario year.

.. note::

    You can check the efficiency gains assumed relative to 2020
    in your scenarios by generating a scenario summary report.

.. code-block:: python

    ndb.generate_scenario_report()


.. note::

    *premise* enforces a lower limit on the fuel consumption per ton of clinker.
    This limit is set to 3.1 GJ/t clinker and is close to the minimum
    theoretical fuel consumption with an moisture content of the raw materials,
    as considered in the 2018 IEA_ cement roadmap report (i.e., 2.8 GJ/t clinker).
    Hence, regardless of the scaling factor, the fuel consumption per ton of clinker
    will never be less than 3.1 GJ/t.

.. _IEA: https://iea.blob.core.windows.net/assets/cbaa3da1-fd61-4c2a-8719-31538f59b54f/TechnologyRoadmapLowCarbonTransitionintheCementIndustry.pdf



Once the new energy input is determined, *premise* scales down the fuel,
and the fossil and biogenic CO2 emissions accordingly, based on the Lower Heating Value
and CO2 emission factors for these fuels.

Note that the change in CO2 emissions only concerns the share
that originates from the combustion of fuels. It does not
concern the calcination emissions due to the production of
calcium oxide (CaO) from calcium carbonate (CaCO3), which is set
at a fix emission rate of 525 kg CO2/t clinker.


Carbon Capture and Storage
--------------------------

If the IAM scenario indicates that a share of the CO2 emissions
for the cement sector in a given region and year is sequestered and stored,
*premise* adds CCS to the corresponding clinker production dataset.

The CCS dataset used to that effect is from Muller_ et al., 2024.
The dataset described the capture of CO2 from a cement plant,
using a monoethanolamine-based sorbent.
To that dataset, *premise* adds another dataset that models the storage
of the CO2 underground, from Volkart_ et al, 2013.


Besides electricity, the CCS process requires heat, water and others inputs
to regenerate the amine-based sorbent. We use two data points to approximate the heat
requirement: 3.76 MJ/kg CO2 captured in 2020 (minus 30% coming from the kiln as recovered heat),
and 2.6 MJ/kg in 2050. The first number is from Muller_ et al., 2024, while the second number is described
as the best-performing pilot project today, according to the 2022 review of pilot
projects by the Global CCS Institute_. It is further assumed that the heat requirement
is fulfilled to an extent of 30% by the recovery of excess heat, as found in numerous studies.

.. _Volkart: https://doi.org/10.1016/j.ijggc.2013.03.003
.. _Institute: https://www.globalccsinstitute.com/wp-content/uploads/2022/05/State-of-the-Art-CCS-Technologies-2022.pdf


.. note::

    You can check the the carbon capture rate for cement production assumed
    in your scenarios by generating a scenario summary report.

.. code-block:: python

    ndb.generate_scenario_report()

Cement markets
--------------

Run

.. code-block:: python

    from premise import *
    import brightway2 as bw

    bw.projects.set_current("my_project)

    ndb = NewDatabase(
        scenarios=[
                {"model":"remind", "pathway":"SSP2-Base", "year":2028}
            ],
        source_db="ecoinvent 3.7 cutoff",
        source_version="3.7.1",
        key='xxxxxxxxxxxxxxxxxxxxxxxxx'
    )
    ndb.update("cement")



When clinker production datasets are created for each IAM region,
*premise* duplicates cement production datasets for each IAM region
as well. These cement production datasets link the newly created
clinker production dataset, corresponding to their IAM region.

Clinker-to-cement ratio
-----------------------

*premise* used to modify the composition of cement markets to reflect
a lower clinker content over time, based on external projections. This is
no longer performed, as it is not an assumption stemming from the IAM model,
but rather a projection of the cement industry.

Original market datasets
------------------------

Market datasets originally present in the ecoinvent LCI database are cleared
from any inputs. Instead, an input from the newly created regional market
is added, depending on the location of the dataset.

The table below shows the example of the clinker market
for South Africa, which now only includes an input from the "SAF"
regional market, which "includes" it in terms of geography.


 ============================================ =========== ================ ===========
  Output                                       _           _                _
 ============================================ =========== ================ ===========
  producer                                     amount      unit             location
  market for clinker                           1.00E+00    kilogram         **ZA**
  Input                                        _           _                _
  supplier                                     amount      unit             location
  market for clinker                           1.00E+00    kilogram         ***SAF**
 ============================================ =========== ================ ===========


Relinking
---------

Once cement production and market datasets are created, *premise*
re-links cement-consuming activities to the new regional markets for
cement. The regional market it re-links to depends on the location
of the consumer.

Steel production
++++++++++++++++

Run

.. code-block:: python

    from premise import *
    import brightway2 as bw

    bw.projects.set_current("my_project)

    ndb = NewDatabase(
        scenarios=[
                {"model":"remind", "pathway":"SSP2-Base", "year":2028}
            ],
        source_db="ecoinvent 3.7 cutoff",
        source_version="3.7.1",
        key='xxxxxxxxxxxxxxxxxxxxxxxxx'
    )
    ndb.update("steel")"



The modelling of future improvements in the steel sector is relatively
simple at the moment, and does not involve the emergence of new
technologies (e.g., hydrogen-based DRI, electro-winning).

Dataset proxies
---------------

*premise* duplicates steel production datasets in ecoinvent for the
production of primary and secondary steel (called respectively
"steel production, converter" and "steel production, electric")
so as to create a proxy dataset for each IAM region.

The location of the proxy datasets used for a given IAM region is a location
included in the IAM region. If no valid dataset is found, *premise* resorts
to using a rest-of-the-world (RoW) dataset to represent the IAM region.

*premise* changes the location of these duplicated datasets and fill
in different fields, such as that of *production volume*.

Efficiency adjustment
---------------------

Regarding primary steel production (using BO-BOF), *premise* adjusts
the inputs of fuels found in:

* the pig iron production datasets,
* the steel production datasets,

assuming an integrated steel mill unit, by multiplying these fuel
inputs by a *scaling factor* provided by the IAM scenario.

Typical fuel inputs for these process are natural gas, coal, coal-based coke.
Emissions of (fossil) CO2 are scaled accordingly.

Regarding the production of secondary steel (using EAF),
*premise* adjusts the input of electricity based on the scaling factor
provided by the IAM scenario.


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
--------------------------

If the IAM scenario indicates that a share of the CO2 emissions
from the steel sector in a given region and year is sequestered and stored,
*premise* adds a corresponding input from a CCS dataset.
The datatset used to that effect is from Meunier_ et al., 2020.
The dataset described the capture of CO2 from a cement plant, not a steel mill,
but it is assumed to be an acceptable approximation since the CO2 concentration
in the flue gases should not be significantly different.

To that dataset, *premise* adds another dataset that models the storage
of the CO2 underground, from Volkart_ et al, 2013.

Besides electricity, the CCS process requires heat, water and others inputs
to regenerate the amine-based sorbent. We use two data points to approximate the heat
requirement: 3.66 MJ/kg CO2 captured in 2020, and 2.6 MJ/kg in 2050.
The first number is from Meunier_ et al., 2020, while the second number is described
as the best-performing pilot project today, according to the 2022 review of pilot
projects by the Global CCS Institute_. It is further assumed that the heat requirement
is fulfilled to an extent of 15% by the recovery of excess heat, as mentioned in
the 2018 IEA_ cement roadmap report, which is assumed to be also valid in the
case of a steel mill.


Steel markets
-------------

*premise* create a dataset "market for steel, low-alloyed" for each IAM region.
Within each dataset, the supply shares of primary and secondary steel
are adjusted to reflect the projections from the IAM scenario, for a given region
and year, based on the variables described in the steel_ mapping file.

.. _steel: https://github.com/polca/premise/blob/master/premise/data/battery/scenario.csv

The table below shows an example of the market for India, where 66% of the steel comes
from an oxygen converter process (primary steel), while 34% comes from an electric arc
furnace process (secondary steel).

 ================================================================= ============ ================ ===========
  Output                                                            _            _                _
 ================================================================= ============ ================ ===========
  producer                                                          amount       unit             location
  market for steel, low-alloyed                                     1            kilogram         IND
  Input
  supplier                                                          amount       unit             location
  market group for transport, freight, inland waterways, barge      0.5          ton kilometer    GLO
  market group for transport, freight train                         0.35         ton kilometer    GLO
  market for transport, freight, sea, bulk carrier for dry goods    0.38         ton kilometer    GLO
  transport, freight, lorry, unspecified, regional delivery         0.12         ton kilometer    IND
  steel production, **converter**, low-alloyed                      0.66         kilogram         IND
  steel production, **electric**, low-alloyed                       0.34         kilogram         IND
 ================================================================= ============ ================ ===========


Original market datasets
------------------------

Market datasets originally present in the ecoinvent LCI database are cleared
from any inputs. Instead, an input from the newly created regional market
is added, depending on the location of the dataset.

The table below shows the example of the clinker market
for South Africa, which now only includes an input from the "SAF"
regional market, which "includes" it in terms of geography.


 ============================================ =========== ================ ===========
  Output                                       _           _                _
 ============================================ =========== ================ ===========
  producer                                     amount      unit             location
  market for clinker                           1.00E+00    kilogram         **ZA**
============================================ =========== ================ ============
  Input                                        _           _                _
============================================ =========== ================ ============
  supplier                                     amount      unit             location
  market for clinker                           1.00E+00    kilogram         **SAF**
 ============================================ =========== ================ ===========


Relinking
---------

Once steel production and market datasets are created, *premise*
re-links steel-consuming activities to the new regional markets for
steel. The regional market it re-links to depends on the location
of the consumer.

Transport
+++++++++

Run

.. code-block:: python

    from premise import *
    import brightway2 as bw

    bw.projects.set_current("my_project)

    ndb = NewDatabase(
        scenarios=[
                {"model":"remind", "pathway":"SSP2-Base", "year":2028}
            ],
        source_db="ecoinvent 3.7 cutoff",
        source_version="3.7.1",
        key='xxxxxxxxxxxxxxxxxxxxxxxxx'
    )
    ndb.update("two_wheelers")
    ndb.update("cars")
    ndb.update("trucks")
    ndb.update("buses")
    ndb.update("trains")


*premise* imports inventories for transport activity operated by:

* two-wheelers
* passenger cars
* medium and heavy duty trucks
* buses
* trains

Inventories are available for current vehicles. Future vehicle inventories
are obtained by scaling down the current inventories based on the
vehicle efficiency improvements projected by the IAM scenario.

Trucks
------

The following size classes of medium and heavy duty trucks are imported:

- 3.5t
- 7.5t
- 18t
- 26t
- 40t

These weights refer to the vehicle gross mass (the maximum weight the vehicle is
allowed to reach, fully loaded).

Each truck is available for a variety of powertrain types:

- fuel cell electric
- battery electric
- diesel hybrid
- plugin diesel hybrid
- diesel
- compressed gas

but also for different driving cycles, to which a range autonomy
of the vehicle is associated:

- urban delivery (required range autonomy of 150 km)
- regional delivery (required range autonomy of 400 km)
- long haul (required range autonomy of 800 km)

Those are driving cycles developed for the software VECTO_,
which have become standard in measuring the CO2 emissions of trucks.

.. _VECTO: https://ec.europa.eu/clima/eu-action/transport-emissions/road-transport-reducing-co2-emissions-vehicles/vehicle-energy-consumption-calculation-tool-vecto_en

The truck vehicle model is from Sacchi_ et al, 2021.

.. _Sacchi: https://pubs.acs.org/doi/abs/10.1021/acs.est.0c07773

.. note::

    Not all powertrain types are available for regional and long haul driving cycles.
    This is specifically the case for battery electric trucks, for which the mass
    and size prevent them from completing the cycle, or surpasses the vehicle gross weight.



Fleet average trucks
--------------------

REMIND, IMAGE and TIAM-UCL provide fleet composition data, per scenario, region and year.

The fleet data is expressed in "ton-kilometers" performed by each
type of vehicle for passenger transport, in a given region and year.

*premise* uses the fleet data to produce fleet average trucks for each
IAM region, and more specifically:

* a fleet average truck, all powertrains and size classes considered
* a fleet average truck, all powertrains considered, for a given size class

They appear in the LCI database as the following:

 ========================================================================================= =============================================================
  truck transport dataset name                                                              description
 ========================================================================================= =============================================================
  transport, freight, lorry, 3.5t gross weight, unspecified powertrain, long haul           fleet average, for 3.5t size class, long haul
  transport, freight, lorry, 7.5t gross weight, unspecified powertrain, long haul           fleet average, for 7.5t size class, long haul
  transport, freight, lorry, 18t gross weight, unspecified powertrain, long haul            fleet average, for 18t size class, long haul
  transport, freight, lorry, 26t gross weight, unspecified powertrain, long haul            fleet average, for 26t size class, long haul
  transport, freight, lorry, 40t gross weight, unspecified powertrain, long haul            fleet average, for 26t size class, long haul
  transport, freight, lorry, unspecified, long haul                                         fleet average, all powertrain types, all size classes
 ========================================================================================= =============================================================

The mapping file linking IAM variables to the truck datasets is available
here: https://github.com/polca/premise/blob/master/premise/iam_variables_mapping/transport_road_freight.yaml

Relinking
---------

Regarding trucks, *premise* re-links truck transport-consuming activities
to the newly created fleet average truck datasets.

The following table shows the correspondence between the original
truck transport datasets and the new ones replacing them:

+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| Transport Type                                            | REMIND               | IMAGE                | TIAM-UCL             |
+===========================================================+======================+======================+======================+
| transport, freight, lorry 16-32 metric ton, EURO1         | 26t gross weight     | 18t gross weight     | 18t gross weight     |
|                                                           | unspecified powertrain,| unspecified powertrain,| unspecified powertrain,|
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry 3.5-7.5 metric ton, EURO3       | 7.5t gross weight    | 18t gross weight     | 7.5t gross weight    |
|                                                           | unspecified powertrain,| unspecified powertrain,| unspecified powertrain,|
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry 16-32 metric ton, EURO5         | 26t gross weight     | 18t gross weight     | 18t gross weight     |
|                                                           | unspecified powertrain,| unspecified powertrain,| unspecified powertrain,|
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry >32 metric ton, EURO1           | 40t gross weight     | 40t gross weight     | 40t gross weight     |
|                                                           | unspecified powertrain,| unspecified powertrain,| unspecified powertrain,|
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry 3.5-7.5 metric ton, EURO4       | 7.5t gross weight    | 18t gross weight     | 7.5t gross weight    |
|                                                           | unspecified powertrain,| unspecified powertrain,| unspecified powertrain,|
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry, all sizes, EURO1 to market     | unspecified, long haul| unspecified, long haul| unspecified, long haul|
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry 7.5-16 metric ton, EURO6        | 18t gross weight     | 18t gross weight     | 18t gross weight     |
|                                                           | unspecified powertrain,| unspecified powertrain,| unspecified powertrain,|
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry 7.5-16 metric ton, EURO1        | 18t gross weight     | 18t gross weight     | 18t gross weight     |
|                                                           | unspecified powertrain,| unspecified powertrain,| unspecified powertrain,|
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry, all sizes, EURO3 to market     | unspecified, long haul| unspecified, long haul| unspecified, long haul|
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry 16-32 metric ton, EURO6         | 26t gross weight     | 18t gross weight     | 18t gross weight     |
|                                                           | unspecified powertrain,| unspecified powertrain,| unspecified powertrain,|
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry 7.5-16 metric ton, EURO2        | 18t gross weight     | 18t gross weight     | 18t gross weight     |
|                                                           | unspecified powertrain,| unspecified powertrain,| unspecified powertrain,|
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry 7.5-16 metric ton, EURO3        | 18t gross weight     | 18t gross weight     | 18t gross weight     |
|                                                           | unspecified powertrain,| unspecified powertrain,| unspecified powertrain,|
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry 7.5-16 metric ton, EURO4        | 18t gross weight     | 18t gross weight     | 18t gross weight     |
|                                                           | unspecified powertrain,| unspecified powertrain,| unspecified powertrain,|
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry 16-32 metric ton, EURO2         | 26t gross weight     | 18t gross weight     | 18t gross weight     |
|                                                           | unspecified powertrain,| unspecified powertrain,| unspecified powertrain,|
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry >32 metric ton, EURO6           | 40t gross weight     | 40t gross weight     | 40t gross weight     |
|                                                           | unspecified powertrain,| unspecified powertrain,| unspecified powertrain,|
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry 3.5-7.5 metric ton, EURO2       | 7.5t gross weight    | 18t gross weight     | 7.5t gross weight    |
|                                                           | unspecified powertrain,| unspecified powertrain,| unspecified powertrain,|
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry 3.5-7.5 metric ton, EURO1       | 7.5t gross weight    | 18t gross weight     | 7.5t gross weight    |
|                                                           | unspecified powertrain,| unspecified powertrain,| unspecified powertrain,|
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry, all sizes, EURO2 to market     | unspecified, long haul| unspecified, long haul| unspecified, long haul|
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry 16-32 metric ton, unregulated   | 26t gross weight     | 18t gross weight     | 18t gross weight     |
|                                                           | unspecified powertrain,| unspecified powertrain,| unspecified powertrain,|
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry >32 metric ton, unregulated     | 40t gross weight     | 40t gross weight     | 40t gross weight     |
|                                                           | unspecified powertrain,| unspecified powertrain,| unspecified powertrain,|
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry >32 metric ton, EURO3           | 40t gross weight     | 40t gross weight     | 40t gross weight     |
|                                                           | unspecified powertrain,| unspecified powertrain,| unspecified powertrain,|
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry 3.5-7.5 metric ton, unregulated | 7.5t gross weight    | 18t gross weight     | 7.5t gross weight    |
|                                                           | unspecified powertrain,| unspecified powertrain,| unspecified powertrain,|
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry 7.5-16 metric ton, EURO5        | 18t gross weight     | 18t gross weight     | 18t gross weight     |
|                                                           | unspecified powertrain,| unspecified powertrain,| unspecified powertrain,|
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry 3.5-7.5 metric ton, EURO6       | 7.5t gross weight    | 18t gross weight     | 7.5t gross weight    |
|                                                           | unspecified powertrain,| unspecified powertrain,| unspecified powertrain,|
|                                                           | long haul            | long haul            | long haul            |
+-----------------------------------------------------------+----------------------+----------------------+----------------------+
| transport, freight, lorry 7.5-16 metric ton, unregulated  | 18t gross weight     | 18t gross weight     | 18t gross weight     |
|                                                           | unspecified powertrain,| unspecified powertrain,| unspecified powertrain,|
|
+-----------------------------------------------------------+----------------------+----------------------+----------------------+

Direct Air Capture
++++++++++++++++++

Run

.. code-block:: python

    from premise import *
    import brightway2 as bw

    bw.projects.set_current("my_project)

    ndb = NewDatabase(
        scenarios=[
                {"model":"remind", "pathway":"SSP2-Base", "year":2028}
            ],
        source_db="ecoinvent 3.7 cutoff",
        source_version="3.7.1",
        key='xxxxxxxxxxxxxxxxxxxxxxxxx'
    )
    ndb.update("dac")



*premise* creates different region-specific Direct Air Capture (DAC)
datasets, based on the inventories from Qiu_ et al., 2022.

If provided by the IAM scenario, *premise* scales the inputs of electricity
and heat of the DAC datasets to reflect changes in efficiency.

.. _Qiu: https://doi.org/10.1038/s41467-022-31146-1

Fuels
+++++

Run

.. code-block:: python

    from premise import *
    import brightway2 as bw

    bw.projects.set_current("my_project)

    ndb = NewDatabase(
        scenarios=[
                {"model":"remind", "pathway":"SSP2-Base", "year":2028}
            ],
        source_db="ecoinvent 3.7 cutoff",
        source_version="3.7.1",
        key='xxxxxxxxxxxxxxxxxxxxxxxxx'
    )
    ndb.update("fuels")



*premise* create different region-specific fuel supply chains
and fuel markets, based on data from the IAM scenario.


Efficiency adjustment
---------------------

Biofuels
--------

The biomass-to-fuel efficiency ratio of bioethanol and biodiesel
production datasets is adjusted according to the IAM scenario projections.

Inputs to the biofuel production datasets are multiplied by a *scaling factor*
that represents the change in efficiency relative to today (2020).

Land use and land use change
----------------------------

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

Regional supply chains
----------------------

*premise* builds several supply chains for synthetic fuels, for each IAM
region. THe reason for this is that synthetic fuels can be produced from
a variety of hydrogen and CO2 sources. Additionally, hydrogen can be supplied
by different means of transport, and in different states.

Hydrogen
--------

Several pathways for hydrogen production are modeled in *premise* (see Hydrogen section under EXTRACT>Import of additional inventories).

The efficiency of hydrogen production pathways is adjusted according to the IAM scenario projections, if available.
A scaling factor is calculated for each pathway, which is the ratio
between the IAM variable value for the year in question
and the current efficiency value (i.e., in 2020). *premise*
uses this scaling factor to adjust the amount of feedstock
input to produce 1 kg of hydrogen (e.g., m3 of natural gas per kg hydrogen).

If not available, external projection data is used to adjust future efficiencies (see Hydrogen section under EXTRACT>Import of additional inventories).


Hydrogen supply chains
----------------------

*premise* starts by building different supply chains for hydrogen by varying:

* the transport mode: truck, hydrogen pipeline, re-assigned CNG pipeline, ship,
* the distance: 500 km, 2000 km
* the state of the hydrogen: gaseous, liquid, liquid organic compound,
* the hydrogen production route: electrolysis, SMR, biomass gasifier (coal, woody biomass)

Hence, for each IAM region, the following supply chains for hydrogen are built:

- hydrogen supply, from electrolysis, by ship, as liquid, over 2000 km
- hydrogen supply, from gasification of biomass by heatpipe reformer, by H2 pipeline, as gaseous, over 500 km
- hydrogen supply, from ATR of from natural gas, by truck, as gaseous, over 500 km
- hydrogen supply, from gasification of biomass by heatpipe reformer, by truck, as liquid organic compound, over 500 km
- hydrogen supply, from SMR of from natural gas, with CCS, by truck, as liquid organic compound, over 500 km
- hydrogen supply, from SMR of from natural gas, with CCS, by ship, as liquid, over 2000 km
- hydrogen supply, from coal gasification, by CNG pipeline, as gaseous, over 500 km
- hydrogen supply, from SMR of from natural gas, by ship, as liquid, over 2000 km
- hydrogen supply, from coal gasification, by truck, as liquid, over 500 km
- hydrogen supply, from gasification of biomass by heatpipe reformer, by truck, as liquid, over 500 km
- hydrogen supply, from ATR of from natural gas, with CCS, by truck, as liquid organic compound, over 500 km
- hydrogen supply, from SMR of from natural gas, with CCS, by truck, as liquid, over 500 km
- hydrogen supply, from electrolysis, by truck, as liquid organic compound, over 500 km
- hydrogen supply, from gasification of biomass, by truck, as liquid organic compound, over 500 km
- hydrogen supply, from SMR of from natural gas, with CCS, by truck, as gaseous, over 500 km
- hydrogen supply, from SMR of biogas, with CCS, by CNG pipeline, as gaseous, over 500 km
- hydrogen supply, from SMR of from natural gas, by truck, as gaseous, over 500 km
- hydrogen supply, from SMR of from natural gas, by H2 pipeline, as gaseous, over 500 km
- hydrogen supply, from gasification of biomass, with CCS, by truck, as liquid organic compound, over 500 km
- hydrogen supply, from gasification of biomass, by ship, as liquid, over 2000 km

Each supply route is associated with specific losses.
Losses for the transport of H2 by truck and hydrogen pipelines, and losses
at the regional storage storage (salt cavern) are from Wulf_ et al, 2018.
Boil-off loss values during shipping are from Hank_ et al, 2020.
Losses when transporting H2 via re-assigned CNG pipelines are from Cerniauskas_ et al, 2020.
Losses along the pipeline are from Schori_ et al, 2012., but to be considered conservative, as those
are initially for natural gas (and hydrogen has a higher potential for leaking).

.. _Wulf: https://www.sciencedirect.com/science/article/pii/S095965261832170X
.. _Cerniauskas: https://doi.org/10.1016/j.ijhydene.2020.02.121
.. _Hank: https://pubs.rsc.org/en/content/articlelanding/2020/se/d0se00067a
.. _Schori: https://treeze.ch/fileadmin/user_upload/downloads/PublicLCI/Schori_2012_NaturalGas.pdf

 ========================== ================= ======== ======= ============== =============== ====================
  _                          _                 truck    ship    H2 pipeline    CNG pipeline    reference flow
 ========================== ================= ======== ======= ============== =============== ====================
  gaseous                    compression       0.5%             0.5%           0.5%            per kg H2
  _                          storage buffer                     2.3%           2.3%            per kg H2
  _                          storage leak                       1.0%           1.0%            per kg H2
  _                          pipeline leak                      0.004%         0.004%          per kg H2, per km
  _                          purification                                      7.0%            per kg H2
  liquid                     liquefaction      1.3%     1.3%                                   per kg H2
  _                          vaporization      2.0%     2.0%                                   per kg H2
  _                          boil-off          0.2%     0.2%                                   per kg H2, per day
  liquid organic compound    hydrogenation     0.5%                                            per kg H2
 ========================== ================= ======== ======= ============== =============== ====================

Losses are cumulative along the supply chain and range anywhere between 5 and 20%.
The table below shows the example of 1 kg of hydrogen transport via re-assigned CNG pipelines,
as a gas, over 500 km.
A total of 0.13 kg of hydrogen is lost along the supply chain (13% loss):


 =============================================================================== ============== ================ ===========
  Output                                                                          _              _                _
 =============================================================================== ============== ================ ===========
  producer                                                                        amount         unit             location
  hydrogen supply, from electrolysis, by CNG pipeline, as gaseous, over 500 km    1              kilogram         OCE
  Input
  supplier                                                                        amount         unit             location
  hydrogen production, gaseous, 25 bar, from electrolysis                         1.133          kilogram         OCE
  market group for electricity, low voltage                                       3.091          kilowatt hour    OCE
  market group for electricity, low voltage                                       0.516          kilowatt hour    OCE
  hydrogen embrittlement inhibition                                               1              kilogram         OCE
  geological hydrogen storage                                                     1              kilogram         OCE
  Hydrogen refuelling station                                                     1.14E-07       unit             OCE
  distribution pipeline for hydrogen, reassigned CNG pipeline                     1.56E-08       kilometer        RER
  transmission pipeline for hydrogen, reassigned CNG pipeline                     1.56E-08       kilometer        RER
 =============================================================================== ============== ================ ===========


- 7% during the purification of hydrogen: when using CNG pipelines, the hydrogen has to be
  mixed with another gas to prevent the embrittlement of the pipelines. The separation process
  at the other end leads to significant losses
- 2% lost along the 500 km of pipeline
- 3% at the regional storage (salt cavern)

Also, in this same case, electricity is used:

- 1.9 kWh to compress the H2 from 25 bar to 100 bar to inject it into the pipeline
- 1.2 kWh to recompress the H2 along the pipeline every 250 km
- 0.34 kWh for injecting and pumping H2 into a salt cavern
- 2.46 kWh to blend the H2 with oxygen on one end, and purify on the other
- 0.5 kWh to pre-cool the H2 at the fuelling station (necessary if used in fuel cells, for example)


Fuel markets
------------

*premise* builds markets for the following fuels:

- market for petrol, unleaded
- market for petrol, low-sulfur
- market for diesel, low-sulfur
- market for diesel
- market for natural gas, high pressure
- market for hydrogen, gaseous
- market for kerosene
- market for liquefied petroleum gas

The market shares are based on the IAM scenario data regarding the composition of
liquid and gaseous secondary energy carriers. The ampping between the IAM scenario
data and the fuel markets is described under: https://github.com/polca/premise/tree/master/premise/iam_variables_mapping/fuels.yaml


.. warning::

    Some fuel types are not properly represented in the LCI database. Available inventories for biomass-based methanol production do not differentiate
    between wood and grass as the feedstock.

.. note::

    **Modelling choice**: *premise* builds several potential supply chains for hydrogen.
    Because the logistics to supply hydrogen in the future is not known or indicated by the IAM,
    the choice is made to supply it by truck over 500 km, in a gaseous state.

Influence of differing LHV on fuel market composition
-----------------------------------------------------

Because not all competing fuels of a same type have similar calorific values,
some adjustments are made. The table below shows the example of the market for
gasoline, for the IMAGE region of Western Europe in 2050.
The sum of fuel inputs is superior to 1 (i.e., 1.4 kg).
This is because the market dataset as "1 kg" as reference unit, and
methanol and bioethanol have low
calorific values comparatively to petrol
(i.e., 19.9 and 26.5 MJ/kg respectively, vs. 42.6 MJ/kg for gasoline).
Hence, their inputs are scaled up to reach an average calorific value
of 42.6 MJ/kg of fuel supplied by the market.

This is necessary as gasoline-consuming activities in the lCI database
are modelled with the calorific value of conventional gasoline.

 =================================================================== ========= =========== ===========
  Output                                                              _         _           _
 =================================================================== ========= =========== ===========
  producer                                                            amount    unit        location
  market for petrol, low-sulfur                                       1         kilogram    WEU
  Input
  supplier                                                            amount    unit        location
  petrol production, low-sulfur                                       0.550     kilogram    CH
  market for methanol, from biomass                                   0.169     kilogram    CH
  market for methanol, from biomass                                   0.148     kilogram    CH
  market for methanol, from biomass                                   0.122     kilogram    CH
  market for methanol, from biomass                                   0.122     kilogram    CH
  Ethanol production, via fermentation, from switchgrass              0.060     kilogram    WEU
  Ethanol production, via fermentation, from switchgrass, with CCS    0.053     kilogram    WEU
  Ethanol production, via fermentation, from sugarbeet                0.051     kilogram    WEU
  Ethanol production, via fermentation, from sugarbeet, with CCS      0.051     kilogram    WEU
  Ethanol production, via fermentation, from poplar, with CCS         0.041     kilogram    WEU
  Ethanol production, via fermentation, from poplar                   0.041     kilogram    WEU
 =================================================================== ========= =========== ===========

Heat
++++

Run

.. code-block:: python

    from premise import *
    import brightway2 as bw

    bw.projects.set_current("my_project)

    ndb = NewDatabase(
        scenarios=[
                {"model":"remind", "pathway":"SSP2-Base", "year":2028}
            ],
        source_db="ecoinvent 3.7 cutoff",
        source_version="3.7.1",
        key='xxxxxxxxxxxxxxxxxxxxxxxxx'
    )
    ndb.update("heat")

Datasets that supply heat and steam via the combustion of natural gas and diesel
are regionalized (made available for each region of the IAM model) and relinked
to regional fuel markets. If the fuel market contains a share of non-fossil fuels,
the CO2 emissions of the heat and steam production are split between fossil and
non-fossil emissions. Once regionalized, the heat and steam production datasets
relink to activities that require heat within the same region.

Here is a list of the heat and steam production datasets that are regionalized:

- diesel, burned in ...
- steam production, as energy carrier, in chemical industry
- heat production, natural gas, ...
- heat and power co-generation, natural gas, ...
- heat production, light fuel oil, ...
- heat production, softwood chips from forest, ...
- heat production, hardwood chips from forest, ...

These datasets are relinked to the corresponding regionalized fuel market only
if `.update("fuels")` has been run.
Also, heat production datasets that use biomass as fuel input (e.g., softwood and
hardwood chips) relink to the dataset `market for biomass, used as fuel` if
`update("biomass")` has been run previously.


CO2 emissions update
--------------------

*premise* iterates through activities that consume any of the newly created
fuel markets to update the way CO2 emissions are modelled. Based on the fuel
market composition, CO2 emissions within the fuel-consuming activity are
split between fossil and non-fossil emissions.

The table below shows the example where the CO2 emissions of a 3.5t truck
have been split into biogenic and fossil fractions after re-link to the
new diesel market of the REMIND region for India.

 ========================================== =========== ========== ================ ===========
  Output                                     before      after      _                _
 ========================================== =========== ========== ================ ===========
  producer                                   amount      amount     unit             location
  transport, freight, lorry, diesel, 3.5t    1           1          ton-kilometer    IND
  Input
  supplier                                   amount      amount     unit             location
  treatment of tyre wear emissions, lorry    -0.0009     -0.0009    kilogram         RER
  market for road maintenance                0.0049      0.0049     meter-year       RER
  market for road                            0.0041      0.0041     meter-year       GLO
  treatment of road wear emissions, lorry    -0.0008     -0.0008    kilogram         RER
  market for refrigerant R134a               2.84E-05    2.84E-05   kilogram         GLO
  treatment of brake wear emissions, lorry   -0.0005     -0.0005    kilogram         RER
  Light duty truck, diesel, 3.5t             1.39E-05    1.39E-05   unit             RER
  market for diesel, low-sulfur              0.1854      0.1854     kilogram         IND
  **Carbon dioxide, fossil**                 0.5840      0.5667     kilogram         _
  **Carbon dioxide, non-fossil**             0.0000      0.0173     kilogram         _
  Nitrogen oxides                            0.0008      0.0008     kilogram         _
  Nitrogen oxides                            0.0003      0.0003     kilogram         _
 ========================================== =========== ========== ================ ===========


Geographical mapping
++++++++++++++++++++

IAM models have slightly different geographical resolutions and definitions.


Map of IMAGE regions

.. image:: map_image.png
   :width: 500pt
   :align: center


Map of REMIND regions

.. image:: map_remind.png
   :width: 500pt
   :align: center


*premise* uses the following correspondence between ecoinvent locations
and IAM regions. This mapping is performed by the constructive_geometries_
implementation in the wurst_ library.

.. _constructive_geometries: https://github.com/cmutel/constructive_geometries
.. _wurst: https://github.com/polca/wurst


 =============== ================================= ================================ ======================== =========================== ========================
  Country Code    message-topology.json             gcam-topology.json              tiam-ucl-topology.json   remind-topology.json        image-topology.json
 =============== ================================= ================================ ======================== =========================== ========================
  AF              SAS                               South Asia                      ODA                      OAS                         RSAS
  AG              LAM                               Central America and Caribbean   CSA                      LAM                         N/A
  AI              LAM                               Central America and Caribbean   CSA                      LAM                         RCAM
  AL              EEU                               Europe_Non_EU                   WEU                      NEU                         CEU
  AM              FSU                               Central Asia                    FSU                      REF                         RUS
  AO              AFR                               Africa_Southern                 AFR                      SSA                         RSAF
  AR              LAM                               Argentina                       CSA                      LAM                         RSAM
  AS              PAS                               Southeast Asia                  ODA                      OAS                         OCE
  AT              WEU                               EU-15                           WEU                      EUR                         WEU
  AU              PAO                               Australia_NZ                    AUS                      CAZ                         OCE
  AZ              FSU                               Central Asia                    FSU                      REF                         RUS
  BA              EEU                               Europe_Non_EU                   EEU                      NEU                         CEU
  BD              SAS                               South Asia                      ODA                      OAS                         RSAS
  BE              WEU                               EU-15                           WEU                      EUR                         WEU
  BF              AFR                               Africa_Western                  AFR                      SSA                         WAF
  BG              EEU                               EU-12                           EEU                      EUR                         CEU
  BH              MEA                               Middle East                     MEA                      MEA                         ME
  BI              AFR                               Africa_Eastern                  AFR                      SSA                         EAF
  BJ              AFR                               Africa_Western                  AFR                      SSA                         WAF
  BN              PAS                               Southeast Asia                  MEA                      OAS                         SEAS
  BO              LAM                               South America_Southern          CSA                      LAM                         RSAM
  BR              LAM                               Brazil                          CSA                      LAM                         BRA
  BS              LAM                               Central America and Caribbean   CSA                      LAM                         RCAM
  BT              SAS                               South Asia                      ODA                      OAS                         RSAS
  BW              AFR                               Africa_Southern                 AFR                      SSA                         RSAF
  BY              FSU                               Europe_Eastern                  FSU                      REF                         UKR
  BZ              LAM                               Central America and Caribbean   CSA                      LAM                         RCAM
  CA              NAM                               Canada                          CAN                      CAZ                         CAN
  CD              AFR                               Africa_Western                  AFR                      SSA                         WAF
  CF              AFR                               Africa_Western                  AFR                      SSA                         WAF
  CG              AFR                               Africa_Western                  AFR                      SSA                         WAF
  CH              WEU                               European Free Trade Association WEU                      NEU                         WEU
  CI              AFR                               Africa_Western                  AFR                      SSA                         WAF
  CL              LAM                               South America_Southern          CSA                      LAM                         RSAM
  CM              AFR                               Africa_Western                  AFR                      SSA                         WAF
  CN              CHN                               China                           CHI                      CHA                         CHN
  CO              LAM                               Colombia                        CSA                      LAM                         RSAM
  CR              LAM                               Central America and Caribbean   CSA                      LAM                         RCAM
  CU              LAM                               Central America and Caribbean   CSA                      LAM                         RCAM
  CY              WEU                               EU-12                           MEA                      EUR                         N/A
  CZ              EEU                               EU-12                           EEU                      EUR                         CEU
  DE              WEU                               EU-15                           WEU                      EUR                         WEU
  DJ              AFR                               Africa_Eastern                  AFR                      SSA                         EAF
  DK              WEU                               EU-15                           WEU                      EUR                         WEU
  DM              LAM                               Central America and Caribbean   CSA                      LAM                         RCAM
  DO              LAM                               Central America and Caribbean   CSA                      LAM                         RCAM
  DZ              MEA                               Africa_Northern                 AFR                      MEA                         NAF
  EC              LAM                               South America_Southern          CSA                      LAM                         RSAM
  EE              EEU                               EU-12                           FSU                      EUR                         CEU
  EG              MEA                               Africa_Northern                 AFR                      MEA                         NAF
  ER              AFR                               Africa_Eastern                  AFR                      SSA                         EAF
  ES              WEU                               EU-15                           WEU                      EUR                         WEU
  ET              AFR                               Africa_Eastern                  AFR                      SSA                         EAF
  FI              WEU                               EU-15                           WEU                      EUR                         WEU
  FJ              PAS                               Southeast Asia                  ODA                      OAS                         OCE
  FR              WEU                               EU-15                           WEU                      EUR                         WEU
  GA              AFR                               Africa_Western                  AFR                      SSA                         WAF
  GB              WEU                               EU-15                           UK                       EUR                         WEU
  GE              FSU                               Central Asia                    FSU                      REF                         RUS
  GF              LAM                               South America_Northern          CSA                      LAM                         RSAM
  GH              AFR                               Africa_Western                  AFR                      SSA                         WAF
  GI              WEU                               EU-15                           WEU                      EUR                         WEU
  GL              WEU                               EU-15                           NEU                      NEU                         WEU
  GM              AFR                               Africa_Western                  AFR                      SSA                         WAF
  GN              AFR                               Africa_Western                  AFR                      SSA                         WAF
  GQ              AFR                               Africa_Western                  AFR                      SSA                         WAF
  GR              WEU                               EU-15                           WEU                      EUR                         WEU
  GT              LAM                               Central America and Caribbean   CSA                      LAM                         RCAM
  GW              AFR                               Africa_Western                  AFR                      SSA                         WAF
  GY              LAM                               South America_Northern          CSA                      LAM                         RSAM
  HK              CHN                               China                           CHI                      CHA                         CHN
  HN              LAM                               Central America and Caribbean   CSA                      LAM                         RCAM
  HR              EEU                               Europe_Non_EU                   EEU                      EUR                         CEU
  HT              LAM                               Central America and Caribbean   CSA                      LAM                         RCAM
  HU              EEU                               EU-12                           EEU                      EUR                         CEU
  ID              PAS                               Indonesia                       ODA                      OAS                         INDO
  IE              WEU                               EU-15                           WEU                      EUR                         WEU
  IL              MEA                               Middle East                     MEA                      MEA                         ME
  IN              SAS                               India                           IND                      IND                         INDIA
  IQ              MEA                               Middle East                     MEA                      MEA                         ME
  IR              MEA                               Middle East                     MEA                      MEA                         ME
  IS              WEU                               European Free Trade Association WEU                      NEU                         WEU
  IT              WEU                               EU-15                           WEU                      EUR                         WEU
  JM              LAM                               Central America and Caribbean   CSA                      LAM                         RCAM
  JO              MEA                               Middle East                     MEA                      MEA                         ME
  JP              PAO                               Japan                           JPN                      JPN                         JAP
  KE              AFR                               Africa_Eastern                  AFR                      SSA                         EAF
  KG              FSU                               Central Asia                    FSU                      REF                         STAN
  KH              RCPA                              Southeast Asia                  ODA                      OAS                         SEAS
  KI              PAS                               Southeast Asia                  ODA                      OAS                         OCE
  KM              AFR                               Africa_Eastern                  AFR                      SSA                         EAF
  KN              LAM                               Central America and Caribbean   CSA                      LAM                         RCAM
  KP              RCPA                              Southeast Asia                  ODA                      OAS                         KOR
  KR              PAS                               South Korea                     SKO                      OAS                         KOR
  KW              MEA                               Middle East                     MEA                      MEA                         ME
  KY              LAM                               Central America and Caribbean   CSA                      LAM                         RCAM
  KZ              FSU                               Central Asia                    FSU                      REF                         STAN
  LA              RCPA                              Southeast Asia                  ODA                      OAS                         SEAS
  LB              MEA                               Middle East                     MEA                      MEA                         ME
  LC              LAM                               Central America and Caribbean   CSA                      LAM                         RCAM
  LI              WEU                               EU-15                           WEU                      NEU                         WEU
  LK              SAS                               South Asia                      ODA                      OAS                         RSAS
  LR              AFR                               Africa_Western                  AFR                      SSA                         WAF
  LS              AFR                               Africa_Southern                 AFR                      SSA                         RSAF
  LT              EEU                               EU-12                           FSU                      EUR                         CEU
  LU              WEU                               EU-15                           WEU                      EUR                         WEU
  LV              EEU                               EU-12                           FSU                      EUR                         CEU
  LY              MEA                               Africa_Northern                 AFR                      MEA                         NAF
  MA              MEA                               Africa_Northern                 AFR                      MEA                         NAF
  MC              WEU                               EU-15                           WEU                      NEU                         WEU
  MD              FSU                               Europe_Eastern                  FSU                      REF                         UKR
  ME              EEU                               Europe_Non_EU                   EEU                      NEU                         CEU
  MG              AFR                               Africa_Eastern                  AFR                      SSA                         RSAF
  MK              EEU                               Europe_Non_EU                   EEU                      NEU                         CEU
  ML              AFR                               Africa_Western                  AFR                      SSA                         WAF
  MM              PAS                               Southeast Asia                  ODA                      OAS                         SEAS
  MN              RCPA                              Central Asia                    ODA                      OAS                         CHN
  MO              CHN                               China                           CHI                      CHA                         CHN
  MR              AFR                               Africa_Western                  AFR                      SSA                         WAF
  MS              LAM                               Central America and Caribbean   CSA                      LAM                         RCAM
  MT              WEU                               EU-12                           WEU                      EUR                         WEU
  MU              AFR                               Africa_Eastern                  ODA                      SSA                         EAF
  MW              AFR                               Africa_Southern                 AFR                      SSA                         RSAF
  MX              LAM                               Mexico                          MEX                      MEX                         MEX
  MY              PAS                               Southeast Asia                  ODA                      OAS                         SEAS
  MZ              AFR                               Africa_Southern                 AFR                      SSA                         RSAF
  NA              AFR                               Africa_Southern                 AFR                      SSA                         RSAF
  NE              AFR                               Africa_Western                  AFR                      SSA                         WAF
  NG              AFR                               Africa_Western                  AFR                      SSA                         WAF
  NI              LAM                               Central America and Caribbean   CSA                      LAM                         RCAM
  NL              WEU                               EU-15                           WEU                      EUR                         WEU
  NO              WEU                               European Free Trade Association WEU                      NEU                         WEU
  NP              SAS                               South Asia                      ODA                      OAS                         RSAS
  NR              PAS                               Southeast Asia                  ODA                      OAS                         OCE
  NU              PAS                               Southeast Asia                  ODA                      OAS                         OCE
  NZ              PAO                               Australia_NZ                    AUS                      CAZ                         OCE
  OM              MEA                               Middle East                     MEA                      MEA                         ME
  PA              LAM                               Central America and Caribbean   CSA                      LAM                         RCAM
  PE              LAM                               South America_Southern          CSA                      LAM                         RSAM
  PF              PAS                               Southeast Asia                  ODA                      OAS                         OCE
  PG              PAS                               Southeast Asia                  ODA                      OAS                         INDO
  PH              PAS                               Southeast Asia                  ODA                      OAS                         SEAS
  PK              SAS                               Pakistan                        ODA                      OAS                         RSAS
  PL              EEU                               EU-12                           EEU                      EUR                         CEU
  PT              WEU                               EU-15                           WEU                      EUR                         WEU
  PY              LAM                               South America_Southern          CSA                      LAM                         RSAM
  QA              MEA                               Middle East                     MEA                      MEA                         ME
  RE              AFR                               Africa_Eastern                  AFR                      SSA                         EAF
  RO              EEU                               EU-12                           EEU                      EUR                         CEU
  RS              EEU                               Europe_Non_EU                   EEU                      NEU                         CEU
  RW              AFR                               Africa_Eastern                  AFR                      SSA                         EAF
  SA              MEA                               Middle East                     MEA                      MEA                         ME
  SB              PAS                               Southeast Asia                  ODA                      OAS                         OCE
  SC              AFR                               Africa_Eastern                  AFR                      SSA                         EAF
  SD              MEA                               Africa_Eastern                  AFR                      MEA                         EAF
  SE              WEU                               EU-15                           WEU                      EUR                         WEU
  SG              PAS                               Southeast Asia                  ODA                      OAS                         SEAS
  SH              AFR                               Africa_Western                  AFR                      SSA                         WAF
  SI              EEU                               EU-12                           EEU                      EUR                         CEU
  SK              EEU                               EU-12                           EEU                      EUR                         CEU
  SL              AFR                               Africa_Western                  AFR                      SSA                         WAF
  SM              WEU                               EU-15                           WEU                      NEU                         WEU
  SN              AFR                               Africa_Western                  AFR                      SSA                         WAF
  SO              AFR                               Africa_Eastern                  AFR                      SSA                         EAF
  SR              LAM                               South America_Northern          CSA                      LAM                         RSAM
  SS              AFR                               Africa_Eastern                  AFR                      SSA                         EAF
  ST              AFR                               Africa_Western                  AFR                      SSA                         WAF
  SV              LAM                               Central America and Caribbean   CSA                      LAM                         RCAM
  SY              MEA                               Middle East                     MEA                      MEA                         ME
  SZ              AFR                               Africa_Southern                 AFR                      SSA                         RSAF
  TC              LAM                               Central America and Caribbean   CSA                      LAM                         RCAM
  TD              AFR                               Africa_Western                  AFR                      SSA                         WAF
  TG              AFR                               Africa_Western                  AFR                      SSA                         WAF
  TH              PAS                               Southeast Asia                  ODA                      OAS                         SEAS
  TJ              FSU                               Central Asia                    FSU                      REF                         STAN
  TL              PAS                               Southeast Asia                  ODA                      OAS                         INDO
  TM              FSU                               Central Asia                    FSU                      REF                         STAN
  TN              MEA                               Africa_Northern                 AFR                      MEA                         NAF
  TO              PAS                               Southeast Asia                  ODA                      OAS                         OCE
  TR              WEU                               EU-15                           MEA                      MEA                         TUR
  TT              LAM                               Central America and Caribbean   CSA                      LAM                         RCAM
  TV              PAS                               Southeast Asia                  ODA                      OAS                         OCE
  TZ              AFR                               Africa_Southern                 AFR                      SSA                         RSAF
  UA              FSU                               Europe_Eastern                  FSU                      REF                         UKR
  UG              AFR                               Africa_Eastern                  AFR                       SSA                         EAF
  US              NAM                               USA                             USA                      USA                         USA
  UY              LAM                               South America_Southern          CSA                      LAM                         RSAM
  UZ              FSU                               Central Asia                    FSU                      REF                         STAN
  VC              LAM                               Central America and Caribbean   CSA                      LAM                         RCAM
  VE              LAM                               South America_Northern          CSA                      LAM                         RSAM
  VG              N/A                               N/A                             N/A                      LAM                         RCAM
  VI              NAM                               Central America and Caribbean   CSA                      LAM                         RCAM
  VN              RCPA                              Southeast Asia                  ODA                      OAS                         SEAS
  VU              PAS                               Southeast Asia                  ODA                      OAS                         OCE
  YE              MEA                               Middle East                     MEA                      MEA                         ME
  ZA              AFR                               South Africa                    AFR                      SSA                         SAF
  ZM              AFR                               Africa_Southern                 AFR                      SSA                         RSAF
  ZW              AFR                               Africa_Southern                 AFR                      SSA                         RSAF
 =============== ================================= ================================ ======================== =========================== ========================



The mapping between ecoinvent locations and IAM regions is available under the following directory:
https://github.com/polca/premise/blob/master/premise/iam_variables_mapping/topologies

Regionalization
+++++++++++++++

Several of the integration steps described above involve the
regionalization of datasets. It is the case, for example, when introducing
datasets representing a process for each of the IAM regions.
In such case, the datasets are regionalized by selecting the most
representative suppliers of inputs for each region. If a dataset
in a specific IAM region requires tap water, for example, the regionalization process will
select the most representative water suppliers in that region.

If more than one supplier is available, the regionalization process will
allocated a supply share to each candidate supplier based on their
respective production volume. If no adequate supplier is found for a given region,
the regionalization process will select all the existing suppliers and
allocate a supply share to each supplier based on their respective
production volume.

Here is the decision tree followed:

.. _decision-tree:

**Decision Tree for Processing Datasets**


The process begins with a dataset that requires processing.

.. contents::
   :local:

Decision: Is the Exchange in Cache?
-----------------------------------

- **Yes**

  - Use :func:`process_cached_exchange`.

    - Retrieve cached data.
    - Update ``new_exchanges`` with cached data.

- **No**

  - Use :func:`process_uncached_exchange`.

    Decision: Number of Possible Datasets
    ------------------------------------

    - **None**

      - Print a warning and return.

    - **One**

      - Use :func:`handle_single_possible_dataset`.

        - Use the single matched dataset.
        - Update ``new_exchanges`` with this dataset information.

    - **Multiple**

      - Use :func:`handle_multiple_possible_datasets`.

        Decision: Does Dataset Location Match Possible Dataset Locations?
        -----------------------------------------------------------------

        - **Yes**

          - Use the matched dataset location.

        - **No**

          - Use :func:`process_complex_matching_and_allocation`.

            Decision: Dataset Location Type
            --------------------------------

            - **IAM Region**

              - Use :func:`handle_iam_region`.

                - Match IAM region to ecoinvent locations.
                - Update ``new_exchanges`` with IAM region-specific data.
                - Cache the new entry.

            - **Global ('GLO', 'RoW', 'World')**

              - Use :func:`handle_global_and_row_scenarios`.

                - Allocate inputs for global datasets.
                - Update ``new_exchanges`` with global data.
                - Cache the new entry.

            - **Others**

              - Perform GIS matching.

                - Determine intersecting locations with GIS.
                - Allocate inputs based on GIS matches.
                - Update ``new_exchanges`` with GIS-specific data.
                - Cache the new entry.

Final Steps
-----------

- If no match is found, use :func:`handle_default_option`.

  - Integrate new exchanges into the dataset.


GAINS emission factors
++++++++++++++++++++++

Emissions factors from the air pollution model GAINS are used to scale
non-CO2 emissions in various datasets. The emission factors are available under:

premise/data/GAINS_emission_factors

Run

.. code-block:: python

    from premise import *
    import brightway2 as bw

    bw.projects.set_current("my_project)

    ndb = NewDatabase(
        scenarios=[
                {"model":"remind", "pathway":"SSP2-Base", "year":2028}
            ],
        source_db="ecoinvent 3.7 cutoff",
        source_version="3.7.1",
        key='xxxxxxxxxxxxxxxxxxxxxxxxx'
    )
    ndb.update("emissions")

When using `update("emissions")`, emission factors from the GAINS-EU_ and GAINS-IAM_ models are used to scale
non-CO2 emissions in various datasets.

.. _GAINS-EU: https://gains.iiasa.ac.at/gains/EUN/index.login
.. _GAINS-IAM: https://gains.iiasa.ac.at/gains/IAM/index.login

The emission factors are available under
https://github.com/polca/premise/tree/master/premise/data/GAINS_emission_factors

Emission factors from GAINS-EU are applied to activities in European countries.
Emission factors from GAINS-IAM are applied to activities in non-European countries,
or to European activities if an emission facor from GAINS-EU has not been
applied first.

Emission factors are specific to:

* an activity type,
* a year,
* a country (for GAINS-EU, otherwise a region),
* a fuel type,
* a technology type,
* and a scenario.

The mapping between GAINS and ecoinvent activities is available under the following file:
https://github.com/polca/premise/blob/master/premise/data/GAINS_emission_factors/gains_ecoinvent_sectoral_mapping.yaml

The table below shows the mapping between ecoinvent and GAINS emission flows.

+-------------------------------------------------------------------+----------------+
| ecoinvent species                                                 | GAINS species  |
+===================================================================+================+
| Sulfur dioxide                                                    |  SO2           |
+-------------------------------------------------------------------+----------------+
| Sulfur oxides                                                     |  SO2           |
+-------------------------------------------------------------------+----------------+
| Carbon monoxide, fossil                                           |  CO            |
+-------------------------------------------------------------------+----------------+
| Carbon monoxide, non-fossil                                       |  CO            |
+-------------------------------------------------------------------+----------------+
| Carbon monoxide, from soil or biomass stock                       |  CO            |
+-------------------------------------------------------------------+----------------+
| Nitrogen oxides                                                   |  NOx           |
+-------------------------------------------------------------------+----------------+
| Ammonia                                                           |  NH3           |
+-------------------------------------------------------------------+----------------+
| NMVOC, non-methane volatile organic compounds, unspecified origin |  VOC           |
+-------------------------------------------------------------------+----------------+
| VOC, volatile organic compounds, unspecified origin               |  VOC           |
+-------------------------------------------------------------------+----------------+
| Methane                                                           |  CH4           |
+-------------------------------------------------------------------+----------------+
| Methane, fossil                                                   |  CH4           |
+-------------------------------------------------------------------+----------------+
| Methane, non-fossil                                               |  CH4           |
+-------------------------------------------------------------------+----------------+
| Methane, from soil or biomass stock                               |  CH4           |
+-------------------------------------------------------------------+----------------+
| Dinitrogen monoxide                                               |  N2O           |
+-------------------------------------------------------------------+----------------+
| Particulates, > 10 um                                             |  PM10          |
+-------------------------------------------------------------------+----------------+
| Particulates, > 2.5 um, and < 10um                                |  PM25          |
+-------------------------------------------------------------------+----------------+
| Particulates, < 2.5 um                                            |  PM1           |
+-------------------------------------------------------------------+----------------+

We consider emission factors in ecoinvent as representative of the current situation.
Hence, we calculate a *scaling factor* from the GAINS emission factors for the year of
the scenario relative to the year 2020. note that premise prevents scaling factors to be
inferior to 1 if the year is inferior to 2020. Inversely, scaling factors cannot be superior to 1
if the year is superior to 2020.

Two GAINS-IAM scenarios are available:

* **CLE**: **C**urrent **LE**gislation scenario
* **MFR**: **M**aximum **F**easible **R**eduction scenario

By default, the CLE scenario is used. To use the MFR scenario:

.. code-block:: python

    ndb = NewDatabase(
        ...
        gains_scenario="MFR",
    )

Finally, unlike GAINS-EU, GAINS-IAM uses IAM-like regions, not countries.
The mapping between IAM regions and GAINS-IAM regions is available under the following file:

https://github.com/polca/premise/blob/master/premise/iam_variables_mapping/gains_regions_mapping.yaml

For questions related to GAINS modelling, please contact the respective GAINS team:

* GAINS-EU: https://gains.iiasa.ac.at/gains/EUN/index.login
* GAINS-IAM: https://gains.iiasa.ac.at/gains/IAM/index.login

Logs
++++

*premise* generates a spreadsheet report detailing changes made to the database
for each scenario. The report is saved in the current working directory and
is automatically generated after database export.

The report lists the datasets added, updated and emptied.
It also gives a number of indicators relating to efficiency,
emissions, etc. for each scenario.

Finally, it also contains a "Validation" tab that lists datasets
which potentially present erroneous values. These datasets are
to be checked by the user.

This report can also be generated manually using the `generate_change_report()` method.
