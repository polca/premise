IAM model comparison
======================

.. raw:: html

   <span id="choosing-the-right-iam"></span>


.. contents:: On this page
   :local:
   :depth: 1

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Model
     - Description
   * - REMIND
     - REMIND (Regionalized Model of Investment and Development) links economic growth, energy investment and climate policy. It models how regions invest in technologies and energy resources under climate constraints.
   * - REMIND-EU
     - REMIND-EU represents European energy systems and policies in more regional detail than the global REMIND model.
   * - IMAGE
     - IMAGE (Integrated Model to Assess the Global Environment) links energy, land use, agriculture and climate. It represents environmental processes alongside changes in population and economic activity.
   * - TIAM-UCL
     - TIAM-UCL (TIMES Integrated Assessment Model by University College London) models energy technologies, resources and supply chains. It uses the TIMES framework to examine the costs and technology choices of decarbonization pathways.
   * - MESSAGE
     - MESSAGEix-GLOBIOM-GAINS (MESSAGE) couples the MESSAGEix energy system with the GLOBIOM land-use model and GAINS air-pollution module. It is used to explore long-term energy and land-use transitions and their climate and air-quality implications under different policy scenarios.
   * - GCAM
     - GCAM (Global Change Analysis Model) links energy, water, land use, climate and economic systems. It represents their responses to policies, technology changes and socioeconomic assumptions.


Quick Reference
-----------------

.. list-table::
   :header-rows: 1
   :widths: 20 15 15 15 15 15 15

   * - Property
     - REMIND
     - REMIND-EU
     - IMAGE
     - TIAM-UCL
     - MESSAGE
     - GCAM
   * - **Model Type**
     - CGE + Energy
     - CGE + Energy
     - IAM (PEM)
     - Bottom-up
     - Bottom-up (energy system)
     - IAM (PEM)
   * - **Foresight**
     - ✓ Perfect
     - ✓ Perfect
     - ✗ Myopic
     - ✓ Perfect
     - ✓ Perfect
     - ✗ Myopic
   * - **Energy System**
     - ✓ Detailed
     - ✓ Detailed
     - ✓ Moderate
     - ✓ Very detailed
     - ✓ Very detailed
     - ✓ Moderate
   * - **Land Use**
     - ✓ (MAGPIE)
     - ✓ (MAGPIE)
     - ✓ Integrated
     - ✗
     - ✓ Integrated (GLOBIOM)
     - ✓ Integrated
   * - **Regional Focus**
     - Global
     - EU + Global
     - Global
     - Global
     - Global
     - Global
   * - **Key Strength**
     - Energy-economy
     - EU policies
     - Land & climate
     - Tech pathways
     - Energy system + land/air
     - Coupled land–water–energy

Choosing an IAM
-----------------

Choose models whose reported variables cover the processes and regions needed
for your study. The descriptions above explain each model's focus; they do not
show which outputs are available in a particular scenario file or used by
Premise. See :doc:`coverage` for the configured sector mappings.

Compare results across models for a similar climate target where possible.
Different models can meet that target with different technologies and resources,
producing different inventories. Check scenario assumptions and sector coverage
before attributing a result to the choice of model alone.

The table below lists the number of variables mapped in Premise for each IAM and sector:

.. list-table::
   :header-rows: 1
   :widths: 20 15 15 15 15 15 15

   * - Sector
     - image
     - remind
     - remind-eu
     - tiam-ucl
     - gcam
     - message
   * - Biomass
     - 4
     - 2
     - 2
     - 2
     - 2
     - 3
   * - Carbon Dioxide Removal
     - 5
     - 11
     - 11
     - 3
     - 4
     - 7
   * - Cement
     - 45
     - 4
     - 4
     - 7
     - 12
     - 26
   * - Crops
     - 10
     - 0
     - 0
     - 1
     - 10
     - 0
   * - Electricity
     - 51
     - 34
     - 34
     - 61
     - 41
     - 43
   * - Fuels
     - 56
     - 49
     - 49
     - 55
     - 48
     - 24
   * - Heat
     - 44
     - 32
     - 32
     - 11
     - 14
     - 34
   * - Other
     - 4
     - 4
     - 4
     - 4
     - 4
     - 4
   * - Steel
     - 117
     - 42
     - 42
     - 119
     - 23
     - 14
   * - Transport Bus
     - 10
     - 8
     - 8
     - 12
     - 10
     - 0
   * - Transport Passenger Cars
     - 10
     - 60
     - 60
     - 29
     - 30
     - 0
   * - Transport Rail Freight
     - 10
     - 7
     - 7
     - 6
     - 10
     - 0
   * - Transport Road Freight
     - 38
     - 40
     - 40
     - 90
     - 29
     - 0
   * - Transport Sea Freight
     - 16
     - 15
     - 15
     - 37
     - 12
     - 0
   * - Transport Two Wheelers
     - 0
     - 12
     - 12
     - 0
     - 6
     - 0




And here is a plot of the same data:

.. image:: /mapped_vars_comparison.png
   :width: 600pt
   :align: center
   :alt: Comparison plot of mapped variables across IAM models

The table and plot show how *premise* connects to IMAGE, REMIND, REMIND-EU,
TIAM-UCL, GCAM, and MESSAGE, focusing on electricity, heat, fuels, industry, and
transport:

* TIAM-UCL has the largest coverage in this table (437 variables), with strong detail in steel (119), electricity (61), fuels (55), and road freight (90), plus secondary heat (11).
* IMAGE also offers broad integration (420 variables), with high counts in steel (117), fuels (56), electricity (51), cement (45), and heat (44). Two-wheelers are not covered by IMAGE.
* REMIND and REMIND-EU have identical coverage (320 variables each), with particularly strong detail in passenger cars (60), fuels (49), road freight (40), and heat (32).
* GCAM provides moderate coverage (255 variables), with strength in electricity (41), fuels (48), heat (14), and cross-sector integration of land, water, agriculture, and energy.
* MESSAGE includes 155 mapped variables and currently does not include transport-sector mappings.

Sectoral observations:

* Electricity, fuels, and heat are mapped across all models, although the level and type of heat detail varies.
* Transport sub-sectors (bus, passenger cars, rail, road, and sea freight) are well represented in REMIND(-EU), TIAM-UCL, and GCAM, with IMAGE covering all except two-wheelers.
* MESSAGE scenarios currently do not have transport-sector mappings in *premise*.
* Industrial sectors are strongly represented in IMAGE and TIAM-UCL, especially for steel and cement.


**IMAGE**

*Strengths:*

* Strong coverage of electricity (51 variables), fuels (56 variables), and heat (44 variables).
* Detailed industrial sectors, especially cement (45) and steel (117).
* Broad mapping across transport sub-sectors, except for two-wheelers.

*Limitation:*

* No coverage of two-wheelers, and fewer transport details than REMIND for passenger cars.

**REMIND**

*Strengths:*

* Broad coverage of electricity (34), fuels (49), and heat (32).
* Highly detailed transport, with 60 variables for passenger cars and 40 for road freight.
* Coverage of carbon dioxide removal (11).

*Limitation:*

* Less detailed in cement and steel compared to IMAGE and TIAM-UCL.

**REMIND-EU**

*Strengths:*

* Same broad mapping as REMIND, but with EU-specific detail.
* Excellent coverage of transport and fuels, aligned with EU decarbonization pathways.
* Includes CO₂ removal (11), electricity (34), and heat (32) in high detail.


*Limitations:*

* Industrial coverage (cement 4, steel 42) is moderate compared to IMAGE and TIAM-UCL.
* Not as many scenarios available as for REMIND.

**TIAM-UCL**

*Strengths:*

* Strong focus on electricity (61) and fuels (55).
* Detailed road freight (90) and transport mapping.
* Good coverage of passenger cars (29 variables).

*Limitation:*

* Heat detail is limited to secondary supply (11 variables); buildings and
  industrial end-use technology mixes are not available.
* Limited representation of the carbon dioxide removal sector (3 variables).


**GCAM**

*Strengths:*

* Integrated coverage of land, energy, water, and agriculture systems — GCAM’s key advantage over the other IAMs.
* Moderate detail in electricity (41), fuels (48), and heat (14), sufficient for energy–land–water linkages.
* Includes biomass and CDR pathways with explicit land-use competition interactions.

*Limitations:*

* Transport coverage is lower than REMIND(-EU) and TIAM-UCL for passenger cars and road freight.
* Industrial detail is uneven across sectors (cement 12, steel 23), reflecting its broader systems focus rather than technology detail.

**MESSAGE**

*Strengths:*

* Good coverage in electricity (43), fuels (24), and heat (34).
* Strong representation of cement-related mappings (26), alongside steel (14).
* Includes biomass (3) and carbon dioxide removal pathways (7).

*Limitations:*

* No transport-sector mappings are currently available (bus, passenger cars, rail, road, sea freight, and two-wheelers).
* Coverage is lower than IMAGE, REMIND(-EU), and TIAM-UCL in several sectors, with 155 mapped variables in total.

Heat variable counts include the raw IAM variables used by sums and residual
expressions. They do not imply equal layer coverage: see :ref:`heat-transformation` for the
buildings, industrial, and secondary-supply breakdown and the behavior used when
a layer is absent.
