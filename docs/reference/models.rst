IAM model comparison
======================

.. contents:: On this page
   :local:
   :depth: 1

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Model
     - Description
   * - REMIND
     - REMIND (Regionalized Model of Investment and Development) is an integrated assessment model that combines macroeconomic growth, energy system, and climate policy analysis. It is designed to analyze long-term energy transition pathways, accounting for technological, economic, and environmental factors. REMIND simulates how regions invest in different technologies and energy resources to balance economic growth and climate targets, while considering factors like energy efficiency, emissions, and resource availability. The model is particularly strong in its detailed representation of energy markets and macroeconomic interactions across regions, making it valuable for global climate policy assessments.
   * - REMIND-EU
     - REMIND-EU is a regionalized version of the REMIND model, specifically tailored to analyze energy systems and climate policies within the European Union. It incorporates detailed representations of EU member states' energy markets, technological options, and policy frameworks. It allows for a more granular analysis of how EU-specific policies, such as the European Green Deal, affect energy transition pathways, emissions reductions, and economic development within the EU context.
   * - IMAGE
     - IMAGE (Integrated Model to Assess the Global Environment) is a comprehensive IAM developed to explore the interactions between human development, energy consumption, and environmental systems over the long term. It focuses on assessing how land use, food systems, energy systems, and climate change interact under different policy scenarios. The model integrates biophysical processes, such as land-use change and greenhouse gas emissions, with socio-economic drivers like population growth and economic development. IMAGE is commonly used for analyzing sustainable development strategies, climate impacts, biodiversity loss, and exploring mitigation and adaptation options.
   * - TIAM-UCL
     - TIAM-UCL (TIMES Integrated Assessment Model by University College London) is a global energy system model based on the TIMES (The Integrated MARKAL-EFOM System) framework, developed to evaluate long-term decarbonization pathways for global energy systems. It provides detailed insights into energy technology options, resource availability, and emission reduction strategies under various climate policy scenarios. The model focuses on the trade-offs and synergies between energy security, economic costs, and environmental outcomes. TIAM-UCL is frequently used to analyze scenarios consistent with the Paris Agreement and examine technological innovation's role in mitigating climate change globally.
   * - MESSAGE
     - MESSAGEix-GLOBIOM-GAINS (MESSAGE) couples the MESSAGEix energy system with the GLOBIOM land-use model and GAINS air-pollution module. It is used to explore long-term energy and land-use transitions and their climate and air-quality implications under different policy scenarios.
   * - GCAM
     - GCAM (Global Change Analysis Model) is an integrated assessment model that simulates the interactions between energy, water, land use, climate, and economic systems on a global scale. It is designed to analyze how different policy scenarios, technological developments, and socio-economic factors influence greenhouse gas emissions, energy production and consumption, land use changes, and climate outcomes. GCAM incorporates detailed representations of energy technologies, agricultural systems, and land-use dynamics, allowing for comprehensive assessments of mitigation strategies and their implications for sustainable development. The model is widely used for exploring pathways to achieve climate targets while considering trade-offs across multiple sectors.


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

**REMIND**

REMIND (Regionalized Model of Investment and Development) is a CGE-based energy-economy IAM with perfect
foresight. Its main strength lies in capturing interactions between macroeconomic growth and energy
transitions across 12–13 global regions. Compared to IMAGE, REMIND provides more detailed energy market
and investment dynamics, but it lacks IMAGE's rich land-use and biodiversity modules. Compared to
TIAM-UCL, REMIND emphasizes macroeconomic feedbacks over technological granularity, making it
better for studying long-term global climate policies rather than detailed technology pathways.
REMIND-EU builds directly on REMIND but adds EU-specific regionalization.

**REMIND-EU**

REMIND-EU is a regionalized version of REMIND, designed to analyze the European Union’s energy
transition with country-level resolution (at least, for France, Germany, and the UK). It retains
REMIND’s CGE approach and perfect foresight but includes EU-specific policies and technologies,
which are less detailed in the global REMIND model. Compared to IMAGE, REMIND-EU still lacks a
strong land-use component, but its granularity for  EU energy systems makes it preferable for
studying European Green Deal scenarios. Compared to TIAM-UCL, REMIND-EU has less technology detail
but better macroeconomic and cross-sectoral insights for EU policymaking.

**IMAGE**

IMAGE (Integrated Model to Assess the Global Environment) is a simulation-based IAM with a
recursive-dynamic structure (myopic foresight). It excels in land-use, agriculture, and
biodiversity modeling, making it the best choice for scenarios that involve climate–ecosystem
interactions. Compared to REMIND and TIAM-UCL, IMAGE has less detailed energy system modeling and
no explicit macroeconomic CGE framework. However, its biophysical integration and land-use modeling
(unlike TIAM-UCL, which lacks this entirely) makes it complementary to energy-focused models.

**TIAM-UCL**

TIAM-UCL is a bottom-up, technology-rich energy system model based on linear optimization
with perfect foresight. It focuses on detailed technology pathways, energy supply chains,
and long-term decarbonization strategies. Compared to REMIND and IMAGE, TIAM-UCL lacks
macroeconomic modeling and has no integrated land-use module, but it provides superior
technology detail and resource-specific analyses (e.g., hydrogen pathways, renewables
deployment). It is particularly suited for Paris Agreement-compliant energy transitions
and cost-optimal technology portfolios.

**MESSAGE**

MESSAGEix-GLOBIOM-GAINS (MESSAGE) is an energy-system optimization IAM coupled with the GLOBIOM
land-use model and the GAINS air-pollution module. It provides detailed energy system pathways
with explicit links to land-use and air-quality outcomes. Compared to REMIND, it is less focused
on macroeconomic feedbacks but offers stronger coupling to land-use and air-pollution dynamics.
Compared to GCAM and IMAGE, it emphasizes cost-optimal energy system transformations while still
capturing land-use interactions through GLOBIOM.

**GCAM**

GCAM (Global Change Analysis Model) is a recursive-dynamic IAM based on partial equilibrium
with myopic foresight. Its distinguishing feature is the tight coupling of energy, land,
water, and agriculture systems within a single framework. Compared to REMIND, GCAM lacks
intertemporal optimization and macroeconomic feedbacks but offers richer integration of
land and water systems. Compared to IMAGE, GCAM places stronger emphasis on regional
bioenergy–land-use trade-offs and water constraints, although its energy system detail
is slightly more stylized. Unlike TIAM-UCL, GCAM is not technology-optimization–driven,
but it captures market-driven transitions in land and energy under policy constraints.
This makes it especially suitable for analyzing cross-sectoral impacts of climate, land,
and water policies in a globally consistent framework.

Choosing the Right IAM
------------------------

Selecting the appropriate IAM for use with *premise* depends on the focus of your study:

- **REMIND** is best suited for **global energy–economy transition analyses** where the interplay between macroeconomic growth, energy markets, and climate policies is key.
- **REMIND-EU** is ideal for **EU-focused studies**, particularly those assessing the **European Green Deal** or country-level decarbonization strategies within the EU.
- **IMAGE** is the preferred choice when **land-use change, agriculture, biodiversity, or climate–ecosystem interactions** are central to the analysis. Its biophysical and environmental modules complement energy-focused IAMs.
- **TIAM-UCL** is most appropriate for exploring **detailed technology pathways**, resource allocation, and **cost-optimal energy system designs**, particularly for **Paris Agreement-compatible scenarios**.
- **MESSAGE** is most suitable when you need **cost-optimal energy system pathways** with explicit **land-use** and **air-pollution** linkages.
- **GCAM** is most suitable when the cross-sectoral links between land, water, energy, and agriculture are crucial. It is especially useful for questions involving bioenergy deployment, water scarcity constraints, or food–land competition under climate policy.

Our recommendation is to assess the sensitivity of your results across different IAMs for a given climate target.
IAMs will deploy different technologies and resources to achieve the same climate target, which will lead to different life cycle inventories.

Additionally, the level of sectoral integration in *premise* varies across IAMs, which can affect the results.

This table below summarize the numbers of variables mapping with *premise* for each IAM and sector:

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
* Comprehensive coverage of carbon dioxide removal (11).

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
* Industrial detail is uneven across sectors (cement 12, steel 23), reflecting its broader systems focus rather than technology granularity.

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
