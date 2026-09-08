Photovoltaics
===============


.. contents:: On this page
   :local:
   :depth: 1

Scope and outputs
-------------------

Premise represents PV manufacturing, installation and electricity production
with distinct datasets. Country electricity datasets supply 1 kWh; the
scenario transformation adjusts module area for installation efficiency while
retaining the documented country technology recipes and yields.
The 2026 core is selected only for ecoinvent 3.12 cut-off.

.. figure:: /_static/process-diagrams/electricity-photovoltaics.svg
   :class: process-diagram
   :alt: Photovoltaic inventory and scenario changes: Import the selected PV inventory; Read external module-efficiency trajectory; Reduce area when efficiency increases; Use country suppliers in electricity markets.
   :align: center

Inputs and applicability
--------------------------

The IEA report, packaged workbooks, inherited Atlas yields and IRENA weights
are inputs to preparation of the country inventories. The build imports these
prepared inventories during initialization. ``ndb.update("electricity")``
then applies the module-efficiency trajectory and regional electricity-market
mapping. It does not download yields or rebuild country recipes each time.

Technologies and reference systems
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The core includes single-crystalline silicon (single-Si), multicrystalline
silicon (multi-Si), and cadmium telluride (CdTe) supply chains. The single-Si
reference systems combine TOPCon and PERC modules: approximately 85.6% TOPCon
and 14.4% PERC by module area. They should therefore not be interpreted as
pure TOPCon systems.

The 45 original electricity functional units comprise 33 national production
mixes and 12 reference-system datasets. The latter represent six single-Si
and CdTe system configurations, each available at **1000 or 1300
kWh/kWp/year**. These yields are alternative site assumptions, not weights
in a technology mix. The reference-system label ``REF`` is retained as
metadata, while the packaged datasets use the valid location ``GLO``.

For the country extension, installations are grouped as follows:

.. list-table:: Country electricity datasets (each producing 1 kWh)
   :header-rows: 1
   :widths: 45 30 25

   * - Dataset name
     - Installation types
     - Reference product
   * - ``electricity production, photovoltaic, residential``
     - 10 kWp rooftop and facade systems
     - ``electricity, low voltage``
   * - ``electricity production, photovoltaic, commercial``
     - 250 kWp rooftop and 10 MW ground-mounted systems
     - ``electricity, medium voltage``
   * - ``electricity production, photovoltaic, production mix``
     - Combined residential and commercial generation
     - ``electricity, photovoltaic, at plant``

These voltage assignments define the generation boundary. No additional
transformer inventory or unreported distribution loss is introduced.
The existing IAM aliases select the residential and commercial datasets.

Transformation
----------------

Country mixes, yields and lifetime
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The extension covers **250 locations: 249 ISO countries and territories,
plus Kosovo**. For the 33 countries represented in the report, installation
inputs are converted back to electricity shares by multiplying each input
amount by installation capacity, annual yield and a 30-year lifetime.
Shares are normalised to remove source rounding and separately normalised
within the residential and commercial segments.

For other countries, the technology and installation recipe is the mean
of the 33 report-country recipes, weighted by their **2023 PV electricity
generation** from `IRENA Renewable Energy Statistics 2025
<https://www.irena.org/Publications/2025/Jul/Renewable-energy-statistics-2025>`_.
This is a proxy composition, not a measured national technology mix.
Manufacturing suppliers retain their source geographies; extending an
electricity dataset to a country does not imply domestic module production.

Annual yields are assigned in this order:

* Report-country yields take precedence where available.
* Elsewhere, ground-mounted yields are inherited from the Global Solar Atlas
  values stored in ``lci-PV.xlsx``. Rooftop and facade yields are,
  respectively, 0.94 and 0.66 times the ground-mounted yield.
* Missing or zero Atlas values use the generation-weighted mean report yield.
  Dataset comments explicitly identify this fallback.

The Atlas values are retained numerical inputs, not a newly downloaded
release. The parameters and their sources are recorded in
``premise/data/solar/pv_2026_parameters.json``; the IRENA generation data are
in ``premise/data/solar/pv_generation_2023.csv``.

For an installation type :math:`i` in country :math:`c`, its input per kWh is:

.. math::

   a_{i,c} = \frac{s_{i,c}}{P_i \, Y_{i,c} \, L}

where :math:`s_{i,c}` is its normalised **electricity share** in the selected
mix, :math:`P_i` is capacity in kWp, :math:`Y_{i,c}` is annual yield in
kWh/kWp/year, and :math:`L = 30` years. For example, a 10 kWp installation
with a 20% electricity share and a yield of 1000 kWh/kWp/year contributes
:math:`0.2 / (10 \times 1000 \times 30) = 6.67 \times 10^{-7}`
installation units per kWh.

.. note::

   The source coefficients use lifetime multiplied by annual yield.
   Although source comments mention 0.7% annual degradation, no additional
   degradation multiplier is applied. This preserves the workbook's
   numerical convention; whether the reported yield already includes
   lifetime degradation remains to be clarified with the authors.

Cleaning in the extensions uses 20 litres of water per square metre of
module over its lifetime: 90% becomes wastewater and 10% evaporates.
Extensions beyond the report countries use Swiss water and wastewater
suppliers as explicit proxies.

Production volumes used for regional aggregation are 2023 generation in
GWh, multiplied by one million and by the relevant segment's electricity
share. Missing or rounded-zero generation observations retain zero volume
and do not contribute to the donor weights. A segment with zero historical
share receives a mean segment recipe so that it remains available as a
prospective supplier, while its historical production volume stays zero.

Supplier linking and modelling adaptations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The source inventories use UVEK/BAFU background suppliers. Their exchanges
are linked to ecoinvent 3.12 cut-off through crosswalks and explicit
supplier choices. These include geographic proxies and explicit unit
conversions, not only dataset renaming. Examples include:

.. list-table:: Examples of background supplier adaptations
   :header-rows: 1
   :widths: 35 45 20

   * - Source supplier
     - ecoinvent supplier or proxy
     - Amount adjustment
   * - Medium-voltage grid electricity, CN
     - ``market group for electricity, medium voltage``, CN
     - Unchanged
   * - Ammonia, liquid, at regional storehouse, CH
     - ``market for ammonia, anhydrous, liquid``, RER
     - Unchanged
   * - Silicon tetrachloride, DE
     - ``silicon tetrachloride production``, GLO
     - Unchanged
   * - Hard fibreboard, CH, in kg
     - Fibreboard production, in cubic metres
     - Divide by 900 kg/m³
   * - Gypsum board, CH, in square metres
     - Plasterboard production, in kg
     - Multiply by 10 kg/m²

Waste-treatment exchanges also receive the sign required by the target
waste-product convention. Where no direct aggregate supplier is available,
explicit helper activities combine background production routes. Liquid
hydrogen, for example, is approximated by 1 kg of steam-methane-reforming
hydrogen plus 12 kWh of electricity for liquefaction, without additional
liquefaction infrastructure or boil-off. These choices can affect impacts
and should be considered when comparing against results obtained with the
original background database.

The US multi-Si wafer market uses domestic US wafer production and retains
transport inputs. The core uses cut-off allocated recycling. Separate CIGS,
perovskite and GaAs supplements remain available under their own selection
rules.

Scenario updates and validation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Country technology and installation shares remain fixed over time because
IAM scenarios do not specify these detailed sub-technology mixes. Background
supply chains and module efficiencies are updated for the scenario year.
See :ref:`pv-efficiency-transformation` for efficiency trajectories.

Installation efficiency is calculated from explicit capacity and total module
area across all module inputs. When efficiency increases, module area,
associated mounting, module recycling and lifetime cleaning scale together;
capacity-dependent equipment remains unchanged. Cleaning scales relative
to its original coefficient. Existing efficiencies above the scenario
assumption are not reduced.

The 20 silicon-cell activities in the new core preserve their source material
inputs during the metals update. Metallization pastes and other components
already contain metals; applying generic kg/MW material intensities directly
to a square metre of cell would add incorrect amounts and double-count
materials. This preservation does not disable background scenario updates.

Module-efficiency trajectories and scaling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Photovoltaic installation and construction datasets are updated from the
module-efficiency trajectories stored in
``premise/data/renewables/efficiency_solar_PV.csv``. The file provides the technology-specific year and efficiency values.

*premise* currently reads only the columns ``technology``, ``year``, ``mean``,
``min`` and ``max`` when building the efficiency time series. Other metadata columns do not filter or change the efficiency trajectory.

The current anchor years in the CSV are 2010, 2020, 2023, 2025, 2027, 2030,
2035 and 2050. Selected configured values are:

.. list-table:: Selected PV efficiency values
   :header-rows: 1

   * - Technology family
     - Anchor years
     - Notes
   * - ``single-Si``
     - 2025, 2035
     - 25.4% module record in 2025 (NREL Champion Module Efficiencies,
       revision 2024-12-18) and a 25.23% mainstream silicon projection in 2035
       (NREL Spring 2025 Solar Industry Update / ITRPV).
   * - ``CIGS`` and ``CdTe``
     - 2025
     - 2025 record-module anchors of 19.2% and 19.9%, respectively.
   * - ``GaAs``
     - 2010, 2020, 2025
     - The 2025 module-efficiency anchor is 25.1%.
   * - ``perovskite``
     - 2025
     - 21.1% single-junction module record in 2025; kept separate from tandem
       modules.
   * - ``perovskite-Si tandem``
     - 2027, 2030, 2035
     - Tandem trajectory: 27.0%, 30.0%
       and 30.5%.
   * - ``multi-Si``
     - 2025
     - The 2025 module-efficiency anchor is 20.4%.
   * - ``micro-Si`` and ``CIS``
     - existing 2010, 2020, 2050 anchors
     - Separate technology trajectories.

The CSV combines Fraunhofer ISE / literature values, NREL champion-module
records and tandem roadmap projections (ITRPV and Oxford PV) for near- and medium-term updates.
The main references currently cited in the CSV are:

* Historical thin-film and silicon anchors: IEA PV roadmap
  (https://iea.blob.core.windows.net/assets/3a99654f-ffff-469f-b83c-bf0386ed8537/pv_roadmap.pdf),
  Treeze / IEA PVPS Task 12
  (https://treeze.ch/fileadmin/user_upload/downloads/Publications/Case_Studies/Energy/Future-PV-LCA-IEA-PVPS-Task-12-March-2015.pdf)
  and Fraunhofer ISE Photovoltaics Report
  (https://www.ise.fraunhofer.de/content/dam/ise/de/documents/publications/studies/Photovoltaics-Report.pdf).
* CdTe anchors: Solar Energy Materials and Solar Cells article
  (https://www.sciencedirect.com/science/article/pii/S0927024823001101).
* GaAs long-term anchor: The International Journal of Life Cycle Assessment
  article (https://link.springer.com/article/10.1007/s11367-020-01791-z).
* Perovskite long-term anchor: RSC Energy & Environmental Science article
  (https://pubs.rsc.org/en/content/articlelanding/2022/se/d2se00096b) and CSEM
  note on a 31.25% cell result
  (https://www.csem.ch/en/news/photovoltaic-technology-breakthrough-achieving-31.25-efficiency/).
* 2025 record-module anchors for ``single-Si``, ``multi-Si``, ``CIGS``,
  ``CdTe``, ``GaAs`` and ``perovskite``: NREL Champion Module Efficiencies,
  revision 2024-12-18
  (https://www.nrel.gov/docs/libraries/pv/champion-module-efficiencies.pdf).
* 2035 mainstream silicon and tandem projections: NREL Spring 2025 Solar
  Industry Update / ITRPV (https://docs.nrel.gov/docs/fy25osti/95135.pdf).
* 2027 tandem commercialization anchor: ITRPV 15th edition 2024
  (https://www.qualenergia.it/wp-content/uploads/2024/06/ITRPV-15th-Edition-2024-2.pdf).
* 2030 tandem midpoint: Oxford PV roadmap page
  (https://www.oxfordpv.com/mainstream).

For full row-level attribution, refer directly to
``premise/data/renewables/efficiency_solar_PV.csv``.

.. figure:: /pv_module_efficiency_plot.png
   :alt: Photovoltaic module efficiency trajectories used in premise
   :width: 100%
   :align: center

   Overview of the photovoltaic module-efficiency trajectories currently
   encoded in ``premise/data/renewables/efficiency_solar_PV.csv``, including
   source labels and min-max uncertainty bands.

For the imported inventories and the scope of the IEA PVPS 2026 update, see
:doc:`/reference/inventories`, section "Photovoltaic panels". Efficiency-trajectory sources
are recorded in ``premise/data/renewables/efficiency_solar_PV.csv``.

Given a scenario year, *premise* iterates through the different PV panel installation
datasets to update their efficiency accordingly.
To do so, the required panel area (in m2) per kW of capacity is
adjusted down when the projected efficiency exceeds the current efficiency.
Existing higher efficiencies are preserved. Dataset names
are matched against technology aliases in ``premise/electricity.py``. Datasets containing ``perovskite-on-silicon tandem`` are mapped
to the dedicated ``perovskite-Si tandem`` trajectory when it is available in
the CSV; when that trajectory is absent they fall back to the generic
``perovskite`` trajectory.

To calculate the current efficiency of a PV installation, *premise* assumes a solar
irradiation of 1000 W/m2. The current efficiency is calculated as::

    current_eff [fraction] = installation_power [W] / (panel_surface [m2] * 1000 [W/m2])

The *scaling factor* is calculated as::

    scaling_factor = current_eff / new_eff

The required PV panel area in the dataset is then adjusted as follows::

    new_surface = current_surface * scaling_factor

The mean, minimum and maximum module efficiencies are propagated as a
triangular uncertainty on the panel area exchange. For years between anchor
points, *premise* interpolates the efficiency values linearly. For years
outside the CSV range, it extrapolates them linearly and clips the resulting
efficiencies to the 10-30% interval. The update is applied only when the
projected mean efficiency is higher than the efficiency inferred from the
existing dataset.


The table below provides such an example where a 450 kWp flat-roof installation
sees its 2020 reference module efficiency improving from 20% to 27% by 2050.
The area of PV panel (and mounting system) and the end-of-life treatment flow
are multiplied by ``0.20 / 0.27 = 0.74``, all other inputs remaining unchanged.

 =================================================================== ========= ======== =======
  450kWp flat roof installation                                       before    after    unit
 =================================================================== ========= ======== =======
  photovoltaic flat-roof installation, 450 kWp, single-SI, on roof    1         1        unit
  inverter production, 500 kW                                         1.5       1.5      unit
  photovoltaic mounting system, …                                     2300      1704     m2
  photovoltaic panel, single-SI                                       2500      1852     m2
  treatment, single-SI PV module                                      30000     22222    kg
  electricity, low voltage                                            25        25       kWh
  module efficiency                                                   20%       27%      %
 =================================================================== ========= ======== =======

Markets and downstream links
------------------------------

The residential and commercial country activities are mapped into regional
electricity supply. Their production volumes weight available country
suppliers; IAM generation volumes determine the broader technology shares.
The combined country production mix has a distinct at-plant reference product.
See :doc:`markets` for its relationship to the voltage layers.

Assumptions and limitations
-----------------------------

Country extensions use explicit technology and supplier proxies. Detailed
sub-technology shares and lifetime/yield conventions are fixed inventory
assumptions, not IAM projections. The degradation convention and source
geographies below must be retained when interpreting comparisons. The metals
update preserves the documented silicon-cell material inputs.

Worked example and checks
---------------------------

The installation-input calculation below illustrates electricity-share
weighting. For scenario checks, compare capacity and module area before and
after the update: area-dependent mounting, cleaning and recycling should
follow the area adjustment, while capacity-dependent equipment stays fixed.
For an impact comparison, use the same reference system, yield, lifetime
and background database before attributing a difference to module efficiency.

Sources and inventory details
-------------------------------

Inventory preparation
~~~~~~~~~~~~~~~~~~~~~~~

For **ecoinvent 3.12 cut-off**, *premise* imports the inventories from
`IEA PVPS Task 12, report T12-33:2026 <https://doi.org/10.69766/WJTE1771>`_
and its accompanying life-cycle inventory workbook.
Other ecoinvent versions and the consequential system model continue to use
``lci-PV.xlsx`` inventories, under their current import rules.

The packaged inventories are:

.. list-table:: PV inventory files
   :header-rows: 1
   :widths: 35 65

   * - File
     - Contents
   * - ``lci-PV-2026.xlsx``
     - 324 activities covering manufacturing, installations, operation and
       end-of-life, including the 45 original electricity functional units.
   * - ``lci-PV-2026-electricity.xlsx``
     - Residential, commercial and combined electricity datasets for 250
       countries and territories, plus one compatibility activity for the
       existing hydrogen model.
   * - ``lci-PV-CIGS.xlsx``
     - Nine activities for the distinct CIS/CIGS supplement
       and its foreground dependencies.

The workbooks are in ``premise/data/additional_inventories/``. Dataset names
and reference products follow ecoinvent conventions. The ``source`` field
identifies the publication; comments describe the dataset and any linking
or modelling assumptions. Source-material licence terms remain separate
from the *premise* software licence: the report specifies CC BY-NC-SA 4.0,
subject to the exceptions in its licence notice.

Separate emerging-technology supplements
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Emerging technologies for photovoltaic panels are also imported, namely:

* Gallium Arsenide (GaAs) panels, with a conversion efficiency of 28%, from `Pallas <https://doi.org/10.1007/s11367-020-01791-z>`__ et al., 2020.
* Perovskite-on-silicon tandem panels, with a conversion efficiency of 25%, from `Roffeis <https://doi.org/10.1039/D2SE90051C>`__ et al., 2022.

They are available in the following locations:

 ============================================================================================ ===========
  Emerging PV technologies                                                                     location
 ============================================================================================ ===========
  electricity production, photovoltaic, 0.28kWp, GaAs                                          GLO
  electricity production, photovoltaic, 0.5kWp, perovskite-on-silicon tandem                   RER
 ============================================================================================ ===========

.. note::

    These two technologies are not included in the current country-specific production mix datasets, as IAM scenarios do not specify sub-technology mixes.

.. raw:: html

   <span id="source-provenance-and-currency"></span>

.. _pv-efficiency-transformation:

Sources and data dates
~~~~~~~~~~~~~~~~~~~~~~~~

.. include:: /reference/generated/source-pv.inc
