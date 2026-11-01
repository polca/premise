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
The 2026 core is selected for all supported ecoinvent versions and system
models. Premise applies its migration and consequential mapping data to link
the inventories to the target database.

Import audits cover ecoinvent 3.8–3.11 cut-off and consequential, and 3.12
consequential. The 3.8 imports have no unlinked technosphere exchanges but omit
nine nitric-acid air emissions per import because the flow is unavailable in
the 3.8 biosphere. The audited 3.9–3.11 and 3.12 consequential imports have no
unlinked exchanges or ignored biosphere flows. Automatic inventory selection
for older versions does not imply that their imports have been validated.

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

GaAs substrate reuse and recurring processing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The transfer-route reference amortizes manufacture of the complete original
GaAs substrate over 100 uses: each square metre processed purchases 0.01 m²
of substrate manufacture. Lithography, gold deposition, growth and epitaxial
lift-off/polishing are charged for every processed m². The previous installation
instead purchased 0.01 m² of the entire processing chain, dividing recurring
operations by 100 as well. Intermediate scores before and after this boundary
correction therefore do not describe the same supplied processing service.

Pallas et al. (2020), sections 2.7–2.10, assume 100 reuses prospectively and
reports 5–10 achieved at that time. Retaining 100 uses is a future technology
reference, not demonstrated commercial durability or an attrition model.
The baseline retains polishing after each lift-off. Polishing every ten cycles
and 90% gold recovery are separate optimization cases and are not silently
included in this reference.

Substrate-reuse sensitivity was assessed with unchanged recurring process coefficients and background feedback. Numerical LCIA outputs are retained locally.

GaAs fresh-wafer material balance and supporting references
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The published supplementary S6 recipe declares one kg of GaAs from 0.236 kg
gallium and 0.516 kg arsenic. That declared feed does not close the Ga/As mass
balance; the original author's batch interpretation remains unverified. The
active balanced engineering reference uses the GaAs stoichiometric retained
fractions 0.482029764 kg Ga and 0.517970236 kg As per kg product. At the selected
95% overall material retention it purchases 0.507399752 kg Ga and 0.545231827 kg
As, assigning 0.052631579 kg process loss to a hazardous-disposal proxy.
This is a distinct engineering reference, not a verbatim reproduction of S6.
Separate upstream refining and downstream crystal-growth/sawing losses remain.

Boule-retention sensitivity holds substrate reuse and other inputs fixed. Hazardous underground disposal is a fate proxy; actual Ga/As volatilization, recovery and treatment remain unresolved. Numerical LCIA outputs are retained locally.

The remaining supporting references are retained with their documented limits:

* The crucible is pyrolytic boron nitride (BN). The source labelled it pBN but
  linked boron carbide (B4C). The current CVD engineering reference retains
  0.01 kg crucible per kg boule in both active growth stages and keeps each
  associated disposal purchase once. Its 50% precursor deposition utilization,
  95% finished-solid retention and 250 kWh/kg electricity are engineering
  assumptions. High-salinity scrubber treatment, fugitive emissions and
  semiconductor-grade precursor finishing remain qualified gaps.
* Gallium refining retains the source 1.4285714 kg feed and electricity per kg
  7N gallium, explicitly collecting the 0.4285714 kg reject without a recovery
  credit. This does not establish actual industrial reject fate.
* The arsenic proxy converts the source's 1.4705882 kg elemental-As feed to
  1.529944367 kg arsine using its As content. The conversion records As reject
  and liberated hydrogen separately. Original utilities/purge allowances are
  retained; the actual refining route, purge fates and treatment are unvalidated.
* Module assembly adds a cell-free assembly reference once, including glass,
  encapsulation, frame, interconnections and processing. The existing 1 m² Si
  cell, mounting and inverter purchases remain separate. The transfer of a
  conventional module assembly to 1 m² active tandem cell per nominal module
  m² is an explicit geometry proxy; packing, optical fill factor and nano-layer
  end-of-life are unresolved. The 0.28 kWp output and electricity denominator
  are unchanged.

Utility-PV transformer pressboard units
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The 2.5 MW transformer reference in ``lci-PV-2026.xlsx`` specifies 36.96 kg
pressboard but purchases a hard-fibreboard proxy whose unit is m³. The current
0.03696 m³ purchase uses an explicit density of 1,000 kg/m³, correcting the
former 36.96 m³ purchase. The documented density sensitivity of 800–1,040 kg/m³
corresponds to 0.0462–0.0355385 m³. The original 71.07 kg cellulose disposal
(36.96 kg pressboard plus 34.11 kg kraft paper) is retained. This unit correction
leaves transformer quantity, other materials, operating performance and the PV
electricity denominator unchanged; the fibreboard grade remains a proxy.

.. raw:: html

   <span id="source-provenance-and-currency"></span>

.. _pv-efficiency-transformation:

Sources and data dates
~~~~~~~~~~~~~~~~~~~~~~~~

.. include:: /reference/generated/source-pv.inc
