EXTRACT
=======

The **EXTRACT** phase consists of the following steps:

* Extraction and cleaning of the ecoinvent database
* Import and cleaning of additional inventories
* Import and cleaning of user-provided inventories (optional)
* Caching, if these database and inventories are imported for the first time
* Loading of IAM data

Current IAM scenarios
"""""""""""""""""""""

*premise* includes several Integrated Assessment Model (IAM) scenarios, but you can also use other scenarios.
For a detailed description of the models and scenarios available, see the Introduction_.

.. _Introduction: introduction.rst

.. note::

    A summary report of the main variables of the scenarios
    selected is generated automatically after each database export.
    There is also an `online dashboard <https://premisedash-6f5a0259c487.herokuapp.com/>`_.
    You can also generate it manually:

.. code-block:: python

    ndb = NewDatabase(...)
    ndb.generate_scenario_report()


Supported versions of ecoinvent
"""""""""""""""""""""""""""""""

*premise* currently works with the following ecoinvent database versions:

* **v.3.5, cut-off**
* **v.3.6, cut-off**
* **v.3.7 and v.3.7.1, cut-off**
* **v.3.8, cut-off and consequential**
* **v.3.9 and v.3.9.1, cut-off and consequential**
* **v.3.10 and v.3.10.1, cut-off and consequential**
* **v.3.11, cut-off and consequential**
* **v.3.12, cut-off and consequential**


Supported sources of ecoinvent
""""""""""""""""""""""""""""""

*premise* can extract the ecoinvent database from:

* a brightway2_ project that contains the ecoinvent database
* ecosposld2 files, that can be downloaded from the ecoinvent_ website

.. _ecoinvent: https://ecoinvent.org
.. _brightway2: https://brightway.dev/


.. note::

        The ecoinvent database is not included in *premise*.
        You need to have a valid license to download and use it.
        Also, please read carefully ecoinvent's EULA_ before using *premise*.

.. _EULA: https://ecoinvent.org/app/uploads/2024/01/EULA_new_branding_08_11_2023.pdf


From a brightway2 project
-------------------------

To extract from an ecoinvent database located in a brightway2 project, simply
indicate the database name in `source_db` and its version in `source_version`:

.. code-block:: python

  from premise import *
  import brightway2 as bw

  bw.projects.set_current("my_project")

  ndb = NewDatabase(
        scenarios=[
                {"model":"remind", "pathway":"SSP2-Base", "year":2028}
            ],
        source_db="ecoinvent 3.7 cutoff", # <-- this is NEW.
        source_version="3.7.1", # <-- this is NEW
        key='xxxxxxxxxxxxxxxxxxxxxxxxx',
        keep_imports_uncertainty=True, # True by default, set to False to drop uncertainty in additional inventories
        keep_source_db_uncertainty=False # False by default, set to True if you want to keep ecoinvent's uncertainty data
    )

.. note::

    ``source_db`` must match the **name of the database in your Brightway project** (it is not a fixed string).

Note that a cache of the database will be created the first time and
stored in the library folder. Any subsequent creation of databases
using the same ecoinvent version will no longer require this extraction
step.

If you wish to clear that cache folder (database and *premise* additional inventories), do:

.. code-block:: python

    from premise import *

    clear_cache()



To clear only the *premise* additional inventories, do:

.. code-block:: python

    from premise import *

    clear_inventory_cache()

.. note::

    After a version update, databases and inventories are automatically
    re-extracted and re-imported. This is to ensure that the data is
    consistent with the new version of *premise*.


From ecospold2 files
--------------------

To extract from a set of ecospold2 files, you need to point to the location of
those files in `source_file_path`, as well as indicate the database format in
`source_type`:

.. code-block:: python

    from premise import *

    ndb = NewDatabase(
        scenarios = [
            {"model":"remind", "pathway":"SSP2-Base", "year":2028}
                    ],
        source_type="ecospold", # <--- this is NEW
        source_file_path=r"C:\file\path\to\ecoinvent 3.5_cutoff_ecoSpold02\datasets", # <-- this is NEW
        source_version="3.5",
    )

.. note::

    When using ``source_type="ecospold"``, no Brightway project or biosphere
    database is needed unless you later export the result back to Brightway.

Import of additional inventories
""""""""""""""""""""""""""""""""

After the ecoinvent database is extracted and checked, a number of additional inventories
are imported, regardless of the year of scenario that is being considered.

All inventories can be found in the `premise/data/additional_inventories`_ folder.

.. _premise/data/additional_inventories: https://github.com/polca/premise/tree/master/premise/data/additional_inventories


Power generation
----------------

A number of  datasets relating to power generation not originally present in
ecoinvent are imported. The next sub-sections lists such datasets.

Power plants with CCS
*********************

Datasets for power generation with Carbon Capture and Storage (CCS) are imported.
They originate from Volkart_ et al. 2013, and can be consulted here: LCI_Power_generation_.
An exception to this are the inventories for biomass-based integrated gasification combined cycle power plants (BIGCCS),
which are from Briones-Hidrovo_ et al, 2020.

.. _Volkart: https://doi.org/10.1016/j.ijggc.2013.03.003
.. _Briones-Hidrovo: https://doi.org/10.1016/j.jclepro.2020.125680
.. _LCI_Power_generation: https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-Carma-CCS.xlsx

The table below lists the names of the new activities (only production datasets are shown).

 ============================================================================================================= ===========
  Power generation with CCS (activities list)                                                                   location
 ============================================================================================================= ===========
  electricity production, at power plant/hard coal, IGCC, no CCS                                                RER
  electricity production, at power plant/hard coal, PC, no CCS                                                  RER
  electricity production, at power plant/hard coal, oxy, pipeline 200km, storage 1000m                          RER
  electricity production, at power plant/hard coal, oxy, pipeline 400km, storage 3000m                          RER
  electricity production, at power plant/hard coal, post, pipeline 200km, storage 1000m                         RER
  electricity production, at power plant/hard coal, post, pipeline 400km, storage 1000m                         RER
  electricity production, at power plant/hard coal, post, pipeline 400km, storage 3000m                         RER
  electricity production, at power plant/hard coal, pre, pipeline 200km, storage 1000m                          RER
  electricity production, at power plant/hard coal, pre, pipeline 400km, storage 3000m                          RER
  electricity production, at power plant/lignite, IGCC, no CCS                                                  RER
  electricity production, at power plant/lignite, PC, no CCS                                                    RER
  electricity production, at power plant/lignite, oxy, pipeline 200km, storage 1000m                            RER
  electricity production, at power plant/lignite, oxy, pipeline 400km, storage 3000m                            RER
  electricity production, at power plant/lignite, post, pipeline 200km, storage 1000m                           RER
  electricity production, at power plant/lignite, post, pipeline 400km, storage 3000m                           RER
  electricity production, at power plant/lignite, pre, pipeline 200km, storage 1000m                            RER
  electricity production, at power plant/lignite, pre, pipeline 400km, storage 3000m                            RER
  electricity production, at power plant/natural gas, ATR H2-CC, no CCS                                         RER
  electricity production, at power plant/natural gas, NGCC, no CCS/kWh                                          RER
  electricity production, at power plant/natural gas, post, pipeline 200km, storage 1000m                       RER
  electricity production, at power plant/natural gas, post, pipeline 400km, storage 1000m                       RER
  electricity production, at power plant/natural gas, post, pipeline 400km, storage 3000m                       RER
  electricity production, at power plant/natural gas, pre, pipeline 200km, storage 1000m                        RER
  electricity production, at power plant/natural gas, pre, pipeline 400km, storage 3000m                        RER
  electricity production, at wood burning power plant 20 MW, truck 25km, no CCS                                 RER
  electricity production, at wood burning power plant 20 MW, truck 25km, post, pipeline 200km, storage 1000m    RER
  electricity production, at wood burning power plant 20 MW, truck 25km, post, pipeline 400km, storage 3000m    RER
 ============================================================================================================= ===========


Natural gas
***********

Updated inventories relating to natural gas extraction and distribution
are imported to substitute some of the original ecoinvent dataset.
These datasets originate from ESU Services and come with a report_,
and can be consulted here: LCI_Oil_NG_.

.. _LCI_Oil_NG: https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-ESU-oil-and-gas.xlsx

They have been adapted to a brightway2-compatible format.
These new inventories have, among other things, higher methane slip
emissions along the natural gas supply chain, especially at extraction.

.. _report: http://www.esu-services.ch/fileadmin/download/publicLCI/meili-2021-LCI%20for%20the%20oil%20and%20gas%20extraction.pdf


 ========================================================== ==============================================================
  Original dataset                                           Replaced by
 ========================================================== ==============================================================
  natural gas production (natural gas, high pressure), DE    natural gas, at production (natural gas, high pressure), DE
  natural gas production (natural gas, high pressure), DZ    natural gas, at production (natural gas, high pressure), DZ
  natural gas production (natural gas, high pressure), US    natural gas, at production (natural gas, high pressure), US
  natural gas production (natural gas, high pressure), RU    natural gas, at production (natural gas, high pressure), RU
  petroleum and gas production, GB                           natural gas, at production (natural gas, high pressure), GB
  petroleum and gas production, NG                           natural gas, at production (natural gas, high pressure), NG
  petroleum and gas production, NL                           natural gas, at production (natural gas, high pressure), NL
  petroleum and gas production, NO                           natural gas, at production (natural gas, high pressure), NO
 ========================================================== ==============================================================

The original natural gas datasets are preserved, but they do not provide input to any
other datasets in the database. The new datasets provide natural gas at high pressure to
the original supply chains, which remain unchanged.

The table below lists the names of the new activities (only high pressure datasets are shown).

 ============================= ===========
  Natural gas extraction        location
 ============================= ===========
  natural gas, at production    AZ
  natural gas, at production    RO
  natural gas, at production    LY
  natural gas, at production    SA
  natural gas, at production    IQ
  natural gas, at production    RU
  natural gas, at production    NL
  natural gas, at production    DZ
  natural gas, at production    NG
  natural gas, at production    DE
  natural gas, at production    KZ
  natural gas, at production    NO
  natural gas, at production    QA
  natural gas, at production    GB
  natural gas, at production    MX
  natural gas, at production    US
 ============================= ===========

.. note::

    This import does not occur when using ecoinvent v.3.9
    as those dataset updates are already included.

Photovoltaic panels
*******************

For **ecoinvent 3.12 cut-off**, *premise* imports the inventories from
`IEA PVPS Task 12, report T12-33:2026 <https://doi.org/10.69766/WJTE1771>`_
and its accompanying life-cycle inventory workbook. These replace the legacy
IEA PVPS core inventory previously imported from ``lci-PV.xlsx``.
Other ecoinvent versions and the consequential system model continue to use
the legacy inventory, pending separate migration and validation.

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
     - Nine activities preserving the distinct legacy CIS/CIGS supplement
       and its foreground dependencies.

The workbooks are in ``premise/data/additional_inventories/``. Dataset names
and reference products follow ecoinvent conventions. The ``source`` field
identifies the publication; comments describe the dataset and any linking
or modelling assumptions. Source-material licence terms remain separate
from the *premise* software licence: the report specifies CC BY-NC-SA 4.0,
subject to the exceptions in its licence notice.

Technologies and reference systems
++++++++++++++++++++++++++++++++++++++

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

Country mixes, yields and lifetime
++++++++++++++++++++++++++++++++++++++

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
  values stored in the legacy workbook. Rooftop and facade yields are,
  respectively, 0.94 and 0.66 times the ground-mounted yield.
* Missing or zero Atlas values use the generation-weighted mean report yield.
  Dataset comments explicitly identify this fallback.

The Atlas values are retained numerical inputs, not a newly downloaded
release. The parameters and their provenance are recorded in
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
++++++++++++++++++++++++++++++++++++++++++++++

The source inventories use UVEK/BAFU background suppliers. Their exchanges
are linked to ecoinvent 3.12 cut-off through existing crosswalks and reviewed
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

The empty US multi-Si wafer market is supplied by the report's **domestic
US wafer production**, retaining transport. Unused historical battery
inventories, avoided-burden processes, the report's CIGS branch, and its
perovskite-silicon tandem branch and exclusive dependencies are removed
from the reviewed core. Cut-off allocated recycling remains. The separate
legacy CIGS, perovskite and GaAs supplements remain available.

Scenario updates and validation
+++++++++++++++++++++++++++++++++++

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

The 45 original electricity functional units have calculated ecoinvent 3.12
cut-off GWP100 regression benchmarks in
``premise/data/solar/pv_2026_gwp100_reference.csv``. These are *premise*
validation results, **not characterized scores published in the report**.
``scripts/validate_pv_2026.py`` checks the imported inventory graph, these
benchmarks, and an all-sector IMAGE SSP2-M 2050 scenario. Rebuilding the
country inventories uses ``scripts/build_pv_2026.py``; further reproduction
notes are in ``premise/data/solar/README.txt``.

Compared with the legacy inventory, GWP changes can reflect manufacturing
inventories, supplier geography, inverters, technology shares and yields,
as well as module efficiency. They should not be attributed to efficiency
improvements alone.

.. _IEA_PV: https://iea-pvps.org/wp-content/uploads/2020/12/IEA-PVPS-LCI-report-2020.pdf

Separate emerging-technology supplements
++++++++++++++++++++++++++++++++++++++++++++

Emerging technologies for photovoltaic panels are also imported, namely:

* Gallium Arsenide (GaAs) panels, with a conversion efficiency of 28%, from Pallas_ et al., 2020.
* Perovskite-on-silicon tandem panels, with a conversion efficiency of 25%, from Roffeis_ et al., 2022.

They are available in the following locations:

 ============================================================================================ ===========
  Emerging PV technologies                                                                     location
 ============================================================================================ ===========
  electricity production, photovoltaic, 0.28kWp, GaAs                                          GLO
  electricity production, photovoltaic, 0.5kWp, perovskite-on-silicon tandem                   RER
 ============================================================================================ ===========

.. _Pallas: https://doi.org/10.1007/s11367-020-01791-z
.. _Roffeis: https://doi.org/10.1039/D2SE90051C

.. note::

    These two technologies are not included in the current country-specific production mix datasets, as IAM scenarios do not specify sub-technology mixes.

Geothermal
**********

Heat production by means of a geothermal well are not represented in ecoinvent.
The geothermal power plant construction inventories are from Maeder_ Bachelor Thesis.

.. _Maeder: https://www.psi.ch/sites/default/files/import/ta/PublicationTab/BSc_Mattia_Maeder_2016.pdf

The co-generation unit has been removed and replaced by heat exchanger and
district heating pipes. Gross heat output of 1,483 TJ, with 80% efficiency.

The inventories can be consulted here: LCIgeothermal_.

.. _LCIgeothermal: https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-geothermal.xlsx

They introduce the following datasets (only heat production datasets shown):

 =================================== ===========
  Geothermal heat production          location
 =================================== ===========
  heat production, deep geothermal    RAS
  heat production, deep geothermal    GLO
  heat production, deep geothermal    RAF
  heat production, deep geothermal    RME
  heat production, deep geothermal    RLA
  heat production, deep geothermal    RU
  heat production, deep geothermal    CA
  heat production, deep geothermal    JP
  heat production, deep geothermal    US
  heat production, deep geothermal    IN
  heat production, deep geothermal    CN
  heat production, deep geothermal    RER
 =================================== ===========


Hydrogen
--------

Hydrogen production
*******************

*premise* imports inventories for hydrogen production. The table below
gives an overview of the different pathways and their assumed specific energy use
in 2020 and 2050.


+-------------------------------------------------------------------------------------------------------------------+-------------+-----+----------+----------+----------+----------+-------+-----+------------------------------------+
| Dataset                                                                                                           | Feedstock   | U   | 2020 avg | 2020 rng | 2050 avg | 2050 rng |Floor  |Loc  | Literature reference               |
+===================================================================================================================+=============+=====+==========+==========+==========+==========+=======+=====+====================================+
| hydrogen production, steam methane reforming                                                                      | natural gas | m^3 |   N/A    |   N/A    |   N/A    |   N/A    |  3.5  | CH  | Antonini_ et al. 2021 [LCI_SMR_]   |
+-------------------------------------------------------------------------------------------------------------------+-------------+-----+----------+----------+----------+----------+-------+-----+------------------------------------+
| hydrogen production, steam methane reforming, with CCS                                                            | natural gas | m^3 |   N/A    |   N/A    |   N/A    |   N/A    |  3.5  | CH  | Antonini_ et al. 2021 [LCI_SMR_]   |
+-------------------------------------------------------------------------------------------------------------------+-------------+-----+----------+----------+----------+----------+-------+-----+------------------------------------+
| hydrogen production, steam methane reforming, from biomethane                                                     | biomethane  | kg  |   N/A    |   N/A    |   N/A    |   N/A    |  3.2  | CH  | Antonini_ et al. 2021 [LCI_SMR_]   |
+-------------------------------------------------------------------------------------------------------------------+-------------+-----+----------+----------+----------+----------+-------+-----+------------------------------------+
| hydrogen production, steam methane reforming, from biomethane, with CCS                                           | biomethane  | kg  |   N/A    |   N/A    |   N/A    |   N/A    |  3.2  | CH  | Antonini_ et al. 2021 [LCI_SMR_]   |
+-------------------------------------------------------------------------------------------------------------------+-------------+-----+----------+----------+----------+----------+-------+-----+------------------------------------+
| hydrogen production, auto-thermal reforming, from biomethane                                                      | biomethane  | kg  |   N/A    |   N/A    |   N/A    |   N/A    |  3.2  | CH  | Antonini_ et al. 2021 [LCI_ATR_]   |
+-------------------------------------------------------------------------------------------------------------------+-------------+-----+----------+----------+----------+----------+-------+-----+------------------------------------+
| hydrogen production, auto-thermal reforming, from biomethane, with CCS                                            | biomethane  | kg  |   N/A    |   N/A    |   N/A    |   N/A    |  3.2  | CH  | Antonini_ et al. 2021 [LCI_ATR_]   |
+-------------------------------------------------------------------------------------------------------------------+-------------+-----+----------+----------+----------+----------+-------+-----+------------------------------------+
| hydrogen production, gaseous, 25 bar, from heatpipe reformer gasification of woody biomass with CCS               | wood chips  | kg  |   N/A    |   N/A    |   N/A    |   N/A    |  7.0  | CH  | Antonini2_ et al. 2021 [LCI_woody_]|
+-------------------------------------------------------------------------------------------------------------------+-------------+-----+----------+----------+----------+----------+-------+-----+------------------------------------+
| hydrogen production, gaseous, 25 bar, from heatpipe reformer gasification of woody biomass                        | wood chips  | kg  |   N/A    |   N/A    |   N/A    |   N/A    |  7.0  | CH  | Antonini2_ et al. 2021 [LCI_woody_]|
+-------------------------------------------------------------------------------------------------------------------+-------------+-----+----------+----------+----------+----------+-------+-----+------------------------------------+
| hydrogen production, gaseous, 25 bar, from gasification of woody biomass in entrained flow gasifier, with CCS     | wood chips  | kg  |   N/A    |   N/A    |   N/A    |   N/A    |  7.0  | CH  | Antonini2_ et al. 2021 [LCI_woody_]|
+-------------------------------------------------------------------------------------------------------------------+-------------+-----+----------+----------+----------+----------+-------+-----+------------------------------------+
| hydrogen production, gaseous, 25 bar, from gasification of woody biomass in entrained flow gasifier               | wood chips  | kg  |   N/A    |   N/A    |   N/A    |   N/A    |  7.0  | CH  | Antonini2_ et al. 2021 [LCI_woody_]|
+-------------------------------------------------------------------------------------------------------------------+-------------+-----+----------+----------+----------+----------+-------+-----+------------------------------------+
| hydrogen production, coal gasification                                                                            | hard coal   | kg  |   N/A    |   N/A    |   N/A    |   N/A    |  5.0  |RER  | Wokaun_, Li_ [LCI_coal_]           |
+-------------------------------------------------------------------------------------------------------------------+-------------+-----+----------+----------+----------+----------+-------+-----+------------------------------------+
| hydrogen production, gaseous, 30 bar, from PEM electrolysis, from grid electricity                                | electricity | kWh |   54.0   |52.9–55.1 |  48.9    |45.3–52.5 | 45.3  |RER  | Gerloff_ 2021 [LCI_electrolysis_]  |
+-------------------------------------------------------------------------------------------------------------------+-------------+-----+----------+----------+----------+----------+-------+-----+------------------------------------+
| hydrogen production, gaseous, 20 bar, from AEC electrolysis, from grid electricity                                | electricity | kWh |   51.8   |48.7–54.9 |  48.5    |47.1–49.9 | 47.1  |RER  | Gerloff_ 2021 [LCI_electrolysis_]  |
+-------------------------------------------------------------------------------------------------------------------+-------------+-----+----------+----------+----------+----------+-------+-----+------------------------------------+
| hydrogen production, gaseous, 1 bar, from SOEC electrolysis, from grid electricity                                | electricity | kWh |   42.3   |41.2–43.4 |  40.6    |40.0–41.2 | 40.0  |RER  | Gerloff_ 2021 [LCI_electrolysis_]  |
+-------------------------------------------------------------------------------------------------------------------+-------------+-----+----------+----------+----------+----------+-------+-----+------------------------------------+
| hydrogen production, gaseous, 1 bar, from SOEC electrolysis, with steam input, from grid electricity              | electricity | kWh |  42.3*   |41.2–43.4 |  40.6    |40.0–41.2 | 40.0  |RER  | Gerloff_ 2021 [LCI_electrolysis_]  |
+-------------------------------------------------------------------------------------------------------------------+-------------+-----+----------+----------+----------+----------+-------+-----+------------------------------------+
| hydrogen production, gaseous, 25 bar, from thermochemical water splitting, at solar tower                         | solar       | MJ  |   N/A    |   N/A    |   N/A    |   N/A    | 180   |RER  | Zhang2_ 2022                       |
+-------------------------------------------------------------------------------------------------------------------+-------------+-----+----------+----------+----------+----------+-------+-----+------------------------------------+
| hydrogen production, gaseous, 100 bar, from methane pyrolysis                                                     | natural gas | m^3 |   N/A    |   N/A    |   N/A    |   N/A    |  6.5  |RER  | Al-Qahtani_, Postels_              |
+-------------------------------------------------------------------------------------------------------------------+-------------+-----+----------+----------+----------+----------+-------+-----+------------------------------------+

Future efficiencies for electrolyzers are based on Studie IndWEDe_ (see p.176).
The SOEC inventory with a steam input uses the same performance assumptions as
the standard SOEC inventory because no separate performance data are available.

.. _Antonini: https://pubs.rsc.org/en/content/articlelanding/2020/se/d0se00222d
.. _Antonini2: https://pubs.rsc.org/en/Content/ArticleLanding/2021/SE/D0SE01637C
.. _Wokaun: https://www.cambridge.org/core/books/transition-to-hydrogen/43144AF26ED80E7106B675A6E83B1579
.. _Li: https://doi.org/10.1016/j.jclepro.2022.132514
.. _Gerloff: https://doi.org/10.1016/j.est.2021.102759
.. _Zhang2: https://doi.org/10.1016/j.ijhydene.2022.02.150
.. _Al-Qahtani: https://doi.org/10.1016/j.apenergy.2020.115958
.. _Postels: https://doi.org/10.1016/j.ijhydene.2016.09.167
.. _LCI_SMR: https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-hydrogen-smr-atr-natgas.xlsx
.. _LCI_ATR: https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-hydrogen-smr-atr-natgas.xlsx
.. _LCI_woody: https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-hydrogen-wood-gasification.xlsx
.. _LCI_coal: https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-hydrogen-coal-gasification.xlsx
.. _LCI_electrolysis: https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-hydrogen-electrolysis.xlsx
.. _IndWEDe: https://www.now-gmbh.de/wp-content/uploads/2020/09/indwede-studie_v04.1.pdf

Hydrogen storage and distribution
*********************************

A number of datasets relating to hydrogen storage and distribution are also imported.

They are necessary to model the distribution of hydrogen:

* via re-assigned transmission and distribution CNG pipelines, in a gaseous state
* via dedicated transmission and distribution hydrogen pipelines, in a gaseous state
* as a liquid organic compound, by hydrogenation
* via truck, in a liquid state
* hydrogen refuelling station

Small and large storage solutions are also provided:
* high pressure hydrogen storage tank
* geological storage tank

These datasets originate from the work of Wulf_ et al. 2018, and can be
consulted here: LCI_H2_distr_. For re-assigned CNG pipelines, which require the hydrogen
to be mixed together with oxygen to limit metal embrittlement,
some parameters are taken from the work of Cerniauskas_ et al. 2020.

The datasets introduced are listed in the table below.

 ================================================================== ===========
  Hydrogen distribution                                              location
 ================================================================== ===========
  hydrogen refuelling station                                        GLO
  high pressure hydrogen storage tank                                GLO
  pipeline, hydrogen, low pressure distribution network              RER
  compressor assembly for transmission hydrogen pipeline             RER
  pipeline, hydrogen, high pressure transmission network             RER
  zinc coating for hydrogen pipeline                                 RER
  hydrogenation of hydrogen                                          RER
  dehydrogenation of hydrogen                                        RER
  dibenzyltoluene production                                         RER
  solution mining for geological hydrogen storage                    RER
  geological hydrogen storage                                        RER
  hydrogen embrittlement inhibition                                  RER
  distribution pipeline for hydrogen, reassigned CNG pipeline        RER
  transmission pipeline for hydrogen, reassigned CNG pipeline        RER
 ================================================================== ===========


.. _Wulf: https://www.sciencedirect.com/science/article/pii/S095965261832170X
.. _LCI_H2_distr: https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-hydrogen-distribution.xlsx
.. _Cerniauskas: https://doi.org/10.1016/j.ijhydene.2020.02.121


Hydrogen turbine
****************

A dataset for a hydrogen turbine is also imported, to model the production of electricity
from hydrogen, with an efficiency of 51%. The efficiency of the H2-fed gas turbine is based
on the parameters of Ozawa_ et al. (2019), accessible here: LCI_H2_turbine_.

.. _Ozawa: https://doi.org/10.1016/j.ijhydene.2019.02.230
.. _LCI_H2_turbine: https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-hydrogen-turbine.xlsx


Steel
-----

*premise* imports inventories for a wide range of steel production technologies.
These include conventional blast furnace-basic oxygen furnace (BF-BOF) routes,
as well as emerging processes such as direct reduction (DRI), hydrogen-based
production, electrowinning, and carbon capture (CCS) variants. They are from Harpprecht_ et al. (2025).
They can be found here: LCI_steel_.

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

.. _Harpprecht: https://doi.org/10.1039/D5EE01356A
.. _LCI_steel: https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-steel.xlsx


Cement
------

*premise* introduces inventories for capturing carbon dioxide at cement
production plants using three prospective technologies:

* Post-combustion capture using monoethanolamine (MEA)
* Direct separation
* Oxyfuel combustion

These inventories represent the gate-to-gate capture of 1 kg of CO2 and
include upstream material and energy inputs as well as transport and storage
of the captured CO2. They are from Muller_ et al. (2024). They can be found
here: LCI_cement_.

==============================================================================  ==========
Carbon capture at cement production plants                                       location
==============================================================================  ==========
carbon dioxide, captured, at cement production plant, using monoethanolamine     RER
carbon dioxide, captured, at cement production plant, using direct separation    RER
carbon dioxide, captured, at cement production plant, using oxyfuel              RER
==============================================================================  ==========

A fourth cement-related activity is available for CDR accounting rather than
ordinary cement transformation:
``carbon dioxide, captured and stored, at cement production plant, from
non-fossil carbon dioxide, using monoethanolamine``. It is scaled to 1 kg of
non-fossil CO2 stored, includes a 1 kg ``Carbon dioxide, in air`` input, uses
the MEA cement capture inputs and the common CO2 compression, transport and
storage module, and intentionally ignores fossil CO2 co-captured from the
cement flue gas.

Monoethanolamine (MEA)
**********************

Represents conventional post-combustion carbon capture using MEA solvents,
based on the CEMCAP study (Voldsund, 2019). The dataset includes heat and
electricity demand for regeneration and compression, solvent losses, chemical
pretreatment (NaOH), and incineration of spent solvents. The source inventory
uses 4.0556 MJ/kg CO2 of industrial heat for solvent regeneration and
low-voltage electricity for fans, cooling and CO2 processing.

Direct separation
*****************

Models CO₂ capture via a separate calciner (as in the LEILAC project),
allowing for nearly pure CO₂ stream separation without additional chemical
solvents. Includes extra electricity consumption for calciner operation
and CO₂ compression.

Oxyfuel combustion
******************

Simulates complete fuel combustion in a controlled O₂/CO₂ atmosphere.
The resulting flue gas has high CO₂ purity, reducing downstream separation
needs. Liquid oxygen is supplied via an air separation unit (ASU), and waste
heat is recovered to offset some electricity needs. Emissions of SOₓ, NOₓ, CO,
and Hg are significantly reduced.

All three capture routes include subsequent CO2 compression, transport, and
storage via the carbon dioxide compression, transport and storage dataset
from *premise*.

.. _Muller: https://doi.org/10.1016/j.jclepro.2024.141884
.. _LCI_cement: https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-carbon-capture.xlsx

Ammonia
-------

*premise* imports inventories for ammonia production using the following routes:

* steam methane reforming (Haber-Bosch)
* steam methane reforming (Haber-Bosch) with CCS of syngas
* steam methane reforming (Haber-Bosch) with CCS of syngas and flue gas
* partial oxidation of oil
* hydrogen from coal gasification
* hydrogen from coal gasification with CCS
* hydrogen from electrolysis
* hydrogen from natural gas pyrolysis

These inventories are published in Boyce_ et al., 2023,
and are largely based on Carlo d' Angelo_ et al., 2021.

The supply of hydrogen in the ammonia production process
(coal gasification, electrolysis, etc.) is represented by the
inventories described in the sections above.

.. _Boyce: https://doi.org/10.1016/j.heliyon.2024.e27547
.. _Angelo: https://doi.org/10.1021/acssuschemeng.1c01915


Biofuels
--------

Inventories for energy crops- and residues-based production of bioethanol and biodiesel
are imported, and can be accessed here: LCI_biofuels_. They include the farming of the crop,
the conversion of the biomass to fuel, as well as its distribution. The conversion process
often leads to the production of co-products (dried distiller's grain, electricity, CO2, bagasse.).
Hence, energy, economic and system expansion partitioning approaches are available.
These inventories originate from several different sources
(Wu_ et al. 2006 (2020 update), Cozzolino_ 2018, Pereira_ et al. 2019 and Gonzalez-Garcia_ et al. 2012),
Cavalett_ & Cherubini 2022, as indicated in the table below.

.. _LCI_biofuels: https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-biofuels.xlsx
.. _Cozzolino: https://www.psi.ch/sites/default/files/2019-09/Cozzolino_377125_%20Research%20Project%20Report.pdf
.. _Gonzalez-Garcia: https://doi.org/10.1016/j.scitotenv.2012.07.044
.. _Wu: http://greet.es.anl.gov/publication-2lli584z
.. _Pereira: http://task39.sites.olt.ubc.ca/files/2019/04/Task-39-GHS-models-Final-Report-Phase-1.pdf
.. _Cavalett: https://doi.org/10.1002/bbb.2395

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

Synthetic fuels
---------------

*premise* imports inventories for the synthesis of hydrocarbon fuels
following two pathways:

* *Fischer-Tropsch*: it uses hydrogen and CO (from CO2 via a reverse water gas
  shift process) to produce "syncrude", which is distilled into diesel, kerosene,
  naphtha and lubricating oil and waxes. Inventories are from van der Giesen_ et al. 2014.
* *Methanol-to-liquids*: methanol is synthesized from hydrogen and CO2, and further
  distilled into gasoline, diesel, LGP and kerosene. Synthetic methanol inventories
  are from Hank_ et al. 2019. The methanol to fuel process specifications are from
  FVV_ 2013.
* *Electro-chemical methanation*: methane is produced from hydrogen and CO2 using
  a Sabatier methanation reactor. Inventories are from Zhang_ et al, 2019.

.. _Giesen: https://pubs.acs.org/doi/abs/10.1021/es500191g
.. _Hank: https://doi.org/10.1039/C9SE00658C
.. _FVV: https://www.fvv-net.de/fileadmin/user_upload/medien/materialien/FVV-Kraftstoffstudie_LBST_2013-10-30.pdf
.. _Zhang: https://doi.org/10.1039/C9SE00986H

In their default configuration, these fuels use hydrogen from electrolysis and CO2
from direct air capture (DAC). However, *premise* builds different configurations
(i.e., CO2 and hydrogen sources) for these fuels, for each IAM region:

 ============================================================================================================================================================================ ================== =============================
  Fuel production dataset                                                                                                                                                      location           source
 ============================================================================================================================================================================ ================== =============================
  Diesel production, synthetic, from Fischer Tropsch process, hydrogen from coal gasification, at fuelling station                                                             all IAM regions    van der Giesen et al. 2014
  Diesel production, synthetic, from Fischer Tropsch process, hydrogen from coal gasification, with CCS, at fuelling station                                                   all IAM regions    van der Giesen et al. 2014
  Diesel production, synthetic, from Fischer Tropsch process, hydrogen from electrolysis, at fuelling station                                                                  all IAM regions    van der Giesen et al. 2014
  Diesel production, synthetic, from Fischer Tropsch process, hydrogen from wood gasification, at fuelling station                                                             all IAM regions    van der Giesen et al. 2014
  Diesel production, synthetic, from Fischer Tropsch process, hydrogen from wood gasification, with CCS, at fuelling station                                                   all IAM regions    van der Giesen et al. 2014
  Diesel production, synthetic, from methanol, hydrogen from coal gasification, at fuelling station                                                                            all IAM regions    Hank et al, 2019
  Diesel production, synthetic, from methanol, hydrogen from coal gasification, with CCS, at fuelling station                                                                  all IAM regions    Hank et al, 2019
  Diesel production, synthetic, from methanol, hydrogen from electrolysis, CO2 from cement plant, at fuelling station                                                          all IAM regions    Hank et al, 2019
  Diesel production, synthetic, from methanol, hydrogen from electrolysis, CO2 from DAC, at fuelling station                                                                   all IAM regions    Hank et al, 2019
  Gasoline production, synthetic, from methanol, hydrogen from coal gasification, at fuelling station                                                                          all IAM regions    Hank et al, 2019
  Gasoline production, synthetic, from methanol, hydrogen from coal gasification, with CCS, at fuelling station                                                                all IAM regions    Hank et al, 2019
  Gasoline production, synthetic, from methanol, hydrogen from electrolysis, CO2 from cement plant, at fuelling station                                                        all IAM regions    Hank et al, 2019
  Gasoline production, synthetic, from methanol, hydrogen from electrolysis, CO2 from DAC, at fuelling station                                                                 all IAM regions    Hank et al, 2019
  Kerosene production, from methanol, hydrogen from coal gasification                                                                                                          all IAM regions    Hank et al, 2019
  Kerosene production, from methanol, hydrogen from electrolysis, CO2 from cement plant                                                                                        all IAM regions    Hank et al, 2019
  Kerosene production, from methanol, hydrogen from electrolysis, CO2 from DAC                                                                                                 all IAM regions    Hank et al, 2019
  Kerosene production, synthetic, Fischer Tropsch process, hydrogen from coal gasification                                                                                     all IAM regions    van der Giesen et al. 2014
  Kerosene production, synthetic, Fischer Tropsch process, hydrogen from coal gasification, with CCS                                                                           all IAM regions    van der Giesen et al. 2014
  Kerosene production, synthetic, Fischer Tropsch process, hydrogen from electrolysis                                                                                          all IAM regions    van der Giesen et al. 2014
  Kerosene production, synthetic, Fischer Tropsch process, hydrogen from wood gasification                                                                                     all IAM regions    van der Giesen et al. 2014
  Kerosene production, synthetic, Fischer Tropsch process, hydrogen from wood gasification, with CCS                                                                           all IAM regions    van der Giesen et al. 2014
  Lubricating oil production, synthetic, Fischer Tropsch process, hydrogen from coal gasification                                                                              all IAM regions    van der Giesen et al. 2014
  Lubricating oil production, synthetic, Fischer Tropsch process, hydrogen from electrolysis                                                                                   all IAM regions    van der Giesen et al. 2014
  Lubricating oil production, synthetic, Fischer Tropsch process, hydrogen from wood gasification                                                                              all IAM regions    van der Giesen et al. 2014
  Lubricating oil production, synthetic, Fischer Tropsch process, hydrogen from wood gasification, with CCS                                                                    all IAM regions    van der Giesen et al. 2014
  Methane, synthetic, gaseous, 5 bar, from coal-based hydrogen, at fuelling station                                                                                            all IAM regions    Zhang et al, 2019
  Methane, synthetic, gaseous, 5 bar, from electrochemical methanation (H2 from electrolysis, CO2 from DAC using heat pump heat), at fuelling station, using heat pump heat    all IAM regions    Zhang et al, 2019
  Methane, synthetic, gaseous, 5 bar, from electrochemical methanation (H2 from electrolysis, CO2 from DAC using waste heat), at fuelling station, using waste heat            all IAM regions    Zhang et al, 2019
  Methane, synthetic, gaseous, 5 bar, from electrochemical methanation, at fuelling station                                                                                    all IAM regions    Zhang et al, 2019
  Naphtha production, synthetic, Fischer Tropsch process, hydrogen from coal gasification                                                                                      all IAM regions    van der Giesen et al. 2014
  Naphtha production, synthetic, Fischer Tropsch process, hydrogen from electrolysis                                                                                           all IAM regions    van der Giesen et al. 2014
  Naphtha production, synthetic, Fischer Tropsch process, hydrogen from wood gasification                                                                                      all IAM regions    van der Giesen et al. 2014
  Naphtha production, synthetic, Fischer Tropsch process, hydrogen from wood gasification, with CCS                                                                            all IAM regions    van der Giesen et al. 2014
  Liquefied petroleum gas production, synthetic, from methanol, hydrogen from electrolysis, CO2 from DAC, at fuelling station                                                  all IAM regions    Hank et al, 2019
 ============================================================================================================================================================================ ================== =============================

In the case of wood and coal gasification-based fuels, the CO2 needed to produce methanol
or syncrude originates from the gasification process itself. This also implies
that in the methanol and/or RWGS process, a carbon balance correction is applied to reflect the
fact that a part of the CO2 from the gasification process is redirected into
the fuel production process.

If the CO2 originates from:

* a gasification process without CCS, a negative carbon correction is added to
  reflect the fact that part of the CO2 has not been emitted but has ended in the fuel instead.
* the gasification process with CCS, no carbon correction is necessary, because the CO2 is stored
  in the fuel instead of being stored underground, which from a carbon accounting standpoint is
  similar.

Carbon Capture
--------------

Two sets of inventories for Direct Air Capture (DAC) are available in *premise*:
one for a solvent-based system and one for a sorbent-based system. The
inventories were developed by Qiu_ and are available in the LCI_DAC_
spreadsheet. For each, a variant including the subsequent compression,
transport and storage of the captured CO2 is also available.

They can be consulted here: LCI_DAC_.

.. _Qiu: https://doi.org/10.1038/s41467-022-31146-1
.. _LCI_DAC: https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-carbon-capture.xlsx

Additionally, two datasets for carbon capture at point sources are available:
one at a cement plant from Meunier_ et al, 2020, and another one at a
municipal solid waste incineration plant (MSWI) from Bisinella_ et al, 2021.

.. _Meunier: https://doi.org/10.1016/j.renene.2019.07.010
.. _Bisinella: https://doi.org/10.1016/j.wasman.2021.04.046

They introduce the following datasets:

 =============================================================================================================== ===========
  Activity                                                                                                         Location
 =============================================================================================================== ===========
  carbon dioxide, captured, with a sorbent-based direct air capture system, 100ktCO2                               RER
  carbon dioxide, captured and stored, with a sorbent-based direct air capture system, 100ktCO2                    RER
  carbon dioxide, captured, with a solvent-based direct air capture system, 1MtCO2                                 RER
  carbon dioxide, captured and stored, with a solvent-based direct air capture system, 1MtCO2                      RER
  carbon dioxide, captured at municipal solid waste incineration plant, for subsequent reuse                       RER
  carbon dioxide, captured at cement production plant, for subsequent reuse                                        RER
 =============================================================================================================== ===========

The same workbook also contains harmonized capture-only modules for biogenic
and industrial CDR routes:

* ``carbon dioxide, captured and stored, at wood burning power plant, pipeline
  200km, storage 1000m`` excludes the host power plant and represents the
  additional capture and storage system. The energy penalty is modelled as
  3.565 MJ/kg CO2 of wood-derived industrial heat from
  ``heat and power co-generation, wood chips, 6667 kW``.
* ``carbon dioxide, captured and stored, from a hydrogen production plant using
  steam methane reforming of biomethane`` excludes the host SMR plant. It uses
  methyldiethanolamine (MDEA), 0.11598 kWh/kg CO2 of low-voltage electricity
  and 0.948 MJ/kg CO2 of biomethane-based heat inferred from Antonini_ et al.
  The final CO2 compression electricity is omitted to avoid double counting
  with the common storage module.
* ``carbon dioxide, captured and stored, from a biomass fermentation plant``
  excludes the host fermentation plant. Because the fermentation stream is
  already high-purity CO2, no solvent, sorbent, regeneration heat or additional
  capture electricity is included; the dataset uses the common compression,
  transport and storage module.

Using the transformation function ``update("cdr")``, *premise* regionalizes the
CDR support activities listed in ``premise/data/cdr/cdr_activities.yaml`` and
then regionalizes the CDR technologies mapped in
``carbon_dioxide_removal.yaml``. Regionalized DAC support variants include
industrial steam heat, high-temperature heat-pump heat and waste heat where the
source inventory supports them:

 ======================================================================================================================================================= ==================
  name                                                                                                                                                      location
 ======================================================================================================================================================= ==================
  carbon dioxide, captured and stored, with a solvent-based direct air capture system, 1MtCO2, with industrial steam heat, and grid electricity            all IAM regions
  carbon dioxide, captured and stored, with a solvent-based direct air capture system, 1MtCO2, with heat pump heat, and grid electricity                   all IAM regions
  carbon dioxide, captured and stored, with a sorbent-based direct air capture system, 100ktCO2, with waste heat, and grid electricity                     all IAM regions
  carbon dioxide, captured and stored, with a sorbent-based direct air capture system, 100ktCO2, with industrial steam heat, and grid electricity          all IAM regions
  carbon dioxide, captured and stored, with a sorbent-based direct air capture system, 100ktCO2, with heat pump heat, and grid electricity                 all IAM regions
 ======================================================================================================================================================= ==================

Note that only solid sorbent DAC can use waste heat, as the heat requirement
for liquid solvent DAC is too high (~900 C). CDR efficiency adjustments, when
provided by the IAM scenario, scale electricity exchanges separately from heat
and fuel exchanges; material inputs and biosphere flows are not scaled.

Li-ion batteries
----------------

When using ecoinvent 3.8 as a database, *premise* imports new inventories for lithium-ion batteries.
NMC-111, NMC-622 NMC-811 and NCA Lithium-ion battery inventories are originally
from Dai_ et al. 2019. They have been adapted to ecoinvent by Crenna_ et al, 2021.
LFP and LTO Lithium-ion battery inventories are from  Schmidt_ et al. 2019.
Li-S (Lithium-sulfur) battery inventories are from Wickerts_ et al. 2023.
Li-O2 (Lithium-air) battery inventories are from Wang_ et al. 2020.
Finally, SIB (Sodium-ion) battery inventories are from Zhang22_ et al. 2024.
Ecoinvent provides also inventories for LMO (Lithium Maganese Oxide) batteries.

They introduce the following datasets:

 ============================================================= =========== ======================================
  Battery components                                            location    source
 ============================================================= =========== ======================================
  battery management system production, for Li-ion battery        GLO         Schmidt et al. 2019
  market for battery, Li-ion, NMC111, rechargeable, prismatic     GLO         Dai et al. 2019, Crenna et al. 2021
  market for battery, Li-ion, NMC622, rechargeable, prismatic     GLO         Dai et al. 2019, Crenna et al. 2021
  market for battery, Li-ion, NMC811, rechargeable, prismatic     GLO         Dai et al. 2019, Crenna et al. 2021
  market for battery, Li-ion, NCA, rechargeable, prismatic        GLO         Dai et al. 2019, Crenna et al. 2021
  market for battery, Li-ion, LFP, rechargeable, prismatic        GLO         Schmidt et al. 2019
  market for battery cell, Li-ion, LTO                            GLO         Schmidt et al. 2019
  market for battery, Li-sulfur, Li-S                             GLO         Wickerts et al. (2023)
  market for battery, Li-oxygen, Li-O2                            GLO         Wang et al. (2020)
  market for battery, Sodium-ion, SiB                             GLO         Zhang et al. (2024)
  market for battery, NaCl, rechargeable, prismatic               GLO         Galloway & Dustmann (2003)
 ============================================================= =========== ======================================

These battery inventories are mostly used by battery electric vehicles,
stationary energy storage systems, etc. (also imported by *premise*).

NMC-111, NMC-811, LFP and NCA inventories can be found here: LCI_batteries1_.
NMC-622 and LTO inventories can be found here: LCI_batteries2_.
Li-S inventories can be found here: LCI_batteries3_.
Li-O2 inventories can be found here: LCI_batteries4_.
And SIB inventories can be found here: LCI_batteries5_.

When using ecoinvent 3.9 and above, the NMC-111, NMC-811, LFP and NCA battery inventories
are not imported (as are already present the ecoinvent database).

Graphite
--------

*premise* includes new inventories for:

* natural graphite, from Engels_ et al. 2022,
* synthetic graphite, from Surovtseva_ et al. 2022,

forming a new market for graphite, with the following datasets:

 ===================================== =========== ===========
  Activity                              Location
 ===================================== =========== ===========
  market for graphite, battery grade                1.0
  graphite, natural                     CN          0.8
  graphite, synthetic                   CN          0.2
 ===================================== =========== ===========

to represent a 80:20 split between natural and synthetic graphite,
according to Surovtseva_ et al, 2022.

These inventories can be found here: LCI_graphite_.

Cobalt
------

New inventories of cobalt are added, from the work of Dai, Kelly and Elgowainy_, 2018.
They are available under the following datasets:

=================================================================================== ===========
Activity                                                                             Location
=================================================================================== ===========
cobalt sulfate production, from copper mining, economic allocation                   CN
cobalt sulfate production, from copper mining, energy allocation                     CN
cobalt metal production, from copper mining, via electrolysis, economic allocation   CN
cobalt metal production, from copper mining, via electrolysis, energy allocation     CN
=================================================================================== ===========

We recommend using those rather than the original ecoinvent inventories for cobalt, provided
by the Cobalt Development Institute (CDI) since ecoinvent 3.7, which seem to lack transparency.

These inventories can be found here: LCI_cobalt_.

Lithium
-------

New inventories for lithium extraction are also added,
from the work of Schenker_ et al., 2022.
They cover lithium extraction from five different locations in Chile, Argentina and China.
They are available under the following datasets for battery production:

=================================================================================== ===========
Activity                                                                             Location
=================================================================================== ===========
market for lithium carbonate, battery grade                                          GLO
market for lithium hydroxide, battery grade                                          GLO
=================================================================================== ===========

These inventories can be found here: LCI_lithium_.

.. _Dai: https://www.mdpi.com/2313-0105/5/2/48
.. _Crenna: https://doi.org/10.1016/j.resconrec.2021.105619
.. _Wickerts: https://doi.org/10.1021/acssuschemeng.3c00141
.. _Wang: https://doi.org/10.1016/j.jclepro.2020.121339
.. _Zhang22: https://doi.org/10.1016/j.resconrec.2023.107362
.. _Schmidt: https://doi.org/10.1021/acs.est.8b05313
.. _Engels: https://doi.org/10.1016/j.jclepro.2022.130474
.. _Surovtseva: https://doi.org/10.1111/jiec.13234
.. _Elgowainy: https://greet.es.anl.gov/publication-update_cobalt
.. _Schenker: https://doi.org/10.1016/j.resconrec.2022.106611
.. _LCI_batteries1: https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-batteries-NMC111-811-NCA-LFP.xlsx.xlsx
.. _LCI_batteries2: https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-batteries-NMC622-LTO.xlsx.xlsx
.. _LCI_batteries3: https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-batteries-LiS.xlsx
.. _LCI_batteries4: https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-batteries-LiO2.xlsx
.. _LCI_batteries5: https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-batteries-SIB.xlsx
.. _LCI_graphite: https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-graphite.xlsx
.. _LCI_cobalt: https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-cobalt.xlsx
.. _LCI_lithium: https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-lithium.xlsx

Vanadium Redox Flow Batteries
-----------------------------

*premise* imports inventories for the production of a vanadium redox flow battery, used
for grid-balancing, from the work of Weber_ et al. 2021.
It is available under the following dataset:

* vanadium-redox flow battery system assembly, 8.3 megawatt hour

The dataset providing electricity is the following:

* electricity supply, high voltage, from vanadium-redox flow battery system

The power capacity for this application is 1MW and the net storage capacity 6 MWh.
The net capacity considers the internal inefficiencies of the batteries and the
min Sate-of-Charge, requiring a certain oversizing of the batteries.
For providing net 6 MWh, a nominal capacity of 8.3 MWh is required for the
VRFB with the assumed operation parameters. The assumed lifetime of the stack
is 10 years. The lifetime of the system is 20 years or 8176
cycle-life (49,000 MWh).

.. _Weber: https://doi.org/10.1021/acs.est.8b02073

These inventories can be found here: LCI_vanadium_redox_flow_batteries_.

.. _LCI_vanadium_redox_flow_batteries: https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-vanadium-redox-flow-battery.xlsx

This publication also provides LCIs for Vanadium mining and refining from iron ore.
The end product is vanadium pentoxide, which is available under the following dataset:

* vanadium pentoxide production

These inventories can be found here: LCI_vanadium_.

.. _LCI_vanadium: https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-vanadium.xlsx

Road vehicles
-------------

*premise* imports inventories for different types of on-road vehicles.

Two-wheelers
************

The following datasets for two-wheelers are imported.
Inventories are from Sacchi_ et al. 2022. The vehicles are available
for different years and emission standards. *premise* will only
import vehicles which production year is equal or inferior to
the scenario year considered. The inventories can be consulted
here: LCItwowheelers_.


.. _Sacchi: https://zenodo.org/deposit/5720779
.. _LCItwowheelers: https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-two_wheelers.xlsx

 ================================================= ==================
  Two-wheeler datasets                              location
 ================================================= ==================
  transport, Kick Scooter, electric, <1kW           all IAM regions
  transport, Bicycle, conventional, urban           all IAM regions
  transport, Bicycle, electric (<25 km/h)           all IAM regions
  transport, Bicycle, electric (<45 km/h)           all IAM regions
  transport, Bicycle, electric, cargo bike          all IAM regions
  transport, Moped, gasoline, <4kW, EURO-5          all IAM regions
  transport, Scooter, gasoline, <4kW, EURO-5        all IAM regions
  transport, Scooter, gasoline, 4-11kW, EURO-5      all IAM regions
  transport, Scooter, electric, <4kW                all IAM regions
  transport, Scooter, electric, 4-11kW              all IAM regions
  transport, Motorbike, gasoline, 4-11kW, EURO-5    all IAM regions
  transport, Motorbike, gasoline, 11-35kW, EURO-5   all IAM regions
  transport, Motorbike, gasoline, >35kW, EURO-5     all IAM regions
  transport, Motorbike, electric, <4kW              all IAM regions
  transport, Motorbike, electric, 4-11kW            all IAM regions
  transport, Motorbike, electric, 11-35kW           all IAM regions
  transport, Motorbike, electric, >35kW             all IAM regions
 ================================================= ==================

These inventories do not supply inputs to other activities in the LCI database.


Passenger cars
**************

The following datasets for passenger cars are imported.

 =============================================================================== ==================
  Passenger car datasets                                                          location
 =============================================================================== ==================
  transport, passenger car, gasoline, Large                                       all IAM regions
  transport, passenger car, diesel, Large                                         all IAM regions
  transport, passenger car, compressed gas, Large                                 all IAM regions
  transport, passenger car, plugin gasoline hybrid, Large                         all IAM regions
  transport, passenger car, plugin diesel hybrid, Large                           all IAM regions
  transport, passenger car, fuel cell electric, Large                             all IAM regions
  transport, passenger car, battery electric Large                                all IAM regions
  transport, passenger car, gasoline hybrid, Large                                all IAM regions
  transport, passenger car, diesel hybrid, Large                                  all IAM regions
  transport, passenger car, gasoline, Large SUV                                   all IAM regions
  transport, passenger car, diesel, Large SUV                                     all IAM regions
  transport, passenger car, compressed gas, Large SUV                             all IAM regions
  transport, passenger car, plugin gasoline hybrid, Large SUV                     all IAM regions
  transport, passenger car, plugin diesel hybrid, Large SUV                       all IAM regions
  transport, passenger car, fuel cell electric, Large SUV                         all IAM regions
  transport, passenger car, battery electric Large SUV                            all IAM regions
  transport, passenger car, gasoline hybrid, Large SUV                            all IAM regions
  transport, passenger car, diesel hybrid, Large SUV                              all IAM regions
  transport, passenger car, gasoline, Lower medium                                all IAM regions
  transport, passenger car, diesel, Lower medium                                  all IAM regions
  transport, passenger car, compressed gas, Lower medium                          all IAM regions
  transport, passenger car, plugin gasoline hybrid, Lower medium                  all IAM regions
  transport, passenger car, plugin diesel hybrid, Lower medium                    all IAM regions
  transport, passenger car, fuel cell electric, Lower medium                      all IAM regions
  transport, passenger car, battery electric Lower medium                         all IAM regions
  transport, passenger car, gasoline hybrid, Lower medium                         all IAM regions
  transport, passenger car, diesel hybrid, Lower medium                           all IAM regions
  transport, passenger car, gasoline, Medium                                      all IAM regions
  transport, passenger car, diesel, Medium                                        all IAM regions
  transport, passenger car, compressed gas, Medium                                all IAM regions
  transport, passenger car, plugin gasoline hybrid, Medium                        all IAM regions
  transport, passenger car, plugin diesel hybrid, Medium                          all IAM regions
  transport, passenger car, fuel cell electric, Medium                            all IAM regions
  transport, passenger car, battery electric Medium                               all IAM regions
  transport, passenger car, gasoline hybrid, Medium                               all IAM regions
  transport, passenger car, diesel hybrid, Medium                                 all IAM regions
  transport, passenger car, gasoline, Medium SUV                                  all IAM regions
  transport, passenger car, diesel, Medium SUV                                    all IAM regions
  transport, passenger car, compressed gas, Medium SUV                            all IAM regions
  transport, passenger car, plugin gasoline hybrid, Medium SUV                    all IAM regions
  transport, passenger car, plugin diesel hybrid, Medium SUV                      all IAM regions
  transport, passenger car, fuel cell electric, Medium SUV                        all IAM regions
  transport, passenger car, battery electric Medium SUV                           all IAM regions
  transport, passenger car, gasoline hybrid, Medium SUV                           all IAM regions
  transport, passenger car, diesel hybrid, Medium SUV                             all IAM regions
  transport, passenger car, battery electric Micro                                all IAM regions
  transport, passenger car, gasoline, Mini                                        all IAM regions
  transport, passenger car, diesel, Mini                                          all IAM regions
  transport, passenger car, compressed gas, Mini                                  all IAM regions
  transport, passenger car, plugin gasoline hybrid, Mini                          all IAM regions
  transport, passenger car, plugin diesel hybrid, Mini                            all IAM regions
  transport, passenger car, fuel cell electric, Mini                              all IAM regions
  transport, passenger car, battery electric Mini                                 all IAM regions
  transport, passenger car, gasoline hybrid, Mini                                 all IAM regions
  transport, passenger car, diesel hybrid, Mini                                   all IAM regions
  transport, passenger car, gasoline, Small                                       all IAM regions
  transport, passenger car, diesel, Small                                         all IAM regions
  transport, passenger car, compressed gas, Small                                 all IAM regions
  transport, passenger car, plugin gasoline hybrid, Small                         all IAM regions
  transport, passenger car, plugin diesel hybrid, Small                           all IAM regions
  transport, passenger car, fuel cell electric, Small                             all IAM regions
  transport, passenger car, battery electric Small                                all IAM regions
  transport, passenger car, gasoline hybrid, Small                                all IAM regions
  transport, passenger car, diesel hybrid, Small                                  all IAM regions
  transport, passenger car, gasoline, Van                                         all IAM regions
  transport, passenger car, diesel, Van                                           all IAM regions
  transport, passenger car, compressed gas, Van                                   all IAM regions
  transport, passenger car, plugin diesel hybrid, Van                             all IAM regions
  transport, passenger car, fuel cell electric, Van                               all IAM regions
  transport, passenger car, battery electric Van                                  all IAM regions
  transport, passenger car, gasoline hybrid, Van                                  all IAM regions
  transport, passenger car, diesel hybrid, Van                                    all IAM regions
 =============================================================================== ==================

Inventories are from Sacchi2_ et al. 2022. The vehicles are available
for different years and emission standards and for each IAM region.


Note that NOx and PM 2.5 emissions are further updated based on
remote-sensing data from Sjodin_ et al. 2018.

.. _Sjodin: https://www.bafu.admin.ch/dam/en/sd-web/lggkMIGXeKNu/real-driving-emissions-from-diesel-passenger-cars-measured-by-remote-sensing-and-as-compared-with-pems-and-chassis-dynamometer-measurements-conox-task-2-report.pdf


When doing:

.. code-block:: python

    update("cars")

*premise* will create fleet average vehicles for each IAM region. The inventories can be consulted
here: LCIpasscars_.

.. _Sacchi2: https://www.psi.ch/en/media/72391/download
.. _LCIpasscars: https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-pass_cars.xlsx

At the moment, these inventories do not supply inputs to other activities in the LCI database.


Medium and heavy duty trucks
****************************

The following datasets for medium and heavy-duty trucks are imported.

 ================================================================================== ==================
  Truck datasets                                                                     location
 ================================================================================== ==================
  transport, freight, lorry, battery electric 3.5t gross weight                      all IAM regions
  transport, freight, lorry, fuel cell electric, 3.5t gross weight                   all IAM regions
  transport, freight, lorry, diesel hybrid, 3.5t gross weight, EURO-VI               all IAM regions
  transport, freight, lorry, diesel, 3.5t gross weight, EURO-VI                      all IAM regions
  transport, freight, lorry, compressed gas, 3.5t gross weight, EURO-VI              all IAM regions
  transport, freight, lorry, plugin diesel hybrid, 3.5t gross weight, EURO-VI        all IAM regions
  transport, freight, lorry, battery electric 7.5t gross weight                      all IAM regions
  transport, freight, lorry, fuel cell electric, 7.5t gross weight                   all IAM regions
  transport, freight, lorry, diesel hybrid, 7.5t gross weight, EURO-VI               all IAM regions
  transport, freight, lorry, diesel, 7.5t gross weight, EURO-VI                      all IAM regions
  transport, freight, lorry, compressed gas, 7.5t gross weight, EURO-VI              all IAM regions
  transport, freight, lorry, plugin diesel hybrid, 7.5t gross weight, EURO-VI        all IAM regions
  transport, freight, lorry, battery electric 18t gross weight                       all IAM regions
  transport, freight, lorry, fuel cell electric, 18t gross weight                    all IAM regions
  transport, freight, lorry, diesel hybrid, 18t gross weight, EURO-VI                all IAM regions
  transport, freight, lorry, diesel, 18t gross weight, EURO-VI                       all IAM regions
  transport, freight, lorry, compressed gas, 18t gross weight, EURO-VI               all IAM regions
  transport, freight, lorry, plugin diesel hybrid, 18t gross weight, EURO-VI         all IAM regions
  transport, freight, lorry, battery electric 26t gross weight                       all IAM regions
  transport, freight, lorry, fuel cell electric, 26t gross weight                    all IAM regions
  transport, freight, lorry, diesel hybrid, 26t gross weight, EURO-VI                all IAM regions
  transport, freight, lorry, diesel, 26t gross weight, EURO-VI                       all IAM regions
  transport, freight, lorry, compressed gas, 26t gross weight, EURO-VI               all IAM regions
  transport, freight, lorry, plugin diesel hybrid, 26t gross weight, EURO-VI         all IAM regions
  transport, freight, lorry, battery electric 32t gross weight                       all IAM regions
  transport, freight, lorry, fuel cell electric, 32t gross weight                    all IAM regions
  transport, freight, lorry, diesel hybrid, 32t gross weight, EURO-VI                all IAM regions
  transport, freight, lorry, diesel, 32t gross weight, EURO-VI                       all IAM regions
  transport, freight, lorry, compressed gas, 32t gross weight, EURO-VI               all IAM regions
  transport, freight, lorry, plugin diesel hybrid, 32t gross weight, EURO-VI         all IAM regions
  transport, freight, lorry, battery electric 40t gross weight                       all IAM regions
  transport, freight, lorry, fuel cell electric, 40t gross weight                    all IAM regions
  transport, freight, lorry, diesel hybrid, 40t gross weight, EURO-VI                all IAM regions
  transport, freight, lorry, diesel, 40t gross weight, EURO-VI                       all IAM regions
  transport, freight, lorry, compressed gas, 40t gross weight, EURO-VI               all IAM regions
  transport, freight, lorry, plugin diesel hybrid, 40t gross weight, EURO-VI         all IAM regions
 ================================================================================== ==================


Inventories are from Sacchi3_ et al. 2021. The vehicles are available
for different years and emission standards and for each IAM region.

When doing:

.. code-block:: python

    update("trucks")

*premise* will create fleet average vehicles for each IAM region. The inventories can be consulted
here: LCItrucks_.

.. _LCItrucks: https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-trucks.xlsx
.. _Sacchi3: https://pubs.acs.org/doi/abs/10.1021/acs.est.0c07773



Buses
*****

The following datasets for city and coach buses are imported.

 =================================================================================================================== ==================
  Bus datasets                                                                                                        location
 =================================================================================================================== ==================
  transport, passenger bus, battery electric - overnight charging 9m midibus                                          all IAM regions
  transport, passenger bus, battery electric - opportunity charging, LTO battery, 9m midibus                          all IAM regions
  transport, passenger bus, fuel cell electric, 9m midibus                                                            all IAM regions
  transport, passenger bus, diesel hybrid, 9m midibus, EURO-VI                                                        all IAM regions
  transport, passenger bus, diesel, 9m midibus, EURO-VI                                                               all IAM regions
  transport, passenger bus, compressed gas, 9m midibus, EURO-VI                                                       all IAM regions
  transport, passenger bus, battery electric - overnight charging 13m single deck urban bus                           all IAM regions
  transport, passenger bus, battery electric - battery-equipped trolleybus, LTO battery, 13m single deck urban bus    all IAM regions
  transport, passenger bus, battery electric - opportunity charging, LTO battery, 13m single deck urban bus           all IAM regions
  transport, passenger bus, fuel cell electric, 13m single deck urban bus                                             all IAM regions
  transport, passenger bus, diesel hybrid, 13m single deck urban bus, EURO-VI                                         all IAM regions
  transport, passenger bus, diesel, 13m single deck urban bus, EURO-VI                                                all IAM regions
  transport, passenger bus, compressed gas, 13m single deck urban bus, EURO-VI                                        all IAM regions
  transport, passenger bus, fuel cell electric, 13m single deck coach bus                                             all IAM regions
  transport, passenger bus, diesel hybrid, 13m single deck coach bus, EURO-VI                                         all IAM regions
  transport, passenger bus, diesel, 13m single deck coach bus, EURO-VI                                                all IAM regions
  transport, passenger bus, compressed gas, 13m single deck coach bus, EURO-VI                                        all IAM regions
  transport, passenger bus, battery electric - overnight charging 13m double deck urban bus                           all IAM regions
  transport, passenger bus, battery electric - opportunity charging, LTO battery, 13m double deck urban bus           all IAM regions
  transport, passenger bus, fuel cell electric, 13m double deck urban bus                                             all IAM regions
  transport, passenger bus, diesel hybrid, 13m double deck urban bus, EURO-VI                                         all IAM regions
  transport, passenger bus, diesel, 13m double deck urban bus, EURO-VI                                                all IAM regions
  transport, passenger bus, compressed gas, 13m double deck urban bus, EURO-VI                                        all IAM regions
  transport, passenger bus, fuel cell electric, 13m double deck coach bus                                             all IAM regions
  transport, passenger bus, diesel hybrid, 13m double deck coach bus, EURO-VI                                         all IAM regions
  transport, passenger bus, diesel, 13m double deck coach bus, EURO-VI                                                all IAM regions
  transport, passenger bus, compressed gas, 13m double deck coach bus, EURO-VI                                        all IAM regions
  transport, passenger bus, battery electric - overnight charging 18m articulated urban bus                           all IAM regions
  transport, passenger bus, battery electric - battery-equipped trolleybus, LTO battery, 18m articulated urban bus    all IAM regions
  transport, passenger bus, battery electric - opportunity charging, LTO battery, 18m articulated urban bus           all IAM regions
  transport, passenger bus, fuel cell electric, 18m articulated urban bus                                             all IAM regions
  transport, passenger bus, diesel hybrid, 18m articulated urban bus, EURO-VI                                         all IAM regions
  transport, passenger bus, diesel, 18m articulated urban bus, EURO-VI                                                all IAM regions
  transport, passenger bus, compressed gas, 18m articulated urban bus, EURO-VI                                        all IAM regions
 =================================================================================================================== ==================

Inventories are from Sacchi_ et al. 2021. The vehicles are available
for different years and emission standards and for each IAM region.

When doing:

.. code-block:: python

    update("buses")

*premise* will create fleet average vehicles for each IAM region. The inventories can be consulted
here: LCIbuses_.

.. _LCIbuses: https://github.com/polca/premise/blob/master/premise/data/additional_inventories/lci-buses.xlsx

At the moment. these inventories do not supply inputs to other activities in the LCI database.
As such, they are optional.


Migration between ecoinvent versions
------------------------------------

Because the additional inventories that are imported may be composed
of exchanges meant to link with an ecoinvent version different
than what the user specifies to *premise* upon the database creation,
it is necessary to be able to "translate" the imported inventories
so that they correctly link to any ecoinvent version *premise* is
compatible with.

Therefore, *premise* has a migration map that is used to convert
certain exchanges to be compatible with a given ecoinvent version.

This migration map is provided here: migrationmap_.

.. _migrationmap: https://github.com/polca/premise/blob/master/premise/data/additional_inventories/migration_map.csv

IAM data collection
"""""""""""""""""""

After extracting the ecoinvent database and additional inventories,
*premise* instantiates the class *IAMDataCollection*, which collects
all sorts of data from the IAM output file and store it into
multi-dimensional arrays.


Production volumes and efficiencies
-----------------------------------

The mapping between IAM variables and *premise* variables regarding production
volumes and efficiencies can be found in the mapping_ file.

.. _mapping: https://github.com/polca/premise/blob/master/premise/iam_variables_mapping/mapping_overview.xlsx

Land use and land use change
----------------------------

The mapping between IAM variables and *premise* variables regarding land use
and emissions caused by land use change can be found in the mapping_ file.


Carbon Capture and Storage
--------------------------

The mapping between IAM variables and *premise* variables regarding carbon capture
and storage can be found in the mapping_ file.


Data sources external to the IAM
--------------------------------

*premise* tries to adhere to the IAM scenario data as much as possible. There are
however a number of cases where external data sources are used. This is notably the case
for non-CO2 pollutants emissions for different sectors (electricity, heat, steel, and cement),
as well as expected efficiency gains for photovoltaic panels and batteries.

Air emissions
*************

*premise* relies on projections from the air emissions model GAINS-IAM_
to adjust the emissions of pollutants for different sectors.
As with efficiencies, *premise* stores the change in emissions (called *scaling factor*)
of a given technology relative to 2020. This is based on the fact that the emissions of
ecoinvent datasets are believed to reflect the current (2020) situation.
Hence, if a technology, in a given region, has a *scaling factor* of 1.2 in 2030,
this means that the corresponding ecoinvent dataset is adjusted so that its emissions
of a given substance is improved by 20%. In other words, *premise* does not use
the emissions level given by GAINS, but rather its change over time relative to 2020.

For more information about this step, refer to sub-section "GAINS emission factors" in the
EXTRACT section.

.. _GAINS-IAM: https://gains.iiasa.ac.at/gains/IAM/index.login


Photovoltaic panels
*******************

Module efficiencies in 2010 for micro-Si and single-Si are from IEA_PV_ Task 12
report. For multi-Si, CIGS, CIS and CdTe, they are from IEA2_ road map report
on PV panels.

.. _IEA2: https://iea.blob.core.windows.net/assets/3a99654f-ffff-469f-b83c-bf0386ed8537/pv_roadmap.pdf

Current (2020) module efficiencies for all PV types are given by a 2021
report from the Fraunhofer_ Institute.

The efficiencies indicated for 2050 are what has been obtained in laboratory
conditions by the Fraunhofer_ Institute. In other words, it is assumed that by 2050,
solar PVs will reach production level efficiencies equal to those observed today
in laboratories.

.. _Fraunhofer: https://www.ise.fraunhofer.de/content/dam/ise/de/documents/publications/studies/Photovoltaics-Report.pdff

 ====================== =========== ============ =========== ======= ====== =======
  % module efficiency    micro-Si    single-Si    multi-Si    CIGS    CIS    CdTe
 ====================== =========== ============ =========== ======= ====== =======
  2010                   10          15.1         14          11      11     10
  2020                   11.9        17.9         16.8        14      14     16.8
  2050                   12.5        26.7         24.4        23.4    23.4   21
 ====================== =========== ============ =========== ======= ====== =======
