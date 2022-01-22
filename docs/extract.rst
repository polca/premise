EXTRACT
=======

The EXTRACT phase consists of the following steps:

* extraction and cleaning of the ecoinvent database
* import and cleaning of additional inventories
* import and cleaning of user-provided inventories (optional)
* loading of IAM data

Supported versions of ecoinvent
"""""""""""""""""""""""""""""""

*premise* currently works with the following ecoinvent database versions:

* v.3.5, cut-off
* v.3.6, cut-off
* v.3.7, cut-off
* v.3.71., cut-off
* v.3.8, cut-off

Work is being carried out to develop compatibility with the *consequential*
version of the ecoinvent database.

Supported sources of ecoinvent
""""""""""""""""""""""""""""""

*premise* can extract the ecoinvent database from:

* a brightway2_ project that contains the ecoinvent database
* ecosposld2 files, that can be downloaded from the ecoinvent_ website

.. _ecoinvent: https://ecoinvent.org
.. _brightway2: https://brightway.dev/



From a brightway2 project
-------------------------

To extract from an ecoinvent database located in a brightway2 project, simply
indicate the database name in `source_db` and its version in `source_version`:

.. code-block:: python

  from premise import *
  import brightway2 as bw

  bw.projects.set_current("my_project)

  ndb = NewDatabase(
        scenarios=[
                {"model":"remind", "pathway":"SSP2-Base", "year":2028}
            ],
        source_db="ecoinvent 3.7 cutoff", # <-- this is NEW.
        source_version="3.7.1", # <-- this is NEW
        key='xxxxxxxxxxxxxxxxxxxxxxxxx'
    )

Note that a cache of the database will be created the first time and
store in the library folder. Any subsequent creation of databases
using the same ecoinvent version will no longer require this extraction
step.

If you wish to clear that cache folder, do:

.. code-block:: python

    from premise import *

    clear_cache()

From ecospold2 files
--------------------

To extract from a set of ecospold2 files, you need to point to the location of those files
in `source_file_path`, as well as indicate the database format in `source_type`:

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

Import of additional inventories
""""""""""""""""""""""""""""""""

After the ecoinvent database is extracted and checked, a number of additional invenotries
are imported, regardless of the year of scenario that is being considered.


Power generation
----------------

A number of  datasets relating to power generation not originally present in
ecoinvent are imported. The next sub-sections lists such datasets.

Power plants with CCS
*********************

Datasets for power generation with Carbon Capture and Storage are imported.
They originate from Volkart et al. 2013_, and can be consulted here: LCI_Power_generation_.

.. _2013: https://doi.org/10.1016/j.ijggc.2013.03.003
.. _LCI_Power_generation: https://github.com/romainsacchi/premise/blob/master/premise/data/additional_inventories/lci-Carma-CCS.xlsx

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

.. _LCI_Oil_NG: https://github.com/romainsacchi/premise/blob/master/premise/data/additional_inventories/lci-ESU-oil-and-gas.xlsx

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


Photovoltaic panels
*******************

Photovoltaic panel inventories originate the IEA's Task 12 project IEA_PV_. They have been adapted
into a brightway2-friendly format. They can be consulted here: LCI_PV_.

.. _IEA_PV: https://iea-pvps.org/wp-content/uploads/2020/12/IEA-PVPS-LCI-report-2020.pdf
.. _LCI_PV: https://github.com/romainsacchi/premise/blob/master/premise/data/additional_inventories/lci-PV.xlsx

They consist of the following PV installation types:

 ============================================================================================ ===========
  PV installation                                                                              location
 ============================================================================================ ===========
  photovoltaic slanted-roof installation, 1.3 MWp, multi-Si, panel, mounted, on roof           CH
  photovoltaic flat-roof installation, 156 kWp, multi-Si, on roof                              CH
  photovoltaic flat-roof installation, 156 kWp, single-Si, on roof                             CH
  photovoltaic flat-roof installation, 280 kWp, multi-Si, on roof                              CH
  photovoltaic flat-roof installation, 280 kWp, single-Si, on roof                             CH
  photovoltaic flat-roof installation, 324 kWp, multi-Si, on roof                              DE
  photovoltaic slanted-roof installation, 3 kWp, CIS, laminated, integrated, on roof           CH
  photovoltaic slanted-roof installation, 3 kWp, CIS, laminated, integrated, on roof           RER
  photovoltaic slanted-roof installation, 3 kWp, CdTe, panel, mounted, on roof                 CH
  photovoltaic slanted-roof installation, 3 kWp, CdTe, panel, mounted, on roof                 RER
  photovoltaic slanted-roof installation, 3 kWp, micro-Si, laminated, integrated, on roof      RER
  photovoltaic slanted-roof installation, 3 kWp, micro-Si, panel, mounted, on roof             RER
  photovoltaic flat-roof installation, 450 kWp, single-Si, on roof                             DE
  photovoltaic open ground installation, 560 kWp, single-Si, on open ground                    CH
  photovoltaic open ground installation, 569 kWp, multi-Si, on open ground                     ES
  photovoltaic open ground installation, 570 kWp, CIS, on open ground                          RER
  photovoltaic open ground installation, 570 kWp, CdTe, on open ground                         RER
  photovoltaic open ground installation, 570 kWp, micro-Si, on open ground                     RER
  photovoltaic open ground installation, 570 kWp, multi-Si, on open ground                     ES
  photovoltaic open ground installation, 570 kWp, multi-Si, on open ground                     RER
  photovoltaic open ground installation, 570 kWp, single-Si, on open ground                    RER
  photovoltaic slanted-roof installation, 93 kWp, multi-Si, laminated, integrated, on roof     CH
  photovoltaic slanted-roof installation, 93 kWp, multi-Si, panel, mounted, on roof            CH
  photovoltaic slanted-roof installation, 93 kWp, single-Si, laminated, integrated, on roof    CH
  photovoltaic slanted-roof installation, 93 kWp, single-Si, panel, mounted, on roof           CH
 ============================================================================================ ===========


Although these datasets have a limited number of locations (CH, RER, DE, ES),
the IEA report provides country-specific load factors:

 ======================= =========== ========= ==========
  production [kWh/kWp]    roof-top    façade    central
 ======================= =========== ========= ==========
  PT                      1427        999       1513
  IL                      1695        1187      1798
  SE                      919         643       974
  FR                      968         678       1026
  TR                      1388        971       1471
  NZ                      1240        868       1315
  MY                      1332        933       1413
  CN                      971         679       1029
  TH                      1436        1005      1522
  ZA                      1634        1144      1733
  JP                      1024        717       1086
  CH                      976         683       1040
  DE                      922         645       978
  KR                      1129        790       1197
  AT                      1044        731       1111
  GR                      1323        926       1402
  IE                      796         557       844
  AU                      1240        868       1314
  IT                      1298        908       1376
  MX                      1612        1128      1709
  NL                      937         656       994
  GB                      848         593       899
  ES                      1423        996       1509
  CL                      1603        1122      1699
  HU                      1090        763       1156
  CZ                      944         661       1101
  CA                      1173        821       1243
  US                      1401        981       1485
  NO                      832         583       882
  FI                      891         624       945
  BE                      908         635       962
  DK                      971         680       1030
  LU                      908         635       962
 ======================= =========== ========= ==========


In the report, the generation potential per installation type is multiplied by the number of installations
in each country, to produce country-specific PV power mix datasets normalized to 1 kWh.
The report specifies the production-weighted PV mix for each country, but we further split it
between residential (<=3kWp) and commercial (>3kWp) installations
(as most IAMs make such distinction):

 ==================================================== ===========
  Production-weighted PV mix                           location
 ==================================================== ===========
  electricity production, photovoltaic, residential    PT
  electricity production, photovoltaic, residential    IL
  electricity production, photovoltaic, residential    SE
  electricity production, photovoltaic, residential    FR
  electricity production, photovoltaic, residential    TR
  electricity production, photovoltaic, residential    NZ
  electricity production, photovoltaic, residential    MY
  electricity production, photovoltaic, residential    CN
  electricity production, photovoltaic, residential    TH
  electricity production, photovoltaic, residential    ZA
  electricity production, photovoltaic, residential    JP
  electricity production, photovoltaic, residential    CH
  electricity production, photovoltaic, residential    DE
  electricity production, photovoltaic, residential    KR
  electricity production, photovoltaic, residential    AT
  electricity production, photovoltaic, residential    GR
  electricity production, photovoltaic, residential    IE
  electricity production, photovoltaic, residential    AU
  electricity production, photovoltaic, residential    IT
  electricity production, photovoltaic, residential    MX
  electricity production, photovoltaic, residential    NL
  electricity production, photovoltaic, residential    GB
  electricity production, photovoltaic, residential    ES
  electricity production, photovoltaic, residential    CL
  electricity production, photovoltaic, residential    HU
  electricity production, photovoltaic, residential    CZ
  electricity production, photovoltaic, residential    CA
  electricity production, photovoltaic, residential    US
  electricity production, photovoltaic, residential    NO
  electricity production, photovoltaic, residential    FI
  electricity production, photovoltaic, residential    BE
  electricity production, photovoltaic, residential    DK
  electricity production, photovoltaic, residential    LU
  electricity production, photovoltaic, commercial     PT
  electricity production, photovoltaic, commercial     IL
  electricity production, photovoltaic, commercial     SE
  electricity production, photovoltaic, commercial     FR
  electricity production, photovoltaic, commercial     TR
  electricity production, photovoltaic, commercial     NZ
  electricity production, photovoltaic, commercial     MY
  electricity production, photovoltaic, commercial     CN
  electricity production, photovoltaic, commercial     TH
  electricity production, photovoltaic, commercial     ZA
  electricity production, photovoltaic, commercial     JP
  electricity production, photovoltaic, commercial     CH
  electricity production, photovoltaic, commercial     DE
  electricity production, photovoltaic, commercial     KR
  electricity production, photovoltaic, commercial     AT
  electricity production, photovoltaic, commercial     GR
  electricity production, photovoltaic, commercial     IE
  electricity production, photovoltaic, commercial     AU
  electricity production, photovoltaic, commercial     IT
  electricity production, photovoltaic, commercial     MX
  electricity production, photovoltaic, commercial     NL
  electricity production, photovoltaic, commercial     GB
  electricity production, photovoltaic, commercial     ES
  electricity production, photovoltaic, commercial     CL
  electricity production, photovoltaic, commercial     HU
  electricity production, photovoltaic, commercial     CZ
  electricity production, photovoltaic, commercial     CA
  electricity production, photovoltaic, commercial     US
  electricity production, photovoltaic, commercial     NO
  electricity production, photovoltaic, commercial     FI
  electricity production, photovoltaic, commercial     BE
  electricity production, photovoltaic, commercial     DK
  electricity production, photovoltaic, commercial     LU
 ==================================================== ===========

Hence, inside the *residential* PV mix of Spain ("electricity production, photovoltaic, residential"),
one will find the following inputs for the production of 1kWh:

 ========================================================================================== ============== =========== ============
  name                                                                                       amount         location    unit
 ========================================================================================== ============== =========== ============
  Energy, solar, converted                                                                   3.8503                     megajoule
  Heat, waste                                                                                0.25027                    megajoule
  photovoltaic slanted-roof installation, 3 kWp, CIS, laminated, integrated, on roof         2.48441E-08    CH          unit
  photovoltaic slanted-roof installation, 3 kWp, CdTe, panel, mounted, on roof               4.99911E-07    CH          unit
  photovoltaic slanted-roof installation, 3 kWp, micro-Si, laminated, integrated, on roof    3.93869E-09    RER         unit
  photovoltaic slanted-roof installation, 3 kWp, micro-Si, panel, mounted, on roof           6.55186E-08    RER         unit
  photovoltaic facade installation, 3kWp, multi-Si, laminated, integrated, at building       2.10481E-07    RER         unit
  photovoltaic facade installation, 3kWp, multi-Si, panel, mounted, at building              2.10481E-07    RER         unit
  photovoltaic facade installation, 3kWp, single-Si, laminated, integrated, at building      1.11463E-07    RER         unit
  photovoltaic facade installation, 3kWp, single-Si, panel, mounted, at building             1.11463E-07    RER         unit
  photovoltaic flat-roof installation, 3kWp, multi-Si, on roof                               2.20794E-06    RER         unit
  photovoltaic flat-roof installation, 3kWp, single-Si, on roof                              1.17025E-06    RER         unit
  photovoltaic slanted-roof installation, 3kWp, CIS, panel, mounted, on roof                 4.12805E-07    CH          unit
  photovoltaic slanted-roof installation, 3kWp, CdTe, laminated, integrated, on roof         3.00704E-08    CH          unit
  photovoltaic slanted-roof installation, 3kWp, multi-Si, laminated, integrated, on roof     1.08693E-07    RER         unit
  photovoltaic slanted-roof installation, 3kWp, multi-Si, panel, mounted, on roof            1.81407E-06    RER         unit
  photovoltaic slanted-roof installation, 3kWp, single-Si, laminated, integrated, on roof    5.75655E-08    RER         unit
  photovoltaic slanted-roof installation, 3kWp, single-Si, panel, mounted, on roof           9.6195E-07     RER         unit
 ========================================================================================== ============== =========== ============

with, for example, 2.48E-8 units of "photovoltaic slanted-roof installation, 3 kWp, CIS, laminated, integrated, on roof"
being calculated as:

.. code-block::

    1 / (30 [years] * 1423 [kWh/kWp] * 0.32% [share of PV capacity of such type installed in Spain])

Note that commercial PV mix datasets provide electricity at high voltage, unlike residential
PV mix datasets, which supply at low voltage only.

Geothermal
**********

Heat production by means of a geothermal well are not represented in ecoinvent.
The geothermal power plant construction inventories are from Maeder_ Bachelor Thesis.

.. _Maeder: https://www.psi.ch/sites/default/files/import/ta/PublicationTab/BSc_Mattia_Maeder_2016.pdf

The co-generation unit has been removed and replaced by heat exchanger and
district heating pipes. Gross heat output of 1,483 TJ, with 80% efficiency.

The inventories can be consulted here: LCIgeothermal_.

.. _LCIgeothermal: https://github.com/romainsacchi/premise/blob/master/premise/data/additional_inventories/lci-geothermal.xlsx

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

*premise* imports inventories for hydrogen production via the following pathways:

* Steam Methane Reforming, using natural gas
* Steam Methane Reforming, using natural gas, with Carbon Capture and Storage
* Steam Methane Reforming, using bio-methane
* Steam Methane Reforming, using bio-methane, with Carbon Capture and Storage
* Auto Thermal Reforming, using natural gas
* Auto Thermal Reforming, using natural gas, with Carbon Capture and Storage
* Auto Thermal Reforming, using bio-methane
* Auto Thermal Reforming, using bio-methane, with Carbon Capture and Storage
* Woody biomass gasification, using a fluidized bed
* Woody biomass gasification, using a fluidized bed, with Carbon Capture and Storage
* Woody biomass gasification, using an entrained flow gasifier
* Woody biomass gasification, using an entrained flow gasifier, with Carbon Capture and Storage
* Coal gasification
* Electrolysis

Inventories using Steam Methane Reforming are from Antonini_ et al. 2021.
They can be consulted here: LCI_SMR_.
Inventories using Auto Thermal Reforming are from Antonini_ et al. 2021.
They can be consulted here: LCI_ATR_.
Inventories using Woody biomass gasification are from Antonini2_ et al. 2021.
They can be consulted here: LCI_woody_.
Inventories using coal gasification are from Wokaun_ et al. 2011.
They can be consulted here: LCI_coal_.
Inventories using electrolysis are from Bareiss_ et al. 2019.
They can be consulted here: LCI_electrolysis_.

.. _Antonini: https://pubs.rsc.org/en/content/articlelanding/2020/se/d0se00222d
.. _Antonini2: https://pubs.rsc.org/en/Content/ArticleLanding/2021/SE/D0SE01637C
.. _Wokaun: https://www.cambridge.org/core/books/transition-to-hydrogen/43144AF26ED80E7106B675A6E83B1579
.. _Bareiss: https://www.sciencedirect.com/science/article/pii/S0306261919300017
.. _LCI_SMR: https://github.com/romainsacchi/premise/blob/master/premise/data/additional_inventories/lci-hydrogen-smr-atr-natgas.xlsx
.. _LCI_ATR: https://github.com/romainsacchi/premise/blob/master/premise/data/additional_inventories/lci-hydrogen-smr-atr-natgas.xlsx
.. _LCI_woody: https://github.com/romainsacchi/premise/blob/master/premise/data/additional_inventories/lci-hydrogen-wood-gasification.xlsx
.. _LCI_coal: https://github.com/romainsacchi/premise/blob/master/premise/data/additional_inventories/lci-hydrogen-coal-gasification.xlsx
.. _LCI_electrolysis: https://github.com/romainsacchi/premise/blob/master/premise/data/additional_inventories/lci-hydrogen-electrolysis.xlsx

The new datasets introduced are listed in the table below (only production datasets are shown).

 ======================================================================================================================================= ===========
  Hydrogen production                                                                                                                     location
 ======================================================================================================================================= ===========
  hydrogen production, steam methane reforming of natural gas, 25 bar                                                                     CH
  hydrogen production, steam methane reforming of natural gas, with CCS (MDEA, 98% eff.), 25 bar                                          CH
  hydrogen production, steam methane reforming, from biomethane, high and low temperature, with CCS (MDEA, 98% eff.), 26 bar              CH
  hydrogen production, steam methane reforming, from biomethane, high and low temperature, 26 bar                                         CH
  hydrogen production, auto-thermal reforming, from biomethane, 25 bar                                                                    CH
  hydrogen production, auto-thermal reforming, from biomethane, with CCS (MDEA, 98% eff.), 25 bar                                         CH
  hydrogen production, gaseous, 25 bar, from heatpipe reformer gasification of woody biomass with CCS, at gasification plant              CH
  hydrogen production, gaseous, 25 bar, from heatpipe reformer gasification of woody biomass, at gasification plant                       CH
  hydrogen production, gaseous, 25 bar, from gasification of woody biomass in entrained flow gasifier, with CCS, at gasification plant    CH
  hydrogen production, gaseous, 25 bar, from gasification of woody biomass in entrained flow gasifier, at gasification plant              CH
  hydrogen production, gaseous, 30 bar, from hard coal gasification and reforming, at coal gasification plant                             RER
  hydrogen production, gaseous, 25 bar, from electrolysis                                                                                 RER
 ======================================================================================================================================= ===========

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

These datasets originate from the work of Wulff_ et al. 2018, and can be
consulted here: LCI_H2_distr_. For re-assigned CNG pipelines, which require the hydrogen
to be mixed together with oxygen to limit metal embrittlement,
some parameters are taken from the work of Cerniauskas_ et al. 2020.

The datasets introduced are listed in the table below.

 ================================================================== ===========
  Hydrogen distribution                                              location
 ================================================================== ===========
  Hydrogen refuelling station                                        GLO
  high pressure hydrogen storage tank                                GLO
  distribution pipeline for hydrogen, dedicated hydrogen pipeline    RER
  transmission pipeline for hydrogen, dedicated hydrogen pipeline    RER
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


.. _Wulff:
.. _LCI_H2_distr: https://github.com/romainsacchi/premise/blob/master/premise/data/additional_inventories/lci-hydrogen-distribution.xlsx
.. _Cerniauskas: https://doi.org/10.1016/j.ijhydene.2020.02.121

Biofuels
--------

Inventories for energy crops- and residues-based production of bioethanol and biodiesel
are imported, and can be consulted here: LCI_biofuels_. They include the farming of the crop,
the conversion of hte biomass to fuel, as well as its distribution. The conversion process
often leads to the production of co-products (dried distiller's grain, electricity, CO2, bagasse.).
Hence, energy, economic and system expansion partitioning approaches are available.
These inventories originate from several different sources
(Wu_ et al. 2006 (2020 update), Cozzolini_ 2018, Pereira_ et al. 2019 and Gonzalez-Garcia_ et al. 2012),
indicated in the table below.

.. _LCI_biofuels: https://github.com/romainsacchi/premise/blob/master/premise/data/additional_inventories/lci-biofuels.xlsx
.. _Cozzolini: https://www.psi.ch/sites/default/files/2019-09/Cozzolino_377125_%20Research%20Project%20Report.pdf
.. _Gonzalez-Garcia: https://doi.org/10.1016/j.scitotenv.2012.07.044
.. _Wu: http://greet.es.anl.gov/publication-2lli584z
.. _Pereira: http://task39.sites.olt.ubc.ca/files/2019/04/Task-39-GHS-models-Final-Report-Phase-1.pdf

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
  Farming and supply of wheat grains                                                 RER         Cozzolini 2018
  Farming and supply of wheat straw                                                  RER         Cozzolini 2018
  Farming and supply of corn                                                         RER         Cozzolini 2018/RED II
  Farming and supply of sugarbeet                                                    RER         Cozzolini 2018
  Supply of forest residue                                                           RER         Cozzolini 2018
  Supply and refining of waste cooking oil                                           RER         Cozzolini 2018
  Farming and supply of rapeseed                                                     RER         Cozzolini 2018/RED II
  Farming and supply of palm fresh fruit bunch                                       RER         Cozzolini 2018
  Farming and supply of dry algae                                                    RER         Cozzolini 2018
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
  Ethanol production, via fermentation, from wheat grains                            RER         Cozzolini 2018
  Ethanol production, via fermentation, from wheat straw                             RER         Cozzolini 2018
  Ethanol production, via fermentation, from corn starch                             RER         Cozzolini 2018
  Ethanol production, via fermentation, from sugarbeet                               RER         Cozzolini 2018
  Ethanol production, via fermentation, from forest residue                          RER         Cozzolini 2018
  Biodiesel production, via transesterification, from used cooking oil               RER         Cozzolini 2018
  Biodiesel production, via transesterification, from rapeseed oil                   RER         Cozzolini 2018
  Biodiesel production, via transesterification, from palm oil, energy allocation    RER         Cozzolini 2018
  Biodiesel production, via transesterification, from algae, energy allocation       RER         Cozzolini 2018
 ================================================================================== =========== =============================

Synthetic fuels
---------------

*premise* imports inventories for the synthesis of hydrocarbon fuels
following two pathways:

* *Fischer-Tropsch*: it uses hydrogen and CO (from CO2 via a reverse water gas
  shit process) to produce syncrude, which is cracked into diesel, kerosene,
  naphtha and lubricating oil. Inventories are from van der Giesen_ et al. 2014.
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
  Diesel production, synthetic, from Fischer Tropsch process, hydrogen from electrolysis, at fuelling station                                                                  all IAM regions    van der Giesen et al. 2014
  Diesel production, synthetic, from Fischer Tropsch process, hydrogen from wood gasification, at fuelling station                                                             all IAM regions    van der Giesen et al. 2014
  Diesel production, synthetic, from Fischer Tropsch process, hydrogen from wood gasification, with CCS, at fuelling station                                                   all IAM regions    van der Giesen et al. 2014
  Diesel production, synthetic, from methanol, hydrogen from coal gasification, CO2 from DAC, at fuelling station                                                              all IAM regions    Hank et al, 2019
  Diesel production, synthetic, from methanol, hydrogen from electrolysis, CO2 from cement plant, at fuelling station                                                          all IAM regions    Hank et al, 2019
  Diesel production, synthetic, from methanol, hydrogen from electrolysis, CO2 from DAC, at fuelling station                                                                   all IAM regions    Hank et al, 2019
  Gasoline production, synthetic, from methanol, hydrogen from coal gasification, CO2 from DAC, at fuelling station                                                            all IAM regions    Hank et al, 2019
  Gasoline production, synthetic, from methanol, hydrogen from electrolysis, CO2 from cement plant, at fuelling station                                                        all IAM regions    Hank et al, 2019
  Gasoline production, synthetic, from methanol, hydrogen from electrolysis, CO2 from DAC, at fuelling station                                                                 all IAM regions    Hank et al, 2019
  Kerosene production, from methanol, hydrogen from coal gasification, CO2 from DAC                                                                                            all IAM regions    Hank et al, 2019
  Kerosene production, from methanol, hydrogen from electrolysis, CO2 from cement plant                                                                                        all IAM regions    Hank et al, 2019
  Kerosene production, from methanol, hydrogen from electrolysis, CO2 from DAC                                                                                                 all IAM regions    Hank et al, 2019
  Kerosene production, synthetic, Fischer Tropsch process, hydrogen from coal gasification                                                                                     all IAM regions    van der Giesen et al. 2014
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


Direct Air Capture
------------------

Inventories for direct air capture of CO2 using ClimeWork's low-temperature
process are imported. They originate from the work of Terlouw_ et al. 2021.
They can be consulted here: LCI_DAC_.

.. _Terlouw: https://pubs.acs.org/doi/10.1021/acs.est.1c03263
.. _LCI_DAC: https://github.com/romainsacchi/premise/blob/master/premise/data/additional_inventories/lci-direct-air-capture.xlsx

They introduce the following datasets:


 ================================================================================== ===========
  Activity                                                                           Location
 ================================================================================== ===========
  carbon dioxide, captured from the atmosphere                                       RER
 ================================================================================== ===========

In its default configuration, the DAC process uses "free of burden" excess heat
and grid-supplied electricity. *premise* creates various configurations of that
same process, using different sources for heat (industrial steam heat, heat
pump heat), which are found under the following names, for each IAM region:

 ============================================================================================= ==================
  name                                                                                          location
 ============================================================================================= ==================
  carbon dioxide, captured from atmosphere, with heat pump heat, and grid electricity           all IAM regions
  carbon dioxide, captured from atmosphere, with industrial steam heat, and grid electricity    all IAM regions
  carbon dioxide, captured from atmosphere, with waste heat, and grid electricity               all IAM regions
 ============================================================================================= ==================


Li-ion battery
--------------

NMC-111, NMC-6222 NMC-811 and NCA Lithium-ion battery inventories are originally
from Dai_ et al. 2019. They have been adapted to ecoinvent by Crenna_ et al, 2021.
LFP and LTO Lithium-ion battery inventories are from  Schmidt_ et al. 2019.
Additionally, *premise* uses new inventories for natural graphite, from Engels_
et al. 2022,to partly replace the synthetic graphite used in Dai's inventories,
to represent a 50:50 split between natural and synthetic graphite.
These inventories can be found here: LCI_batteries_.

.. _Dai: https://www.mdpi.com/2313-0105/5/2/48
.. _Crenna: https://doi.org/10.1016/j.resconrec.2021.105619
.. _Schmidt: https://doi.org/10.1021/acs.est.8b05313
.. _Engels: https://doi.org/10.1016/j.jclepro.2022.130474
.. _LCI_batteries: https://github.com/romainsacchi/premise/blob/master/premise/data/additional_inventories/lci-batteries.xlsx

They introduce the following datasets:

 ============================== =========== ======================================
  Battery components             location    source
 ============================== =========== ======================================
  Battery BoP                    GLO         Schmidt et al. 2019
  Battery cell, NMC-111          GLO         Dai et al. 2019, Crenna et al. 2021
  Battery cell, NMC-622          GLO         Dai et al. 2019, Crenna et al. 2021
  Battery cell, NMC-811          GLO         Dai et al. 2019, Crenna et al. 2021
  Battery cell, NCA              GLO         Dai et al. 2019, Crenna et al. 2021
  Battery cell, LFP              GLO         Schmidt et al. 2019
  Battery cell, LTO              GLO         Schmidt et al. 2019
  coating of natural graphite    CN          Engels et al. 2022
  graphite purification          CN          Engels et al. 2022
  graphite ore mining            CN          Engels et al. 2022
 ============================== =========== ======================================

These battery inventories are mostly used by battery electric vehicles
(also imported by *premise*), and are to be preferred to battery
inventories coming with ecoinvent.


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
.. _LCItwowheelers: https://github.com/romainsacchi/premise/blob/master/premise/data/additional_inventories/lci-two_wheelers.xlsx

 ====================================== ==================
  Two-wheeler datasets                   location
 ====================================== ==================
  Kick Scooter, electric, <1kW           all IAM regions
  Bicycle, conventional, urban           all IAM regions
  Bicycle, electric (<25 km/h)           all IAM regions
  Bicycle, electric (<45 km/h)           all IAM regions
  Bicycle, electric, cargo bike          all IAM regions
  Moped, gasoline, <4kW, EURO-3          all IAM regions
  Moped, gasoline, <4kW, EURO-4          all IAM regions
  Moped, gasoline, <4kW, EURO-5          all IAM regions
  Scooter, gasoline, <4kW, EURO-3        all IAM regions
  Scooter, gasoline, <4kW, EURO-4        all IAM regions
  Scooter, gasoline, <4kW, EURO-5        all IAM regions
  Scooter, gasoline, 4-11kW, EURO-3      all IAM regions
  Scooter, gasoline, 4-11kW, EURO-4      all IAM regions
  Scooter, gasoline, 4-11kW, EURO-5      all IAM regions
  Scooter, electric, <4kW                all IAM regions
  Scooter, electric, 4-11kW              all IAM regions
  Motorbike, gasoline, 4-11kW, EURO-3    all IAM regions
  Motorbike, gasoline, 4-11kW, EURO-4    all IAM regions
  Motorbike, gasoline, 4-11kW, EURO-5    all IAM regions
  Motorbike, gasoline, 11-35kW, EURO-3   all IAM regions
  Motorbike, gasoline, 11-35kW, EURO-4   all IAM regions
  Motorbike, gasoline, 11-35kW, EURO-5   all IAM regions
  Motorbike, gasoline, >35kW, EURO-3     all IAM regions
  Motorbike, gasoline, >35kW, EURO-4     all IAM regions
  Motorbike, gasoline, >35kW, EURO-5     all IAM regions
  Motorbike, electric, <4kW              all IAM regions
  Motorbike, electric, 4-11kW            all IAM regions
  Motorbike, electric, 11-35kW           all IAM regions
  Motorbike, electric, >35kW             all IAM regions
 ====================================== ==================


Passenger cars
**************

The following datasets for passenger cars are imported.

 =============================================================================== ==================
  Passenger car datasets                                                          location
 =============================================================================== ==================
  transport, passenger car, gasoline, Large, EURO-2                               all IAM regions
  transport, passenger car, gasoline, Large, EURO-3                               all IAM regions
  transport, passenger car, gasoline, Large, EURO-4                               all IAM regions
  transport, passenger car, gasoline, Large, EURO-6ab                             all IAM regions
  transport, passenger car, gasoline, Large, EURO-6d-TEMP                         all IAM regions
  transport, passenger car, gasoline, Large, EURO-6d                              all IAM regions
  transport, passenger car, diesel, Large, EURO-2                                 all IAM regions
  transport, passenger car, diesel, Large, EURO-3                                 all IAM regions
  transport, passenger car, diesel, Large, EURO-4                                 all IAM regions
  transport, passenger car, diesel, Large, EURO-6ab                               all IAM regions
  transport, passenger car, diesel, Large, EURO-6d-TEMP                           all IAM regions
  transport, passenger car, diesel, Large, EURO-6d                                all IAM regions
  transport, passenger car, compressed gas, Large, EURO-2                         all IAM regions
  transport, passenger car, compressed gas, Large, EURO-3                         all IAM regions
  transport, passenger car, compressed gas, Large, EURO-4                         all IAM regions
  transport, passenger car, compressed gas, Large, EURO-6ab                       all IAM regions
  transport, passenger car, compressed gas, Large, EURO-6d-TEMP                   all IAM regions
  transport, passenger car, compressed gas, Large, EURO-6d                        all IAM regions
  transport, passenger car, plugin gasoline hybrid, Large, EURO-6ab               all IAM regions
  transport, passenger car, plugin gasoline hybrid, Large, EURO-6d-TEMP           all IAM regions
  transport, passenger car, plugin gasoline hybrid, Large, EURO-6d                all IAM regions
  transport, passenger car, plugin diesel hybrid, Large, EURO-6ab                 all IAM regions
  transport, passenger car, plugin diesel hybrid, Large, EURO-6d-TEMP             all IAM regions
  transport, passenger car, plugin diesel hybrid, Large, EURO-6d                  all IAM regions
  transport, passenger car, fuel cell electric, Large                             all IAM regions
  transport, passenger car, battery electric, NMC-622 battery, Large              all IAM regions
  transport, passenger car, gasoline hybrid, Large, EURO-6ab                      all IAM regions
  transport, passenger car, gasoline hybrid, Large, EURO-6d-TEMP                  all IAM regions
  transport, passenger car, gasoline hybrid, Large, EURO-6d                       all IAM regions
  transport, passenger car, diesel hybrid, Large, EURO-6ab                        all IAM regions
  transport, passenger car, diesel hybrid, Large, EURO-6d-TEMP                    all IAM regions
  transport, passenger car, diesel hybrid, Large, EURO-6d                         all IAM regions
  transport, passenger car, gasoline, Large SUV, EURO-2                           all IAM regions
  transport, passenger car, gasoline, Large SUV, EURO-3                           all IAM regions
  transport, passenger car, gasoline, Large SUV, EURO-4                           all IAM regions
  transport, passenger car, gasoline, Large SUV, EURO-6ab                         all IAM regions
  transport, passenger car, gasoline, Large SUV, EURO-6d-TEMP                     all IAM regions
  transport, passenger car, gasoline, Large SUV, EURO-6d                          all IAM regions
  transport, passenger car, diesel, Large SUV, EURO-2                             all IAM regions
  transport, passenger car, diesel, Large SUV, EURO-3                             all IAM regions
  transport, passenger car, diesel, Large SUV, EURO-4                             all IAM regions
  transport, passenger car, diesel, Large SUV, EURO-6ab                           all IAM regions
  transport, passenger car, diesel, Large SUV, EURO-6d-TEMP                       all IAM regions
  transport, passenger car, diesel, Large SUV, EURO-6d                            all IAM regions
  transport, passenger car, compressed gas, Large SUV, EURO-2                     all IAM regions
  transport, passenger car, compressed gas, Large SUV, EURO-3                     all IAM regions
  transport, passenger car, compressed gas, Large SUV, EURO-4                     all IAM regions
  transport, passenger car, compressed gas, Large SUV, EURO-6ab                   all IAM regions
  transport, passenger car, compressed gas, Large SUV, EURO-6d-TEMP               all IAM regions
  transport, passenger car, compressed gas, Large SUV, EURO-6d                    all IAM regions
  transport, passenger car, plugin gasoline hybrid, Large SUV, EURO-6ab           all IAM regions
  transport, passenger car, plugin gasoline hybrid, Large SUV, EURO-6d-TEMP       all IAM regions
  transport, passenger car, plugin gasoline hybrid, Large SUV, EURO-6d            all IAM regions
  transport, passenger car, plugin diesel hybrid, Large SUV, EURO-6ab             all IAM regions
  transport, passenger car, plugin diesel hybrid, Large SUV, EURO-6d-TEMP         all IAM regions
  transport, passenger car, plugin diesel hybrid, Large SUV, EURO-6d              all IAM regions
  transport, passenger car, fuel cell electric, Large SUV                         all IAM regions
  transport, passenger car, battery electric, NMC-622 battery, Large SUV          all IAM regions
  transport, passenger car, gasoline hybrid, Large SUV, EURO-6ab                  all IAM regions
  transport, passenger car, gasoline hybrid, Large SUV, EURO-6d-TEMP              all IAM regions
  transport, passenger car, gasoline hybrid, Large SUV, EURO-6d                   all IAM regions
  transport, passenger car, diesel hybrid, Large SUV, EURO-6ab                    all IAM regions
  transport, passenger car, diesel hybrid, Large SUV, EURO-6d-TEMP                all IAM regions
  transport, passenger car, diesel hybrid, Large SUV, EURO-6d                     all IAM regions
  transport, passenger car, gasoline, Lower medium, EURO-2                        all IAM regions
  transport, passenger car, gasoline, Lower medium, EURO-3                        all IAM regions
  transport, passenger car, gasoline, Lower medium, EURO-4                        all IAM regions
  transport, passenger car, gasoline, Lower medium, EURO-6ab                      all IAM regions
  transport, passenger car, gasoline, Lower medium, EURO-6d-TEMP                  all IAM regions
  transport, passenger car, gasoline, Lower medium, EURO-6d                       all IAM regions
  transport, passenger car, diesel, Lower medium, EURO-2                          all IAM regions
  transport, passenger car, diesel, Lower medium, EURO-3                          all IAM regions
  transport, passenger car, diesel, Lower medium, EURO-4                          all IAM regions
  transport, passenger car, diesel, Lower medium, EURO-6ab                        all IAM regions
  transport, passenger car, diesel, Lower medium, EURO-6d-TEMP                    all IAM regions
  transport, passenger car, diesel, Lower medium, EURO-6d                         all IAM regions
  transport, passenger car, compressed gas, Lower medium, EURO-2                  all IAM regions
  transport, passenger car, compressed gas, Lower medium, EURO-3                  all IAM regions
  transport, passenger car, compressed gas, Lower medium, EURO-4                  all IAM regions
  transport, passenger car, compressed gas, Lower medium, EURO-6ab                all IAM regions
  transport, passenger car, compressed gas, Lower medium, EURO-6d-TEMP            all IAM regions
  transport, passenger car, compressed gas, Lower medium, EURO-6d                 all IAM regions
  transport, passenger car, plugin gasoline hybrid, Lower medium, EURO-6ab        all IAM regions
  transport, passenger car, plugin gasoline hybrid, Lower medium, EURO-6d-TEMP    all IAM regions
  transport, passenger car, plugin gasoline hybrid, Lower medium, EURO-6d         all IAM regions
  transport, passenger car, plugin diesel hybrid, Lower medium, EURO-6ab          all IAM regions
  transport, passenger car, plugin diesel hybrid, Lower medium, EURO-6d-TEMP      all IAM regions
  transport, passenger car, plugin diesel hybrid, Lower medium, EURO-6d           all IAM regions
  transport, passenger car, fuel cell electric, Lower medium                      all IAM regions
  transport, passenger car, battery electric, NMC-622 battery, Lower medium       all IAM regions
  transport, passenger car, gasoline hybrid, Lower medium, EURO-6ab               all IAM regions
  transport, passenger car, gasoline hybrid, Lower medium, EURO-6d-TEMP           all IAM regions
  transport, passenger car, gasoline hybrid, Lower medium, EURO-6d                all IAM regions
  transport, passenger car, diesel hybrid, Lower medium, EURO-6ab                 all IAM regions
  transport, passenger car, diesel hybrid, Lower medium, EURO-6d-TEMP             all IAM regions
  transport, passenger car, diesel hybrid, Lower medium, EURO-6d                  all IAM regions
  transport, passenger car, gasoline, Medium, EURO-2                              all IAM regions
  transport, passenger car, gasoline, Medium, EURO-3                              all IAM regions
  transport, passenger car, gasoline, Medium, EURO-4                              all IAM regions
  transport, passenger car, gasoline, Medium, EURO-6ab                            all IAM regions
  transport, passenger car, gasoline, Medium, EURO-6d-TEMP                        all IAM regions
  transport, passenger car, gasoline, Medium, EURO-6d                             all IAM regions
  transport, passenger car, diesel, Medium, EURO-2                                all IAM regions
  transport, passenger car, diesel, Medium, EURO-3                                all IAM regions
  transport, passenger car, diesel, Medium, EURO-4                                all IAM regions
  transport, passenger car, diesel, Medium, EURO-6ab                              all IAM regions
  transport, passenger car, diesel, Medium, EURO-6d-TEMP                          all IAM regions
  transport, passenger car, diesel, Medium, EURO-6d                               all IAM regions
  transport, passenger car, compressed gas, Medium, EURO-2                        all IAM regions
  transport, passenger car, compressed gas, Medium, EURO-3                        all IAM regions
  transport, passenger car, compressed gas, Medium, EURO-4                        all IAM regions
  transport, passenger car, compressed gas, Medium, EURO-6ab                      all IAM regions
  transport, passenger car, compressed gas, Medium, EURO-6d-TEMP                  all IAM regions
  transport, passenger car, compressed gas, Medium, EURO-6d                       all IAM regions
  transport, passenger car, plugin gasoline hybrid, Medium, EURO-6ab              all IAM regions
  transport, passenger car, plugin gasoline hybrid, Medium, EURO-6d-TEMP          all IAM regions
  transport, passenger car, plugin gasoline hybrid, Medium, EURO-6d               all IAM regions
  transport, passenger car, plugin diesel hybrid, Medium, EURO-6ab                all IAM regions
  transport, passenger car, plugin diesel hybrid, Medium, EURO-6d-TEMP            all IAM regions
  transport, passenger car, plugin diesel hybrid, Medium, EURO-6d                 all IAM regions
  transport, passenger car, fuel cell electric, Medium                            all IAM regions
  transport, passenger car, battery electric, NMC-622 battery, Medium             all IAM regions
  transport, passenger car, gasoline hybrid, Medium, EURO-6ab                     all IAM regions
  transport, passenger car, gasoline hybrid, Medium, EURO-6d-TEMP                 all IAM regions
  transport, passenger car, gasoline hybrid, Medium, EURO-6d                      all IAM regions
  transport, passenger car, diesel hybrid, Medium, EURO-6ab                       all IAM regions
  transport, passenger car, diesel hybrid, Medium, EURO-6d-TEMP                   all IAM regions
  transport, passenger car, diesel hybrid, Medium, EURO-6d                        all IAM regions
  transport, passenger car, gasoline, Medium SUV, EURO-2                          all IAM regions
  transport, passenger car, gasoline, Medium SUV, EURO-3                          all IAM regions
  transport, passenger car, gasoline, Medium SUV, EURO-4                          all IAM regions
  transport, passenger car, gasoline, Medium SUV, EURO-6ab                        all IAM regions
  transport, passenger car, gasoline, Medium SUV, EURO-6d-TEMP                    all IAM regions
  transport, passenger car, gasoline, Medium SUV, EURO-6d                         all IAM regions
  transport, passenger car, diesel, Medium SUV, EURO-2                            all IAM regions
  transport, passenger car, diesel, Medium SUV, EURO-3                            all IAM regions
  transport, passenger car, diesel, Medium SUV, EURO-4                            all IAM regions
  transport, passenger car, diesel, Medium SUV, EURO-6ab                          all IAM regions
  transport, passenger car, diesel, Medium SUV, EURO-6d-TEMP                      all IAM regions
  transport, passenger car, diesel, Medium SUV, EURO-6d                           all IAM regions
  transport, passenger car, compressed gas, Medium SUV, EURO-2                    all IAM regions
  transport, passenger car, compressed gas, Medium SUV, EURO-3                    all IAM regions
  transport, passenger car, compressed gas, Medium SUV, EURO-4                    all IAM regions
  transport, passenger car, compressed gas, Medium SUV, EURO-6ab                  all IAM regions
  transport, passenger car, compressed gas, Medium SUV, EURO-6d-TEMP              all IAM regions
  transport, passenger car, compressed gas, Medium SUV, EURO-6d                   all IAM regions
  transport, passenger car, plugin gasoline hybrid, Medium SUV, EURO-6ab          all IAM regions
  transport, passenger car, plugin gasoline hybrid, Medium SUV, EURO-6d-TEMP      all IAM regions
  transport, passenger car, plugin gasoline hybrid, Medium SUV, EURO-6d           all IAM regions
  transport, passenger car, plugin diesel hybrid, Medium SUV, EURO-6ab            all IAM regions
  transport, passenger car, plugin diesel hybrid, Medium SUV, EURO-6d-TEMP        all IAM regions
  transport, passenger car, plugin diesel hybrid, Medium SUV, EURO-6d             all IAM regions
  transport, passenger car, fuel cell electric, Medium SUV                        all IAM regions
  transport, passenger car, battery electric, NMC-622 battery, Medium SUV         all IAM regions
  transport, passenger car, gasoline hybrid, Medium SUV, EURO-6ab                 all IAM regions
  transport, passenger car, gasoline hybrid, Medium SUV, EURO-6d-TEMP             all IAM regions
  transport, passenger car, gasoline hybrid, Medium SUV, EURO-6d                  all IAM regions
  transport, passenger car, diesel hybrid, Medium SUV, EURO-6ab                   all IAM regions
  transport, passenger car, diesel hybrid, Medium SUV, EURO-6d-TEMP               all IAM regions
  transport, passenger car, diesel hybrid, Medium SUV, EURO-6d                    all IAM regions
  transport, passenger car, battery electric, NMC-622 battery, Micro              all IAM regions
  transport, passenger car, gasoline, Mini, EURO-2                                all IAM regions
  transport, passenger car, gasoline, Mini, EURO-3                                all IAM regions
  transport, passenger car, gasoline, Mini, EURO-4                                all IAM regions
  transport, passenger car, gasoline, Mini, EURO-6ab                              all IAM regions
  transport, passenger car, gasoline, Mini, EURO-6d-TEMP                          all IAM regions
  transport, passenger car, gasoline, Mini, EURO-6d                               all IAM regions
  transport, passenger car, diesel, Mini, EURO-2                                  all IAM regions
  transport, passenger car, diesel, Mini, EURO-3                                  all IAM regions
  transport, passenger car, diesel, Mini, EURO-4                                  all IAM regions
  transport, passenger car, diesel, Mini, EURO-6ab                                all IAM regions
  transport, passenger car, diesel, Mini, EURO-6d-TEMP                            all IAM regions
  transport, passenger car, diesel, Mini, EURO-6d                                 all IAM regions
  transport, passenger car, compressed gas, Mini, EURO-2                          all IAM regions
  transport, passenger car, compressed gas, Mini, EURO-3                          all IAM regions
  transport, passenger car, compressed gas, Mini, EURO-4                          all IAM regions
  transport, passenger car, compressed gas, Mini, EURO-6ab                        all IAM regions
  transport, passenger car, compressed gas, Mini, EURO-6d-TEMP                    all IAM regions
  transport, passenger car, compressed gas, Mini, EURO-6d                         all IAM regions
  transport, passenger car, plugin gasoline hybrid, Mini, EURO-6ab                all IAM regions
  transport, passenger car, plugin gasoline hybrid, Mini, EURO-6d-TEMP            all IAM regions
  transport, passenger car, plugin gasoline hybrid, Mini, EURO-6d                 all IAM regions
  transport, passenger car, plugin diesel hybrid, Mini, EURO-6ab                  all IAM regions
  transport, passenger car, plugin diesel hybrid, Mini, EURO-6d-TEMP              all IAM regions
  transport, passenger car, plugin diesel hybrid, Mini, EURO-6d                   all IAM regions
  transport, passenger car, fuel cell electric, Mini                              all IAM regions
  transport, passenger car, battery electric, NMC-622 battery, Mini               all IAM regions
  transport, passenger car, gasoline hybrid, Mini, EURO-6ab                       all IAM regions
  transport, passenger car, gasoline hybrid, Mini, EURO-6d-TEMP                   all IAM regions
  transport, passenger car, gasoline hybrid, Mini, EURO-6d                        all IAM regions
  transport, passenger car, diesel hybrid, Mini, EURO-6ab                         all IAM regions
  transport, passenger car, diesel hybrid, Mini, EURO-6d-TEMP                     all IAM regions
  transport, passenger car, diesel hybrid, Mini, EURO-6d                          all IAM regions
  transport, passenger car, gasoline, Small, EURO-2                               all IAM regions
  transport, passenger car, gasoline, Small, EURO-3                               all IAM regions
  transport, passenger car, gasoline, Small, EURO-4                               all IAM regions
  transport, passenger car, gasoline, Small, EURO-6ab                             all IAM regions
  transport, passenger car, gasoline, Small, EURO-6d-TEMP                         all IAM regions
  transport, passenger car, gasoline, Small, EURO-6d                              all IAM regions
  transport, passenger car, diesel, Small, EURO-2                                 all IAM regions
  transport, passenger car, diesel, Small, EURO-3                                 all IAM regions
  transport, passenger car, diesel, Small, EURO-4                                 all IAM regions
  transport, passenger car, diesel, Small, EURO-6ab                               all IAM regions
  transport, passenger car, diesel, Small, EURO-6d-TEMP                           all IAM regions
  transport, passenger car, diesel, Small, EURO-6d                                all IAM regions
  transport, passenger car, compressed gas, Small, EURO-2                         all IAM regions
  transport, passenger car, compressed gas, Small, EURO-3                         all IAM regions
  transport, passenger car, compressed gas, Small, EURO-4                         all IAM regions
  transport, passenger car, compressed gas, Small, EURO-6ab                       all IAM regions
  transport, passenger car, compressed gas, Small, EURO-6d-TEMP                   all IAM regions
  transport, passenger car, compressed gas, Small, EURO-6d                        all IAM regions
  transport, passenger car, plugin gasoline hybrid, Small, EURO-6ab               all IAM regions
  transport, passenger car, plugin gasoline hybrid, Small, EURO-6d-TEMP           all IAM regions
  transport, passenger car, plugin gasoline hybrid, Small, EURO-6d                all IAM regions
  transport, passenger car, plugin diesel hybrid, Small, EURO-6ab                 all IAM regions
  transport, passenger car, plugin diesel hybrid, Small, EURO-6d-TEMP             all IAM regions
  transport, passenger car, plugin diesel hybrid, Small, EURO-6d                  all IAM regions
  transport, passenger car, fuel cell electric, Small                             all IAM regions
  transport, passenger car, battery electric, NMC-622 battery, Small              all IAM regions
  transport, passenger car, gasoline hybrid, Small, EURO-6ab                      all IAM regions
  transport, passenger car, gasoline hybrid, Small, EURO-6d-TEMP                  all IAM regions
  transport, passenger car, gasoline hybrid, Small, EURO-6d                       all IAM regions
  transport, passenger car, diesel hybrid, Small, EURO-6ab                        all IAM regions
  transport, passenger car, diesel hybrid, Small, EURO-6d-TEMP                    all IAM regions
  transport, passenger car, diesel hybrid, Small, EURO-6d                         all IAM regions
  transport, passenger car, gasoline, Van, EURO-2                                 all IAM regions
  transport, passenger car, gasoline, Van, EURO-3                                 all IAM regions
  transport, passenger car, gasoline, Van, EURO-4                                 all IAM regions
  transport, passenger car, gasoline, Van, EURO-6ab                               all IAM regions
  transport, passenger car, gasoline, Van, EURO-6d-TEMP                           all IAM regions
  transport, passenger car, gasoline, Van, EURO-6d                                all IAM regions
  transport, passenger car, diesel, Van, EURO-2                                   all IAM regions
  transport, passenger car, diesel, Van, EURO-3                                   all IAM regions
  transport, passenger car, diesel, Van, EURO-4                                   all IAM regions
  transport, passenger car, diesel, Van, EURO-6ab                                 all IAM regions
  transport, passenger car, diesel, Van, EURO-6d-TEMP                             all IAM regions
  transport, passenger car, diesel, Van, EURO-6d                                  all IAM regions
  transport, passenger car, compressed gas, Van, EURO-2                           all IAM regions
  transport, passenger car, compressed gas, Van, EURO-3                           all IAM regions
  transport, passenger car, compressed gas, Van, EURO-4                           all IAM regions
  transport, passenger car, compressed gas, Van, EURO-6ab                         all IAM regions
  transport, passenger car, compressed gas, Van, EURO-6d-TEMP                     all IAM regions
  transport, passenger car, compressed gas, Van, EURO-6d                          all IAM regions
  transport, passenger car, plugin gasoline hybrid, Van, EURO-6ab                 all IAM regions
  transport, passenger car, plugin gasoline hybrid, Van, EURO-6d-TEMP             all IAM regions
  transport, passenger car, plugin gasoline hybrid, Van, EURO-6d                  all IAM regions
  transport, passenger car, plugin diesel hybrid, Van, EURO-6ab                   all IAM regions
  transport, passenger car, plugin diesel hybrid, Van, EURO-6d-TEMP               all IAM regions
  transport, passenger car, plugin diesel hybrid, Van, EURO-6d                    all IAM regions
  transport, passenger car, fuel cell electric, Van                               all IAM regions
  transport, passenger car, battery electric, NMC-622 battery, Van                all IAM regions
  transport, passenger car, gasoline hybrid, Van, EURO-6ab                        all IAM regions
  transport, passenger car, gasoline hybrid, Van, EURO-6d-TEMP                    all IAM regions
  transport, passenger car, gasoline hybrid, Van, EURO-6d                         all IAM regions
  transport, passenger car, diesel hybrid, Van, EURO-6ab                          all IAM regions
  transport, passenger car, diesel hybrid, Van, EURO-6d-TEMP                      all IAM regions
  transport, passenger car, diesel hybrid, Van, EURO-6d                           all IAM regions
 =============================================================================== ==================

Inventories are from Sacchi2_ et al. 2022 (in review). The vehicles are available
for different years and emission standards and for each IAM region. *premise* will only
import vehicles which production year is equal or inferior to
the scenario year considered. *premise* will create fleet average vehicles
during the *Transport* transformation for each IAM region. The inventories can be consulted
here: LCIpasscars_.

.. _Sacchi2: https://www.psi.ch/en/media/72391/download
.. _LCIpasscars: https://github.com/romainsacchi/premise/blob/master/premise/data/additional_inventories/lci-pass_cars.xlsx


Medium and heavy duty trucks
****************************

The following datasets for medium and heavy-duty trucks are imported.

 ===================================================================================================== ==================
  Truck datasets                                                                                        location
 ===================================================================================================== ==================
  transport, freight, lorry, battery electric, NMC-622 battery, 3.5t gross weight, long haul            all IAM regions
  transport, freight, lorry, fuel cell electric, 3.5t gross weight, long haul                           all IAM regions
  transport, freight, lorry, diesel hybrid, 3.5t gross weight, EURO-VI, long haul                       all IAM regions
  transport, freight, lorry, diesel, 3.5t gross weight, EURO-III, long haul                             all IAM regions
  transport, freight, lorry, diesel, 3.5t gross weight, EURO-IV, long haul                              all IAM regions
  transport, freight, lorry, diesel, 3.5t gross weight, EURO-V, long haul                               all IAM regions
  transport, freight, lorry, diesel, 3.5t gross weight, EURO-VI, long haul                              all IAM regions
  transport, freight, lorry, compressed gas, 3.5t gross weight, EURO-III, long haul                     all IAM regions
  transport, freight, lorry, compressed gas, 3.5t gross weight, EURO-IV, long haul                      all IAM regions
  transport, freight, lorry, compressed gas, 3.5t gross weight, EURO-V, long haul                       all IAM regions
  transport, freight, lorry, compressed gas, 3.5t gross weight, EURO-VI, long haul                      all IAM regions
  transport, freight, lorry, plugin diesel hybrid, 3.5t gross weight, EURO-VI, long haul                all IAM regions
  transport, freight, lorry, battery electric, NMC-622 battery, 7.5t gross weight, long haul            all IAM regions
  transport, freight, lorry, fuel cell electric, 7.5t gross weight, long haul                           all IAM regions
  transport, freight, lorry, diesel hybrid, 7.5t gross weight, EURO-VI, long haul                       all IAM regions
  transport, freight, lorry, diesel, 7.5t gross weight, EURO-III, long haul                             all IAM regions
  transport, freight, lorry, diesel, 7.5t gross weight, EURO-IV, long haul                              all IAM regions
  transport, freight, lorry, diesel, 7.5t gross weight, EURO-V, long haul                               all IAM regions
  transport, freight, lorry, diesel, 7.5t gross weight, EURO-VI, long haul                              all IAM regions
  transport, freight, lorry, compressed gas, 7.5t gross weight, EURO-III, long haul                     all IAM regions
  transport, freight, lorry, compressed gas, 7.5t gross weight, EURO-IV, long haul                      all IAM regions
  transport, freight, lorry, compressed gas, 7.5t gross weight, EURO-V, long haul                       all IAM regions
  transport, freight, lorry, compressed gas, 7.5t gross weight, EURO-VI, long haul                      all IAM regions
  transport, freight, lorry, plugin diesel hybrid, 7.5t gross weight, EURO-VI, long haul                all IAM regions
  transport, freight, lorry, battery electric, NMC-622 battery, 18t gross weight, long haul             all IAM regions
  transport, freight, lorry, fuel cell electric, 18t gross weight, long haul                            all IAM regions
  transport, freight, lorry, diesel hybrid, 18t gross weight, EURO-VI, long haul                        all IAM regions
  transport, freight, lorry, diesel, 18t gross weight, EURO-III, long haul                              all IAM regions
  transport, freight, lorry, diesel, 18t gross weight, EURO-IV, long haul                               all IAM regions
  transport, freight, lorry, diesel, 18t gross weight, EURO-V, long haul                                all IAM regions
  transport, freight, lorry, diesel, 18t gross weight, EURO-VI, long haul                               all IAM regions
  transport, freight, lorry, compressed gas, 18t gross weight, EURO-III, long haul                      all IAM regions
  transport, freight, lorry, compressed gas, 18t gross weight, EURO-IV, long haul                       all IAM regions
  transport, freight, lorry, compressed gas, 18t gross weight, EURO-V, long haul                        all IAM regions
  transport, freight, lorry, compressed gas, 18t gross weight, EURO-VI, long haul                       all IAM regions
  transport, freight, lorry, plugin diesel hybrid, 18t gross weight, EURO-VI, long haul                 all IAM regions
  transport, freight, lorry, battery electric, NMC-622 battery, 26t gross weight, long haul             all IAM regions
  transport, freight, lorry, fuel cell electric, 26t gross weight, long haul                            all IAM regions
  transport, freight, lorry, diesel hybrid, 26t gross weight, EURO-VI, long haul                        all IAM regions
  transport, freight, lorry, diesel, 26t gross weight, EURO-III, long haul                              all IAM regions
  transport, freight, lorry, diesel, 26t gross weight, EURO-IV, long haul                               all IAM regions
  transport, freight, lorry, diesel, 26t gross weight, EURO-V, long haul                                all IAM regions
  transport, freight, lorry, diesel, 26t gross weight, EURO-VI, long haul                               all IAM regions
  transport, freight, lorry, compressed gas, 26t gross weight, EURO-III, long haul                      all IAM regions
  transport, freight, lorry, compressed gas, 26t gross weight, EURO-IV, long haul                       all IAM regions
  transport, freight, lorry, compressed gas, 26t gross weight, EURO-V, long haul                        all IAM regions
  transport, freight, lorry, compressed gas, 26t gross weight, EURO-VI, long haul                       all IAM regions
  transport, freight, lorry, plugin diesel hybrid, 26t gross weight, EURO-VI, long haul                 all IAM regions
  transport, freight, lorry, battery electric, NMC-622 battery, 32t gross weight, long haul             all IAM regions
  transport, freight, lorry, fuel cell electric, 32t gross weight, long haul                            all IAM regions
  transport, freight, lorry, diesel hybrid, 32t gross weight, EURO-VI, long haul                        all IAM regions
  transport, freight, lorry, diesel, 32t gross weight, EURO-III, long haul                              all IAM regions
  transport, freight, lorry, diesel, 32t gross weight, EURO-IV, long haul                               all IAM regions
  transport, freight, lorry, diesel, 32t gross weight, EURO-V, long haul                                all IAM regions
  transport, freight, lorry, diesel, 32t gross weight, EURO-VI, long haul                               all IAM regions
  transport, freight, lorry, compressed gas, 32t gross weight, EURO-III, long haul                      all IAM regions
  transport, freight, lorry, compressed gas, 32t gross weight, EURO-IV, long haul                       all IAM regions
  transport, freight, lorry, compressed gas, 32t gross weight, EURO-V, long haul                        all IAM regions
  transport, freight, lorry, compressed gas, 32t gross weight, EURO-VI, long haul                       all IAM regions
  transport, freight, lorry, plugin diesel hybrid, 32t gross weight, EURO-VI, long haul                 all IAM regions
  transport, freight, lorry, battery electric, NMC-622 battery, 40t gross weight, long haul             all IAM regions
  transport, freight, lorry, fuel cell electric, 40t gross weight, long haul                            all IAM regions
  transport, freight, lorry, diesel hybrid, 40t gross weight, EURO-VI, long haul                        all IAM regions
  transport, freight, lorry, diesel, 40t gross weight, EURO-III, long haul                              all IAM regions
  transport, freight, lorry, diesel, 40t gross weight, EURO-IV, long haul                               all IAM regions
  transport, freight, lorry, diesel, 40t gross weight, EURO-V, long haul                                all IAM regions
  transport, freight, lorry, diesel, 40t gross weight, EURO-VI, long haul                               all IAM regions
  transport, freight, lorry, compressed gas, 40t gross weight, EURO-III, long haul                      all IAM regions
  transport, freight, lorry, compressed gas, 40t gross weight, EURO-IV, long haul                       all IAM regions
  transport, freight, lorry, compressed gas, 40t gross weight, EURO-V, long haul                        all IAM regions
  transport, freight, lorry, compressed gas, 40t gross weight, EURO-VI, long haul                       all IAM regions
  transport, freight, lorry, plugin diesel hybrid, 40t gross weight, EURO-VI, long haul                 all IAM regions
  transport, freight, lorry, battery electric, NMC-622 battery, 3.5t gross weight, regional delivery    all IAM regions
  transport, freight, lorry, fuel cell electric, 3.5t gross weight, regional delivery                   all IAM regions
  transport, freight, lorry, diesel hybrid, 3.5t gross weight, EURO-VI, regional delivery               all IAM regions
  transport, freight, lorry, diesel, 3.5t gross weight, EURO-III, regional delivery                     all IAM regions
  transport, freight, lorry, diesel, 3.5t gross weight, EURO-IV, regional delivery                      all IAM regions
  transport, freight, lorry, diesel, 3.5t gross weight, EURO-V, regional delivery                       all IAM regions
  transport, freight, lorry, diesel, 3.5t gross weight, EURO-VI, regional delivery                      all IAM regions
  transport, freight, lorry, compressed gas, 3.5t gross weight, EURO-III, regional delivery             all IAM regions
  transport, freight, lorry, compressed gas, 3.5t gross weight, EURO-IV, regional delivery              all IAM regions
  transport, freight, lorry, compressed gas, 3.5t gross weight, EURO-V, regional delivery               all IAM regions
  transport, freight, lorry, compressed gas, 3.5t gross weight, EURO-VI, regional delivery              all IAM regions
  transport, freight, lorry, plugin diesel hybrid, 3.5t gross weight, EURO-VI, regional delivery        all IAM regions
  transport, freight, lorry, battery electric, NMC-622 battery, 7.5t gross weight, regional delivery    all IAM regions
  transport, freight, lorry, fuel cell electric, 7.5t gross weight, regional delivery                   all IAM regions
  transport, freight, lorry, diesel hybrid, 7.5t gross weight, EURO-VI, regional delivery               all IAM regions
  transport, freight, lorry, diesel, 7.5t gross weight, EURO-III, regional delivery                     all IAM regions
  transport, freight, lorry, diesel, 7.5t gross weight, EURO-IV, regional delivery                      all IAM regions
  transport, freight, lorry, diesel, 7.5t gross weight, EURO-V, regional delivery                       all IAM regions
  transport, freight, lorry, diesel, 7.5t gross weight, EURO-VI, regional delivery                      all IAM regions
  transport, freight, lorry, compressed gas, 7.5t gross weight, EURO-III, regional delivery             all IAM regions
  transport, freight, lorry, compressed gas, 7.5t gross weight, EURO-IV, regional delivery              all IAM regions
  transport, freight, lorry, compressed gas, 7.5t gross weight, EURO-V, regional delivery               all IAM regions
  transport, freight, lorry, compressed gas, 7.5t gross weight, EURO-VI, regional delivery              all IAM regions
  transport, freight, lorry, plugin diesel hybrid, 7.5t gross weight, EURO-VI, regional delivery        all IAM regions
  transport, freight, lorry, battery electric, NMC-622 battery, 18t gross weight, regional delivery     all IAM regions
  transport, freight, lorry, fuel cell electric, 18t gross weight, regional delivery                    all IAM regions
  transport, freight, lorry, diesel hybrid, 18t gross weight, EURO-VI, regional delivery                all IAM regions
  transport, freight, lorry, diesel, 18t gross weight, EURO-III, regional delivery                      all IAM regions
  transport, freight, lorry, diesel, 18t gross weight, EURO-IV, regional delivery                       all IAM regions
  transport, freight, lorry, diesel, 18t gross weight, EURO-V, regional delivery                        all IAM regions
  transport, freight, lorry, diesel, 18t gross weight, EURO-VI, regional delivery                       all IAM regions
  transport, freight, lorry, compressed gas, 18t gross weight, EURO-III, regional delivery              all IAM regions
  transport, freight, lorry, compressed gas, 18t gross weight, EURO-IV, regional delivery               all IAM regions
  transport, freight, lorry, compressed gas, 18t gross weight, EURO-V, regional delivery                all IAM regions
  transport, freight, lorry, compressed gas, 18t gross weight, EURO-VI, regional delivery               all IAM regions
  transport, freight, lorry, plugin diesel hybrid, 18t gross weight, EURO-VI, regional delivery         all IAM regions
  transport, freight, lorry, battery electric, NMC-622 battery, 26t gross weight, regional delivery     all IAM regions
  transport, freight, lorry, fuel cell electric, 26t gross weight, regional delivery                    all IAM regions
  transport, freight, lorry, diesel hybrid, 26t gross weight, EURO-VI, regional delivery                all IAM regions
  transport, freight, lorry, diesel, 26t gross weight, EURO-III, regional delivery                      all IAM regions
  transport, freight, lorry, diesel, 26t gross weight, EURO-IV, regional delivery                       all IAM regions
  transport, freight, lorry, diesel, 26t gross weight, EURO-V, regional delivery                        all IAM regions
  transport, freight, lorry, diesel, 26t gross weight, EURO-VI, regional delivery                       all IAM regions
  transport, freight, lorry, compressed gas, 26t gross weight, EURO-III, regional delivery              all IAM regions
  transport, freight, lorry, compressed gas, 26t gross weight, EURO-IV, regional delivery               all IAM regions
  transport, freight, lorry, compressed gas, 26t gross weight, EURO-V, regional delivery                all IAM regions
  transport, freight, lorry, compressed gas, 26t gross weight, EURO-VI, regional delivery               all IAM regions
  transport, freight, lorry, plugin diesel hybrid, 26t gross weight, EURO-VI, regional delivery         all IAM regions
  transport, freight, lorry, battery electric, NMC-622 battery, 32t gross weight, regional delivery     all IAM regions
  transport, freight, lorry, fuel cell electric, 32t gross weight, regional delivery                    all IAM regions
  transport, freight, lorry, diesel hybrid, 32t gross weight, EURO-VI, regional delivery                all IAM regions
  transport, freight, lorry, diesel, 32t gross weight, EURO-III, regional delivery                      all IAM regions
  transport, freight, lorry, diesel, 32t gross weight, EURO-IV, regional delivery                       all IAM regions
  transport, freight, lorry, diesel, 32t gross weight, EURO-V, regional delivery                        all IAM regions
  transport, freight, lorry, diesel, 32t gross weight, EURO-VI, regional delivery                       all IAM regions
  transport, freight, lorry, compressed gas, 32t gross weight, EURO-III, regional delivery              all IAM regions
  transport, freight, lorry, compressed gas, 32t gross weight, EURO-IV, regional delivery               all IAM regions
  transport, freight, lorry, compressed gas, 32t gross weight, EURO-V, regional delivery                all IAM regions
  transport, freight, lorry, compressed gas, 32t gross weight, EURO-VI, regional delivery               all IAM regions
  transport, freight, lorry, plugin diesel hybrid, 32t gross weight, EURO-VI, regional delivery         all IAM regions
  transport, freight, lorry, battery electric, NMC-622 battery, 40t gross weight, regional delivery     all IAM regions
  transport, freight, lorry, fuel cell electric, 40t gross weight, regional delivery                    all IAM regions
  transport, freight, lorry, diesel hybrid, 40t gross weight, EURO-VI, regional delivery                all IAM regions
  transport, freight, lorry, diesel, 40t gross weight, EURO-III, regional delivery                      all IAM regions
  transport, freight, lorry, diesel, 40t gross weight, EURO-IV, regional delivery                       all IAM regions
  transport, freight, lorry, diesel, 40t gross weight, EURO-V, regional delivery                        all IAM regions
  transport, freight, lorry, diesel, 40t gross weight, EURO-VI, regional delivery                       all IAM regions
  transport, freight, lorry, compressed gas, 40t gross weight, EURO-III, regional delivery              all IAM regions
  transport, freight, lorry, compressed gas, 40t gross weight, EURO-IV, regional delivery               all IAM regions
  transport, freight, lorry, compressed gas, 40t gross weight, EURO-V, regional delivery                all IAM regions
  transport, freight, lorry, compressed gas, 40t gross weight, EURO-VI, regional delivery               all IAM regions
  transport, freight, lorry, plugin diesel hybrid, 40t gross weight, EURO-VI, regional delivery         all IAM regions
  transport, freight, lorry, battery electric, NMC-622 battery, 3.5t gross weight, urban delivery       all IAM regions
  transport, freight, lorry, fuel cell electric, 3.5t gross weight, urban delivery                      all IAM regions
  transport, freight, lorry, diesel hybrid, 3.5t gross weight, EURO-VI, urban delivery                  all IAM regions
  transport, freight, lorry, diesel, 3.5t gross weight, EURO-III, urban delivery                        all IAM regions
  transport, freight, lorry, diesel, 3.5t gross weight, EURO-IV, urban delivery                         all IAM regions
  transport, freight, lorry, diesel, 3.5t gross weight, EURO-V, urban delivery                          all IAM regions
  transport, freight, lorry, diesel, 3.5t gross weight, EURO-VI, urban delivery                         all IAM regions
  transport, freight, lorry, compressed gas, 3.5t gross weight, EURO-III, urban delivery                all IAM regions
  transport, freight, lorry, compressed gas, 3.5t gross weight, EURO-IV, urban delivery                 all IAM regions
  transport, freight, lorry, compressed gas, 3.5t gross weight, EURO-V, urban delivery                  all IAM regions
  transport, freight, lorry, compressed gas, 3.5t gross weight, EURO-VI, urban delivery                 all IAM regions
  transport, freight, lorry, plugin diesel hybrid, 3.5t gross weight, EURO-VI, urban delivery           all IAM regions
  transport, freight, lorry, battery electric, NMC-622 battery, 7.5t gross weight, urban delivery       all IAM regions
  transport, freight, lorry, fuel cell electric, 7.5t gross weight, urban delivery                      all IAM regions
  transport, freight, lorry, diesel hybrid, 7.5t gross weight, EURO-VI, urban delivery                  all IAM regions
  transport, freight, lorry, diesel, 7.5t gross weight, EURO-III, urban delivery                        all IAM regions
  transport, freight, lorry, diesel, 7.5t gross weight, EURO-IV, urban delivery                         all IAM regions
  transport, freight, lorry, diesel, 7.5t gross weight, EURO-V, urban delivery                          all IAM regions
  transport, freight, lorry, diesel, 7.5t gross weight, EURO-VI, urban delivery                         all IAM regions
  transport, freight, lorry, compressed gas, 7.5t gross weight, EURO-III, urban delivery                all IAM regions
  transport, freight, lorry, compressed gas, 7.5t gross weight, EURO-IV, urban delivery                 all IAM regions
  transport, freight, lorry, compressed gas, 7.5t gross weight, EURO-V, urban delivery                  all IAM regions
  transport, freight, lorry, compressed gas, 7.5t gross weight, EURO-VI, urban delivery                 all IAM regions
  transport, freight, lorry, plugin diesel hybrid, 7.5t gross weight, EURO-VI, urban delivery           all IAM regions
  transport, freight, lorry, battery electric, NMC-622 battery, 18t gross weight, urban delivery        all IAM regions
  transport, freight, lorry, fuel cell electric, 18t gross weight, urban delivery                       all IAM regions
  transport, freight, lorry, diesel hybrid, 18t gross weight, EURO-VI, urban delivery                   all IAM regions
  transport, freight, lorry, diesel, 18t gross weight, EURO-III, urban delivery                         all IAM regions
  transport, freight, lorry, diesel, 18t gross weight, EURO-IV, urban delivery                          all IAM regions
  transport, freight, lorry, diesel, 18t gross weight, EURO-V, urban delivery                           all IAM regions
  transport, freight, lorry, diesel, 18t gross weight, EURO-VI, urban delivery                          all IAM regions
  transport, freight, lorry, compressed gas, 18t gross weight, EURO-III, urban delivery                 all IAM regions
  transport, freight, lorry, compressed gas, 18t gross weight, EURO-IV, urban delivery                  all IAM regions
  transport, freight, lorry, compressed gas, 18t gross weight, EURO-V, urban delivery                   all IAM regions
  transport, freight, lorry, compressed gas, 18t gross weight, EURO-VI, urban delivery                  all IAM regions
  transport, freight, lorry, plugin diesel hybrid, 18t gross weight, EURO-VI, urban delivery            all IAM regions
  transport, freight, lorry, battery electric, NMC-622 battery, 26t gross weight, urban delivery        all IAM regions
  transport, freight, lorry, fuel cell electric, 26t gross weight, urban delivery                       all IAM regions
  transport, freight, lorry, diesel hybrid, 26t gross weight, EURO-VI, urban delivery                   all IAM regions
  transport, freight, lorry, diesel, 26t gross weight, EURO-III, urban delivery                         all IAM regions
  transport, freight, lorry, diesel, 26t gross weight, EURO-IV, urban delivery                          all IAM regions
  transport, freight, lorry, diesel, 26t gross weight, EURO-V, urban delivery                           all IAM regions
  transport, freight, lorry, diesel, 26t gross weight, EURO-VI, urban delivery                          all IAM regions
  transport, freight, lorry, compressed gas, 26t gross weight, EURO-III, urban delivery                 all IAM regions
  transport, freight, lorry, compressed gas, 26t gross weight, EURO-IV, urban delivery                  all IAM regions
  transport, freight, lorry, compressed gas, 26t gross weight, EURO-V, urban delivery                   all IAM regions
  transport, freight, lorry, compressed gas, 26t gross weight, EURO-VI, urban delivery                  all IAM regions
  transport, freight, lorry, plugin diesel hybrid, 26t gross weight, EURO-VI, urban delivery            all IAM regions
  transport, freight, lorry, battery electric, NMC-622 battery, 32t gross weight, urban delivery        all IAM regions
  transport, freight, lorry, fuel cell electric, 32t gross weight, urban delivery                       all IAM regions
  transport, freight, lorry, diesel hybrid, 32t gross weight, EURO-VI, urban delivery                   all IAM regions
  transport, freight, lorry, diesel, 32t gross weight, EURO-III, urban delivery                         all IAM regions
  transport, freight, lorry, diesel, 32t gross weight, EURO-IV, urban delivery                          all IAM regions
  transport, freight, lorry, diesel, 32t gross weight, EURO-V, urban delivery                           all IAM regions
  transport, freight, lorry, diesel, 32t gross weight, EURO-VI, urban delivery                          all IAM regions
  transport, freight, lorry, compressed gas, 32t gross weight, EURO-III, urban delivery                 all IAM regions
  transport, freight, lorry, compressed gas, 32t gross weight, EURO-IV, urban delivery                  all IAM regions
  transport, freight, lorry, compressed gas, 32t gross weight, EURO-V, urban delivery                   all IAM regions
  transport, freight, lorry, compressed gas, 32t gross weight, EURO-VI, urban delivery                  all IAM regions
  transport, freight, lorry, plugin diesel hybrid, 32t gross weight, EURO-VI, urban delivery            all IAM regions
  transport, freight, lorry, battery electric, NMC-622 battery, 40t gross weight, urban delivery        all IAM regions
  transport, freight, lorry, fuel cell electric, 40t gross weight, urban delivery                       all IAM regions
  transport, freight, lorry, diesel hybrid, 40t gross weight, EURO-VI, urban delivery                   all IAM regions
  transport, freight, lorry, diesel, 40t gross weight, EURO-III, urban delivery                         all IAM regions
  transport, freight, lorry, diesel, 40t gross weight, EURO-IV, urban delivery                          all IAM regions
  transport, freight, lorry, diesel, 40t gross weight, EURO-V, urban delivery                           all IAM regions
  transport, freight, lorry, diesel, 40t gross weight, EURO-VI, urban delivery                          all IAM regions
  transport, freight, lorry, compressed gas, 40t gross weight, EURO-III, urban delivery                 all IAM regions
  transport, freight, lorry, compressed gas, 40t gross weight, EURO-IV, urban delivery                  all IAM regions
  transport, freight, lorry, compressed gas, 40t gross weight, EURO-V, urban delivery                   all IAM regions
  transport, freight, lorry, compressed gas, 40t gross weight, EURO-VI, urban delivery                  all IAM regions
  transport, freight, lorry, plugin diesel hybrid, 40t gross weight, EURO-VI, urban delivery            all IAM regions
 ===================================================================================================== ==================

Inventories are from Sacchi3_ et al. 2021. The vehicles are available
for different years and emission standards and for each IAM region. *premise* will only
import vehicles which production year is equal or inferior to
the scenario year considered. *premise* will create fleet average vehicles
during the *Transport* transformation for each IAM region. The inventories can be consulted
here: LCItrucks_.

.. _LCItrucks: https://github.com/romainsacchi/premise/blob/master/premise/data/additional_inventories/lci-trucks.xlsx
.. _Sacchi3: https://pubs.acs.org/doi/abs/10.1021/acs.est.0c07773

Buses
*****

The following datasets for city and coach buses are imported.

  =================================================================================================================== ==================
  Bus datasets                                                                                                        location
 =================================================================================================================== ==================
  transport, passenger bus, battery electric - overnight charging, NMC-622 battery, 9m midibus                        all IAM regions
  transport, passenger bus, battery electric - opportunity charging, LTO battery, 9m midibus                          all IAM regions
  transport, passenger bus, fuel cell electric, 9m midibus                                                            all IAM regions
  transport, passenger bus, diesel hybrid, 9m midibus, EURO-VI                                                        all IAM regions
  transport, passenger bus, diesel, 9m midibus, EURO-III                                                              all IAM regions
  transport, passenger bus, diesel, 9m midibus, EURO-IV                                                               all IAM regions
  transport, passenger bus, diesel, 9m midibus, EURO-V                                                                all IAM regions
  transport, passenger bus, diesel, 9m midibus, EURO-VI                                                               all IAM regions
  transport, passenger bus, compressed gas, 9m midibus, EURO-III                                                      all IAM regions
  transport, passenger bus, compressed gas, 9m midibus, EURO-IV                                                       all IAM regions
  transport, passenger bus, compressed gas, 9m midibus, EURO-V                                                        all IAM regions
  transport, passenger bus, compressed gas, 9m midibus, EURO-VI                                                       all IAM regions
  transport, passenger bus, battery electric - overnight charging, NMC-622 battery, 13m single deck urban bus         all IAM regions
  transport, passenger bus, battery electric - battery-equipped trolleybus, LTO battery, 13m single deck urban bus    all IAM regions
  transport, passenger bus, battery electric - opportunity charging, LTO battery, 13m single deck urban bus           all IAM regions
  transport, passenger bus, fuel cell electric, 13m single deck urban bus                                             all IAM regions
  transport, passenger bus, diesel hybrid, 13m single deck urban bus, EURO-VI                                         all IAM regions
  transport, passenger bus, diesel, 13m single deck urban bus, EURO-III                                               all IAM regions
  transport, passenger bus, diesel, 13m single deck urban bus, EURO-IV                                                all IAM regions
  transport, passenger bus, diesel, 13m single deck urban bus, EURO-V                                                 all IAM regions
  transport, passenger bus, diesel, 13m single deck urban bus, EURO-VI                                                all IAM regions
  transport, passenger bus, compressed gas, 13m single deck urban bus, EURO-III                                       all IAM regions
  transport, passenger bus, compressed gas, 13m single deck urban bus, EURO-IV                                        all IAM regions
  transport, passenger bus, compressed gas, 13m single deck urban bus, EURO-V                                         all IAM regions
  transport, passenger bus, compressed gas, 13m single deck urban bus, EURO-VI                                        all IAM regions
  transport, passenger bus, fuel cell electric, 13m single deck coach bus                                             all IAM regions
  transport, passenger bus, diesel hybrid, 13m single deck coach bus, EURO-VI                                         all IAM regions
  transport, passenger bus, diesel, 13m single deck coach bus, EURO-III                                               all IAM regions
  transport, passenger bus, diesel, 13m single deck coach bus, EURO-IV                                                all IAM regions
  transport, passenger bus, diesel, 13m single deck coach bus, EURO-V                                                 all IAM regions
  transport, passenger bus, diesel, 13m single deck coach bus, EURO-VI                                                all IAM regions
  transport, passenger bus, compressed gas, 13m single deck coach bus, EURO-III                                       all IAM regions
  transport, passenger bus, compressed gas, 13m single deck coach bus, EURO-IV                                        all IAM regions
  transport, passenger bus, compressed gas, 13m single deck coach bus, EURO-V                                         all IAM regions
  transport, passenger bus, compressed gas, 13m single deck coach bus, EURO-VI                                        all IAM regions
  transport, passenger bus, battery electric - overnight charging, NMC-622 battery, 13m double deck urban bus         all IAM regions
  transport, passenger bus, battery electric - opportunity charging, LTO battery, 13m double deck urban bus           all IAM regions
  transport, passenger bus, fuel cell electric, 13m double deck urban bus                                             all IAM regions
  transport, passenger bus, diesel hybrid, 13m double deck urban bus, EURO-VI                                         all IAM regions
  transport, passenger bus, diesel, 13m double deck urban bus, EURO-III                                               all IAM regions
  transport, passenger bus, diesel, 13m double deck urban bus, EURO-IV                                                all IAM regions
  transport, passenger bus, diesel, 13m double deck urban bus, EURO-V                                                 all IAM regions
  transport, passenger bus, diesel, 13m double deck urban bus, EURO-VI                                                all IAM regions
  transport, passenger bus, compressed gas, 13m double deck urban bus, EURO-III                                       all IAM regions
  transport, passenger bus, compressed gas, 13m double deck urban bus, EURO-IV                                        all IAM regions
  transport, passenger bus, compressed gas, 13m double deck urban bus, EURO-V                                         all IAM regions
  transport, passenger bus, compressed gas, 13m double deck urban bus, EURO-VI                                        all IAM regions
  transport, passenger bus, fuel cell electric, 13m double deck coach bus                                             all IAM regions
  transport, passenger bus, diesel hybrid, 13m double deck coach bus, EURO-VI                                         all IAM regions
  transport, passenger bus, diesel, 13m double deck coach bus, EURO-III                                               all IAM regions
  transport, passenger bus, diesel, 13m double deck coach bus, EURO-IV                                                all IAM regions
  transport, passenger bus, diesel, 13m double deck coach bus, EURO-V                                                 all IAM regions
  transport, passenger bus, diesel, 13m double deck coach bus, EURO-VI                                                all IAM regions
  transport, passenger bus, compressed gas, 13m double deck coach bus, EURO-III                                       all IAM regions
  transport, passenger bus, compressed gas, 13m double deck coach bus, EURO-IV                                        all IAM regions
  transport, passenger bus, compressed gas, 13m double deck coach bus, EURO-V                                         all IAM regions
  transport, passenger bus, compressed gas, 13m double deck coach bus, EURO-VI                                        all IAM regions
  transport, passenger bus, battery electric - overnight charging, NMC-622 battery, 18m articulated urban bus         all IAM regions
  transport, passenger bus, battery electric - battery-equipped trolleybus, LTO battery, 18m articulated urban bus    all IAM regions
  transport, passenger bus, battery electric - opportunity charging, LTO battery, 18m articulated urban bus           all IAM regions
  transport, passenger bus, fuel cell electric, 18m articulated urban bus                                             all IAM regions
  transport, passenger bus, diesel hybrid, 18m articulated urban bus, EURO-VI                                         all IAM regions
  transport, passenger bus, diesel, 18m articulated urban bus, EURO-III                                               all IAM regions
  transport, passenger bus, diesel, 18m articulated urban bus, EURO-IV                                                all IAM regions
  transport, passenger bus, diesel, 18m articulated urban bus, EURO-V                                                 all IAM regions
  transport, passenger bus, diesel, 18m articulated urban bus, EURO-VI                                                all IAM regions
  transport, passenger bus, compressed gas, 18m articulated urban bus, EURO-III                                       all IAM regions
  transport, passenger bus, compressed gas, 18m articulated urban bus, EURO-IV                                        all IAM regions
  transport, passenger bus, compressed gas, 18m articulated urban bus, EURO-V                                         all IAM regions
  transport, passenger bus, compressed gas, 18m articulated urban bus, EURO-VI                                        all IAM regions
 =================================================================================================================== ==================

Inventories are from Sacchi_ et al. 2021. The vehicles are available
for different years and emission standards and for each IAM region. *premise* will only
import vehicles which production year is equal or inferior to
the scenario year considered. *premise* will create fleet average vehicles
during the *Transport* transformation for each IAM region. The inventories can be consulted
here: LCIbuses_.

.. _LCIbuses: https://github.com/romainsacchi/premise/blob/master/premise/data/additional_inventories/lci-buses.xlsx



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

.. _migrationmap:

IAM data collection
"""""""""""""""""""

Production volumes
------------------

Efficiencies
------------

Land use and land use change
----------------------------

Data sources external to the IAM
--------------------------------

Air emissions
*************

Cement production
*****************

