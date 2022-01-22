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

* a brightway2 project that contains the ecoinvent database
* ecosposld2 files, that can be downloaded from the ecoinvent_ website

.. _ecoinvent: https://ecoinvent.org



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
  petroleum and gas production (natural gas, high pressure), GB                           natural gas, at production (natural gas, high pressure), GB
  petroleum and gas production (natural gas, high pressure), NG                           natural gas, at production (natural gas, high pressure), NG
  petroleum and gas production (natural gas, high pressure), NL                           natural gas, at production (natural gas, high pressure), NL
  petroleum and gas production (natural gas, high pressure), NO                           natural gas, at production (natural gas, high pressure), NO
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

Hence, inside the residential PV mix of Spain (electricity production, photovoltaic, residential),
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

Hydrogen
--------

Biofuels
--------

Direct Air Capture
------------------

Li-ion battery
--------------

Synthetic fuels
---------------

Road vehicles
-------------

Two-wheelers
************

Passenger cars
**************

Medium and heavy duty trucks
****************************

Buses
*****

Migration between ecoinvent versions
------------------------------------

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

