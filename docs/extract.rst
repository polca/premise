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
They originate from Volkart et al. 2013_, and can be consulted here_.

.. _2013: https://doi.org/10.1016/j.ijggc.2013.03.003
.. _here: https://github.com/romainsacchi/premise/blob/master/premise/data/additional_inventories/lci-Carma-CCS.xlsx

The table below lists the names of the new activities.

 =============================================================================================================
  Power generation with CCS (activities list)
 =============================================================================================================
  electricity production, at power plant/hard coal, IGCC, no CCS
  electricity production, at power plant/hard coal, PC, no CCS
  electricity production, at power plant/hard coal, oxy, pipeline 200km, storage 1000m
  electricity production, at power plant/hard coal, oxy, pipeline 400km, storage 3000m
  electricity production, at power plant/hard coal, post, pipeline 200km, storage 1000m
  electricity production, at power plant/hard coal, post, pipeline 400km, storage 1000m
  electricity production, at power plant/hard coal, post, pipeline 400km, storage 3000m
  electricity production, at power plant/hard coal, pre, pipeline 200km, storage 1000m
  electricity production, at power plant/hard coal, pre, pipeline 400km, storage 3000m
  electricity production, at power plant/lignite, IGCC, no CCS
  electricity production, at power plant/lignite, PC, no CCS
  electricity production, at power plant/lignite, oxy, pipeline 200km, storage 1000m
  electricity production, at power plant/lignite, oxy, pipeline 400km, storage 3000m
  electricity production, at power plant/lignite, post, pipeline 200km, storage 1000m
  electricity production, at power plant/lignite, post, pipeline 400km, storage 3000m
  electricity production, at power plant/lignite, pre, pipeline 200km, storage 1000m
  electricity production, at power plant/lignite, pre, pipeline 400km, storage 3000m
  electricity production, at power plant/natural gas, ATR H2-CC, no CCS
  electricity production, at power plant/natural gas, NGCC, no CCS/kWh
  electricity production, at power plant/natural gas, post, pipeline 200km, storage 1000m
  electricity production, at power plant/natural gas, post, pipeline 400km, storage 1000m
  electricity production, at power plant/natural gas, post, pipeline 400km, storage 3000m
  electricity production, at power plant/natural gas, pre, pipeline 200km, storage 1000m
  electricity production, at power plant/natural gas, pre, pipeline 400km, storage 3000m
  electricity production, at wood burning power plant 20 MW, truck 25km, no CCS
  electricity production, at wood burning power plant 20 MW, truck 25km, post, pipeline 200km, storage 1000m
  electricity production, at wood burning power plant 20 MW, truck 25km, post, pipeline 400km, storage 3000m
 =============================================================================================================


Natural gas
***********

Updated inventories relating to natural gas extraction and distribution
are imported to substitute some of the original ecoinvent dataset.
These datasets originate from ESU Services and come with a report_.
They have been adapted to a brightway2-compatible format.
These new inventories have, among other things, higher methane slip
emissions along teh natural gas supply chain, especially at extraction.

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

The table below lists the names of the new activities.

 =======================================================================
  Natural gas and crude oil extraction (activities list)
 =======================================================================
  Diesel, burned in diesel-electric generating set, at extraction site
  crude oil, at production
  crude oil, at production offshore
  crude oil, at production offshore
  crude oil, at production onshore
  crude oil, import mix, at long distance transport
  diesel, burned in diesel-electric generating set
  diesel-electric generating set production 10MW
  discharge, produced water, offshore
  discharge, produced water, onshore
  natural gas, at long-distance pipeline
  natural gas, at production
  natural gas, at production offshore
  natural gas, at production onshore
  natural gas, burned in gas turbine
  natural gas, high pressure, at consumer
  natural gas, liquefied, at freight ship
  natural gas, liquefied, at liquefaction plant
  natural gas, low pressure, at consumer
  natural gas, production DE, at long-distance pipeline
  natural gas, production DZ, at evaporation plant
  natural gas, production DZ, at long-distance pipeline
  natural gas, production NG, at evaporation plant
  natural gas, production NG, at long-distance pipeline
  natural gas, production NO, at evaporation plant
  natural gas, production NO, at long-distance pipeline
  natural gas, production QA, at evaporation plant
  natural gas, production QA, at long-distance pipeline
  natural gas, production RU, at evaporation plant
  natural gas, production RU, at long-distance pipeline
  natural gas, production US, at evaporation plant
  natural gas, sour, burned in production flare
  natural gas, sweet, burned in production flare
  natural gas, vented
  plant onshore, natural gas, production
  platform, crude oil, offshore
  production plant crude oil, onshore
  sweet gas, burned in gas turbine, production
  transport, liquefied natural gas DZ, freight ship
  transport, liquefied natural gas NG, freight ship
  transport, liquefied natural gas NO, freight ship
  transport, liquefied natural gas QA, freight ship
  transport, liquefied natural gas RU, freight ship
  transport, liquefied natural gas US, freight ship
  transport, natural gas, offshore pipeline, long distance
  transport, natural gas, onshore pipeline, long distance
  well for exploration and production, offshore
  well for exploration and production, onshore
 =======================================================================


Photovoltaic panels
*******************

Geothermal
**********

Hydrogen
--------

Biofuels
--------

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

