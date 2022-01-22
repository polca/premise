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

After the eecoinvent database is extracted and checked, a number of additional invenotries
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


 =============================================================================================================
  Power generation with CCS (activities lsit)
 =============================================================================================================
  ATR-H2 GT power plant, 400MWe
  CO2 capture/at H2 production plant, pre, pipeline 200km, storage 1000m
  CO2 capture/at H2 production plant, pre, pipeline 400km, storage 3000m
  CO2 capture/at wood burning power plant 20 MW, truck 25km, post, pipeline 200km, storage 1000m
  CO2 capture/at wood burning power plant 20 MW, truck 25km, post, pipeline 400km, storage 3000m
  CO2 capture/hard coal, oxy, pipeline 200km, storage 1000m
  CO2 capture/hard coal, oxy, pipeline 400km, storage 3000m
  CO2 capture/hard coal, post, pipeline 200km, storage 1000m
  CO2 capture/hard coal, post, pipeline 400km, storage 1000m
  CO2 capture/hard coal, post, pipeline 400km, storage 3000m
  CO2 capture/hard coal, pre, pipeline 200km, storage 1000m
  CO2 capture/hard coal, pre, pipeline 400km, storage 3000m
  CO2 capture/lignite, oxy, pipeline 200km, storage 1000m
  CO2 capture/lignite, oxy, pipeline 400km, storage 3000m
  CO2 capture/lignite, post, pipeline 200km, storage 1000m
  CO2 capture/lignite, post, pipeline 400km, storage 3000m
  CO2 capture/lignite, pre, pipeline 200km, storage 1000m
  CO2 capture/lignite, pre, pipeline 400km, storage 3000m
  CO2 capture/natural gas, post, 200km pipeline, storage 1000m
  CO2 capture/natural gas, post, 400km pipeline, storage 1000m
  CO2 capture/natural gas, post, 400km pipeline, storage 3000m
  CO2 capture/natural gas, pre, 200km pipeline, storage 1000m
  CO2 capture/natural gas, pre, 400km pipeline, storage 3000m
  CO2 storage/100% SNG, post, 200km pipeline, storage 1000m
  CO2 storage/100% SNG, post, 400km pipeline, storage 3000m
  CO2 storage/at H2 production plant, pre, pipeline 400km, storage 3000m
  CO2 storage/at wood burning power plant 20 MW, truck 25km, post, pipeline 200km, storage 1000m
  CO2 storage/at wood burning power plant 20 MW, truck 25km, post, pipeline 400km, storage 3000m
  CO2 storage/hard coal, oxy, pipeline 200km, storage 1000m
  CO2 storage/hard coal, oxy, pipeline 400km, storage 3000m
  CO2 storage/hard coal, post, pipeline 200km, storage 1000m
  CO2 storage/hard coal, post, pipeline 400km, storage 1000m
  CO2 storage/hard coal, post, pipeline 400km, storage 3000m
  CO2 storage/hard coal, pre, pipeline 200km, storage 1000m
  CO2 storage/hard coal, pre, pipeline 400km, storage 3000m
  CO2 storage/lignite, oxy, pipeline 200km, storage 1000m
  CO2 storage/lignite, oxy, pipeline 400km, storage 3000m
  CO2 storage/lignite, post, pipeline 200km, storage 1000m
  CO2 storage/lignite, post, pipeline 400km, storage 3000m
  CO2 storage/lignite, pre, pipeline 200km, storage 1000m
  CO2 storage/lignite, pre, pipeline 400km, storage 3000m
  CO2 storage/natural gas, post, 200km pipeline, storage 1000m
  CO2 storage/natural gas, post, 400km pipeline, storage 1000m
  CO2 storage/natural gas, post, 400km pipeline, storage 3000m
  CO2 storage/natural gas, pre, 200km pipeline, storage 1000m
  CO2 storage/natural gas, pre, 400km pipeline, storage 3000m
  Construction, BIGCC power plant 450MW
  Dismantling, BIGCC power plant 450MW
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
  Hard coal IGCC power plant 450MW
  Hard coal IGCC power plant, operation, no CCS
  Hard coal IGCC power plant, operation, with CCS
  Hard coal, burned in power plant/IGCC, no CCS
  Hard coal, burned in power plant/PC, no CCS
  Hard coal, burned in power plant/oxy, pipeline 200km, storage 1000m
  Hard coal, burned in power plant/oxy, pipeline 400km, storage 3000m
  Hard coal, burned in power plant/post, pipeline 200km, storage 1000m
  Hard coal, burned in power plant/post, pipeline 400km, storage 1000m
  Hard coal, burned in power plant/post, pipeline 400km, storage 3000m
  Hard coal, burned in power plant/pre, pipeline 200km, storage 1000m
  Hard coal, burned in power plant/pre, pipeline 400km, storage 3000m
  Hydrogen, from steam reforming of biomass gas, at reforming plant, no CCS
  Hydrogen, from steam reforming of biomassgas, at reforming plant, pre, pipeline 200km, storage 1000m
  Hydrogen, from steam reforming of biomassgas, at reforming plant, pre, pipeline 400km, storage 3000m
  Lignite IGCC power plant 450MW
  Lignite IGCC power plant, operation, no CCS
  Lignite IGCC power plant, operation, with CCS
  Lignite, burned in power plant/IGCC, no CCS
  Lignite, burned in power plant/PC, no CCS
  Lignite, burned in power plant/oxy, pipeline 200km, storage 1000m
  Lignite, burned in power plant/oxy, pipeline 400km, storage 3000m
  Lignite, burned in power plant/post, pipeline 200km, storage 1000m
  Lignite, burned in power plant/post, pipeline 400km, storage 3000m
  Lignite, burned in power plant/pre, pipeline 200km, storage 1000m
  Lignite, burned in power plant/pre, pipeline 400km, storage 3000m
  Natural gas, burned in power plant/NGCC, no CCS
  Natural gas, burned in power plant/post, pipeline 200km, storage 1000m/RER
  Natural gas, burned in power plant/post, pipeline 400km, storage 1000m/RER
  Natural gas, burned in power plant/post, pipeline 400km, storage 3000m/RER
  Natural gas, in ATR H2-CC/no CCS/MJ
  Natural gas, in ATR H2-CC/pre, pipeline 200km, storage 1000m
  Natural gas, in ATR H2-CC/pre, pipeline 400km, storage 3000m
  Operation, H2 power plant 450MW, no CCS
  Operation, H2 power plant 450MW, pre, pipeline 200km, storage 1000m
  Rape Methyl Ester, at plant
  SNG from wood, 70 bar, at consumer, CH (wood transport: lorry, 25km)
  SNG from wood, production plant
  SNG production plant
  Selexol (Dimethylether of polyethylene glycol)/RER
  Syngas production plant
  Syngas, from biomass gasification, no CCS
  Syngas, from biomass gasification, pre, pipeline 200km, storage 1000m
  Syngas, from biomass gasification, pre, pipeline 400km, storage 3000m
  Synthetic natural gas from wood, 70 bar, at plant, CH (wood transport: lorry, 25km)
  Wood chips, burned in power plant 20 MW, truck 25km, no CCS
  Wood chips, burned in power plant 20 MW, truck 25km, post, pipeline 200km, storage 1000m
  Wood chips, burned in power plant 20 MW, truck 25km, post, pipeline 400km, storage 3000m
  Wood combustion power plant 20 MW
  construction, hard coal IGCC power plant 450MW/p
  construction, lignite IGCC power plant 450MW
  dismantling, hard coal IGCC power plant 450MW/p
  dismantling, lignite IGCC power plant 450MW
  market for gas power plant, combined cycle, 400MW electrical
  market for hard coal power plant
  transport, pipeline, supercritical CO2, 200km w recompression
  CO2 storage/at H2 production plant, pre, pipeline 200km, storage 1000m
  transport, pipeline, supercritical CO2, 200km w/o recompression
  pipeline, supercritical CO2/km
  drilling, deep borehole/m
 =============================================================================================================

Natural gas
***********

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

