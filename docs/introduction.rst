# Coupling Brightway2 & Wurst Future Ecoinvent Toolset to the REMIND IAM.

[![Build Status](https://travis-ci.org/romainsacchi/rmnd-lca.svg?branch=master)](https://travis-ci.org/romainsacchi/rmnd-lca) [![Coverage Status](https://coveralls.io/repos/github/romainsacchi/rmnd-lca/badge.svg?branch=master)](https://coveralls.io/github/romainsacchi/rmnd-lca?branch=master) [![Documentation](https://readthedocs.org/projects/rmnd-lca/badge/?version=latest)](https://rmnd-lca.readthedocs.io/en/latest/)


Introduction
============

**rmnd-lca** allows to align the life cycle inventories contained in the **ecoinvent 3.5 and 3.6 cutoff** databases with the output results of
the **REMIND IAM**, in order to produce life cycle inventories under future policy scenarios for any year between 2005
and 2150.

In the current version, this includes:
* electricity generation: alignment of regional electricity production mixes as well as efficiencies for a number of
electricity production technologies, including Carbon Capture and Storage technologies.

In upcoming versions, important sectors such as cement and steel will also be updated.

Documentation
-------------
https://rmnd-lca.readthedocs.io/en/latest/

Objective
---------

The objective is to produce life cycle inventories under future energy policies, by modifying the inventory database
ecoinvent 3 to reflect projected energy policy trajectories.

Requirements
------------
* Python language interpreter 3.x
* License for ecoinvent 3
* Brightway2
* REMIND IAM output files come with the library ("xxx.mif" and "GAINS emission factors.csv")
 and are located by default in the subdirectory "/data/Remind output files".
 A file path can be specified to fetch the REMIND IAM output files elsewhere on your computer.

How to install this package?
----------------------------

In a terminal, from Github:

    pip install git+https://github.com/romainsacchi/rmnd-lca.git

will install the package and the required dependencies.

Alternatively, from Conda:

    conda config --append channels cmutel
    conda config --append channels konstantinstadler
    conda install -c romainsacchi/label/nightly rmnd-lca-dev

How to use it?
--------------

Extract
*******

A preliminary requirement to the use this library is to have a `brightway2` project created and opened, with the
`ecoinvent 3.5 cutoff` or `ecoinvent 3.6 cutoff` database registered, so that:

.. code-block:: python

    import brightway2 as bw
    bw.projects.set_current('remind')
    bw.databases

returns

.. code-block:: python

    Databases dictionary with 2 object(s):
	biosphere3
	ecoinvent 3.5 cutoff

Then, for a chosen scenario (for example: "BAU","RCP26" or "RCP37"; full list of scenarios are listed under \data\Remind output files) and a year between 2005 and 2150 (a year is available at 5-year interval), the following function will:
* extract the ecoinvent database, clean it, add additional inventories for carbon capture and storage, biofuels, etc.

For example, here with the year 2028 and the policy "Business-as-usual":

.. code-block:: python

    ndb = NewDatabase(scenario = 'SSP2-Base',
              year = 2028,
              source_db = 'ecoinvent 3.6 cutoff',
              source_version = 3.6,
             )

Note that, by default, the library will look for REMIND output files ("xxx.mif" files and "GAINS emission factors.csv") in the
"data/Remind output files" subdirectory. If those are not located there, you need to specify the path to
the correct directory, such as:

.. code-block:: python

    ndb = NewDatabase(scenario = 'SSP2-Base',
              year = 2028,
              source_db = 'ecoinvent 3.6 cutoff',
              source_version = 3.6,
              r"C:\Users\username\Documents\Remind output files"
             )

Transform
*********

A series of transformations can be performed on the extracted database.
Currently, only the transformation regarding electricity generation and distribution is implemented.

Electricity
+++++++++++

The following function will:
* remove existing electricity markets
* replace them by regional markets (high, medium and low voltage) with a geographical scope and production mix
  defined by the REMIND model for that year,
* relink electricity-consuming activities to the newly created electricity markets,
* update the efficiency of electricity-producing technologies (fuel-input-to-energy-output ratio),
according to the projections given by REMIND,
* and rescale fuel-related emissions of electricity-producing technologies according to their newly defined efficiency.


.. code-block:: python

    ndb.update_electricity_to_iam_data()

returns

.. code-block:: python

    Remove old electricity datasets
    Create high voltage markets.
    Create medium voltage markets.
    Create low voltage markets.
    Link activities to new electricity markets.
    Log of deleted electricity markets saved in C:\Users\username\Documents\GitHub\rmnd-lca\rmnd_lca\data\logs
    Log of created electricity markets saved in C:\Users\username\Documents\GitHub\rmnd-lca\rmnd_lca\data\logs
    Rescale inventories and emissions for Coal IGCC
    Rescale inventories and emissions for Coal IGCC CCS
    Rescale inventories and emissions for Coal PC
    Rescale inventories and emissions for Coal PC CCS
    Rescale inventories and emissions for Coal CHP
    Rescale inventories and emissions for Gas OC
    Rescale inventories and emissions for Gas CC
    Rescale inventories and emissions for Gas CHP
    Rescale inventories and emissions for Gas CCS
    Rescale inventories and emissions for Oil
    Rescale inventories and emissions for Biomass CHP
    Rescale inventories and emissions for Biomass IGCC CCS
    Rescale inventories and emissions for Biomass IGCC

Load
****

Once the process is completed, the resulting database is registered back into the current Brightway2 project:
.. code-block:: python

    ndb.write_db_to_brightway()

returns

.. code-block:: python

    Write new database to Brightway2.
    15223 datasets
    540424 exchanges
    0 unlinked exchanges

    Writing activities to SQLite3 database:
    Created database: ecoinvent_BAU_2028
