# Coupling Brightway2 & Wurst Future Ecoinvent Toolset to the REMIND IAM.

[![Build Status](https://travis-ci.org/romainsacchi/rmnd-lca.svg?branch=master)](https://travis-ci.org/romainsacchi/rmnd-lca) [![Coverage Status](https://coveralls.io/repos/github/romainsacchi/rmnd-lca/badge.svg?branch=master)](https://coveralls.io/github/romainsacchi/rmnd-lca?branch=master) [![Documentation](https://readthedocs.org/projects/rmnd-lca/badge/?version=latest)](https://rmnd-lca.readthedocs.io/en/latest/)


Introduction
============

**rmnd-lca** allows to align the life cycle inventories contained in the **ecoinvent 3.5 cutoff** database with the output results of
the **REMIND IAM**, in order to produce life cycle inventories under future policy scenarios for any year between 2005
and 2150.

In the latest version, this includes:
* electricity generation: alignment of regional electricity production mixes as well as efficiencies for a number of
electricity production technologies, including Carbon Capture and Storage technologies.

In upcoming versions, important sectors such as cement and steel will also be updated.

Objective
---------

The objective is to produce life cycle inventories under future energy policies, by modifying the inventory database
ecoinvent 3.5 to reflect projected energy policy trajectories.

Requirements
------------
* Python language interpreter 3.x
* Brightway2 library
* License for ecoinvent 3.5
* REMIND IAM output files ("xxx.mif" and "GAINS emission factors.csv") are not installed with this library. They need
to be queried from Alois Dirnaichner <aloisdir@pik-potsdam.de> and placed together in a folder. If not specified
otherwise, the library will look for them in its subdirectory "/data/Remind output files".

How to install this package?
----------------------------

In a terminal, from Github:

    pip install git+https://github.com/romainsacchi/rmnd-lca.git

will install the package and the required dependencies.

Alternatively, from Conda:

    conda install -c romainsacchi/label/nightly rmnd-lca-dev

How to use it?
--------------

A preliminary requirement to the use this library is to have a `brightway2` project created and opened, with the
`ecoinvent 3.5 cutoff` database registered, so that:

    import brightway2 as bw
    bw.projects.set_current('remind')
    bw.databases
    
returns

    Databases dictionary with 2 object(s):
	biosphere3
	ecoinvent 3.5 cutoff

Then, for a chosen policy and year between 2005 and 2150, the following two lines will:
* extract the ecoinvent database, clean it, add additional inventories for carbon capture and storage,
* remove existing electricity markets and replace them by regional markets with a geographical scope and production mix
  defined by the REMIND model for that year,
* relink electricity consuming activities to the newly created electricity markets,
* update the efficiency of electricity-producing technologies, according to the projections given by REMIND


For example, here with the year 2011 and the policy "Business-as-usual":

    ndb = NewDatabase({'BAU':2011}, 'ecoinvent 3.5 cutoff')
    ndb.update_electricity_to_remind_data()
    
returns

    Getting activity data
    100%|█████████████████████████████████| 16022/16022 [00:00<00:00, 45140.97it/s]
    Adding exchange data to activities
    100%|███████████████████████████████| 544735/544735 [00:39<00:00, 13837.57it/s]
    Filling out exchange data
    100%|██████████████████████████████████| 16022/16022 [00:03<00:00, 5132.14it/s]
    Set missing location of datasets to global scope.
    Set missing location of production exchanges to scope of dataset.
    Correct missing location of technosphere exchanges.
    Remove empty exchanges.
    Add Carma CCS inventories
    Extracted 1 worksheets in 0.98 seconds
    Add fossil carbon dioxide storage for CCS technologies.
    Remove old electricity datasets
    Create high voltage markets.
    Create medium voltage markets.
    Create low voltage markets.
    Link activities to new electricity markets.
    Rescale inventories and emissions for  Coal IGCC
    Rescale inventories and emissions for  Coal IGCC CCS
    Rescale inventories and emissions for  Coal PC
    Rescale inventories and emissions for  Coal PC CCS
    Rescale inventories and emissions for  Coal CHP
    Rescale inventories and emissions for  Gas OC
    Rescale inventories and emissions for  Gas CC
    Rescale inventories and emissions for  Gas CHP
    Rescale inventories and emissions for  Gas CCS
    Rescale inventories and emissions for  Oil
    Rescale inventories and emissions for  Biomass CHP
    Rescale inventories and emissions for  Biomass IGCC CCS
    Rescale inventories and emissions for  Biomass IGCC

Note that, by default, the library will look for REMIND output files ("xxx.mif" files and "GAINS emission factors.csv") in the
"data/Remind output files" subdirectory. If those are not located there, you need to specify the path to
the correct directory, as such::

    ndb = NewDatabase({'BAU':2011}, 'ecoinvent 3.5 cutoff', r"C:\Users\username\Documents\Remind output files")

Once the process is completed, the resulting database is registered back into the current Brightway2 project:

    ndb.write_db_to_brightway()
    
returns

    Write new database to Brightway2.
    15223 datasets
    540424 exchanges
    0 unlinked exchanges

    Writing activities to SQLite3 database:
    Created database: ecoinvent_BAU_2011
