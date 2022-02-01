# ``premise``

<div style="text-align:center">
<img src="https://github.com/romainsacchi/premise/raw/master/docs/large.png" height="300"/>
</div>

# **PR**ospective **E**nviron**M**ental **I**mpact As**SE**ssment
## Coupling the ecoinvent database with projections from Integrated Assessment Models (IAM)


<p align="center">
  <a href="https://badge.fury.io/py/premise" target="_blank"><img src="https://badge.fury.io/py/premise.svg"></a>
  <a href="https://github.com/romainsacchi/premise" target="_blank"><img src="https://github.com/romainsacchi/premise/actions/workflows/main.yml/badge.svg?branch=master"></a>
  <a href="https://ci.appveyor.com/project/romainsacchi/premise" target="_blank"><img src="https://ci.appveyor.com/api/projects/status/github/romainsacchi/premise?svg=true"></a>
  <a href="https://coveralls.io/github/romainsacchi/premise" target="_blank"><img src="https://coveralls.io/repos/github/romainsacchi/premise/badge.svg"></a>
  <a href="https://premise.readthedocs.io/en/latest/" target="_blank"><img src="https://readthedocs.org/projects/premise/badge/?version=latest"></a>
</p>
 
Previously named *rmnd-lca*. *rmnd-lca* was designed to work with the IAM model REMIND only.
As it now evolves towards a more IAM-neutral approach, a change of name was considered.

What's new in 1.0.0?
====================

We have just released the first major version of *premise*.

Compared to 0.4.5, here are the some of the most notable changes:

* there's now a [detailed documentation](https://premise.readthedocs.io/en/latest/) for *premise*.
* *premise* works with ecoinvent 3.8, cut-off.
* none of the original datasets in ecoinvent are deleted. This means that any inventories linking successfully
with the original ecoinvent database will link with a premise-generated database.
* uncertainty information is removed from the database.
* *premise* reverts to using "Carbon dioxide, in air" and "Carbon dioxide, non-fossil" to model uptake
and release of biogenic carbon dioxide. If you wish to account for those in the global warming indicator,
you need to execute [premise_gwp](https://github.com/romainsacchi/premise_gwp), which installs the necessary 
GWP LCIA methods.
* *premise* caches the extraction of the database and the import of the inventories the first time 
a database is created, skipping those steps for the next time.
* updates inventories for PV and natural gas.
* updates inventories for two-wheelers, cars, trucks and buses and creates region-specific
 fleet average vehicles (from REMIND or IMAGE fleet data). Activities using transport 
are relinked to these new vehicles.
* creates region-specific biomass markets that feed biomass to power plants,
reflecting the share of biomass coming as forestry or agricultural residue.
* creates liquid and gaseous fuel markets, reflecting the share of biofuels, methanol and synfuels.
Also it modifies the split between fossil and biogenic CO2 emissions in the activities feeding
from the fuel market (based on the fuel mix).


Documentation
-------------
[https://premise.readthedocs.io/en/latest/](https://premise.readthedocs.io/en/latest/)

Objective
---------

The objective is to produce life cycle inventories under future energy policies, by modifying the inventory database
ecoinvent 3 to reflect projected energy policy trajectories.

Requirements
------------
* **Python 3.9**
* License for [ecoinvent 3][1]
* Some IAM output files come with the library and are located by default in the subdirectory "/data/iam_output_files". **If you wish to use
 those files, you need to request (by [email](mailto:romain.sacchi@psi.ch)) an encryption key from the developers**.
 A file path can be specified to fetch IAM output files elsewhere on your computer.
 * [brightway2][2] (optional)

How to install this package?
----------------------------

Two options:

A development version with the latest advancements (but with the risks of unseen bugs),
is available from Anaconda Cloud:

    conda install -c romainsacchi premise

For a more stable and proven version, from Pypi:

    pip install premise

will install the package and the required dependencies.


How to use it?
--------------

The best way is to follow [the examples from the Jupyter Notebook](https://github.com/romainsacchi/premise/blob/master/examples/examples.ipynb). 

# Support

Do not hesitate to contact the development team at [romain.sacchi@psi.ch](mailto:romain.sacchi@psi.ch)
or [aloisdir@pik-potsdam.de](mailto:aloisdir@pik-potsdam.de).

## Maintainers

* [Romain Sacchi](https://github.com/romainsacchi)
* [Alois Dirnaichner](https://github.com/Loisel)
* [Tom Mike Terlouw](https://github.com/tomterlouw)
* [Laurent Vandepaer](https://github.com/lvandepaer)
* [Chris Mutel](https://github.com/cmutel/)

## Contributing

See [contributing](https://github.com/romainsacchi/premise/blob/master/CONTRIBUTING.md).

## References

[1]:https://www.ecoinvent.org/
[2]:https://brightway.dev/

## License

[BSD-3-Clause](https://github.com/romainsacchi/premise/blob/master/LICENSE).
Copyright 2020 Potsdam Institute for Climate Impact Research, Paul Scherrer Institut.
