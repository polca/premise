Overview and boundaries
=========================

Purpose
---------

Premise builds life cycle inventory databases for different scenarios and
years. It starts with an `ecoinvent <https://ecoinvent.org/>`__ database and
uses projections from integrated assessment models (IAMs) to change selected
production processes, technologies and supply mixes.

An IAM describes how energy use, production and other systems could develop
under a set of assumptions about society and policy. A **scenario** specifies
a model, a pathway and a year. It is a possible future, rather than a forecast.
See :doc:`/reference/glossary` for the terms used throughout this guide.

Scope and boundaries
----------------------

The result is a database you can use for prospective life cycle assessment.
For example, a future electricity market can buy from a different mix of
power plants, and those plants can require different fuel inputs.

Premise changes the processes covered by its configuration and data. Other
activities remain in the database, and some assumptions come from external
sources rather than the IAM. The selected model, scenario file, year,
ecoinvent version and system model all affect the result.

Check :doc:`/reference/compatibility` for accepted configurations and
:doc:`/reference/coverage` for the data needed by each sector.

External data dependencies
----------------------------

You supply ecoinvent separately. It is not included with Premise. Prepare an
existing Brightway import or ecospold2 files, as described in
:doc:`/user_guide/source-databases`.

Premise also uses additional inventories and supporting data:

* Published inventory records on `CLIC <https://clic.psi.ch/>`__ describe
  technologies added to the source database. The
  :doc:`inventory catalogue </reference/inventories>` links to their sources.
* IAM data supply production volumes, technology shares and efficiency trends.
* Other inputs supply air-pollutant factors, renewable-technology performance,
  battery density and chemistry projections, material intensities and mining
  assumptions.

Sector chapters explain how these inputs are combined. Keep track of their
versions when comparing builds; using the same pathway name alone does not
ensure the same inputs.

IAM data access
-----------------

The standard encrypted IAM files require an access key from the Premise
developers. :doc:`preparation` explains how to pass it to the build without
storing it in your script.

Publication
-------------

The methodology behind *premise* is described in the following publication:

R. Sacchi, T. Terlouw, K. Siala, A. Dirnaichner, C. Bauer, B. Cox, C. Mutel, V. Daioglou, G. Luderer,
PRospective EnvironMental Impact asSEment (premise): A streamlined approach to producing databases for prospective life cycle assessment using integrated assessment models,
Renewable and Sustainable Energy Reviews, 2022, https://doi.org/10.1016/j.rser.2022.112311.

.. note::

    If you use *premise* in your research, please cite the above publication.

Reproducibility
-----------------

For reproducibility, record the Premise version or commit, source database
identity and modifications, ecoinvent system model, IAM file checksum and
release, constructor options, update order, external scenarios and inventory
files, and relevant dependency versions. Retain report fingerprints and the
LCIA method used. Matching release labels alone does not guarantee identical
results, and changing an input need not change every activity.

Also cite the source ecoinvent database:

Wernet, G. et al. The ecoinvent database version 3 (part I): overview and methodology. Int. J. Life Cycle Assess. 21, 1218–1230 (2016) . http://link.springer.com/10.1007/s11367-016-1087-8.

Cite the IAM used in your study:

* REMIND: Baumstark et al. REMIND2.1: transformation and innovation dynamics of the energy-economic system within climate and sustainability limits, Geoscientific Model Development, 2021.
* IMAGE: Stehfest, Elke, et al. Integrated assessment of global environmental change with IMAGE 3.0: Model description and policy applications. Netherlands Environmental Assessment Agency (PBL), 2014.
* TIAM-UCL: Pye, S., et al. The TIAM-UCL Model (Version 4.1.1) Documentation, 2020.
* MESSAGEix-GLOBIOM-GAINS: Daniel Huppmann, Matthew Gidden, Oliver Fricko, Peter Kolp, Clara Orthofer, Michael Pimmer, Nikolay Kushin, Adriano Vinca, Alessio Mastrucci, Keywan Riahi, Volker Krey, The MESSAGEix Integrated Assessment Model and the ix modeling platform (ixmp): An open framework for integrated and cross-cutting analysis of energy, climate, the environment, and sustainable development, Environmental Modelling & Software, 2019, https://doi.org/10.1016/j.envsoft.2018.11.012.
* GCAM: Calvin, K., Patel, P., Clarke, L., Asrar, G., Bond-Lamberty, B., Cui, R. Y., Di Vittorio, A., Dorheim, K., Edmonds, J., Hartin, C., Hejazi, M., Horowitz, R., Iyer, G., Kyle, P., Kim, S., Link, R., McJeon, H., Smith, S. J., Snyder, A., Waldhoff, S., and Wise, M.: GCAM v5.1: representing the linkages between energy, water, land, climate, and economic systems, Geosci. Model Dev., 12, 677–698, https://doi.org/10.5194/gmd-12-677-2019, 2019.

Next, :doc:`installation` explains how to install Premise.
