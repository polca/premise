Overview and boundaries
=========================

Purpose
---------

*premise* enables the alignment of life cycle inventories within the `ecoinvent <https://ecoinvent.org/>`__
3.5-3.12 database, using either a "cut-off" or "consequential"
system model, to match the output results of Integrated
Assessment Models (IAMs) such as `REMIND <https://www.pik-potsdam.de/en/institute/departments/transformation-pathways/models/remind>`__ (and REMIND-EU), `IMAGE <https://models.pbl.nl/image/index.php/Welcome_to_IMAGE_3.2_Documentation>`__, `TIAM-UCL <https://www.ucl.ac.uk/energy-models/models/tiam-ucl>`__, `MESSAGE <https://docs.messageix.org/>`__ or `GCAM <https://gcims.pnnl.gov/modeling/gcam-global-change-analysis-model>`__.
This allows for the creation of life cycle inventory databases
under future policy scenarios for any year between 2005 and 2100.
Consequential system model support is limited to ecoinvent 3.8+.

Scope and boundaries
----------------------

*premise* updates selected parts of ecoinvent to reflect IAM scenarios.
It does not rebuild the entire database from scratch, and some sectors
remain unchanged unless explicitly mapped. Results depend on the IAM model,
scenario, year, and the ecoinvent version used.

External data dependencies
----------------------------

Beyond IAM scenario files, *premise* relies on curated external datasets and
additional inventories for several sectors. These are packaged in the repository
under ``premise/data`` and include:

* **Additional inventories** used to extend ecoinvent (``premise/data/additional_inventories``).
* **GAINS emission factors** for non-CO2 pollutants (``premise/data/GAINS_emission_factors``).
* **Renewables performance data** such as PV efficiencies and wind tech parameters
  (``premise/data/renewables``).
* **Battery energy density projections** and scenario shares
  (``premise/data/battery``).
* **Metals and mining datasets** for material intensities and market shares
  (``premise/data/metals``, ``premise/data/mining``).
* **Fuel and hydrogen parameters** such as losses and supply-chain data
  (``premise/data/fuels``).
* **Sector-specific inventories** for heat, cement, steel, transport, and CDR
  updates
  (mappings under ``premise/iam_variables_mapping`` and inventories under
  ``premise/data``).

These inputs are versioned with the code to keep results reproducible when using
the same *premise* release.

.. note::

    The ecoinvent database is not included in this package. You need to have a valid license for ecoinvent 3.5-3.12 to use *premise*.
    Also, please read carefully ecoinvent's `EULA <https://ecoinvent.org/app/uploads/2024/01/EULA_new_branding_08_11_2023.pdf>`__ before using *premise*.

IAM data access
-----------------

Some IAM scenario files are encrypted. Access requires an encryption key from
the developers.

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
LCIA method used. Matching release labels alone does not establish numerical
identity, and changing an input need not change every activity.

Additionally, you may want to cite the ecoinvent v.3 database:

Wernet, G. et al. The ecoinvent database version 3 (part I): overview and methodology. Int. J. Life Cycle Assess. 21, 1218–1230 (2016) . http://link.springer.com/10.1007/s11367-016-1087-8.

Finally, you should properly refer the IAM model used with *premise*:

* REMIND: Baumstark et al. REMIND2.1: transformation and innovation dynamics of the energy-economic system within climate and sustainability limits, Geoscientific Model Development, 2021.
* IMAGE: Stehfest, Elke, et al. Integrated assessment of global environmental change with IMAGE 3.0: Model description and policy applications. Netherlands Environmental Assessment Agency (PBL), 2014.
* TIAM-UCL: Pye, S., et al. The TIAM-UCL Model (Version 4.1.1) Documentation, 2020.
* MESSAGEix-GLOBIOM-GAINS: Daniel Huppmann, Matthew Gidden, Oliver Fricko, Peter Kolp, Clara Orthofer, Michael Pimmer, Nikolay Kushin, Adriano Vinca, Alessio Mastrucci, Keywan Riahi, Volker Krey, The MESSAGEix Integrated Assessment Model and the ix modeling platform (ixmp): An open framework for integrated and cross-cutting analysis of energy, climate, the environment, and sustainable development, Environmental Modelling & Software, 2019, https://doi.org/10.1016/j.envsoft.2018.11.012.
* GCAM: Calvin, K., Patel, P., Clarke, L., Asrar, G., Bond-Lamberty, B., Cui, R. Y., Di Vittorio, A., Dorheim, K., Edmonds, J., Hartin, C., Hejazi, M., Horowitz, R., Iyer, G., Kyle, P., Kim, S., Link, R., McJeon, H., Smith, S. J., Snyder, A., Waldhoff, S., and Wise, M.: GCAM v5.1: representing the linkages between energy, water, land, climate, and economic systems, Geosci. Model Dev., 12, 677–698, https://doi.org/10.5194/gmd-12-677-2019, 2019.
