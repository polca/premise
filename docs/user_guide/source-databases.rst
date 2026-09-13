Source databases
==================

.. contents:: On this page
   :local:
   :depth: 1

*premise* can extract the ecoinvent database from:

* a `brightway2 <https://brightway.dev/>`__ project that contains the ecoinvent database
* ecospold2 files, that can be downloaded from the `ecoinvent <https://ecoinvent.org>`__ website

.. note::

        The ecoinvent database is not included in *premise*.
        You need to have a valid license to download and use it.
        Also, please read carefully ecoinvent's `EULA <https://ecoinvent.org/app/uploads/2024/01/EULA_new_branding_08_11_2023.pdf>`__ before using *premise*.

From a brightway2 project
---------------------------

To extract an ecoinvent database from a Brightway project,
indicate the database name in `source_db` and its version in `source_version`:

.. code-block:: python

  import os
  from premise import NewDatabase
  import bw2data as bw

  bw.projects.set_current("my_project")

  ndb = NewDatabase(
        scenarios=[
                {"model":"remind", "pathway":"SSP2-NPi", "year":2025}
            ],
        source_db="ecoinvent-3.12-cutoff",
        source_version="3.12",
        system_model="cutoff",
        inventory_backend="compact",
        biosphere_name=os.environ["PREMISE_BIOSPHERE"],
        key=os.environ["PREMISE_KEY"],
        keep_imports_uncertainty=True, # True by default, set to False to drop uncertainty in additional inventories
        keep_source_db_uncertainty=False # False by default, set to True if you want to keep ecoinvent's uncertainty data
    )

.. note::

    ``source_db`` must match the **name of the database in your Brightway project** (it is not a fixed string).

Note that a cache of the database will be created the first time and
stored in the configured user data directory. Compatible subsequent builds
can reuse it; source and inventory fingerprints determine cache validity.
See :doc:`inventories` for cache and checkpoint behaviour.

If you wish to clear that cache folder (database and *premise* additional inventories), do:

.. code-block:: python

    import os
    from premise import clear_cache

    clear_cache()



To clear only the *premise* additional inventories, do:

.. code-block:: python

    import os
    from premise import clear_inventory_cache

    clear_inventory_cache()

.. note::

    After a version update, databases and inventories are automatically
    re-extracted and re-imported. This is to ensure that the data is
    consistent with the new version of *premise*.


From ecospold2 files
----------------------

To extract from a set of ecospold2 files, you need to point to the location of
those files in `source_file_path`, as well as indicate the database format in
`source_type`:

.. code-block:: python

    import os
    from premise import NewDatabase

    ndb = NewDatabase(
        scenarios = [
            {"model":"remind", "pathway":"SSP2-NPi", "year":2025}
                    ],
        source_type="ecospold",
        key=os.environ["PREMISE_KEY"],
        source_file_path=r"C:\file\path\to\ecoinvent 3.12_cutoff_ecoSpold02\datasets",
        source_version="3.12",
        system_model="cutoff",
        inventory_backend="compact",
        biosphere_name=os.environ["PREMISE_BIOSPHERE"],
    )

.. note::

    When using ``source_type="ecospold"``, no Brightway project or biosphere
    database is needed unless you later export the result back to Brightway.
