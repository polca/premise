Exporting to Brightway
========================

.. raw:: html

   <span id="regular-brightway2-database"></span>

Write the databases
---------------------

Use an active Brightway project containing the configured biosphere database.
After updating ``ndb``, export its scenario with a name of your choice:

.. code-block:: python

    ndb.write_db_to_brightway(name="my-scenario-database")

For several scenarios, provide one name per scenario, in the same order:

.. code-block:: python

    ndb.write_db_to_brightway(name=["scenario-2025", "scenario-2050"])

Omitting ``name`` uses names generated from the scenario settings. Keep output
names distinct from your source database and any results you want to retain.
Then use the written databases with your usual Brightway LCA workflow.

Scenario metadata
-------------------

Premise records the IAM model, pathway, year, ecoinvent version, system model
and Premise version in each written database's metadata. Read these fields
from your own output:

.. code-block:: python

    import bw2data as bd

    metadata = bd.Database("my-scenario-database").metadata
    print(metadata["iam_model"], metadata["pathway"], metadata["representative_time"])
    print(metadata["ecoinvent_version"], metadata["system_model"])

.. note::

    ``bd.databases["db_name"]`` and ``bd.Database("db_name").metadata`` are the same
    mapping, so the fields above can be read either way.

``representative_time`` is the ISO 8601 point in time the database is representative of,
and can be read back with ``datetime.fromisoformat()``.

Databases holding several scenarios (superstructure and scenario-array databases)
list them under a ``scenarios`` key instead, and only carry ``representative_time``
at the top level if all their scenarios share the same year.
User (external) scenarios, if any, are listed under ``external_scenarios``.
