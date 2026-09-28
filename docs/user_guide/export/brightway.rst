Exporting to Brightway
========================

Regular brightway2 database
-----------------------------

*premise* uses its Brightway2/Brightway25 adapters to load the LCI database back into a *brightway2* project.
This is done as follows:

.. code-block:: python

    ndb.write_db_to_brightway()

If several databases have been built, the user can give them specific names, like so:

.. code-block:: python

    ndb.write_db_to_brightway(name=["db_1", "db_2"])

.. note::

    Brightway export requires an active Brightway project with the configured
    biosphere database already registered.


Scenario metadata
-------------------

Every database written to brightway carries, in its brightway metadata, a description
of the scenario and of the point in time it represents. It sits next to the fields
brightway maintains itself:

.. code-block:: python

    import bw2data as bd

    bd.databases["ei_cutoff_3.10.1_remind_SSP2-PkBudg500_2050"]

.. code-block:: python

    {
        # written by brightway
        "format": "Ecoinvent XML",
        "depends": ["ecoinvent-3.10.1-biosphere"],
        "backend": "sqlite",
        "number": 43648,
        "modified": "2026-08-14T12:09:25.945746",
        "processed": "2026-08-14T12:09:56.124243",
        "geocollections": ["world"],
        "searchable": True,
        # written by premise
        "premise_version": "2.4.9.1",
        "iam_model": "remind",
        "pathway": "SSP2-PkBudg500",
        "representative_time": "2050-01-01T00:00:00",
        "ecoinvent_version": "3.10.1",
        "system_model": "cutoff",
    }

.. note::

    ``bd.databases["db_name"]`` and ``bd.Database("db_name").metadata`` are the same
    mapping, so the fields above can be read either way.

``representative_time`` is the ISO 8601 point in time the database is representative of,
and can be read back with ``datetime.fromisoformat()``.

Databases holding several scenarios (superstructure and scenario-array databases)
list them under a ``scenarios`` key instead, and only carry ``representative_time``
at the top level if all their scenarios share the same year.
User (external) scenarios, if any, are listed under ``external_scenarios``.
