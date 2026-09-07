Using external scenarios
==========================

.. contents:: On this page
   :local:
   :depth: 1

Purpose
---------

*premise* enables users to seamlessly integrate custom scenarios,
in addition to (or as an alternative to) existing IAM scenarios. This feature
is particularly useful when users wish to incorporate projections for a sector,
product, or technology that may not be adequately addressed by standard IAM scenarios.

Available user-defined scenarios
----------------------------------

Link to public repository of user-defined scenarios:

https://github.com/premise-community-scenarios


Using user-generated scenarios
--------------------------------

To put it simply, users must first obtain the URL of the datapackage.json file corresponding
to the desired scenario. By utilizing the datapackage library, users can load the scenario package,
which includes a scenario file, inventories, and a configuration file. This package can then be added
as an argument to the *premise* instance. Users have the flexibility to include any number of custom
scenarios in this list. However, compatibility between user-defined scenarios is not guaranteed.

Example

This example needs the source project, IAM access key and external package.
Check the package's declared scenario and ecoinvent compatibility before use.

.. code-block:: python

    import os
    from premise import NewDatabase
    import bw2data as bw
    from datapackage import Package
    bw.projects.set_current("ecoinvent-3.12-cutoff")

    fp = r"https://raw.githubusercontent.com/premise-community-scenarios/cobalt-perspective-2050/main/datapackage.json"
    cobalt = Package(fp)

    external_scenario = [
        {"scenario": "Sustainable development", "data": cobalt},
    ] # several different scenarios can be listed here


    ndb = NewDatabase(
        scenarios = [
            {"model":"remind", "pathway":"SSP2-NPi", "year":2025, "external scenarios": external_scenario},
            {"model":"remind", "pathway":"SSP2-NPi", "year":2030, "external scenarios": external_scenario},
        ],
        source_db="ecoinvent-3.12-cutoff",
        source_version="3.12",
        key=os.environ["PREMISE_KEY"],
    )

.. note::

    ``source_db`` must match the **name of the database in your Brightway project** (it is not a fixed string).


The function **ndb.update("external")** can be called after that
to implement the user-defined scenario in the database.

.. code-block:: python

    ndb.update("external")

Of course, if you wish your database to also integrate the projections
of the global IAM model, you can run the function **ndb.update()**.

.. code-block:: python

    ndb.update()

Or if you just want the IAM projections relating to, for example, electricity,
heat, and steel:

.. code-block:: python

    ndb.update([
        "electricity",
        "heat",
        "steel",
        "external"
    ])

Once the integrations are complete, you can export your databases to
Brightway2, within the activated project:

.. code-block:: python

    ndb.write_db_to_brightway(name=["my_custom_db_2025", "my_custom_db_2030"])

Or as a SuperStructure database, which allows you to export only one database
to Brightway2, regardless of the number of scenarios:

.. code-block:: python

    ndb.write_superstructure_db_to_brightway()


.. note::

    SuperStructure databases can only be used from the Activity-Browser.

You can also export the databases to a csv file, which can be used
by Simapro, or as a set of sparse matrices.
