Using external scenarios
==========================

.. contents:: On this page
   :local:
   :depth: 1

Purpose
---------

Premise can use custom scenarios alongside, or instead of, IAM scenarios.
Use them to add projections for sectors, products or technologies missing
from the supplied IAM data.

Available user-defined scenarios
----------------------------------

Link to public repository of user-defined scenarios:

https://github.com/premise-community-scenarios


Using user-generated scenarios
--------------------------------

Load the scenario's ``datapackage.json`` URL with the ``datapackage`` library.
The package contains scenario data, inventories and a configuration file.
Pass it to ``NewDatabase`` as shown below. You can include several custom
scenarios, but must check that they are compatible with one another.

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

To apply all default updates, including the IAM projections and external
scenarios, call **ndb.update()**.

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

Once the updates are complete, you can export your databases to
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
