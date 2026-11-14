Using external scenarios
==========================

Use an external scenario to add your own production, technology-share or
efficiency projections. It can replace selected IAM assumptions or cover a
technology absent from the supplied data. You still identify the base build
with a model, pathway and year.

Available user-defined scenarios
----------------------------------

The `community scenario repositories
<https://github.com/premise-community-scenarios>`__ contain packages you can
use or adapt. Before using one, check its scenario names, years, regions,
source ecoinvent version and required inventories.

.. raw:: html

   <span id="id1"></span>
   <span id="id2"></span>
   <span id="id3"></span>
   <span id="on-this-page"></span>
   <span id="purpose"></span>

Using user-generated scenarios
--------------------------------

An external scenario package contains a descriptor (``datapackage.json``),
a data table and configuration, plus inventories when needed. Download a
package or prepare one using :doc:`create-external-scenario`.

The configuration below uses the project and source names from
:doc:`/getting_started/preparation`. Replace the package path and custom
scenario name with the values in your package:

.. code-block:: python

    import os
    import bw2data as bd
    from datapackage import Package
    from premise import NewDatabase

    bd.projects.set_current(os.environ["PREMISE_BW_PROJECT"])
    package = Package("path/to/my-package/datapackage.json")
    custom = {"scenario": "My custom scenario", "data": package}
    ndb = NewDatabase(
        scenarios=[{
            "model": "remind",
            "pathway": "SSP2-NPi",
            "year": 2025,
            "external scenarios": [custom],
        }],
        source_db=os.environ["PREMISE_SOURCE_DB"],
        source_version="3.12",
        biosphere_name=os.environ["PREMISE_BIOSPHERE"],
        key=os.environ["PREMISE_KEY"],
    )
    ndb.update()

The normal update applies both the IAM changes and the external scenario.
External changes run near the end, before the emissions update. If several
packages are attached to a build, check whether they change the same activities.
Their effects are not automatically independent.

Use ``ndb.update("external")`` only when you want the custom changes without
running the other sector updates. The starting inventory still includes the
source database and applicable additional inventories. A selective update
does not prepare prospective electricity or fuels automatically; see
:doc:`updates` for dependencies and ordering.

After reviewing the :doc:`change report <reports>`, use any suitable
:doc:`export route <export/index>`. For one Brightway scenario:

.. code-block:: python

    ndb.write_db_to_brightway(name="my-custom-scenario-2025")

For more than one scenario, attach the custom configuration to each scenario
dictionary that should use it and supply one output name per scenario. The
:doc:`package-format reference </reference/external-scenario-format>` describes
how the files, variable names and inventory identities fit together.
