Creating an external scenario
===============================

Start from a package for a similar technology, then change its data and
configuration together. Using an existing example helps you keep the resource
names and file layout consistent.

1. Copy or clone a package from the
   `community repositories <https://github.com/premise-community-scenarios>`__.
2. Edit the scenario data: names, regions, variables, units and year values.
3. Add Brightway-compatible inventories only for activities missing from the
   source database. The importer accepts Excel and CSV inventories.
4. Edit ``config.yaml`` to connect the data to activities and describe the
   markets, efficiency changes or supplier replacements you need.
5. Update ``datapackage.json`` so every resource points to the correct file
   and the declared ecoinvent version matches its inventory sources.
6. Build one scenario first, inspect its changed amounts and suppliers, then
   extend the package to other years or pathways.

Keep original source declarations and attribution when adapting an inventory.
A change in the scenario table has no effect unless the configuration and
transformation use that variable.

Example with Ammonia scenarios
--------------------------------

Using ammonia as an example, this guide demonstrates how to create
prospective databases from custom scenarios and other background scenarios using premise.

First, clone the Ammonia scenario repository:

.. code-block:: bash

    git clone https://github.com/premise-community-scenarios/ammonia-prospective-scenarios.git

This command downloads a copy of the repository to your local machine.
You can then rename and modify it as desired.

An external scenario consists of a package descriptor, scenario data and
configuration, plus an optional inventory resource:

1. datapackage.json: A datapackage descriptor file that specifies the scenario author, name, description, version, and the file names and paths of the scenario file, configuration file, and inventories.

2. scenario_data.csv: A scenario file that outlines various variables (e.g., production volumes, efficiencies) across time, space, and scenarios.

3. config.yaml: A configuration file that instructs premise on the changes to apply. It connects scenario variables to inventory activities
   and specifies any markets and regions to create.

4. ``lci-xxx.xlsx`` or ``lci-xxx.csv``: optional Brightway-compatible inventories
   needed when the source database lacks the required activities. The importer
   supports both formats.

The file-by-file specification continues in :doc:`/reference/external-scenario-format`.

When the files are ready, follow :doc:`external-scenarios` to attach the package
to a build. Review the changed inputs and supplier links before interpreting
impact results. If you plan to share it, document its sources and assumptions.
