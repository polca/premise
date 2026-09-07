Creating an external scenario
===============================

The user can produce his/her own scenario by following the steps below:

1. Clone an existing scenario repository from the public `repository <https://github.com/premise-community-scenarios>`__.
2. Modify the scenario file (**scenario_data/scenario_data.csv**).
3. Add any inventories needed, under **inventories/lci-xxx.csv**.
4. Modify the configuration file (**configuration_file/config.yaml**), to instruct **premise** what to do.
5. Ensure that the file names and paths above are consistent with what is indicated in **datapackage.json**.
6. Once definitive, you can contact the admin of the public repository to add your scenario to the repository.


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

3. config.yaml: A configuration file that instructs premise on the required actions. It provides information on the technologies considered in the scenario, their names in the scenario data file and inventories, and the inventories to use for each technology. Additionally, it indicates the markets to be created and their corresponding regions.

4. ``lci-xxx.xlsx`` or ``lci-xxx.csv``: optional Brightway-compatible inventories
   needed when the source database lacks the required activities. The importer
   supports both formats.

The file-by-file specification continues in :doc:`/reference/external-scenario-format`.
