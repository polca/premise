User-defined scenarios
""""""""""""""""""""""

Purpose
-------

*premise* allows users to integrate user-made scenarios in addition
to an IAM scenario. This is useful for example when a user wants to
integrate projections for a sector, product or a technology
that is not really covered by IAM scenarios.

Available user-defined scenarios
--------------------------------

Link to public repository of user-defined scenarios:

https://github.com/premise-community-scenarios


Using user-generated scenarios
------------------------------

Quite simply, the user needs to fetch the url of the datapackage.json
file of the scenario of interest. Using the library **datapackage**,
the user can then load the scenario package (including a scenario file,
inventories and a configuration file) and include it as an argument
to the premise instance.

Example

.. code-block:: python

    from premise import *
    import brightway2 as bw
    from datapackage import Package
    bw.projects.set_current("ei_38")

    fp = r"https://raw.githubusercontent.com/premise-community-scenarios/cobalt-perspective-2050/main/datapackage.json"
    cobalt = Package(fp)

    ndb = NewDatabase(
    scenarios = [
        {"model":"image", "pathway":"SSP2-Base", "year":2025, "exclude": ["update_two_wheelers", "update_cars", "update_buses"]},
        {"model":"image", "pathway":"SSP2-Base", "year":2030, "exclude": ["update_two_wheelers", "update_cars", "update_buses"]},
    ],
    source_db="ecoinvent cutoff 3.8",
    source_version="3.8",
    key='xxxxxxx',
    external_scenarios=[
        cobalt,
    ]


The function **ndb.update_external_scenario()**can be called after that
to implement the user-defined scenario in the database.

Producing your own scenario
---------------------------

The user can produce his/her own scenario by following the steps below:

1. Clone an existing scenario repository from the public repository_.
2. Modify the scenario file (**scenario_data/scenario_data.csv**). 3. Add any inventories needed, under **inventories/lci-xxx.csv**.
3. Modify the configuration file (**configuration_file/config.yaml**), to instruct **premise** what to do.
4. Ensure that the file names and paths above are consistent with what is indicated in **datapackage.json**.
5. Once you are happy with your scenario, you can contact the admin of the public repository to add your scenario to the repository.


.. _repository: https://github.com/premise-community-scenarios


Example with Ammonia scenarios
------------------------------

Using ammonia as an example, this guide shows how to create prospective databases
from your custom scenarios and other background scenarios from **premise**.
A datapackage needs four files to define a scenario:

1. **datapackage.json**: a datapackage descriptor file, indicating the scenario author,
    scenario name, scenario description, scenario version, and the file names and paths
    of the scenario file, configuration file, and inventories.

2. **scenario_data.csv**: a scenario file, which defines some variables (production volumes,
    efficiencies, etc.) across time, space and scenarios.

3. **config.yaml**: a configuration file, which tells **premise** what to do. Among other things,
    it tells **premise** which technologies the scenario considers, their names in the scenario data
    file and the inventories, and which inventories to use for which technologies. It also
    indicates which markets to create and for which regions.

4. **lci-xxx.csv**: optional, a csv file containing the inventories of the scenario, if needed.


datapackage.json
****************

The datapackage.json file is a descriptor file that indicates the scenario author,
scenario name, scenario description, scenario version, and the file names and paths
of the scenario file, configuration file, and inventories.

Example:

.. code-block:: json

    {
    "profile": "data-package",
    "name": "ammonia-prospective-scenarios",
    "title": "Ammonia decarbonisation pathways and their effects on life cycle assessments: Integrating future ammonia scenarios into background data for prospective LCAs",
    "description": "Implementation of the scenarios on future ammonia supply from the Master thesis of J. Boyce, 2022.",
    "source":"Boyce, J. C. (2022). Ammonia decarbonisation pathways and their effects on life cycle assessments: Integrating future ammonia scenarios into background data for prospective LCAs [Master’s Thesis, Leiden University and TU Delft].",
    "version": "0.0.1",
    "contributors":[
        {
        "title": "Johanna C. Boyce",
        "email": "xxxx@umail.leidenuniv.nl"
    }
    ], ...

The mapping betweenn the IAM scenarios and the user-defined scenarios is
also done in the configuration file. Here, for exmaple, the **SSP2-Base**
scenario from the IAM model **IMAGE** is mapped to the user-defined
scenario **Business As Usual**.


.. code-block:: json

    "scenarios": {
        "Business As Usual": [
            {
                "model": "image",
                "pathway": "SSP2-Base"
            },
            {
                "model": "remind",
                "pathway": "SSP2-Base"
            }
        ],

The resources section of the datapackage.json file indicates the file names, location
of the scenario file, configuration file, and inventories, as well as how their
data should present.

.. code-block:: json

    "resources": [
        {
            "path": "scenario_data/scenario_data.csv",
            "profile": "tabular-data-resource",
            "name": "scenario_data",
            "format": "csv",
            "mediatype": "text/csv",
            "encoding": "utf-8-sig",
            "schema": {
                "fields": [
                    {
                        "name": "model",
                        "type": "string",
                        "format": "default"
                    },

Scenario data
*************

The **scenario_data.csv** file contains the scenario data.

+-------+------------+-------------------------+--------+---------------------------------------------------------+------+------+------+------+------+------+------+------+------+
| model | pathway    | scenario                | region | variables                                               | unit | 2020 | 2025 | 2030 | 2035 | 2040 | 2045 | 2050 | 2100 |
+-------+------------+-------------------------+--------+---------------------------------------------------------+------+------+------+------+------+------+------+------+------+
| image | SSP2-RCP19 | Sustainable development | CHN    | Efficiency|Hydrogen|Alkaline Electrolysis (electricity) | %    | 66   | 67.5 | 69   | 71   | 73   | 74.5 | 76   | 76   |
+-------+------------+-------------------------+--------+---------------------------------------------------------+------+------+------+------+------+------+------+------+------+
| image | SSP2-RCP19 | Sustainable development | INDIA  | Efficiency|Hydrogen|Alkaline Electrolysis (electricity) | %    | 66   | 67.5 | 69   | 71   | 73   | 74.5 | 76   | 76   |
+-------+------------+-------------------------+--------+---------------------------------------------------------+------+------+------+------+------+------+------+------+------+
| image | SSP2-RCP19 | Sustainable development | CAN    | Efficiency|Hydrogen|Alkaline Electrolysis (electricity) | %    | 66   | 67.5 | 69   | 71   | 73   | 74.5 | 76   | 76   |
+-------+------------+-------------------------+--------+---------------------------------------------------------+------+------+------+------+------+------+------+------+------+
| image | SSP2-RCP19 | Sustainable development | USA    | Efficiency|Hydrogen|Alkaline Electrolysis (electricity) | %    | 66   | 67.5 | 69   | 71   | 73   | 74.5 | 76   | 76   |
+-------+------------+-------------------------+--------+---------------------------------------------------------+------+------+------+------+------+------+------+------+------+
| image | SSP2-RCP19 | Sustainable development | MEX    | Efficiency|Hydrogen|Alkaline Electrolysis (electricity) | %    | 66   | 67.5 | 69   | 71   | 73   | 74.5 | 76   | 76   |
+-------+------------+-------------------------+--------+---------------------------------------------------------+------+------+------+------+------+------+------+------+------+

The first column
is the **model** column, which indicates the IAM model that the scenario
maps with. The second column is the **pathway** column, which indicates
the IAM scenario that the user-defined scenario should map with.
The third column is the name of the user-defined scenario. The fourth column
is the region, which can be either a country or a region. The fifth column
is the **variable** column, which indicates the variable that the
scenario data is about. The sixth column is the **unit** column,
which indicates the unit of that variable. The columns after that are the
values of the variable across time.

Variables can be production volumes (used to build markets), efficiencies,
or other variables that are needed to calculate the inventories.

Inventories
***********


config.yaml
***********


Main contributors
-----------------

* `Romain Sacchi <https://github.com/romainsacchi>`_
