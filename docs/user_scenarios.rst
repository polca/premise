User-defined scenarios
""""""""""""""""""""""

Purpose
-------

*premise* allows users to integrate user-made scenarios in addition
(or not) to an IAM scenario. This is useful for example when a user wants to
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


The function **ndb.update_external_scenario()** can be called after that
to implement the user-defined scenario in the database.

Producing your own scenario
---------------------------

The user can produce his/her own scenario by following the steps below:

1. Clone an existing scenario repository from the public repository_.
2. Modify the scenario file (**scenario_data/scenario_data.csv**).
3. Add any inventories needed, under **inventories/lci-xxx.csv**.
4. Modify the configuration file (**configuration_file/config.yaml**), to instruct **premise** what to do.
5. Ensure that the file names and paths above are consistent with what is indicated in **datapackage.json**.
6. Once you are happy with your scenario, you can contact the admin of the public repository to add your scenario to the repository.


.. _repository: https://github.com/premise-community-scenarios


Example with Ammonia scenarios
------------------------------

Using ammonia as an example, this guide shows how to create prospective databases
from your custom scenarios and other background scenarios from **premise**.

You can clone the Ammonia scenario repository:

.. code-block:: bash

    git clone https://github.com/premise-community-scenarios/ammonia-prospective-scenarios.git

This will download a copy of the repository to your local machine.
You can then rename it and modify it to your liking.

A datapackage needs four files (called *resources*) to define a scenario:

#    **datapackage.json**: a datapackage descriptor file, indicating the scenario author,
scenario name, scenario description, scenario version, and the file names and paths
of the scenario file, configuration file, and inventories.

#    **scenario_data.csv**: a scenario file, which defines some variables (production volumes,
efficiencies, etc.) across time, space and scenarios.

#    **config.yaml**: a configuration file, which tells **premise** what to do. Among other things,
it tells **premise** which technologies the scenario considers, their names in the scenario data
file and the inventories, and which inventories to use for which technologies. It also
indicates which markets to create and for which regions.

#    **lci-xxx.csv**: optional, a csv file containing the inventories of the scenario, if needed.


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

The mapping between the IAM scenarios and the user-defined scenarios is
also done in the datapackage.json file. Here, for example, the **SSP2-Base**
scenario from the IAM models **IMAGE** and **REMIND** are mapped to the user-defined
scenario **Business As Usual**. This means that when a user wants to use the
**SSP2-Base** scenario from **IMAGE** and **REMIND**, the user-defined scenario
**Business As Usual** will be picked. While your scenario may not be meant to
be used in addition to an IAM scenario, you must still map it to an IAM scenario
(should be improved in the future).


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

For example, here the scenario file is called **scenario_data.csv**,
and is located in the **scenario_data** folder. The data in the file is in the
**long** format, with the columns **region**, **year**, **scenario**, **variable**, etc.
A scenario is, along with a configuration file, a mandatory resource
of a scenario package -- inventories are optional.

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
Having this file as a csv is mandatory, as it allows to track changes
between scenario versions.
Below are shown some variables that indicate the efficiency of the
production of hydrogen from alkaline-based electrolysers, from 2020
to 2050, for the **Sustainable development** scenario, for several regions.
The actual meaning of this variable is not important here, as it is
defined in the configuration file.


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
or other variables that are needed to modify/adjust inventories.

Inventories
***********

Inventories are stored in csv files (for version control).
The name of the csv file should be similar to what is indicated in the
*datapackage.json* file. For example, if the *datapackage.json* file indicates
that the inventory file is **inventories/lci-xxx.csv**, then the inventory file should
be named **lci-xxx.csv** under the folder **inventories** in the root folder.

config.yaml
***********

The config.yaml file is a configuration file that indicates the mapping between
the variables in the scenario data and the variables in the LCA inventories.

It is composed of two main parts: **production pathways** and **markets**.
The **production pathways** part indicates the mapping between the variables
representing a production route and listed in the scenario data file,
with the names of the LCI datasets.
It is where one can indicate the efficiency of a production route, the amount of
electricity used, the amount of hydrogen used, etc.

Consider the following example:

.. code-block:: yaml

    # `production pathways` lists the different technologies
    production pathways:
      # name given to a technology: this name is internal to premise
      MP:
        # variables to look for in the scenario data file to fetch production volumes
        # values fetched from the scenario data file as production volumes are used to calculate
        # the supply share if markets are to be built
        production volume:
          # `variable` in `production volume` refers to the variable name in the scenario data file
          variable: Production|Ammonia|Methane Pyrolysis
        # dataset in the imported inventories that represents the technology
        ecoinvent alias:
          # name of the original dataset
          name: ammonia production, hydrogen from methane pyrolysis
          # reference product of the original dataset
          reference product: ammonia, anhydrous, liquid
          # indicate whether the dataset exists in the original database
          # or if it should be sourced from the inventories folder
          exists in original database: False
          # indicate whether a region-specific version of the dataset should be created
          regionalize: True

This excerpt from the config.yaml file indicates that the variable
**Production|Ammonia|Methane Pyrolysis** in the scenario data file
should be mapped with the dataset **ammonia production, hydrogen from methane pyrolysis**
in the LCA inventories. The **reference product** of the dataset is
**ammonia, anhydrous, liquid**. The **regionalize** parameter indicates
that a region-specific version of the dataset should be created for
each region listed in the scenario data file in the *region* column.
The **exists in original database** parameter indicates that the
dataset does not exist in the original database, but is sourced from the inventories folder.

Also, consider this other example from the *config.yaml* file:

.. code-block:: yaml

  #adding PEM and AE separately to make a sub-market
  # and allow for efficiency improvements to the
  # electrolysis processes
  AE:
    production volume:
      variable: Production|Hydrogen|Alkaline Electrolysis
    ecoinvent alias:
      name: hydrogen production, alkaline electrolysis
      reference product: hydrogen, alkaline electrolysis
      exists in original database: False
      regionalize: True
    efficiency:
      - variable: Efficiency|Hydrogen|Alkaline Electrolysis (electricity)
        reference year: 2020
        includes:
          # efficiency gains will only apply to flows whose name
          # contains `electricity`
          technosphere:
            - electricity

This is essentially the same as above, but it indicates that the
variable **Efficiency|Hydrogen|Alkaline Electrolysis (electricity)** in the scenario
data file should be mapped with the **efficiency** of the dataset
**hydrogen production, alkaline electrolysis** in the LCA inventories.

The **includes** parameter indicates that the efficiency gains will only
apply to flows of type *technosphere* whose name contains **electricity**.
In practice, this will reduce the input of electricity over time for that dataset.
If you do not specify **includes**, then the efficiency gains will apply to all
flows (of type *technosphere* and *biosphere*).

The field **reference year**
indicates the baseline year **premise** should use to calculate the factor
by which the flows should be scaled by. For example, if the electrolyzer
has an efficiency of 60% in 2020, and 70% in 2030, the input of electricity
will be reduced by 14.3% (1 / (70%/60%)) if the database is created for 2030.


The **markets** part indicates which markets to build, which production routes
these markets should be composed of, which inputs should they provide, and if
they substitute a prior market in the database.

Consider the following example from the *config.yaml* file:

.. code-block:: yaml

  # name of the market dataset
  - name: market for ammonia (APS)
    reference product: ammonia, anhydrous, liquid
    # unit of the market dataset
    unit: kilogram
    # names of datasets that should compose the market
    includes:
      - MP
      - SMR
      - SMR_w_CCS
      - ELE
      - OIL
      - CG
      - CGC
    # 'market for ammonia` will replace the existing markets.
    replaces:
      - name: market for ammonia, anhydrous, liquid
        reference product: ammonia, anhydrous, liquid
    replaces in:
      - location: DE

This tells **premise** to build a market dataset named **market for ammonia (APS)**
with the reference product **ammonia, anhydrous, liquid** and the unit
**kilogram**. The market should be composed of the production routes
**MP**, **SMR**, **SMR_w_CCS**, **ELE**, **OIL**, **CG**, and **CGC**, which
have been defined in the **production pathways** part of the *config.yaml* file.
The market will replace the existing market dataset **market for ammonia, anhydrous, liquid**.

The **replaces** parameter is optional. If it is not provided, the market
will be added to the database without replacing any existing supplier.

The **replaces in** parameter is also optional. If it is not provided, the
market will be replaced in all regions. In this case, the market will
only be replaced in the regions indicated in the **replaces in** parameter.
But **replaces in** is flexible. For example, instead of a region, you can
indicate a string that should be contain in the *name* or *reference product* of activities
to update.

.. code-block:: yaml

  # name of the market dataset
  - name: market for ammonia (APS)
    reference product: ammonia, anhydrous, liquid
    # unit of the market dataset
    unit: kilogram
    # names of datasets that should compose the market
    includes:
      - MP
      - SMR
      - SMR_w_CCS
      - ELE
      - OIL
      - CG
      - CGC
    # 'market for ammonia` will replace the existing markets.
    replaces:
      - name: market for ammonia, anhydrous, liquid
        reference product: ammonia, anhydrous, liquid
    replaces in:
      - reference product: urea
      - location: DE

Hence, in this example, the ammonia supplier will be replaced in all
activities whose reference product contains the string **urea**
and location in **DE**.

Have fun!

Main contributors
-----------------

* `Romain Sacchi <https://github.com/romainsacchi>`_
