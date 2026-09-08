IAM data and variables
========================

After extracting the ecoinvent database and additional inventories,
*premise* instantiates the class *IAMDataCollection*, which collects
all sorts of data from the IAM output file and store it into
multi-dimensional arrays.


Production volumes and efficiencies
-------------------------------------

The mapping between IAM variables and *premise* variables regarding production
volumes and efficiencies can be found in the `sector YAML mappings <https://github.com/polca/premise/tree/76dbf845ef73bb765024dda1143960a24964a5fe/premise/iam_variables_mapping>`__.

Land use and land use change
------------------------------

The mapping between IAM variables and *premise* variables regarding land use
and emissions caused by land use change can be found in the `sector YAML mappings <https://github.com/polca/premise/tree/76dbf845ef73bb765024dda1143960a24964a5fe/premise/iam_variables_mapping>`__.


Carbon Capture and Storage
----------------------------

The mapping between IAM variables and *premise* variables regarding carbon capture
and storage can be found in the `sector YAML mappings <https://github.com/polca/premise/tree/76dbf845ef73bb765024dda1143960a24964a5fe/premise/iam_variables_mapping>`__.

IAM scenario file format
--------------------------

The scenario file should be a comma-separated text file (i.e., csv)
with data presented in a tabular format. The following historical rows
illustrate the layout; they are not the quickstart scenario:

+--------+-------------+--------+------------------+-----------+-------------+-------------+-------------+-------------+-------------+
| Model  | Scenario    | Region | Variable         | Unit      | 2005        | 2010        | 2015        | 2020        | 2025        |
+========+=============+========+==================+===========+=============+=============+=============+=============+=============+
| REMIND | SSP2EU-Base | CAZ    | Emi|CO2|+|Energy | Mt CO2/yr | 1011.34074  | 976.7202877 | 993.8525168 | 957.3199102 | 945.014101  |
+--------+-------------+--------+------------------+-----------+-------------+-------------+-------------+-------------+-------------+
| REMIND | SSP2EU-Base | CHA    | Emi|CO2|+|Energy | Mt CO2/yr | 6720.313463 | 8601.575671 | 10086.37126 | 11281.46999 | 10996.79931 |
+--------+-------------+--------+------------------+-----------+-------------+-------------+-------------+-------------+-------------+
| REMIND | SSP2EU-Base | EUR    | Emi|CO2|+|Energy | Mt CO2/yr | 4235.648974 | 3730.532814 | 3392.421123 | 3114.284044 | 2860.549231 |
+--------+-------------+--------+------------------+-----------+-------------+-------------+-------------+-------------+-------------+
| REMIND | SSP2EU-Base | IND    | Emi|CO2|+|Energy | Mt CO2/yr | 1215.466496 | 1664.185158 | 2146.940653 | 2477.459967 | 2946.357462 |
+--------+-------------+--------+------------------+-----------+-------------+-------------+-------------+-------------+-------------+
| REMIND | SSP2EU-Base | JPN    | Emi|CO2|+|Energy | Mt CO2/yr | 1457.252288 | 1415.666384 | 1345.278014 | 1181.679212 | 1060.684659 |
+--------+-------------+--------+------------------+-----------+-------------+-------------+-------------+-------------+-------------+
| REMIND | SSP2EU-Base | LAM    | Emi|CO2|+|Energy | Mt CO2/yr | 1410.609298 | 1575.558465 | 1682.930038 | 1613.4512   | 1739.260156 |
+--------+-------------+--------+------------------+-----------+-------------+-------------+-------------+-------------+-------------+
| REMIND | SSP2EU-Base | MEA    | Emi|CO2|+|Energy | Mt CO2/yr | 1782.408233 | 2254.050107 | 2607.952516 | 2793.972343 | 3064.426497 |
+--------+-------------+--------+------------------+-----------+-------------+-------------+-------------+-------------+-------------+
| REMIND | SSP2EU-Base | NEU    | Emi|CO2|+|Energy | Mt CO2/yr | 378.1710003 | 421.2277231 | 477.6241091 | 498.465216  | 500.4845903 |
+--------+-------------+--------+------------------+-----------+-------------+-------------+-------------+-------------+-------------+
| REMIND | SSP2EU-Base | OAS    | Emi|CO2|+|Energy | Mt CO2/yr | 1787.07182  | 2073.863804 | 2442.52372  | 2780.880819 | 3264.746917 |
+--------+-------------+--------+------------------+-----------+-------------+-------------+-------------+-------------+-------------+
| REMIND | SSP2EU-Base | REF    | Emi|CO2|+|Energy | Mt CO2/yr | 2551.110779 | 2472.637216 | 2544.690495 | 2607.286302 | 2681.647657 |
+--------+-------------+--------+------------------+-----------+-------------+-------------+-------------+-------------+-------------+


The following columns must be present:

* Region
* Variable
* Unit

as well as the time steps (e..g, 2005 to 2100).
Other columns can be present, but they will be ignored.

You need to point to that file when initiating `NewDatabase`, like so:

.. code-block:: python

    ndb = NewDatabase(
        scenarios = [{"model":"remind", "pathway":"my_special_scenario", "year":2028,
                      "filepath":r"C:\filepath\to\your\scenario\folder"}],
        source_db="ecoinvent-3.12-cutoff", # <-- name of the database
        source_version="3.12", # <-- version of ecoinvent
    )

There are essentially two types of variables needed from the IAM scenario files:

- variables that relate to the production volumes of technologies. These variables are used to scale the production volumes of the corresponding activities in the ecoinvent database. For example, if the IAM scenario file contains a variable named ``Electricity|Production|Wind`` for the region ``EUR``, it will help premise calculate the share of wind power in the electricity consumption mix of that region. Hence, the unit of such variables should refer to a production volume over time (e.g., ``GWh/year``, ``EJ/year``, etc.).
- variables that relate to the efficiency of technologies over time. These variables are used to calculate scaling factors (which are relative by default to 2020), to adjust the energy or material efficiency of the corresponding activities in the ecoinvent database. For example, if the IAM scenario file contains a variable named ``Electricity|Efficiency|Coal`` for the region ``EUR``, it will help premise adjust the amount of coal and related emissions per unit of kWh produced in that region. Hence, the unit of such variables can be unitless, or relate to an efficiency ratio or percentage.
