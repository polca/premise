Supporting another IAM model
==============================

Although *premise* comes with a set of scenarios from the REMIND, IMAGE,
MESSAGE, GCAM, and TIAM-UCL IAM models, it is possible to link it to a new IAM model.
To do so, you need to populate the .yaml mapping files under the
folder https://github.com/polca/premise/tree/76dbf845ef73bb765024dda1143960a24964a5fe/premise/iam_variables_mapping

For each variable in each of the .yaml files, specify the
corresponding IAM variable name as follows:

.. code-block:: yaml

    Biomass CHP:
      iam_aliases:
        remind: SE|Electricity|Biomass|++|Combined Heat and Power w/o CC
        image: Secondary Energy|Electricity|Biomass|w/o CCS|3

        new_IAM: new_IAM_variable_name <--- this is the new IAM variable name

      eff_aliases:
        remind: Tech|Electricity|Biomass|Combined Heat and Power w/o CC|Efficiency
        image: Efficiency|Electricity|Biomass|w/o CCS|3

        new_IAM: new_IAM_efficiency_variable_name <--- this is the new IAM variable name

      ecoinvent_aliases:
        fltr:
          - heat and power co-generation, wood chips
        mask:
          reference product: heat
      ecoinvent_fuel_aliases:
        fltr:
          - market for wood chips, wet, measured as dry mass

If efficiency-related variables are not available, the corresponding
technologies will simply not have their efficiency adjusted.

CDR energy-use aliases
------------------------

Most sector mapping files store ``energy_use_aliases`` directly by IAM model.
Carbon dioxide removal is different because *premise* can scale electricity and
heat/fuel exchanges separately in the CDR inventories. In
``carbon_dioxide_removal.yaml``, keep energy-use aliases explicit by carrier:

.. code-block:: yaml

    cement production, non-fossil CO2, with CCS:
      iam_aliases:
        image: DUMMY|Carbon Removal|Geological Storage|Industry|Cement|Non-Fossil CO2
      energy_use_aliases:
        electricity:
          image: DUMMY|Final Energy|Carbon Management|Industrial Biogenic CCS|Cement|Electricity
        heat:
          image: DUMMY|Final Energy|Carbon Management|Industrial Biogenic CCS|Cement|Heat
      ecoinvent_aliases:
        fltr: carbon dioxide, captured and stored, at cement production plant, from non-fossil carbon dioxide, using monoethanolamine

During IAM data collection, electricity aliases remain in an electricity group.
All other CDR final-energy carriers, such as heat, gases, diesel, hydrogen or
other fuels, are grouped as heat/fuel for the CDR efficiency adjustment. The
adjustment then scales matching electricity exchanges and matching heat/fuel
exchanges independently; material inputs and biosphere exchanges are left
unchanged.

Additionally, add your model name to the models list as well as
the list of geographical regions as LIST_xxx_REGIONS, with xxx
being the IAM model name, in the file iam_variables_mapping/constants.yaml.

Lastly, inform premise about the geographical definitions of
the IAM model you are using.
Create a .json file listing ISO 3166-1 alpha-2 country codes
and their corresponding IAM regions, as shown below, and store it under
premise/iam_variables_mapping/topologies, under the name: iamname-topology.json.

.. code-block:: json

    {
        "EUR": ["AT", "BE", "CH", "DE", "FR"],
        "REF": ["AM", "AZ", "BY", "GE", "KZ", "KG", "MD", "RU", "TJ", "TM", "UA", "UZ"],
        "CAZ": ["AU", "CA", "NZ"],
        "CHA": ["CN", "HK", "MO", "TW"],
        "IND": ["IN"],
        "JPN": ["JP"],
        "USA": ["US", "PM"],
        "World": ["GLO", "RoW"]
    }

Note that the IAM region names must be identical to the ones used in the IAM scenario files.
