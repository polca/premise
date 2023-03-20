MAPPING
=======

Link to a new IAM model
-----------------------

Although *premise* comes with a set of scenarios from the REMIND
and IMAGE IAM models, it is possible to link it to a new IAM model.
To do so, you need to populate the .yaml mapping files under the
folder https://github.com/polca/premise/tree/master/premise/iam_variables_mapping.

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

Additionally, add your model name to the models list as well as
the list of geographical regions as LIST_xxx_REGIONS, with xxx
being the IAM model name, in the file iam_variables_mapping/constants.yaml.

Lastly, inform premise about the geographical definitions of
the IAM model you are using. These definitions are already
stored for REMIND and IMAGE, but not for new IAM models.
Create a .json file listing ISO 3166-1 alpha-2 country codes
and their corresponding IAM regions, as shown below.

.. code-block:: json
    {
        ...
        "REF": ["AM", "AZ", "BY", "GE", "KZ", "KG", "MD", "RU", "TJ", "TM", "UA", "UZ"],
        "CAZ": ["AU", "CA", "NZ"],
        "CHA": ["CN", "HK", "MO", "TW"],
        "IND": ["IN"],
        "JPN": ["JP"],
        "USA": ["US", "PM"],
        "World": ["GLO", "RoW"]
    }

Then, you need to add to the `constants.yaml` file the path
to this file:

.. code-block:: yaml

    EXTRA_TOPOLOGY:
      my_iam_name: filepath/to/the/geographical/definitions.json

In the long term, this information can be integrated
into premise so that it is not necessary to provide it manually.