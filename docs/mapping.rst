MAPPING
=======

Link to a new IAM model
-----------------------

While `premise` comes with a set of scenarios from the REMIND and IMAGE IAM models,
it is possible to link it to a new IAM model.
To do so, one needs to populate the .yaml mapping files under the folder `premise/iam_variables_mapping`.

For each variable in in each of the .yaml file, the user needs to specify the corresponding IAM variable name, like so:

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

If efficiency-related variables are not available, the corresponding technologies will
simply not have their efficiency adjusted.

You also need to add your model name to the models list
as well as the list of geographical regions as `LIST_xxx_REGIONS`,
with xxx being your model name, in the file
`iam_variables_mapping/constants.yaml`.

Finally, you need to inform `premise` about the geographical definitions
of the IAM model you are using. These definitions are already stored for REMIND
and IMAGE, but not for new IAM models. To do so, you need to create a .json file
which lists ISO 3166-1 alpha-2 country codes and their corresponding IAM regions. See example below.

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

then, you need to add to the `constants.yaml` file the path to this file:

.. code-block:: yaml
    ...
    EXTRA_TOPOLOGY:
      my_iam_name: filepath/to/the/geographical/definitions.json

On the longer-term, we can integrate this information in `premise`
so that it is not necessary to provide it manually.