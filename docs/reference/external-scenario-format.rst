External-scenario format
==========================

.. contents:: On this page
   :local:
   :depth: 1

This page specifies the files used by :doc:`/user_guide/external-scenarios`.
For a guided authoring workflow, start with
:doc:`/user_guide/create-external-scenario`.

A package needs resources named ``scenario_data`` and ``config``. An
``inventories`` resource is optional when all selected activities already exist
in the source database. Paths are relative to ``datapackage.json``.

datapackage.json
------------------

The descriptor names the package, its sources, its scenario labels and the
ecoinvent version used to author its inventories. This example shows a small
package without an inventory resource. Adapt its names and paths to your files:

.. code-block:: json

    {
        "profile": "data-package",
        "name": "my-scenario-package",
        "title": "My technology projections",
        "description": "Document the projection assumptions and source publications here.",
        "version": "1.0.0",
        "ecoinvent": {"version": "3.12", "system model": "cutoff"},
        "scenarios": {
            "My custom scenario": [
                {"model": "remind", "pathway": "SSP2-NPi"}
            ]
        },
        "resources": [
            {
                "name": "scenario_data",
                "path": "scenario_data/scenario_data.csv",
                "profile": "tabular-data-resource",
                "format": "csv"
            },
            {
                "name": "config",
                "path": "configuration_file/config.yaml",
                "format": "yaml"
            }
        ]
    }

The custom scenario label must match the ``scenario`` column in the data and
the label passed in the build's ``external scenarios`` entry. Model/pathway
associations record which base IAM scenarios the custom projection accompanies.
Include source and contributor attribution in your own package.

Scenario data
---------------

The CSV contains one row per scenario, region and variable, with year values
in separate columns. Required columns are ``scenario``, ``region``,
``variables`` and ``unit``; note the plural spelling ``variables``. Year-column
names must be integers from 2005 to 2100, and required cells must not be empty.
The example also carries model/pathway labels as context.

The table below illustrates an electricity-based hydrogen-efficiency trajectory
for several regions. The later configuration examples illustrate ammonia
production instead; their variable and scenario names must be replaced to
match your own data.


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
is the ``variables`` column, which indicates the variable that the
scenario data is about. The sixth column is the **unit** column,
which indicates the unit of that variable. The columns after that are the
values of the variable across time.

Variables can be production volumes (used to build markets), efficiencies,
or other variables that are needed to modify/adjust inventories.

Inventories
-------------

Inventories can be Brightway-compatible Excel or CSV files. Declare the
``inventories`` resource and its exact path in ``datapackage.json``. For a path
such as ``inventories/lci-example.csv``, that file must exist in the package's
``inventories`` directory. Preserve its product, unit, location and source
version so Premise can identify and migrate its suppliers.

config.yaml
-------------

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
          # indicate some string that should not be contained in the dataset name
          mask: solid
          # indicate whether the dataset exists in the original database
          # or if it should be sourced from the inventories folder
          exists in original database: False
          # indicate whether a region-specific version of the dataset should be created
          regionalize: True
          # indicate if the production volume from the scenario data should be multiplied by a factor
          # to account, for exmaple, for a difference in units relative to the other inputs (e.g., here, cubic meter instead of kilogram)
          ratio: 0.78

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
          # efficiency gains will only apply to technosphere flows whose name
          # contains `electricity`
          technosphere:
            - electricity
        excludes:
            # but not to flows whose name contains `renewable` and `hydro`
            technosphere:
                - renewable
                - hydro

This is essentially the same as above, but it indicates that the
variable **Efficiency|Hydrogen|Alkaline Electrolysis (electricity)** in the scenario
data file should be mapped with the **efficiency** of the dataset
**hydrogen production, alkaline electrolysis** in the LCA inventories.

The **includes** parameter indicates that the efficiency gains will only
apply to flows of type *technosphere* whose name contains **electricity**.
In practice, this will reduce the input of electricity over time for that dataset.
If you do not specify **includes**, then the efficiency gains will apply to all
flows (of type *technosphere* and *biosphere*).

When a production pathway applies **efficiency** changes to an existing
inventory, **premise** creates a dedicated provider copy before applying those
changes. The custom markets and exported demand mappings use that copy. Other
consumers retain the original provider and its background-scenario treatment.
Separate pathways receive independent copies, including when they start from the
same inventory. Production allocation metadata and source identity are retained.
Explicit **replaces** and **replaces in** instructions still control intentional
supplier replacement; copying does not disable those instructions. A direct
**regionalize** entry for the same inventory retains its existing precedence over
pathway transformation settings.

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
    # but only in German datasets
    replaces in:
      - location: DE

    # indicates that the market is a fuel market and emissions of activities
    # using this market as a supplier should be adjusted
    is fuel:
      petrol:
        Carbon dioxide, fossil: 3.15
        Carbon dioxide, non-fossil: 0.0
      bioethanol:
        Carbon dioxide, fossil: 0.0
        Carbon dioxide, non-fossil: 3.15

    # we also want to manually add some emissions to the market
    add:
      - name: market for electricity, low voltage
        reference product: electricity, low voltage
        amount: 0.0067

    # If true, flip signs
    waste market: False

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

The **is fuel** parameter is optional. It indicates that the market is a fuel market.
The **petrol** and **bioethanol** parameters indicate the emissions associated with
the production of petrol and bioethanol, respectively. The emissions are in kg CO2 per kg of fuel.
Indicating this will adjust the indicated flows in any activity that uses the market as a supplier.

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

In this example, the ammonia supplier will be replaced in all
activities whose reference product contains the string **urea**
and location in **DE**.


Explicit bounds for external absolute efficiencies
----------------------------------------------------

An external production pathway may declare optional limits alongside an
absolute efficiency variable::

    efficiency:
      - variable: Example electrical efficiency
        absolute: true
        bounds:
          min: 0.05
          max: 0.30
        # Optional diagnostic; key of another configured production pathway:
        heat pathway: example heat output

``bounds`` uses the same units as the supplied target (for an electrical
fuel-conversion efficiency, fractions rather than percentages). Both limits
must be finite and strictly positive, with ``min <= max``. Premise clips the
target first and then computes ``current_efficiency / applied_target``.
There are no implicit limits and no technology identification: the datapackage
author chooses the eligible pathways, their physical basis and their bounds.
The mechanism is restricted to absolute external-scenario targets. Built-in
IAM transformations and relative efficiency changes are unchanged.

Omitting ``bounds`` applies the supplied target without clipping. A zero target
retains the existing missing/inactive-target behaviour and does not become the
lower bound. Negative or nonfinite targets are rejected. Source scenario values
remain unchanged. The activity's ``efficiency bounds audit`` records requested
and applied targets, limits, clipping status and an optional combined-output
check. The ordinary transformation log records the applied efficiency.

Neither electricity nor heat production data are needed for clipping. The
optional ``heat pathway`` must name another configured production pathway.
When both output series are available with convertible annual energy units,
Premise reports ``electrical_efficiency * (1 + heat/electricity)`` for the raw
and bounded targets. Missing values or incompatible units produce a
``not_available`` diagnostic and do not prevent electrical-efficiency clipping.
This ratio is inferred from the declared electrical-efficiency basis; it does
not independently verify fuel consumption. Values above 100% are reported,
not automatically corrected, because the heating-value basis must be known.

If you are updating an older package, declare the intended bounds explicitly;
do not rely on the historical implicit scaling limits.
