Biomass
=========


.. contents:: On this page
   :local:
   :depth: 1

.. raw:: html

   <span id="key-outputs"></span>

Scope and outputs
-------------------

The ``biomass`` update regionalizes mapped biomass supplies and creates
``market for lignocellulosic biomass, used as fuel`` activities. Electricity, heat and fuels
consume those supplies through their own transformations.

.. figure:: /_static/process-diagrams/biomass.svg
   :class: process-diagram
   :alt: Biomass supply: Regionalize biomass activities; Build lignocellulosic biomass markets; Replace eligible wood inputs; Relink suppliers and check.
   :align: center

Inputs and applicability
--------------------------

The update is skipped when ``biomass_mix`` is absent. The regional market
reference product is ``lignocellulosic biomass``, in kilograms.

The source database and supplementary residue inventories provide suppliers.
Mapped scenario production distinguishes purpose-grown and residual biomass.
The regional composition follows those inputs, with the system-model
restriction below. Run ``ndb.update("biomass")`` before dependent sectors.

Using this update
~~~~~~~~~~~~~~~~~~~

After the setup in :doc:`/getting_started/first-scenario`:

.. code-block:: python

   ndb.update("biomass")

Transformation
----------------

Premise creates regional supplier proxies and combines the mapped biomass
categories. Supplier identity and geography come from the activity mappings
and available inventories; a regional proxy does not establish local forestry
management or residue availability.

Markets and downstream links
------------------------------

Replacement targets kilogram inputs from wood-chip and wood-pellet markets
in matched electricity, heat, power, hydrogen, biomethane, ethanol and
synthetic-gas activities. Log-, residual- and oil-related activity names are
excluded. The exchange is redirected only when the regional biomass market
is available; its amount is retained.

Regional biomass markets
~~~~~~~~~~~~~~~~~~~~~~~~~~

*premise* creates regional markets for biomass which is meant to be used as fuel
in biomass-fired powerplants or heat generators. Originally in ecoinvent, the biomass being supplied
to biomass-fired powerplants is "purpose grown" biomass that originate forestry
activities (called "market for wood chips" in ecoinvent). While this type of biomass
is suitable for such purpose, it is considered a co-product of the forestry activity,
and bears a share of the environmental burden of the process it originates from (notably
the land footprint, emissions, potential use of chemicals, etc.).

However, not all the biomass projected to be used in IAM scenarios is "purpose grown".
In fact, significant shares are expected to originate from forestry residues. In such
cases, the environmental burden of the forestry activity is entirely allocated to the
determining product (e.g., timber), not to the residue, which comes "free of burden".

Here, "free of burden" refers to the excluded upstream forestry burdens in
this inventory convention. It does not mean zero collection/transport impacts
or automatic climate neutrality. Counterfactual residue use, decay and carbon
timing are separate assumptions; see :doc:`/user_guide/interpreting-results`.

Premise creates average regional markets for biomass, which represents the
average shares of "purpose grown" and "residual" biomass being fed to biomass-fired powerplants.

The following market is created for each IAM region:

``market for lignocellulosic biomass, used as fuel`` supplies 1 kg of
``lignocellulosic biomass`` per kilogram of market output.

inside of which, the shares of "purpose grown" and "residual" biomass
is represented by the following activities:

* market for wood chips (for "purpose grown" biomass)
* market for wood chips (for "purpose grown" woody biomass)
* supply of forest residue (for "residual" biomass)

The sum of those shares equal 1. The activity "supply of forest residue" includes
the energy, embodied biogenic CO2, transport and associated emissions to chip the residual biomass
and transport it to the powerplant, but no other forestry-related burden is included.

.. note::

    You can check the share of residual biomass used for power generation
    assumed in your scenarios by generating a scenario summary report.

.. note::

    When running *premise* with the consequential method, the biomass market
    is only composed of purpose-grown biomass. This is because the residual biomass
    cannot be considered a marginal supplier for an increase in demand for biomass.


.. code-block:: python

    ndb.generate_scenario_report()

Assumptions and limitations
-----------------------------

The residue burden convention excludes upstream forestry burdens but retains
specified collection, processing and transport. It is not a demonstration of
carbon neutrality. Consequential biomass markets exclude residual biomass
under the configured marginal-supplier assumption.

Worked example and checks
---------------------------

For an illustrative average mix with 0.7 purpose-grown and 0.3 residual
supply, check that the category shares sum to one and inspect each route's
forestry and processing inputs. Compare the consequential supplier selection
separately. Check that an updated biomass-fired consumer actually reaches the
regional biomass market.

Sources and inventory details
-------------------------------

.. raw:: html

   <span id="source-provenance-and-currency"></span>

Sources and data dates
~~~~~~~~~~~~~~~~~~~~~~~~

.. include:: /reference/generated/source-biomass.inc
