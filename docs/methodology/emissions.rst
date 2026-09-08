Air-emission adjustments
==========================

.. raw:: html

   <span id="air-emissions"></span>

.. contents:: On this page
   :local:
   :depth: 1

Scope and outputs
-------------------

The ``emissions`` update scales mapped non-CO2 biosphere exchanges using
changes in the packaged GAINS-IAM emissions series. Product supply links and
production exchanges are unchanged by this operation.

.. figure:: /_static/process-diagrams/emissions.svg
   :class: process-diagram
   :alt: Non-CO2 emission adjustments: Map activities, fuels and pollutant flows; Calculate scenario/reference factor; Apply factors strictly between zero and one; Retain the existing supply chain.
   :align: center

Inputs and applicability
--------------------------

The data loader reads the chosen CLE or MFR files under
``premise/data/GAINS_emission_factors/iam_data`` and maps their regions to the
selected IAM. The current update uses this regional GAINS-IAM dataset for
European and non-European activities alike. There is no separate country-first
GAINS-EU pass in this transformation.

The activity/sector mapping and ``GAINS_ei_pollutants.yaml`` select the
activities and elementary flows. Activities without an eligible mapped region
are left unchanged. If ``gains_data_IAM`` is absent, the update is skipped.

Using this update
~~~~~~~~~~~~~~~~~~~

After the setup in :doc:`/getting_started/first-scenario`, call:

.. code-block:: python

   ndb.update("emissions")

``gains_scenario="CLE"`` is the constructor default; choose ``"MFR"`` for the
packaged maximum-feasible-reduction series. These labels describe the selected
GAINS inputs, not the IAM climate pathway.

Transformation
----------------

The loader fills missing annual observations within each series. Premise
constructs a world series by summing regional emissions **before** forming
scenario/reference ratios, rather than averaging regional ratios.

For each pollutant and sector, the reference is 2020 or the nearest available
year. The selected scenario year is interpolated if necessary. A missing or
zero reference denominator is replaced by one for the division; a resulting
missing or zero ratio is then replaced by one.

Only factors strictly between zero and one are applied. Factors at or above
one leave the inventory amount unchanged. The ratio represents a change in
the supplied regional emissions series; it is applied to the source inventory
exchange rather than replacing it with an absolute GAINS emission amount.

Markets and downstream links
------------------------------

This update creates no market and changes no consumer links. Its effect on a
life-cycle result depends on which modified activities the functional unit
uses. Fuel composition and energy-efficiency changes occur in separate sector
operations and can also affect direct emissions.

Assumptions and limitations
-----------------------------

The application is reduction-only, including for years before 2020. A larger
GAINS value does not produce an increased inventory exchange in this step.

Adjustment is recorded per activity and GAINS pollutant. If several biosphere
rows map to the same GAINS species, only the first eligible row is scaled;
subsequent rows for that species are left unchanged. An already recorded
pollutant is not scaled again. Check individual exchanges when interpreting
an activity's total emissions.

.. raw:: html

   <span id="process-context"></span>

Worked example and checks
---------------------------

For one 10 g exchange, a factor of 0.8 gives 8 g, while 1.2 leaves 10 g.
If two eligible rows of 10 g and 2 g map to the same GAINS pollutant, the first
becomes 8 g and the second remains 2 g. Their total is 10 g, not 9.6 g.
Inspect the region, sector, reference value and each biosphere row, as well as
the recorded applied factor.

Sources and inventory details
-------------------------------

The table below shows the mapping between ecoinvent and GAINS emission flows.

+-------------------------------------------------------------------+----------------+
| ecoinvent species                                                 | GAINS species  |
+===================================================================+================+
| Sulfur dioxide                                                    |  SO2           |
+-------------------------------------------------------------------+----------------+
| Sulfur oxides                                                     |  SO2           |
+-------------------------------------------------------------------+----------------+
| Carbon monoxide, fossil                                           |  CO            |
+-------------------------------------------------------------------+----------------+
| Carbon monoxide, non-fossil                                       |  CO            |
+-------------------------------------------------------------------+----------------+
| Carbon monoxide, from soil or biomass stock                       |  CO            |
+-------------------------------------------------------------------+----------------+
| Nitrogen oxides                                                   |  NOx           |
+-------------------------------------------------------------------+----------------+
| Ammonia                                                           |  NH3           |
+-------------------------------------------------------------------+----------------+
| NMVOC, non-methane volatile organic compounds, unspecified origin |  VOC           |
+-------------------------------------------------------------------+----------------+
| VOC, volatile organic compounds, unspecified origin               |  VOC           |
+-------------------------------------------------------------------+----------------+
| Methane                                                           |  CH4           |
+-------------------------------------------------------------------+----------------+
| Methane, fossil                                                   |  CH4           |
+-------------------------------------------------------------------+----------------+
| Methane, non-fossil                                               |  CH4           |
+-------------------------------------------------------------------+----------------+
| Methane, from soil or biomass stock                               |  CH4           |
+-------------------------------------------------------------------+----------------+
| Dinitrogen monoxide                                               |  N2O           |
+-------------------------------------------------------------------+----------------+
| Particulates, > 10 um                                             |  PM10          |
+-------------------------------------------------------------------+----------------+
| Particulates, > 2.5 um, and < 10um                                |  PM25          |
+-------------------------------------------------------------------+----------------+
| Particulates, < 2.5 um                                            |  PM1           |
+-------------------------------------------------------------------+----------------+

.. raw:: html

   <span id="source-provenance-and-currency"></span>

Sources and data dates
~~~~~~~~~~~~~~~~~~~~~~~~

.. include:: /reference/generated/source-emissions.inc
