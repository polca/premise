Ammonia
=========

.. raw:: html

   <span id="source-inventories-ammonia"></span>

.. contents:: On this page
   :local:
   :depth: 1

Scope and outputs
-------------------

Ammonia inventories provide production routes per kilogram of ammonia.
The fuels update regionalizes the mapped market-average-hydrogen ammonia
activity. There is no separate ``ammonia`` update or dedicated IAM ammonia
market mapping in the current fuels configuration.

.. figure:: /_static/process-diagrams/fuels-ammonia.svg
   :class: process-diagram
   :alt: Ammonia within the fuels update: Import ammonia route inventories; Select the market-average-hydrogen route; Create regional variants and relink; Supply activities that request this route.
   :align: center

Inputs and applicability
--------------------------

Production routes are imported during initialization according to the
inventory-selection rules. The supporting-activity filter in
``premise/data/fuels/liquid_fuel_activities.yml`` selects
``ammonia production, with market-average hydrogen``. It is processed within
``ndb.update("fuels")`` when that update's blend-data condition is met.

Transformation
----------------

Premise creates regional variants of the selected supporting activity and
relinks available upstream suppliers. This operation has no ammonia-specific
efficiency adjustment or ammonia-demand series. Other imported route variants
remain available inventories; listing them does not imply automatic scenario
selection or a changing ammonia technology mix.

Markets and downstream links
------------------------------

The market-average-hydrogen route can draw on the hydrogen supply prepared
by the fuels update. Whether ammonia affects a study depends on downstream
exchanges reaching it. The presence of an ammonia route in the database does
not demonstrate that fertilizer consumers have all been redirected to it.

Assumptions and limitations
-----------------------------

Retain the source Haber-Bosch/conversion coefficients unless a documented
transformation changes them. Hydrogen supply can become prospective without
an IAM-driven ammonia mix. Allocation and capture boundaries differ across
source routes and should be checked before comparing their scores.

Worked example and checks
---------------------------

Select a regionalized market-average-hydrogen activity and compare its
hydrogen input amount and supplier with the source activity. If only the
supplier changes, describe the difference as an upstream supply change.
Then inspect an actual ammonia-consuming activity to establish whether it
uses this route; do not infer deployment from the catalogue.

Sources and inventory details
-------------------------------

Process context
~~~~~~~~~~~~~~~~~

*premise* imports inventories for ammonia production using the following routes:

* steam methane reforming (Haber-Bosch)
* steam methane reforming (Haber-Bosch) with CCS of syngas
* steam methane reforming (Haber-Bosch) with CCS of syngas and flue gas
* partial oxidation of oil
* hydrogen from coal gasification
* hydrogen from coal gasification with CCS
* hydrogen from electrolysis
* hydrogen from natural gas pyrolysis

These inventories are published in `Boyce <https://doi.org/10.1016/j.heliyon.2024.e27547>`__ et al., 2024,
and are largely based on Carlo d' `Angelo <https://doi.org/10.1021/acssuschemeng.1c01915>`__ et al., 2021.

The supply of hydrogen in the ammonia production process
(coal gasification, electrolysis, etc.) is represented by the
hydrogen inventories described in :doc:`hydrogen`.

.. raw:: html

   <span id="source-provenance-and-currency"></span>

Sources and data dates
~~~~~~~~~~~~~~~~~~~~~~~~

.. include:: /reference/generated/source-ammonia.inc
