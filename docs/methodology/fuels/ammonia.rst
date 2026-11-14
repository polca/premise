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
`ammonia production, with market-average hydrogen <https://clic.psi.ch/datasets/ammonia-production-with-market-average-hydrogen/>`__. It is processed within
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

The market-average-hydrogen route uses the regional IAM hydrogen market
prepared by the fuels update, when available. A hydrogen-specific step maps
native liquid, generic gaseous, and medium-pressure merchant market identities
to the generated gaseous low-pressure market before geographic relinking.
This adopts its documented 30-bar pipeline-delivery boundary, retaining the
source hydrogen quantity and existing process-energy inputs. It is an explicit
scenario supply assumption, not a claim that the native delivery boundaries
are physically equivalent. The source inventory is unchanged.

Whether ammonia affects a study depends on downstream
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

Suppose an illustrative activity supplies 1 kg of ammonia and purchases
0.2 kg of hydrogen. Regionalization connects its hydrogen input to an
available regional IAM hydrogen market. For this example, hold the other
input amounts fixed.

.. list-table:: A supplier change with the same hydrogen quantity
   :header-rows: 1
   :widths: 40 30 30

   * - Field
     - Before
     - After
   * - Ammonia output
     - 1 kg
     - 1 kg
   * - Hydrogen purchase
     - 0.2 kg
     - 0.2 kg
   * - Hydrogen supplier
     - Source database supplier
     - Regional scenario hydrogen market

The unchanged 0.2 kg purchase means this example has no direct improvement
in ammonia conversion efficiency. The hydrogen supply chain can still have
different inputs and impacts. Other available energy suppliers can also be
relinked during a build.

Inspect the hydrogen amount and supplier in the regionalized activity, then
check a consuming activity. An imported ammonia route affects that consumer
only if an exchange actually reaches it.

Sources and inventory details
-------------------------------

Process context
~~~~~~~~~~~~~~~~~

*premise* imports inventories for ammonia production using the following routes:

* `steam methane reforming (Haber-Bosch) <https://clic.psi.ch/datasets/ammonia-production-steam-reforming/>`__
* `steam methane reforming (Haber-Bosch) with CCS of syngas <https://clic.psi.ch/datasets/ammonia-production-steam-reforming-with-syngas-ccs/>`__
* `steam methane reforming (Haber-Bosch) with CCS of syngas and flue gas <https://clic.psi.ch/datasets/ammonia-production-steam-reforming-with-syngas-and-flue-gas-ccs/>`__
* `partial oxidation of oil <https://clic.psi.ch/datasets/ammonia-production-partial-oxidation-of-oil/>`__
* `hydrogen from coal gasification <https://clic.psi.ch/datasets/ammonia-production-hydrogen-from-coal-gasification/>`__
* `hydrogen from coal gasification with CCS <https://clic.psi.ch/datasets/ammonia-production-hydrogen-from-coal-gasification-with-ccs/>`__
* `hydrogen from electrolysis <https://clic.psi.ch/datasets/ammonia-production-hydrogen-from-electrolysis/>`__
* `hydrogen from natural gas pyrolysis <https://clic.psi.ch/datasets/ammonia-production-hydrogen-from-methane-pyrolysis/>`__

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
