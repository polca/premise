Premise 2.6 release and migration guide
=======================================

Version 2.6.0 adds physical inventory auditing and EN15804 support, corrects
combustion-carbon classification and resource attribution, and incorporates
reviewed manufacturing and reference-unit corrections. Python modules are
grouped into topic packages, and bundled inventory selection uses shared
import profiles instead of component-specific patch modules.

Upgrading to 2.6.0
------------------

* Regenerate scenario databases and exports. Updating the package does not
  modify previously written inventories. Fuel-carbon classification, resource
  flows and manufacturing corrections can change results.
* Retain the ecoinvent source version, system model, IAM files and generation
  settings with each study. Compare rebuilt results against a documented
  reference and review differences in inventory boundaries and quantities.
* Modern Brightway supports Python 3.12 through 3.15 with NumPy 2 and current
  SciPy, pandas, xarray and PyArrow. Legacy Brightway remains on Python 3.12
  with NumPy 1.x. The unused companion tools ``unfold`` and ``premise_gwp``
  are installed separately, so their NumPy caps do not constrain Premise.
  Brightpath 1.0.0a14 or newer is required for correct SimaPro normal
  uncertainty export. See
  :doc:`/getting_started/installation` for environment setup.
* The supported ``from premise import NewDatabase`` entry point and inventory
  store API are unchanged. Existing module aliases and cached inventory class
  paths remain supported. New implementation imports use topic packages such
  as ``premise.transport`` and ``premise.inventory``; see :doc:`architecture`.
* Keep scientific assumptions separate from implementation checks. The
  reviewed references remain qualified models with the limitations documented
  in the sector methodology and :doc:`scientific-update-backlog`.

Fuel carbon and resource attribution
------------------------------------

Conventional kerosene and the natural-gas/CCS kerosene route retain fossil
combustion CO2. Previously, mismatched IAM variable names could incorrectly
classify aviation exhaust as non-fossil. Fuel quantities and total emitted CO2
are preserved by the classification correction.

Rail exhaust now follows the final regional fuel supplier after linking and
efficiency scaling. The recorded positive exhaust total is retained; this
correction does not impose a different combustion-completeness assumption.
See :doc:`/methodology/fuels/markets` and :doc:`/methodology/transport/rail`.

The economic-allocation iridium proxy includes its missing in-ground resource
flow at one kilogram per kilogram of supplied iridium. This corrects resource
attribution without assuming additional recovery losses. Other reviewed metals
changes distinguish primary extraction boundaries from intermediate processing
and avoid counting the same resource twice. See :doc:`/methodology/metals`.

Reviewed inventory references
-----------------------------

Battery, electrolysis, transport, fuel, capture and power inventories include
documented corrections to quantities, suppliers and reference-unit boundaries.
Examples include distinct all-solid-state battery chemistries, complete NMC955
pack supply, vanadium-flow electrolyte and capacity accounting, matched PEM
catalyst designs, SOEC ceramic composition and electric-car mechanical inputs.
The Canadian cement-plant refractory correction restores the per-plant basis.

These changes retain explicit source qualifications and unresolved design,
performance and process-coverage gaps. Consult :doc:`/methodology/batteries`,
:doc:`/methodology/fuels/hydrogen`,
:doc:`/methodology/transport/passenger-cars` and :doc:`/methodology/cement`,
and the full ``CHANGELOG.md`` for the affected inventories.

Source-specific native references are selected by the existing import pipeline
using declared version and system-model boundaries. Recipe and supplier checks
run before mutation; unrelated inputs and uncertainty are preserved. Older
source profiles remain available for targets where modern recipes cannot be
reliably migrated backwards. Applicable profile data participate in cache
invalidation.

EN15804 system model
--------------------

Select ``system_model="EN15804"`` through the existing ``NewDatabase`` argument,
using the corresponding EN15804 source database. The system-model label is
retained through import, transformation and export. Native scrap suppliers and
their signed reference outputs are preserved. See
:doc:`/methodology/system-models` for scope and allocation conventions.

Physical inventory auditing
---------------------------

The reusable ``premise.audit`` API and ``python -m premise.audit`` command inspect
inventory stores and written Brightway databases. Sampling combines random,
stratified and risk-based selections. The indicator panel includes physical
CO2, water and land flows and 65 RELICS resource indicators. Checks cover
carbon origin, missing resources, quantities, supplier boundaries and numerical
consistency; reports include evidence and matched scenario comparisons.

Audits do not change inventories. A numerical agreement or absence of flags
does not establish scientific correctness. Review findings alongside source
evidence and explicit physical contracts. Generated scores and licensed
regression references stay local. See :doc:`/user_guide/physical-audits` for API,
command-line and optional licensed-database pytest usage.

Tests and reference data
------------------------

Inventory regression tests inspect the current packaged workbooks against
explicit scientific quantities, formulas and uncertainty. Historical workbook
substitution, campaign receipts and one-time patching programs are removed.
Scientific references and canonical supplier contracts remain in ``tests/data``;
reusable reference builders remain in ``tests/support``. The RELICS indicator
definitions ship with their license; generated audit results do not ship.

For earlier inventory-API and exporter migrations, see :doc:`releases-2-5`.

Licensed integration runs use ``PREMISE_LCIA_REFERENCE_SCORES`` to locate private
approved numerical references. When these are unavailable, the tests issue an
explicit warning and still run their LCIA smoke calculations using the matching
source-version method. They do not substitute generated scores as expected
values. Release certification with private references checks the numerical
regressions separately from public CI's finite-result smoke coverage.
