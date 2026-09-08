Supported configurations
==========================

*premise* currently works with the following ecoinvent database versions:

* **v.3.5, cut-off**
* **v.3.6, cut-off**
* **v.3.7 and v.3.7.1, cut-off**
* **v.3.8, cut-off and consequential**
* **v.3.9 and v.3.9.1, cut-off and consequential**
* **v.3.10 and v.3.10.1, cut-off and consequential**
* **v.3.11, cut-off and consequential**
* **v.3.12, cut-off and consequential**

Compatibility boundaries
--------------------------

The following lists are generated from the checked-out configuration. A model
identifier being accepted does not establish complete sector coverage or
availability of a particular scenario file; see :doc:`coverage`.

.. include:: generated/supported.inc

Selected constructor defaults
-------------------------------

These values are generated from the constructor signature. The first-scenario
example uses the compact backend.

.. include:: generated/defaults.inc

Python and Brightway requirements are maintained in
:doc:`/getting_started/installation`. Compatibility with an ecoinvent release
does not imply identical additional inventories across system models; consult
:doc:`inventories` and the relevant sector page.

The package does not contain the ecoinvent source database. Database and
biosphere identifiers must match your local import.
