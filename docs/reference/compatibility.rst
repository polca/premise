Supported configurations
==========================

Use a Python and Brightway combination described in
:doc:`/getting_started/installation`. Your ecoinvent source database, its
system model and the ``source_version`` setting must agree.

Compatibility boundaries
--------------------------

The lists below are generated from this version of Premise. A model
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

Dependency constraints
----------------------

The ``bw25`` option installs the modern Brightway dependencies. NumPy 2 and
current SciPy, xarray and PyArrow releases are supported. pandas stays below
3.1 until the country-conversion dependency supports its changed CSV parsing;
DeepDiff stays below 9 while its native dependency lacks Python 3.15 support.
On Python 3.12, PyArrow stays below 26 so existing NumPy 1.x environments remain
usable. Newer Python versions use NumPy 2 and can install PyArrow 26. The Conda
package retains PyArrow below 26, including its builds for newer Python
versions, to keep compatibility with NumPy 1.x on Python 3.12.

Flexible priority lets Conda select a compatible ``pyecospold`` build from
``cmutel``. The conda-forge build currently pins an older lxml release that
blocks Python versions above 3.12.
