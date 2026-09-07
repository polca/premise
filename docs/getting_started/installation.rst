Installation and requirements
===============================

The package requires **Python 3.10 or newer**, as declared in
``pyproject.toml``. Use a separate environment for each Brightway generation.
Ecoinvent data are supplied by the user, not bundled with *premise*.

Modern Brightway
------------------

Install the explicitly declared modern Brightway dependency set:

.. code-block:: console

   python -m pip install "premise[bw25]"

This extra requires ``bw2data>=4.3``. The unqualified ``premise`` installation
uses the default dependencies; the explicit extra makes the intended
Brightway generation clear.

Legacy Brightway
------------------

For workflows requiring Brightway 2, install the legacy extra in a different
environment:

.. code-block:: console

   python -m pip install "premise[bw2]"

It pins ``bw2data==3.6.6``. Do not install both extras into one environment.
Choose compatibility based on the versions required by your downstream tools.

Data and access
-----------------

Prepare an ecoinvent database and matching biosphere in a Brightway project,
or use ecospold files as described in :doc:`/user_guide/source-databases`.
Encrypted IAM files require an access key passed through ``key=``. Examples
read that key from the ``PREMISE_KEY`` environment variable; this is an example
convention, not a claim that the constructor reads it automatically.

See :doc:`/reference/compatibility` for ecoinvent system-model support and
:doc:`first-scenario` for the complete workflow.
