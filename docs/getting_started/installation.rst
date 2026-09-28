Installation and requirements
===============================

The package requires **Python 3.10 or newer**, as declared in
``pyproject.toml``. Use a separate Python environment for each Brightway generation.
Ecoinvent data are supplied by the user, not bundled with *premise*.

Modern Brightway
------------------

Install Premise with the Brightway 2.5 dependencies:

.. code-block:: console

   python -m pip install "premise[bw25]"

The ``bw25`` option installs dependencies for Brightway 2.5, including
``bw2data>=4.3``.

Legacy Brightway
------------------

For workflows requiring Brightway 2, install the legacy extra in a different
Python environment:

.. code-block:: console

   python -m pip install "premise[bw2]"

It pins ``bw2data==3.6.6``. Do not install both extras into one Python environment.
Choose compatibility based on the versions required by the other tools you use.

Data and access
-----------------

Prepare an ecoinvent database and matching biosphere in a Brightway project,
or use ecospold files as described in :doc:`/user_guide/source-databases`.
Encrypted IAM files require an access key passed through ``key=``. Examples
read that key from the ``PREMISE_KEY`` environment variable and pass it to
the constructor. The constructor does not read this variable automatically.

See :doc:`/reference/compatibility` for ecoinvent system-model support and
:doc:`first-scenario` for the complete workflow.
