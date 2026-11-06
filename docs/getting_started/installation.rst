Installation and requirements
===============================

Use **Python 3.12 or newer** with modern Brightway. The compatibility matrix
covers Python 3.12, 3.13, 3.14 and 3.15. Legacy Brightway requires Python 3.12
and NumPy 1.x. Use a separate Python environment for each Brightway generation.
Ecoinvent data are supplied by the user, not bundled with *premise*.

Modern Brightway
------------------

Install Premise with the Brightway 2.5 dependencies:

.. code-block:: console

   python -m pip install "premise[bw25]"

The ``bw25`` option installs the modern Brightway dependencies. NumPy 2 and
current SciPy, xarray and PyArrow releases are supported. pandas stays below
3.1 until the country-conversion dependency supports its changed CSV parsing;
DeepDiff stays below 9 while its native dependency lacks Python 3.15 support.
On Python 3.12, PyArrow stays below 26 so existing NumPy 1.x environments remain
usable. Newer Python versions use NumPy 2 and can install PyArrow 26. The Conda
package retains PyArrow below 26, including its builds for newer Python
versions, to keep compatibility with NumPy 1.x on Python 3.12.

Conda packages support Python 3.12–3.14. Use pip for Python 3.15 until upstream
Conda datapackage dependencies stop requiring the unavailable ``cchardet``
build. Use the required channels with flexible priority in a separate
environment:

.. code-block:: console

   conda create -n premise python=3.14
   conda activate premise
   conda config --env --set channel_priority flexible
   conda install -c conda-forge -c cmutel -c romainsacchi premise-bw25

Flexible priority lets Conda select a compatible ``pyecospold`` build from
``cmutel``. The conda-forge build currently pins an older lxml release that
blocks Python versions above 3.12.

Legacy Brightway
------------------

For workflows requiring Brightway 2, install the legacy extra in a separate
**Python 3.12** environment:

.. code-block:: console

   python -m pip install "premise[bw2]"

It pins ``bw2data==3.6.6``, NumPy below 2 and SciPy below 1.14 because this
backend uses APIs removed from newer numerical libraries. These legacy-only
constraints do not apply to modern Brightway. Do not install both extras into
one Python environment. Legacy support is provided through pip; the
``premise-bw2`` Conda variant is no longer generated because its calculator
has no Python 3.12 Conda build.

Optional companion tools
--------------------------

``unfold`` and ``premise_gwp`` are separate tools, installed only when needed.
Premise can generate scenarios and export datapackages without either package.
Their current releases require NumPy below 2; install them in a separate Python
3.12 environment to consume exported datapackages or install their LCIA methods.
Check each tool's requirements before adding it to an existing environment.

Data and access
-----------------

Prepare an ecoinvent database and matching biosphere in a Brightway project,
or use ecospold files as described in :doc:`/user_guide/source-databases`.
Encrypted IAM files require an access key passed through ``key=``. Examples
read that key from the ``PREMISE_KEY`` environment variable and pass it to
the constructor. The constructor does not read this variable automatically.

See :doc:`/reference/compatibility` for ecoinvent system-model support and
:doc:`first-scenario` for the complete workflow.
