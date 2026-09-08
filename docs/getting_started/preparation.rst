Prepare and check your inputs
===============================

This walkthrough uses an existing ecoinvent 3.12 cut-off import in modern
Brightway. Obtain ecoinvent separately and retain its matching biosphere.
For the standard encrypted IAM scenarios, request the key from
`the Premise developers <mailto:romain.sacchi@psi.ch>`_. The key gives access
to all standard scenarios in the Premise catalogue.

Identify your local names
---------------------------

A **Python environment** (for example, a conda environment) contains installed
packages. A **Brightway project** contains databases, including ecoinvent and
its biosphere database.

With your Python environment activated, list the available Brightway projects,
select the project containing your ecoinvent database, and list its databases:

.. code-block:: python

   import bw2data as bd
   print([project.name for project in bd.projects])
   # Replace this with one of the names just printed.
   bd.projects.set_current("ecoinvent-3.12-cutoff")
   print(list(bd.databases))

Choose an existing Brightway project. Brightway can create a new empty project when
given an unfamiliar name. Database names are local identifiers: copying a
name from an example does not import ecoinvent.

Set the configuration
-----------------------

In a macOS/Linux terminal, before starting Python:

.. code-block:: console

   export PREMISE_BW_PROJECT="ecoinvent-3.12-cutoff"
   export PREMISE_SOURCE_DB="ecoinvent-3.12-cutoff"
   export PREMISE_BIOSPHERE="ecoinvent-3.12-biosphere"
   export PREMISE_OUTPUT_DB="docs-example-remind-SSP2-NPi-2025"
   read -rs PREMISE_KEY
   export PREMISE_KEY

Paste the key when ``read`` waits for input, then press Enter. It is not
displayed. For PowerShell, use:

.. code-block:: powershell

   $env:PREMISE_BW_PROJECT = "ecoinvent-3.12-cutoff"
   $env:PREMISE_SOURCE_DB = "ecoinvent-3.12-cutoff"
   $env:PREMISE_BIOSPHERE = "ecoinvent-3.12-biosphere"
   $env:PREMISE_OUTPUT_DB = "docs-example-remind-SSP2-NPi-2025"
   $secret = Read-Host "IAM key" -AsSecureString
   $env:PREMISE_KEY = [System.Net.NetworkCredential]::new("", $secret).Password

These variables configure the examples; the constructor does not automatically
read them. Optionally set ``PREMISE_IAM_DIR`` to the directory containing your
scenario files. Restart notebook kernels from this terminal to inherit its
environment variables, or set those variables in the notebook's launch configuration.

Run the preflight
-------------------

Download :download:`preflight.py </examples/preflight.py>` and run
``python preflight.py``. The script checks names and required configuration,
restores the previously selected project and writes no inventory database.
It does not prove that the key decrypts the selected scenario or that every
supplier can be linked.

.. literalinclude:: /examples/preflight.py
   :language: python

Successful output includes ``Names and configuration checked`` and
``No database built``. Continue to :doc:`first-scenario`. If a check fails,
correct the named setting before starting the full build.
