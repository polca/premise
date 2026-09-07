Build, validate and export your first scenario
================================================

This example builds REMIND SSP2-NPi for 2025 with ecoinvent 3.12 cut-off.
It applies all default updates and writes a separate Brightway database.

Before running
----------------

Complete :doc:`preparation` first, including the preflight checks.
This is an executable example requiring licensed inputs. It explicitly uses
``inventory_backend="compact"``; the constructor default remains ``legacy``.

Install :doc:`installation`. Prepare a Brightway project containing the
source database and its biosphere. Obtain the encrypted IAM file and its key.
Set ``PREMISE_KEY`` in your environment without storing it in the script.
The project, source database and biosphere names below must match your local
installation. Set ``PREMISE_IAM_DIR`` if the scenario file is stored outside
the default scenario directory.

Complete example
------------------

.. literalinclude:: /examples/first_scenario.py
   :language: python

The output name includes ``docs-example`` to distinguish it from the source.
The script refuses to overwrite an existing output database. Rename the output
when repeating a run. Expect a full database build to require substantial
memory and several minutes or more, depending on your environment and caches.

Understanding the result
--------------------------

The output is an ecoinvent database whose mapped sectors have been updated
for the selected scenario. Unmapped activities remain part of its background.
A successful validation report checks implemented contracts; it is not an
independent validation of the IAM or every scientific assumption.

Use :doc:`/user_guide/inventories` to inspect activities without materializing
the entire database, :doc:`/user_guide/reports` to review changes, and
:doc:`/user_guide/export/index` for other output formats.
