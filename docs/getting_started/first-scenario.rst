Build, validate and export your first scenario
================================================

This example builds REMIND SSP2-NPi for 2025 with ecoinvent 3.12 cut-off.
It applies all default updates and writes a separate Brightway database.

Before running
----------------

Complete :doc:`preparation`, including the preflight checks, in the Python
environment where you installed Premise. Keep the environment variables set:
the script uses them for your project, source database, biosphere, output name
and IAM key. Set ``PREMISE_IAM_DIR`` only if your IAM files are outside the
default scenario directory.

This example uses compact inventory storage, selected explicitly in the script.

Complete example
------------------

.. literalinclude:: /examples/first_scenario.py
   :language: python

Save the code as ``first_scenario.py`` (or download
:download:`the script </examples/first_scenario.py>`) and run it from the
configured terminal with ``python first_scenario.py``.

The output name includes ``docs-example`` to distinguish it from the source.
The script refuses to overwrite an existing output database. Rename the output
when repeating a run. Expect a full database build to require substantial
memory and several minutes or more, depending on your computer and cached data.

Understanding the result
--------------------------

The output is an ecoinvent database whose mapped sectors have been updated
for the selected scenario. Unmapped activities remain part of its background.
A successful validation report checks inventory rules; it is not an
independent validation of the IAM or every scientific assumption.

Use :doc:`/user_guide/inventories` to inspect activities without copying
the entire database into Python dictionaries, :doc:`/user_guide/reports` to review changes, and
:doc:`/user_guide/export/index` for other output formats.
