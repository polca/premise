Exporting results
===================

Choose the output format required by your analysis software.

SimaPro and openLCA exports use `Brightpath
<https://github.com/romainsacchi/brightpath>`_, a library for converting LCA
inventories between software formats. Premise builds, links, and validates the
scenario inventory; Brightpath writes its processes and exchanges in the target
format. This export step uses the scenario's existing ecoinvent version and
system model. It does not run a background-database migration.

.. list-table:: SimaPro and openLCA export routes
   :header-rows: 1
   :widths: 20 45 35

   * - Destination
     - Premise command
     - Output
   * - SimaPro
     - ``write_db_to_simapro()``
     - Brightpath CSV and export report
   * - openLCA
     - ``write_db_to_olca(method_package=...)``
     - Brightpath JSON-LD ZIP and biosphere coverage report

Both Brightpath routes require Python 3.12 or newer. Brightpath
is a required dependency installed with Premise, including both Conda variants.
Until a compatible registry release is available, Python installations use
the pinned GitHub revision ``5c0cf00940556d5f379941df636276148db594c9``
and require Git. Conda builds package that same revision as a separate
Brightpath dependency. It provides the SimaPro classification, folder and
full-inventory unit mappings, and the versioned openLCA method-mapping API.

Export checks report missing Python or library support before preparing
scenarios. SimaPro CSV and openLCA JSON-LD exports both use Brightpath exclusively.

Each export writes files for you to import into the destination application.
The openLCA route additionally needs a matching local LCIA method package to
identify elementary flows. Read the accompanying export report before comparing
impact results across applications.

.. toctree::
   :maxdepth: 2

   /user_guide/export/brightway
   /user_guide/export/superstructure
   /user_guide/export/scenario-arrays
   /user_guide/export/matrices
   /user_guide/export/simapro
   /user_guide/export/openlca
   /user_guide/export/datapackage
