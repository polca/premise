Exporting results
===================

Choose the format your next analysis step needs. All examples assume you have
created and updated ``ndb`` as in :doc:`/getting_started/first-scenario`.
Review its :doc:`change report </user_guide/reports>` before interpreting results.

.. list-table:: Choose an export
   :header-rows: 1
   :widths: 30 70

   * - Goal
     - Route
   * - Calculate in Brightway
     - :doc:`brightway`: one database for each scenario.
   * - Switch scenarios in Activity Browser
     - :doc:`superstructure`: one database and a scenario-difference file.
   * - Enumerate scenarios in modern Brightway
     - :doc:`scenario-arrays`: one shared database with arrays of changing amounts.
   * - Work directly with inventory coefficients
     - :doc:`matrices`: sparse-matrix coordinates and index files.
   * - Import into SimaPro
     - :doc:`simapro`: a CSV and an export report for each scenario.
   * - Import into openLCA
     - :doc:`openlca`: a JSON-LD ZIP and flow-coverage report for each scenario.
   * - Reconstruct scenarios elsewhere using the same source database
     - :doc:`datapackage`: a package of differences for use with unfold.

SimaPro and openLCA exports use Brightpath, which is installed with Premise.
They convert the inventory to the destination format using its existing
ecoinvent version and system model. They do not migrate it to a different
background database. OpenLCA also needs a matching local LCIA method package
for its elementary-flow definitions.

Read the destination's export report and check the imported flows and
suppliers. Writing a file successfully does not establish that another
application's LCIA methods cover all its flows.

.. toctree::
   :maxdepth: 1
   :titlesonly:

   /user_guide/export/brightway
   /user_guide/export/superstructure
   /user_guide/export/scenario-arrays
   /user_guide/export/matrices
   /user_guide/export/simapro
   /user_guide/export/openlca
   /user_guide/export/datapackage
