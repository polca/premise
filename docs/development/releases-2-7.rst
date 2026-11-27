Premise 2.7 release and migration guide
=======================================

Version 2.7.0 makes CLIC the standard supplemental inventory pipeline, adds an
optional commercial-license policy, and improves import, validation, cache and
export reliability. Python and Brightway compatibility are unchanged from 2.6.

Version 2.7.1 applies CLIC inventories to consequential builds, migrates bundled
and user-supplied inventories through Brightpath, and raises the minimum
ecoinvent background version to 3.8.

Upgrade existing builds
-----------------------

* Remove ``inventory_source`` from calls to ``NewDatabase`` and
  ``PathwaysDataPackage``. There is no provider selector in 2.7, including for
  callers that previously passed ``inventory_source="clic"``. The inherited
  ``IncrementalDatabase`` constructor follows the same interface.
* In 2.7.1, replace any ecoinvent 3.7 or 3.7.1 background with a supported
  release and update ``source_version``. Keep your system model, IAM inputs and
  additional inventory arguments. Premise selects CLIC inventories and retains
  required workbook-only recipes automatically.
* Rebuild scenario databases and exports. Moving from the 2.6 workbook pipeline
  can change source inventories and results. Updating the package does not
  alter saved databases.
* Review warnings and final validation reports. Certification always runs;
  missing or stale reports are regenerated. Custom code must not depend on the
  removed private ``_validation_enabled`` bypass.

CLIC selection and reproducibility
----------------------------------

Premise resolves required CLIC roots and suppliers from a verified Zenodo
archive, using scenario technologies and the sectors requested by ``update``.
Inventories are prepared before the scenario is transformed. Subsequent sector
updates expand the selection; ``prepare_inventories(sectors)`` can also prepare
it explicitly. User inventories and workbook-only requirements remain in the
hybrid graph.

The default freshness policy checks selected versions against live CLIC
metadata. A first build needs access to the archive and migration resources.
For snapshot or offline work, configure ``clic_options`` explicitly and retain
the required local archive/cache. A missing migration route or unmatched
destination supplier stops preparation instead of silently dropping a purchase.

Newer-source assets can use reviewed native manufacturing alternatives when
targeting older backgrounds. Those substitutions preserve the requested role
and reference output, document technology and geography differences, and are
recorded as target-compatibility replacements. Their quantities and material
boundaries can differ from the published CLIC process.

After updating the required sectors, save a recipe:

.. code-block:: python

   database.update(["electricity", "fuels", "battery", "cars"])
   database.write_inventory_recipe("inventory-recipe.json")

Replay using ``inventory_recipe="inventory-recipe.json"`` with the same source
database, scenario inputs and sectors. Recipes preserve version pins and
replacement fingerprints; changed replacement rules or native inventories
require a reviewed new recipe. CLIC version, migration and substitution details
also appear in scenario reports and their sidecar files.

See :doc:`/reference/inventories`, :doc:`/reference/inventory-migration` and
:doc:`/reference/clic-commercial-mappings` for source selection and mappings.

Commercial-license policy
-------------------------

Commercial screening remains optional:

.. code-block:: python

   database = NewDatabase(
       **build_arguments,
       clic_options={"license_policy": "commercial"},
   )

It screens the entire selected CLIC dependency graph, applies reviewed
alternatives, prunes unused dependencies, and stops on unresolved requirements.
Attribution and ShareAlike conditions remain in the license report. Commercial
recipes restore the policy automatically on replay.

The registry includes native alternatives for PV, batteries, chargers, vehicle
components and specialized materials, plus an independently sourced
methane-pyrolysis reference. Some mappings are low-confidence manufacturing
proxies, not equivalent process models. Review the actual substitution report
and LCIA differences before using results for a comparative assessment.

Reviewed commercial selections were checked for cutoff 3.8, 3.9, 3.9.1, 3.10,
3.10.1, 3.11 and 3.12, and EN15804 3.12. These checks concern the reviewed archive
and available suppliers, not every future CLIC dataset. The policy assesses
declared CLIC permissions; ecoinvent, retained workbooks and user inventories
require separate rights assessments. See :doc:`/reference/clic-commercial`.

Consequential inventory adaptation
----------------------------------

Brightpath migrates published CLIC inventories between cut-off releases.
Premise applies its consequential supplier rules and validates the links against
the supplied consequential database. The inventory manifest records the
adaptations. Commercial CLIC screening covers cut-off and EN15804.

Premise accepts ecoinvent background versions 3.8 through 3.12. To update a
workflow using 3.7 or 3.7.1, use a supported ecoinvent database and set
``source_version`` to its release. Existing workbooks and external
datapackages can still be migrated from their original source versions.

Corrections and compatibility
-----------------------------

Version 2.7.0 also fixes cache-base preservation, metadata-backed compact-store
amounts, equivalent-compartment biosphere filling, IMAGE and GAINS alias
handling, native gas carbon origin, transport validation, metal resource
boundaries, import classifications, duplicate removal and provider-key alignment
in exports. These fixes can affect inventories or diagnostics independently of
the commercial policy. The full ``CHANGELOG.md`` describes their scope.

Modern Brightway supports Python 3.12 through 3.15. Legacy Brightway remains
available through pip on Python 3.12 with NumPy 1.x. The compact and legacy
inventory backends remain supported. For the earlier inventory/API changes,
see :doc:`releases-2-6` and :doc:`releases-2-5`.
