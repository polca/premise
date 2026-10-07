Exporting to SimaPro
===================

``write_db_to_simapro`` uses `Brightpath
<https://github.com/romainsacchi/brightpath>`_ to write one SimaPro CSV per
scenario. Install Brightpath in the same Python 3.12+ environment as Premise,
as described in :doc:`index`.

Write and import a scenario
--------------------------

After creating and updating ``ndb`` (see :doc:`/getting_started/first-scenario`):

.. code-block:: python

    files = ndb.write_db_to_simapro(filepath="export/simapro")

``files`` contains the paths to the generated CSVs. Omitting ``filepath`` writes
into ``export/simapro`` relative to the current working directory.

Each scenario produces:

- ``simapro_export_<model>_<pathway>_<year>.csv``: processes, products,
  technosphere exchanges, and supported elementary flows.
- ``simapro_export_<model>_<pathway>_<year>.export-report.json``: classification
  decisions requiring a fallback, excluded exchanges, and Brightpath diagnostics.

Import the CSV using SimaPro's CSV import facility, then check process linking
and elementary-flow matching in the destination database. The file contains
the complete scenario inventory; it does not install LCIA methods. Methods and
their elementary-flow definitions must be available in SimaPro to calculate
impacts. A complete REMIND SSP1-PkBudg1000 2050 scenario from ecoinvent 3.12
cut-off was imported into SimaPro without errors or warnings. This verifies
import structure for that scenario; SimaPro LCIA equivalence remains unverified.

How the export works
--------------------

Premise runs its scenario certification and export preparation before handing a
detached copy to Brightpath. It verifies that each technosphere input resolves
to one process in the exported scenario. Brightpath writes the CSV using the
existing ecoinvent version and system model, with SimaPro names, units, exchange
sections, and uncertainty conventions. Missing or ambiguous providers, invalid
reference outputs, and serialization errors stop the export before replacing
an existing CSV. Premise's automatic reports still run after a successful export.

The writer preserves non-unit reference quantities and numeric precision. Waste
treatment exchanges use SimaPro's sign convention, including the corresponding
uncertainty transformation. Water emissions are converted from cubic metres to
kilograms with their uncertainty parameters. Electricity and heat inputs have
their own CSV section. Parameters, source comments, geography, process identifiers,
and scenario provenance accompany the inventory.

Process folders and category types
----------------------------------

Folders use the same ISIC revision 4 hierarchy as :doc:`openlca`, with CPC and
``Unclassified`` fallbacks. These folders can differ from those in SimaPro's
installed ecoinvent library.
Long folder components receive a stable shortened label while retaining the
ISIC codes and hierarchy. The full-to-shortened mapping is recorded under
``shortened_category_paths`` in the export report. Generated paths stay within
240 characters to leave room below SimaPro Desktop's 255-character limit, and
each folder name stays within its separate 60-character limit.
The system-description reference uses a label of at most 50 characters;
its definition retains the complete scenario provenance and a required category.

Product or waste-treatment status determines exchange placement and signs;
folder placement is a separate decision. Brightpath uses explicit production
categories where present, otherwise ISIC/CPC evidence together with the reference
product, unit, and quantity. Native SimaPro category-type metadata is also
preserved. Non-waste products are classified as material, energy, transport,
processing, or use from product and unit evidence. These rules apply across
ecoinvent releases and system models; they do not require a 3.9.1 lookup.
For unresolved waste status or product roles, Premise uses its bundled
category mapping. If that mapping is also missing, the existing ``material``
default is used. Every such fallback is listed in the export report, including
whether it came from a mapping or the default category. A fallback that would
change a resolved non-waste product into waste treatment stops the export.

Review the export report
------------------------

The report records the Brightpath version, source version and system model,
scenario, waste and category-type inference rules, classification fallbacks, and
rendering diagnostics. Excluded biosphere
exchanges include their activity, name, compartment, unit, quantity, and reason.
``Waste mass, total, placed in landfill`` and ``Organic carbon, placed in landfill``
are retained in ``Final waste flows``, including their quantities and uncertainty.
Imported indicators with native ``Final waste flows`` provenance are retained too.
Brightpath also retains supported oxygen, radionuclide, land-occupation, turbine
water, reservoir-volume, and primary-forest-energy flows and validates their units.

Other inventory indicators without a reviewed SimaPro representation remain
excluded with reason ``unsupported_inventory_indicator``. In particular, the
hazardous and non-hazardous waste indicators are not automatically equated to
landfill flows. Brightpath blacklist exclusions are reported separately.
A warning points to this report when exclusions or classification fallbacks occur.
Blacklisted technosphere inputs cause an error.

These exclusions and the destination's elementary-flow mappings affect which
burdens can be characterized. Successful CSV generation establishes that the
writer completed; compare selected inventory and impact results after import.
Full scenarios are materialized in memory during serialization.
