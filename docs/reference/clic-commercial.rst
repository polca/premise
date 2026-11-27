.. _commercial-clic-inventory-selection:

Commercial use of CLIC inventories
==================================

Some CLIC datasets restrict commercial use. We recommend the workflow below
when using Premise commercially: it checks the CLIC data your build needs and
uses reviewed replacements for restricted datasets where available.

.. _activation-and-defaults:

Turn on commercial screening
----------------------------

Add ``clic_options`` to your usual ``NewDatabase`` settings. In this example,
``build_arguments`` contains your scenario, source database and IAM key settings.
Choose the sectors your study needs:

.. code-block:: python

   database = NewDatabase(
       **build_arguments,
       clic_options={"license_policy": "commercial"},
   )
   sectors = ["electricity", "fuels", "battery", "cars"]
   database.update(sectors)
   database.generate_scenario_report(
       filepath="export/commercial", name="scenario_report.xlsx"
   )
   database.write_inventory_recipe("export/commercial/inventory-recipe.json")

Premise prepares the required inventories when you call ``update``. Further
updates add any newly required data. CLIC is selected automatically; there is
no ``inventory_source`` argument. Without commercial screening, the selection
can include non-commercial datasets.

.. _license-decisions:

Accepted licenses
-----------------

The policy accepts ``CC0-1.0``, ``CC-BY-3.0``, ``CC-BY-4.0`` and ``CC-BY-SA-4.0``.
Their conditions still apply, including crediting sources and sharing adapted
material under the same license where required. The report records these
attribution and ShareAlike requirements.

Non-commercial, missing or unrecognized licenses need a reviewed alternative.
Premise also flags conflicts where a source note mentions a non-commercial
restriction despite an otherwise accepted license declaration. These checks
use the published license information. Premise cannot verify the uploader's
right to license the data.
See the `CLIC license guide <https://clic.psi.ch/licenses/>`__ for details.

.. _how-alternatives-are-applied:

How replacements work
---------------------

Premise checks the selected CLIC datasets and the CLIC datasets they use as
inputs. It chooses replacements from a reviewed list and checks that the
required suppliers exist in your database. Most alternatives use ecoinvent
inventories; methane-pyrolysis hydrogen uses a separately published reference.

Premise applies the documented scaling and removes inputs that are no longer
needed. Shared inputs stay while another selected dataset still uses them.
The final supplier, emission and resource links must all match.

Some replacements are approximate. For example, a different battery chemistry
may represent manufacturing while the vehicle keeps its assumed battery life.
Review the actual replacements, their scaling and their limitations in the
report. The full :doc:`clic-commercial-mappings` catalogue provides the datasets,
sources, conversions and confidence levels.

.. _validated-targets-and-result-differences:

Supported versions and changes in results
-----------------------------------------

The reviewed selections were tested with cutoff 3.8, 3.9, 3.9.1, 3.10, 3.10.1,
3.11 and 3.12, and EN15804 3.12. New or updated CLIC datasets may need further
review.

Commercial screening applies to supported cutoff and EN15804 builds.
Consequential builds use the workbook pipeline.

Older-version builds may use replacements for compatibility even when commercial
screening is off. The report marks these as ``older_target_compatibility``.
If you have saved a compatibility recipe, start a new build with commercial
screening and save a new recipe.

Replacements can substantially change results. In one ecoinvent 3.12 cutoff
comparison, 100-year climate change scores (IPCC 2013 GWP100) rose by 0.1% to
324.3% for PV-related inventories and changed by -31.5% to +45.0% for battery
inventories.
These figures describe individual inventories before scenario updates. Assess
the effects on your own study.

.. _reports-and-reproducibility:

Review and repeat your build
----------------------------

Open ``scenario_report.xlsx`` and review the ``CLIC Licenses`` and
``CLIC Replacements`` sheets. The latter appears when replacements were used;
``CLIC Inventories`` lists the selected datasets. Files beside the workbook
provide details for further analysis:

* ``scenario_report.inventory-manifest.json``: versions, licenses, replacements,
  retained workbooks and conversion history.
* ``scenario_report.inventory-licenses-N.csv``: licenses, citations and conditions.
* ``scenario_report.inventory-substitutions-N.csv``: replacements, suppliers,
  conversions, confidence and limitations.
* ``scenario_report.inventory-recipe-N.json``: the selection for scenario ``N``,
  numbered from one.

The saved ``inventory-recipe.json`` records versions and replacement choices.
To use the same selection again, keep the same source database, scenario
settings and sectors, and load that recipe:

.. code-block:: python

   replay = NewDatabase(
       **build_arguments,
       inventory_recipe="export/commercial/inventory-recipe.json",
   )
   replay.update(sectors)

A commercial recipe restores commercial screening automatically. If the target,
replacement rules or copied inventories change, review the changes and save a
new recipe. Cached data and fixed versions still undergo license checks.

The default ``freshness="live"`` checks declarations online, including versions
fixed in a commercial recipe. ``freshness="snapshot"`` or ``offline=True`` uses
the checked archive snapshot instead. The report records this choice. Offline
builds need the archive and conversion resources available locally or in cache.

.. _failures-and-scope:

When a build stops
------------------

Premise stops if a required dataset has a license issue with no reviewed
alternative, a supplier is missing or ambiguous, a link is invalid, or a saved
recipe no longer matches. It does not switch to a restricted workbook to bypass
a license issue.
For license failures, ``ClicLicenseError`` lists the first ten blocked datasets;
its ``issues`` attribute contains the full list and shows which datasets need them.

The downloaded archive contains datasets with different licenses. Screening
covers the CLIC datasets used in your build. You still need the appropriate
permissions for `ecoinvent <https://support.ecoinvent.org/licensing>`__, retained
workbooks and inventories you supply yourself before using or sharing the result.
