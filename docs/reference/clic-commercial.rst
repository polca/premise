.. _commercial-clic-inventory-selection:

Commercial use of CLIC inventories
==================================

Some inventories available through CLIC have licenses that restrict commercial
use. If you plan to use Premise commercially, we recommend turning on commercial
screening. Premise then checks the CLIC datasets your build needs and uses
reviewed alternatives for restricted datasets where possible.

This check covers the selected CLIC data. You still need the appropriate rights
to use ecoinvent, retained workbooks and any inventories you supply yourself.

.. _activation-and-defaults:

Turn on commercial screening
----------------------------

Add ``clic_options`` to your usual ``NewDatabase`` call. Here,
``build_arguments`` is a dictionary containing your existing scenario, source
database and IAM key settings:

.. code-block:: python

   database = NewDatabase(
       **build_arguments,
       clic_options={"license_policy": "commercial"},
   )
   sectors = ["electricity", "fuels", "battery", "cars"]
   database.update(sectors)

Choose the sectors your study needs. Premise prepares the extra inventories
before updating those sectors. Further ``update`` calls add any newly required
inventories. You can also prepare them directly with
``prepare_inventories(sectors)``.

Premise uses CLIC automatically; there is no ``inventory_source`` argument.
Commercial screening is optional. If you leave it off, the default selection
can include datasets with non-commercial licenses.

.. _license-decisions:

Accepted licenses
-----------------

The policy accepts these CLIC license declarations: ``CC0-1.0``, ``CC-BY-3.0``,
``CC-BY-4.0`` and ``CC-BY-SA-4.0``. Their conditions still apply. For example,
you may need to credit the source or share adapted material under the same
license. The license report records attribution and ShareAlike requirements.

Datasets with non-commercial restrictions, missing licenses or unrecognized
licenses need an alternative. Premise also flags cases where a source note
mentions a non-commercial restriction despite an otherwise accepted license
declaration. Such a conflict needs review before the dataset can be used.

Premise checks the recorded license information; it cannot establish who owns
the underlying rights. See the `CLIC license guide <https://clic.psi.ch/licenses/>`__
for more about the platform's licenses.

.. _how-alternatives-are-applied:

How replacements work
---------------------

Premise checks both the CLIC datasets selected for your sectors and the CLIC
datasets they use as inputs. It does not check unrelated catalogue entries.

When a required dataset has a restriction, Premise:

1. Looks for a reviewed replacement listed in the mapping catalogue.
2. Checks that the replacement's suppliers exist in your ecoinvent database.
3. Applies the documented conversion, such as scaling a PV installation to the
   required power capacity.
4. Removes inputs that are no longer needed. An input stays if another selected
   dataset still uses it.
5. Checks the final links to suppliers, emissions and resources.

Most replacements use inventories already in your ecoinvent database. They use
those inventories' quantities, rather than copying the restricted dataset's
recipe. Methane-pyrolysis hydrogen uses a separately published reference recipe.
Only the listed, reviewed alternatives are considered. If none is available,
Premise stops instead of leaving out a required technology.

The report identifies the replacement, its location, any scaling and its
limitations. Some are approximate: a different battery chemistry may represent
manufacturing, for example, while the vehicle keeps its assumed battery life.
That choice needs to be considered when interpreting the results.

For battery packs that exclude electronics, Premise removes the control-system
inputs and scales the remaining inputs back to one kilogram of pack. Controls
bought separately are still included. Other rules replace an unavailable
supplier while keeping the quantity purchased and the license of the dataset
making that purchase.

See the full :doc:`clic-commercial-mappings` catalogue for the replacement
datasets, conversions, sources, confidence levels and limitations.

.. _validated-targets-and-result-differences:

Supported versions and changes in results
-----------------------------------------

The reviewed commercial selections have been tested with:

* Ecoinvent cutoff 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11 and 3.12.
* Ecoinvent EN15804 3.12.

These tests used a particular CLIC archive and the available ecoinvent suppliers.
New or updated CLIC datasets may need further review.

Commercial screening is not available for consequential backgrounds or ecoinvent
3.7. Those builds use workbooks because compatible CLIC routes are
unavailable, and that workbook selection has not been assessed by this policy.

Older ecoinvent versions can also need replacements for newer-source datasets,
even when commercial screening is off. The report labels these choices
``older_target_compatibility``. If you have saved such a build's recipe, create a
new recipe for a commercial build rather than changing the policy on that recipe.

Replacements can substantially change environmental results. In a comparison
using ecoinvent 3.12 cutoff, 100-year climate change scores (IPCC 2013 GWP100)
for PV-related datasets rose by 0.1% to 324.3%; battery-related scores changed by
-31.5% to +45.0%. These figures
compare individual inventories before scenario updates. They do not describe
the change in an entire future database or show that every substitute is an
equivalent model. Review the replacements used in your own study.

.. _reports-and-reproducibility:

Review and repeat your build
----------------------------

After updating your sectors, generate a report and save an inventory recipe:

.. code-block:: python

   database.generate_scenario_report(
       filepath="export/commercial", name="scenario_report.xlsx"
   )
   database.write_inventory_recipe("export/commercial/inventory-recipe.json")

The workbook has ``CLIC Inventories`` and ``CLIC Licenses`` sheets, plus a
``CLIC Replacements`` sheet when replacements were used. Premise also writes
these files beside it:

* ``scenario_report.inventory-manifest.json`` records the selected versions,
  licenses, replacements, retained workbooks and conversion history.
* ``scenario_report.inventory-licenses-N.csv`` lists original and used licenses,
  citations and conditions.
* ``scenario_report.inventory-substitutions-N.csv`` lists replacements, suppliers,
  conversions, confidence levels, locations and limitations.
* ``scenario_report.inventory-recipe-N.json`` saves the selection for scenario
  ``N``. Numbering starts at one.

A recipe records the inventory versions and replacement choices so you can use
the same selection again. Reuse your source database, scenario settings and
sectors, and pass the saved recipe:

.. code-block:: python

   replay = NewDatabase(
       **build_arguments,
       inventory_recipe="export/commercial/inventory-recipe.json",
   )
   replay.update(sectors)

A commercial recipe turns commercial screening back on automatically. It also
keeps records of original versions that were replaced or became unnecessary;
the report shows which inventories were actually used.

Saved checksums detect changes to the replacement rules, target or copied
inventories. If those no longer match, review the changes and create a new
recipe. Using cached data or fixing a dataset version in a recipe does not
skip the license checks.

By default, ``freshness="live"`` checks the license declarations of all used CLIC
versions online, including versions fixed in a commercial recipe. Set
``freshness="snapshot"`` or ``offline=True`` to use the checked archive snapshot
instead. The report records that choice. Offline builds need the archive and
the required conversion resources available locally or in the cache.

.. _failures-and-scope:

When a build stops
------------------

If a required dataset has an unresolved license restriction, preparation stops
with ``ClicLicenseError``. Its message lists the first ten blocked datasets; its
``issues`` attribute holds the full list and shows which selected datasets need
them. Missing or ambiguous suppliers, invalid links and incompatible recipes
also stop preparation. A missing commercial replacement does not trigger a
switch to a restricted workbook.

The downloaded archive contains datasets with different licenses, including
datasets your build does not use. Commercial screening checks the selected
CLIC inventories used in the build. Review the reported sources and replacement
limitations, and make sure you have the separate permissions needed for
`ecoinvent <https://support.ecoinvent.org/licensing>`__, retained workbooks and
your own inventories before using or sharing the result.
