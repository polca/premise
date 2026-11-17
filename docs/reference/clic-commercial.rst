Commercial CLIC inventory selection
===================================

Premise uses CLIC for supplemental foreground selection, retaining required
workbooks for inventories unavailable through that route. Use the optional
commercial license policy to select CLIC foreground inventories with
commercially permitted declarations and replace restricted requirements with
reviewed alternatives. It is an opt-in inventory selection policy; it does not
certify usage rights for the complete scenario database.

Activation and defaults
-----------------------

Add these arguments to your existing ``NewDatabase`` configuration:

.. code-block:: python

   inventory_options = {
       "clic_options": {"license_policy": "commercial"},
   }

The CLIC pipeline is automatic; ``NewDatabase`` and ``PathwaysDataPackage`` have
no ``inventory_source`` argument. Without commercial screening,
``license_policy="default"`` retains the selected CLIC inventories under their
published declarations. This default does not exclude non-commercial licenses.

For older destinations, reviewed native manufacturing references can replace
newer-source assets independently of license screening. Those substitutions are
identified as ``older_target_compatibility`` and frozen in a compatibility recipe.
Create a new recipe when changing that build to commercial policy; the two
replacement plans have different selection criteria.

``update(sectors)`` prepares the required inventories before transforming those
sectors. ``prepare_inventories(sectors)`` can prepare them explicitly. Successive
calls expand the required sector selection. The policy checks that selected
graph, including upstream CLIC suppliers, rather than the entire CLIC catalogue.

License decisions
-----------------

The permitted CLIC declarations are ``CC0-1.0``, ``CC-BY-3.0``, ``CC-BY-4.0`` and
``CC-BY-SA-4.0``. Attribution and ShareAlike conditions remain visible in the
license report. Commercial permission does not imply unrestricted redistribution.

Non-commercial, missing, undisclosed and unrecognized declarations require an
alternative. An explicit non-commercial source-license note also requires review
when the selected declaration otherwise passes. This flags a conflict; it does
not establish that the selected declaration is legally invalid.

The assessment uses declared permissions from the verified archive and, in live
mode, CLIC metadata. It does not determine ownership of underlying rights. See
the `CLIC license guide <https://clic.psi.ch/licenses/>`__ for the platform's
license descriptions.

How alternatives are applied
----------------------------

1. Resolve the required CLIC roots and their upstream dependency graph.
2. Select applicable reviewed rules from
   ``premise/data/clic/commercial_replacements.yaml`` and check that their
   suppliers exist in the user's actual background database.
3. Replace affected activities, preferably complete parent assets. Native
   alternatives copy the native inventory's exchanges and documented conversion;
   they do not copy restricted recipe coefficients or behavior metadata. An
   independently sourced recipe supplies methane-pyrolysis hydrogen.
4. Remove dependencies no longer needed by any retained activity or explicit
   root. A shared supplier is retained while another activity still needs it.
5. Screen all remaining CLIC inventories and the reviewed replacements, migrate
   ordinary CLIC inventories, and validate final supplier and biosphere links.

Identity matching uses the explicitly listed names, products, units and location
preferences. There is no runtime search for a roughly named substitute. Native
candidate lists are tried in order; unavailable candidates lead to a documented
fallback or an unresolved requirement. Missing candidates never silently remove
a required technology.

Whole-activity replacements preserve the requested identity and Premise role
bindings so sector updates can still find the process. Their source geography,
conversion and technological differences are reported. PV installations use
capacity scaling; battery packs excluding electronics remove native control
inputs and renormalize the remaining mass. Separate control purchases remain.

Reviewed background-supplier rules also support older targets when a
permissively licensed CLIC consumer refers to an unavailable supplier. These
replace that purchase's supplier, preserve its amount and the consumer's license,
and undergo final strict target validation. Unknown links still fail.

These are manufacturing proxies. A different battery chemistry does not change
the consumer's assumed cycle life; a material proxy does not establish the
original material's operational performance. Replacement limitations must be
considered when interpreting results.

The complete :doc:`clic-commercial-mappings` catalogue lists every rule, native
candidate, independent recipe, source and documented limitation.

Validated targets and result differences
----------------------------------------

The reviewed selections were validated against cutoff 3.8, 3.9, 3.9.1, 3.10,
3.10.1, 3.11 and 3.12, and EN15804 3.12. This describes the reviewed archive and
available native suppliers; future CLIC releases can require further review.

Commercial-policy consequential and pre-3.8 builds are rejected before inventory
loading because their automatic workbook compatibility fallbacks have not been
assessed. Without commercial screening, consequential builds retain compatible
workbooks because cutoff CLIC inventories cannot supply that system model;
3.7 builds retain workbooks because reviewed CLIC routes start at 3.8.

Approximate replacements can substantially change LCIA results. For one fixed
3.12 cutoff background, the comparison of 616 untransformed activity roots found
IPCC 2013 GWP100 changes from +0.1% to +324.3% for the PV family and from -31.5%
to +45.0% for the battery family. These are foreground comparisons, not
scenario-wide changes or evidence that a proxy reproduces the original model.
The policy-off comparison reproduced all 10,472 scores across 17 methods.

Reports and reproducibility
---------------------------

Generate a scenario report after updating the required sectors:

.. code-block:: python

   database.generate_scenario_report(
       filepath="export/commercial", name="scenario_report.xlsx"
   )
   database.write_inventory_recipe("export/commercial/inventory-recipe.json")

The workbook contains ``CLIC Inventories``, ``CLIC Licenses`` and, when
substitutions are used, ``CLIC Replacements`` sheets. Alongside it, Premise writes:

* ``scenario_report.inventory-manifest.json``: selected versions, licenses,
  substitutions, retained workbook sources and migration provenance.
* ``scenario_report.inventory-licenses-N.csv``: original and used licenses,
  citations, attribution and ShareAlike flags.
* ``scenario_report.inventory-substitutions-N.csv``: activity or background-supplier
  replacements, actual suppliers, conversions, confidence, geography and limitations.
* ``scenario_report.inventory-recipe-N.json``: a recipe for scenario ``N``;
  numbering starts at one.

The explicit ``inventory-recipe.json`` export freezes the build's current
selection. To replay it, supply ``inventory_recipe`` with the same source
database, scenario configuration and sector selection. A commercial recipe
restores commercial policy automatically, even if it is omitted from
``clic_options``. The recipe retains original version pins, including pruned
activities, to reconstruct the original graph; the manifest identifies which
inventories were actually used.

Policy, registry, target and replacement-inventory fingerprints protect replay
and caches. Changed rules or native inventories require a reviewed new recipe.
Cached and pinned inventories still undergo license assessment. With the
default ``freshness="live"``, declarations of all used CLIC versions are checked,
including specific pinned versions. ``freshness="snapshot"`` or ``offline=True``
assesses the verified archive snapshot instead; the reports record this choice.
Offline builds need an available local or cached archive and migration resources.

Failures and scope
------------------

An essential unresolved requirement stops preparation with ``ClicLicenseError``.
Its ``issues`` attribute contains all blocked datasets and their root-to-supplier
paths; the message displays the first ten. Ambiguous suppliers, invalid links
and incompatible recipes also stop preparation. Commercial mode does not fall
back to a restricted workbook when an alternative is missing.

The downloaded archive remains a mixed-license archive. Screening governs the
selected CLIC inventories used in the build. Ecoinvent, retained workbooks and
user inventories require separate permission assessments, including appropriate
`ecoinvent rights <https://support.ecoinvent.org/licensing>`__. Review the exported
sources and replacement limitations before distributing or using the result.
