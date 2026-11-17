Commercial CLIC replacement mappings
====================================

This catalogue documents the mappings used by :doc:`clic-commercial`. It is
generated from ``premise/data/clic/commercial_replacements.yaml``, the same
registry read at runtime. Every rule below includes its source identities,
ordered native candidates or independent recipe, conversion, evidence,
qualifications and confidence. It contains supplier identities and assumptions,
not copies of native ecoinvent inventory coefficients.

How to read a mapping
---------------------

Commercial screening applies whole-activity rules to restricted requirements
and affected parent activities. Independently, older-target compatibility uses
the same reviewed rules for newer-source assets that need native manufacturing
references. Compatibility selection is recorded as ``older_target_compatibility``;
it is not a commercial-license assessment. One rule can cover multiple CLIC
versions and locations that share the listed activity name and reference unit.

Native supplier identities are shown as name; reference product; unit. Candidate
priority follows the YAML list. Within each candidate, location preferences are
tried from left to right. ``same`` means the requested CLIC activity's location;
``RoW``, ``GLO``, ``RER`` and ``CH`` are explicit fallback locations. A candidate
must exist uniquely in the actual destination background. This is why an older
target may select a different chemistry or material. Candidate-specific
confidence and qualifications describe that fallback and are added to the
actual substitution report.

Registry applicability below describes where a rule may be selected. It is
broader than the validated target combinations listed in :doc:`clic-commercial`;
in particular, EN15804 has been validated for 3.12. Rule applicability alone
does not guarantee that its native suppliers are available in another target.

Conversions describe quantities per requested reference output. Capacity scaling,
excluded electronics and mass renormalization are stated explicitly. Additional
metadata identifies the proxy technology, capacity or boundary output used by
subsequent transformations; it does not establish scientific equivalence.

Background-supplier rules apply only when the original supplier identity is
absent from the target. They preserve the purchasing quantity and the consumer's
CLIC license. Their reported inventory license identifies the replacement
background supplier, rather than a new license for the consumer.

Coverage overview
-----------------

.. list-table::
   :header-rows: 1

   * - Family
     - Alternative
     - Main qualification
   * - Chargers and DC converters
     - Native passenger-car electronics, per kg
     - Other vehicle classes and power ratings can differ.
   * - Residential PV electricity and installations
     - Native conventional PV electricity or capacity-scaled installations
     - Manufacturing vintage, module technology, yield, geography and scale can differ.
   * - Battery cells, packs and controls
     - Native cell/pack inventories; separate control manufacturing
     - Older chemistries are fallbacks; packs excluding controls are mass-renormalized.
   * - Vehicle powertrain and mechanical components
     - Native motors, batteries and documented glider manufacturing proxies
     - Coarse manufacturing boundaries do not reproduce every component or end-of-life treatment.
   * - Electrolyzer materials and semiconductors
     - PTFE/TFE, native converter manufacturing and silicon wafer manufacture
     - PFSA chemistry, SiC fabrication and operational material properties are not reproduced.
   * - Methane-pyrolysis hydrogen
     - Independent CC-BY-4.0 reference recipe with native gas and electricity markets
     - Published 200-bar energy demand is retained for a 100-bar identity; carbon is uncredited.
   * - Missing background suppliers in older targets
     - Ten explicit supplier rules, detailed below
     - Purchase amounts remain unchanged; related materials can be low-confidence proxies.

Use the exported ``inventory-substitutions-N.csv`` to identify which of these
rules and candidates were actually used in your build. Confidence is a
qualitative qualification, not an uncertainty distribution or an accuracy score.
Future registry changes require a new recipe when replay fingerprints differ.

.. include:: generated/clic-commercial-mappings.inc
