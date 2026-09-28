Scenario coverage and interpretation
======================================

Three questions must be answered separately: can the IAM represent a process,
does your scenario file report it, and can Premise use those variables?
Model descriptions answer the first question only.

Configured sector mappings
----------------------------

The table below is generated from non-empty model aliases in the sector
mapping files. It lists configured mappings; it does not show which database builds have passed.
Aliases may cover only part of a sector. Missing aliases do not rule out
fallbacks, derived quantities or externally supplied data.

“Yes” means at least one alias is present; “No” means none was found.

.. include:: generated/mapping-coverage.inc

Battery energy-density/chemistry scenarios and the metals/mining parameter
tables have separate external inputs; their absence from this IAM-alias table
does not mean that these transformations are unavailable.

Check the selected file
-------------------------

Record its release, checksum, model, pathway, region set and reported years.
After constructing a scenario, inspect the scenario-variable summary and the
sector validation coverage. A missing fleet series, for example, is not a
zero-sized fleet. A zero production volume is not a missing variable.

WITCH is a configured model identifier. Its presence in the configuration does
not imply the same sector coverage as REMIND or IMAGE. Check the mappings
and available variables for the model and pathway you select. See :doc:`/methodology/heat` for an example
of model-specific data representation and :doc:`/user_guide/reports` for
inspecting the actual data used.

Temperature classifications
-----------------------------

Do not infer a precise warming outcome from a pathway label alone. The
`AR6 scenario database <https://data.ene.iiasa.ac.at/ar6/static/About.html>`_
provides release-specific scenario metadata and climate-emulator diagnostics.
Matching requires the exact model version and scenario, including the
temperature reference period, probability and overshoot convention.

The inherited temperature categories in :doc:`scenarios` have not been matched
to all locally supplied IAM releases. They are historical, unverified guidance
and should not be used as quantitative study inputs until that match is made.
