# Exclude native PV wiring from plant-efficiency updates

The real five-sector run printed “No numerical value found” for native “Electric installation, photovoltaic plant, at plant”. This is wiring/BOS, not a capacity-bearing PV plant. The existing exclusion covered ecoinvent's lowercase spelling, but missed the native capitalised name.

One case-insensitive predicate in the shared PV filter now enforces the already documented wiring exclusion. A YAML change cannot alter this hardcoded predicate. Both original and native spellings remain untouched by the plant-efficiency update. The native dataset previously reached the missing-capacity skip, so this correction changes no inventory coefficient in the observed scenario. Thirty-seven photovoltaic regressions pass, including both spellings and the unchanged-inventory checks. Ruff adds no findings to the existing electricity-module baseline; the test file passes Ruff. There are no schema, migration-resource, API or user-workflow changes.
