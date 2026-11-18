# Approved source-context forwarding for final energy

The selected source is UVEK 2026 with ecoinvent 3.10 biosphere, but the final-energy
callback does not forward that choice. Its shared transformation and mapping
selector default to ecoinvent. This causes a proven false cycle error: the same
58 supply loops have different electricity product labels in the baseline and
result. None of the largest component's inventories changed.

The two-file fix passes the existing family and biosphere version from
NewDatabase into FinalEnergy, its shared base class and InventorySet. It uses the
same pattern as the other qualified sectors. No transformation formula, market
algorithm, quantity, loss, efficiency, public constructor option or validation
rule changes. Ecoinvent remains the default for existing callers.

A temporary constructor check confirms that the existing shared base applies
the selected UVEK electricity aliases, without changing amounts or input links,
and that ecoinvent defaults remain unchanged. The accompanying YAML extension
adds native aliases for 302 existing migrated foreground roles and 40 low-
voltage grid roles, preserving all original filters, IAM labels and LHVs.
Filter checks and complete source-provenance eligibility are separate from the
constructor check. The full shared callback and exhaustive graph validation now pass with zero
errors, adding 804 regional activities. Supplier coverage remains incomplete:
175 of 203 positive IAM roles have suppliers; 28 direct role gaps remain.
This is not a public-sector qualification or export.

The user approved this exact two-file fix on 18 November 2026, separately from
the earlier classifier exception. Source files match the reviewed proposal
exactly. No other Python change, battery proposal or public-admission bypass
is authorized by that approval. The reviewed patch is retained at
`proposed-final-energy-context-forwarding.patch`. The applied fix passes 183 existing database, sector-contract, mapping and
vehicle-energy tests. The actual shared callback and exhaustive validation pass
with zero errors; see `uvek-final-energy-context-workflow-verification.json`.
