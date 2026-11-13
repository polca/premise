# Standalone CDR uses the shared background-family path

The standalone CDR update previously defaulted to ecoinvent even when
`NewDatabase` selected UVEK. The source baseline applied the existing canonical
electricity product aliases, while CDR did not. This made 232 electricity
identities in an existing cyclic component appear different. No CDR-created
edge or node caused that reported component.

Two runtime modules need a small argument-handoff correction: register CDR in
NewDatabase's existing family/biosphere dispatch list, and forward those values
through the CDR constructor to the existing BaseTransformation and InventorySet.
YAML cannot forward Python arguments that the callback previously discarded.
No CDR algorithm, energy bound, market-composition method, regionalization,
consumer relinking, validation rule or default update selection changes.
Ecoinvent retains the existing constructor defaults.

Selecting the correct family also exposed a support-filter issue. UVEK filters
correctly refuse unbound published records; the historical CDR support file
lacked explicit CLIC role metadata. Without fixing that data, 105 expected
regional support records would disappear while graph validation still passed.

The support YAML now records explicit published slugs and group role keys.
The requirements registry adds `cdr_activities.yaml` support roles using the
retained workbook identities and the **unchanged original filters**. The small
maintainer generator extension replays those bindings. All previous registry
values are unchanged. Binding uses the original per-slug unit-bearing identities;
renamed published records still match and unbound lookalikes do not inherit a
role. No new physical proxy, source omission or quantity conversion is added.

The historical `land use` support filter has no retained template and stays
empty. In the inspected REMIND SSP2-NPi 2050 source selection, industrial CCS,
biochar, enhanced weathering and ocean-liming support groups are also empty in
both the old and new filter paths. This is not evidence that those technologies
are implemented for another IAM scenario. All nonzero roles in this inspected
scenario have providers. Its CDR markets use the shared production-volume and
carrier-splitting logic, which is distinct from treating duplicate raw IAM
aliases as independent production.

The real standalone diagnostic restores all 12,214 records and passes exhaustive
graph validation. Native-family and original-family support/main filters select
the same published slugs. It creates 11 regional CDR markets with positive
supplier shares summing to one, 73 regional main CDR records and 165 regional
support records (the two counts overlap). The original normal skip is retained
when IAM CDR data is absent.

All 436 energy exchanges checked across 48 regional DAC records match the
existing shared energy filters. A complete transformed graph, rather than an
unchanged-native overlay, is solved in memory: 84 regional CDR activities and
markets pass 168 signed LCI calculations. All supplies and inventories are
finite, and signed reversals agree. No Brightway database, activity or processed
file is created by those checks. This does not establish source-emission or
LCIA equivalence, model completeness or stochastic uncertainty equivalence.

The public CDR gate is now enabled after the combined eight-sector run,
actual export and 168 signed checks of the written CDR activities and markets.
See the [standalone verification](uvek-cdr-family-verification.json) and
[combined export qualification](uvek-cdr-qualification.json). The default
three-sector notebook update remains unchanged.
