# BAFU integration checkpoint — 15 November 2026

Eleven of the nineteen standard sectors are qualified for the recorded encrypted
REMIND SSP2-NPi 2050 scenario. Buses join biomass, electricity, heat, cement,
steel, mining, renewable, carbon removal, fuels and ships. The combined export
contains 16,256 datasets, all 11,947 native source codes, 52 regional bus services
and 13 bus fleet markets. Its 130 signed bus LCI calculations pass. The separate
public bus workflow passes without bypassing its sector gate or validation.
The notebook's no-argument default remains biomass, electricity and heat.

Native services now lead the remaining transport work. Existing BAFU bus
services avoid the old manufacturing import graph. Six native two-wheeler
services pass graph, energy-quantity, fixed-input and IAM share checks without
writing a Brightway database. Combined upstream behaviour and finite LCI/export
checks remain pending, so two-wheelers are not yet qualified. The existing
passenger-distance versus vehicle-distance share approximation is explicit in
their YAML warnings and review notes.

Electric and diesel rail now select complete native services. This removes
twelve CLIC root imports. Hydrogen trains are genuinely active in the scenario;
their six remaining CLIC roots represent different countries. Their first
failing ecoinvent 3.12 conversion cohort has twenty distinct unapproved rules.
The barium-carbonate to lithium-carbonate candidate is incorrect and remains
unapproved. This is a specific required dependency group, not a global count of
all remaining mapping work. Rail is still unqualified.

The actual IAM role audit finds thirty active car roles and twenty active truck
roles. Their native class, fuel and reference-unit coverage is next to inspect.
Battery, final energy, metals and emissions also remain unfinished. Emissions
must use the shared engine; the earlier alternate native callback's successful
diagnostic does not meet that requirement. Zero CLIC roots in this ledger does
not establish that a sector needs no work: retained workbooks and procedural
updates have separate preparation paths.

Five targeted commits record this batch. Runtime Python changes are limited to
the shared hook carrying source warnings into transport comments and adding
verified buses to the existing sector gate. Two-wheeler and rail additions use
mapping YAML files; transport calculations are unchanged. Tests, source identity
evidence, scientific assumptions and remaining limits are recorded alongside
each mapping. The original user checkout is untouched.

One obsolete owned nine-sector export was deleted after checking that no other
dataset referenced it. The source, biosphere and ten- and eleven-sector exports
retain their expected counts and metadata. About 21 MB of old processed files
were removed. The shared SQLite file has about 9.47 GB available internally for
reuse; its physical file was not compacted. Brightway's automatic compaction
failed during the first cleanup attempt, so registry and file cleanup completed
with compaction disabled only for that operation, then its setting was restored.

The recorded checks use the archive snapshot
`20261110T030235Z-613ea498808a`; live freshness was not checked. Other IAM models,
the remaining eight sectors, the full nineteen-sector run and source/native
score equivalence are not established by this checkpoint. The full goal remains
active. Detailed receipts are in the adjacent JSON and the linked sector reviews.
