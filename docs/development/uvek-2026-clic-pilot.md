# UVEK 2026 CLIC electricity pilot

## Objective and scope

Enable and validate a CLIC-backed premise workflow using UVEK 2026 cut-off
technosphere and ecoinvent 3.10 biosphere through an electricity-sector pilot.
Prioritize the inventories actually required by that pilot. Exhaustive completion
of the earlier Brightpath mapping queue and support for every other sector are
outside this scope.

## User experience

Keep the familiar `NewDatabase`, `update`, reporting and export workflow. Any
background-selection arguments must be minimal and explicit. Users must not
need to run Brightpath, manage intermediate conversions or relink inventories
manually. Existing ecoinvent calls and defaults remain compatible.

## Required work

1. Import the pinned public Brightcon ecoSpold bundle into a dedicated Brightway
   project. Verify available checksums, complete linking and readback; preserve
   amounts, signs, uncertainty and declared aliases. Record its 2,991 upstream
   biosphere exclusions and missing audit artifacts without approving further
   omissions implicitly. Demonstrate a finite destination LCI calculation.
2. Carry background family, technosphere release, system model and biosphere
   release explicitly through ingestion, conversion, linking, caches and export
   provenance. UVEK releases must not enter ecoinvent version comparisons or
   biosphere fallbacks. Cross-system-model conversion remains unsupported.
3. Resolve the required CLIC roots, upstream activities, retained workbook records
   and user inventories. Preserve foreground ownership, formulas, signed
   quantities and uncertainty. Verify suppliers against the actual destination.
4. Introduce reviewed `uvek_aliases` beside `ecoinvent_aliases` in the relevant
   YAML technology mappings, plus UVEK fuel aliases where necessary. Select them
   from the explicit background family; retain shared IAM and CLIC roles. Missing
   required UVEK mappings must produce actionable errors. Biosphere identities
   remain those of ecoinvent 3.10.
5. Reuse scientific approvals only within their recorded scope. Preserve accepted
   proxy limitations and approved omissions in warnings, provenance and audits.
   Refuse ambiguous or unresolved suppliers, unreviewed losses and unsupported
   unit changes. Group genuinely new modelling decisions for user approval.
6. Adapt the electricity transformations, market construction, relinking and
   certification needed for the pilot. Run a reproducible encrypted-IAM scenario
   through the public API, export it to Brightway and obtain a finite LCI while
   preserving the original source database and caller-owned records.
7. Add regression coverage, run relevant offline and integration checks, preserve
   lockfiles/manifests/fingerprints and make focused commits at verified
   checkpoints. Remove superseded temporary caches. Classify remaining blockers
   as premise integration, Brightpath mapping, source-data limitations or new
   modelling decisions.

## Completion gates

The exact context and electricity pilot must work end to end through the public
premise API, with complete links, preserved provenance and passing required
checks. Other sectors must have explicit support boundaries. An imported UVEK
database, an inventory-provider test or a finite source-database LCI alone does
not establish a working prospective scenario.

## Current checkpoint

The destination resolver and CLIC provider accept the exact context. Regression
tests cover actual destination keys, canonical biosphere references, signed
uncertainty/formulas, cache replay and early cross-model refusals. The Brightcon
bundle has been imported locally with all 11,947 activities and 417,072 retained
exchanges, and a source electricity LCI is finite.

The UVEK source extractor reads complete Brightway records and resolves Wurst
labels from their actual linked suppliers. It checks the source's declared
family, release, system model and biosphere, and rejects exchanges outside the
selected databases. Ecoinvent cleanup strategies are not applied. Formulae,
signed uncertainty and native metadata survive extraction on owned copies.
Export normalization uses the explicit biosphere axis, and native database
metadata, labels and report overviews distinguish UVEK 2026 from biosphere 3.10.
Passing 2026 to an ecoinvent biosphere lookup now raises an explicit error.

The native alias layer uses `InventorySet(family="uvek")`, `uvek_aliases` and
`uvek_fuel_aliases` in the electricity and fuel YAML mappings. Its 53 nonempty
alias definitions were checked against both the installed source and the
packaged UVEK 2026 catalog. Matching includes exact names, reference products
and units. Shared IAM roles remain unchanged; CLIC generation requires both a
reviewed slug and an explicit role binding. Grid and PV aggregate nodes are
excluded from native generator roles. Missing aliases are reported and never
inherit ecoinvent filters. This does not establish sector support by itself:
several non-pilot IAM technologies and native nuclear fuel specifications still
lack reviewed definitions.

`NewDatabase` integration, retained workbook conversion and electricity
transformations remain under validation. A comparison with the packaged
Brightpath UVEK catalog finds 45 destination identity differences; these need
source-based investigation before resolving affected purchases. No automatic
geographic substitution is approved by this comparison.
