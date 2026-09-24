# Brightpath JSON-LD export investigation

Investigated on 2026-09-24 against Premise `82b7475f` (`olca-export`)
and the local Brightpath checkout at `f7c8444` (version `1.0.0a1`).
Brightpath has unrelated uncommitted changes; this investigation did not modify it.

## Assessment

Brightpath is a suitable serialization layer, but its current generic JSON-LD
writer needs correctness fixes before replacing Premise's openLCA exporter.
The integration should export each complete prepared scenario directly from
dataset dictionaries, without an intermediate SimaPro file or background migration.

## Existing and proposed paths

`NewDatabase.write_db_to_olca` currently loads and certifies each scenario,
calls `_prepare_database`, records validation, and invokes
`Export.export_db_to_simapro(olca_compartments=True)`. It writes
`simapro_export_<model>_<pathway>_<year>.csv`. The openLCA flag disables
SimaPro subcompartment mapping. The documented import uses a SimaPro mapping
file followed by separately imported ecoinvent impact methods.

Keep the scenario preparation, validation, reporting, and cleanup in Premise.
Replace only the serialization step with a small adapter that:

1. Materializes a scenario as plain dataset dictionaries, copying before changes.
2. Sets explicit ecoinvent version, system model, biosphere, and format context.
3. Constructs Brightpath's `InventoryDocument` and writes one JSON-LD ZIP per
   scenario through its operation pipeline, checking the returned report for failure.
4. Uses scenario-specific process IDs and checks that every internal provider resolves.
5. Writes to a temporary destination and publishes the completed archive only on success.

Relevant Brightpath entry points are `InventoryDocument(data=..., context=...)`,
`InventoryPipeline.write(..., target_format="openlca_jsonld")`, and the lower-level
`write_openlca_jsonld(document, path)`. Confirm the public API and released version
when implementing; the document class describes itself as an internal boundary.

## Required fixes and decisions

| Topic | Finding and consequence |
| --- | --- |
| Elementary-flow identity | `_ensure_flow` derives IDs from flow type, name, and unit, excluding compartments and original biosphere IDs. Same-named emissions to air and water collide; the last category wins. Include complete identity and an explicit mapping to the intended openLCA elementary-flow list. |
| LCIA compatibility | The only packaged exact openLCA reference catalog is UVEK 2025 with ecoinvent 3.10 biosphere. Generic ecoinvent exports synthesize flow, unit-group, and flow-property IDs. A valid ZIP does not establish compatibility with imported LCIA methods. Define and validate the target elementary-flow and quantity references. |
| Uncertainty | `_schema_uncertainty` copies Brightway log-space `loc` and `scale` to openLCA `geomMean` and `geomSd`. These require exponentiation for ordinary positive lognormal distributions. Negative distributions and unsupported uncertainty types need explicit treatment; currently unsupported types return no uncertainty. |
| Scenario identity | Process IDs reuse dataset `code` when present; fallback IDs omit scenario identity. Define whether scenarios must coexist in one openLCA database, and namespace process IDs accordingly to prevent overwrites. Normalize IDs to valid UUID representations. |
| Provider closure | Local suppliers resolve by name, product, location, and unit. The generic fallback can create a provider reference without exporting the provider. Complete-scenario exports should fail on any such unresolved link; ambiguous identities also need detection. |
| Waste and signs | The generic writer creates product flows for technosphere exchanges, sets production as output, and retains signed amounts. Validate waste-treatment reference outputs, negative exchanges and consequential substitution against actual openLCA matrix results before changing these conventions. |
| Python support | Premise supports Python >=3.10; Brightpath declares >=3.12. Options are broadening Brightpath compatibility after testing, raising Premise's minimum, or providing an optional export extra restricted to supported interpreters. Do not make an unconditional dependency that silently removes Python 3.10/3.11 support. |
| Scope | Brightpath exports process inventories and supporting entities, not LCIA methods or product systems. It requires exactly one production exchange per dataset. These are explicit limits, not a complete openLCA database backup. |
| Memory | Brightpath copies canonical input and builds all JSON-LD entities in memory before writing. Benchmark a complete ecoinvent scenario; streaming/two-pass serialization may be needed. |
| Metadata | Unknown fields are retained in extension properties. Check NumPy scalars, tuples, Premise diagnostics and other metadata for JSON serialization, and avoid duplicating large internal metadata unnecessarily. |

## Evidence and validation performed

Read Premise's export implementation and openLCA guide, Brightpath's JSON-LD
codec, reference catalogs, context/document classes, pipeline, requirements,
limitations, and focused test file. Existing JSON-LD tests cover small foreground
round trips and UVEK references; they do not establish full Premise export parity.

Executed the actual `_ensure_flow` and `_schema_uncertainty` helper functions
extracted with Python AST, using minimal schema stubs (not a full package run):

- Zinc in air and Zinc in water both received UUID
  `c8bfc082-dfe8-5e45-88c9-742ead1bbed6`; only one flow remained in the store.
- Lognormal `loc=ln(2), scale=0.2` produced `geomMean=0.693147...,
  geomSd=0.2`, instead of `2` and `1.221402...`.

The uncertainty interpretation was checked against installed `stats_arrays`
and the [official openLCA uncertainty schema](https://greendelta.github.io/olca-schema/classes/Uncertainty.html).

No full Brightpath test suite, ZIP import, real scenario export, or LCIA parity
calculation was run. Inspected local Python environments did not have
`olca_schema` installed. No export behavior or dependencies were changed.

## Implementation sequence

First correct and regression-test Brightpath flow identity, uncertainty conversion,
and exact elementary-flow references. Then implement the Premise adapter with
synthetic fixtures covering compartment distinctions, local providers, resource
inputs, negative amounts, waste, metadata, repeated exports and multiple scenarios.
Finally import a real cutoff and consequential scenario into openLCA, check all
links and characterization coverage, and compare representative inventory and
LCIA results with the source database. Record peak memory and runtime.

Keep the current CSV route selectable during validation. Switch the default only
after these checks, then update the installation requirements, output naming,
import instructions, and release notes.

## Implementation update (2026-09-24)

Brightpath prerequisite fixes are committed: flow identity (`530603b`), lognormal
conversion (`eab8893`), and opt-in local method mapping (`b614533`). The mapping
reads licensed method data locally; it does not bundle the package.

Premise now has a JSON-LD adapter in `premise/olca_export.py`. The default
`write_db_to_olca` route requires Python 3.12+, ecoinvent 3.12, and an explicit
`method_package`; `format="simapro"` retains the previous route. The adapter
checks provider closure and ambiguity, namespaces process and product IDs by
scenario, and stages archives before publishing them with coverage reports.
It calls the codec directly after Premise preparation and closure checks to
avoid the pipeline's repeated full in-memory renders on complete databases.
No background migration or change to signed exchange amounts is performed.

Synthetic integration tests cover local providers, method references, negative
production and consumption, nonmutation, stable and scenario-distinct IDs,
missing suppliers, ambiguous suppliers, and failed-export preservation. Actual
openLCA import, deterministic calculation, and Monte Carlo agreement remain
separate validation steps.

### Full cached-scenario export

Exported the existing ecoinvent 3.12 cut-off REMIND SSP1-PkBudg1000 2050
checkpoint through `NewDatabase.write_db_to_olca`, retaining its semantic
certificate and running Premise export preparation/schema checks. The run used
a separate temporary cache directory to preserve existing user checkpoints.

The archive contains 42,770 processes and 1,386,053 exchanges. All 491,505
technosphere exchanges resolve to exported providers and their reference-product
flows. Every exchange resolves to a flow, flow property, and compatible unit
using the inventory archive plus the original local method package. Of 851,778
biosphere exchanges, 770,985 reference method-package flows; the remaining
80,793 exchanges use 257 known source flows absent from that package. No
unknown-source biosphere identities were found. Missing flows remain exported.

The ZIP is 227,539,127 bytes. Export completed in 212 seconds and peaked at
5.63 GB RSS on the local machine. Full-size testing exposed two quadratic copies
of accumulated exchange metadata in Brightpath (recording consumers and merging
repeated elementary flows); both were removed without dropping metadata.
Brightpath's 915 tests and Premise's 54 export/NewDatabase tests pass. Archive
CRC, provider closure, and quantity/unit-reference checks pass. This is not an
openLCA application import or numerical engine comparison.

### ISIC hierarchy and ordering

The exporter now expands ISIC codes into official UNSD Rev.4 division, group,
and class folders. Ecoinvent extensions sit below recognized ancestors. A
bundled, attributed title table preserves leading zeros and avoids network
requests during export. Processes are serialized in category order, followed
by activity identity; category placement does not change UUIDs or providers.

The same cached scenario was regenerated through `NewDatabase.write_db_to_olca`
with 42,770 processes in 325 folders. Archive checks confirm ordered category
paths, valid official parent relationships, and all provider/quantity references.
Root folders begin `01`, `02`, `03`, `05`, `06`. All 58 focused Premise tests
pass. Application display of the regenerated hierarchy remains to be checked.

### Ecoinvent 3.8 export and numerical comparison

The eight duplicate UUID aliases in `flows_biosphere_38.csv` were resolved
against the imported ecoinvent 3.8 XML biosphere definitions. The table now has
4,427 unique UUIDs. The local 2021-11-30 method package matches 3,741 source
definitions; 476 are absent and 210 have conflicting names or compartments.
Brightpath's explicit preservation policy exports conflicts with separate stable
UUIDs and reports both definitions. No name-based equivalence is assumed.

Restored the existing `test1` database in Brightway project
`ecoinvent-3.8-cutoff`: REMIND SSP3-rollBack, 2050. This was a re-export of a saved
scenario, without a new IAM build. Standard Premise normalization consolidated
11 identical deterministic exchanges by summing their amounts and repaired
uncertainty sign metadata in the export copy. The restored graph supplied the
baseline for certification; this does not revalidate its historical IAM build.
The original Brightway database was unchanged.

`NewDatabase.write_db_to_olca` produced 35,171 processes and 1,036,279 exchanges.
All 385,495 technosphere provider links and all flow/property/unit references
resolve. The inventory uses 1,992 mapped elementary flows, 226 flows absent from
the method package, and three conflicting definitions preserved separately:
`[Deleted]Carfentrazone ethyl ester`, `Indium, in ground`, and
`Carbon dioxide, from soil or biomass stock`. The coverage report distinguishes
these cases and includes original and exported UUIDs.

Imported both original methods and the regenerated inventory into an isolated
openLCA 2.6.2 Derby database and calculated 1 kWh from the EUR low-voltage
electricity market group with `ei - IPCC 2013`. The four climate indicators agree
with a Brightway LCI characterized using the same package factors and exported
coverage within 5.01e-8 relative difference (comparison tolerance 1e-7).
An independent SciPy solve of the serialized matrices agrees with openLCA within
1.49e-10 relative difference. GWP100 is 0.189578476882 kg CO2-eq in openLCA and
0.189578486090 in Brightway. Excluded definitions contributed zero for this
particular demand and method; this does not imply zero impact in other cases.
This is a deterministic climate comparison, not a Monte Carlo or all-method
validation. Results are not asserted to be bit-identical.

Diagnostic scripts are `dev/validate_olca_ei38.py` and
`dev/compare_olca_ei38.py`. Local artifacts, including `lcia-comparison.json`, are
under `export/olca/ei38/`; licensed inventory and method data are not committed.
All 61 focused Premise tests and 953 Brightpath tests pass.
