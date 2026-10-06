# Brightpath SimaPro export investigation

Reviewed 2026-09-24 on `brightpath-simapro`, branched from `olca-export`
at `ddef4b5d`. Comparison uses the local Brightpath checkout at `6f94021`
plus its uncommitted release-preparation changes. No exporter has been switched.

## Conclusion

Brightpath can supply the CSV writer, but its current SimaPro behavior is not
equivalent to Premise's exporter. Resolve the numeric, linking, and coverage
differences before making it the default. Keep scenario preparation and
certification in Premise; use Brightpath for format serialization without any
background migration.

## Differences

| Area | Current Premise | Current Brightpath | Integration consequence |
| --- | --- | --- | --- |
| Entry point | `NewDatabase.write_db_to_simapro()` prepares each scenario, calls `Export.export_db_to_simapro()`, then runs reports and cleanup. | `SimaProInventory` and `write_simapro_csv()` serialize an explicit inventory context. | Retain Premise's preparation, certification, reporting, and cleanup lifecycle. |
| Python | Premise supports 3.10+. | Requires 3.12+. | Use an optional, lazy-loaded backend with an early compatibility check, as for openLCA. |
| Process and link names | Uses original product/process strings and `Cut-off, U` or `Conseq, U`. | Capitalizes product/process initials, rewrites market names, uses `Consequential, U`; displayed process names default to `name location`. | Audit name collisions and ensure every supplier link resolves to the exported product, especially for modified scenario markets. |
| Categories | Bundled process/product lookup, with `material` and `Others\\Transformation` fallback; records unmatched categories. | Requires a production `simapro category`; optional existing-category inference is currently backed by ecoinvent 3.9/3.9.1 cut-off references. | Supply Premise's resolved categories explicitly for each version; preserve diagnostics. Do not assume inference covers 3.12 or consequential. |
| Waste classification | Dataset and supplier categories control waste sections. | Dataset `type` and name heuristics control sections; explicit `type='process'` prevents waste-process detection, while supplier detection still uses names. | Use consistent supplier-aware classification. Generic Brightway process types must not override waste semantics. |
| Waste signs | Waste-to-treatment exchanges are multiplied by -1. | Waste-to-treatment exchanges use absolute values; negative non-waste inputs inside waste activities can also become positive. | Preserve credits and signed exchanges using an explicit, tested convention. |
| Production amounts | Writes 1.0 and 100% allocation regardless of supplied production quantity. | Retains production quantity and allocation; makes waste production positive. | Check reference-product normalization and coproduct assumptions; compare normalized inventories, not only CSV text. |
| Precision | Exchange amounts use `.3E` (four significant digits). | Exchange amounts use `.15g`. | Higher precision is useful; expected rounding differences should be separated from semantic differences. |
| Uncertainty and formulas | Writes undefined uncertainty and zero distribution parameters; no parameter sections. | Writes supported uncertainties, formulas, and process/database/project parameters. Unknown distribution codes currently fall back to Undefined. | Require explicit reporting/rejection of unsupported uncertainty and test sign/unit transformations. |
| Water | Converts water emissions to air/water from m3 to kg by multiplying the amount by 1000, in place. | Converts non-resource flows named Water on a copy, including soil; distribution fields are not rescaled. | Guard conversion by source unit, preserve input, and transform all uncertainty/formula fields consistently. |
| Biosphere names | 331 name mappings. | 278 name mappings; 22 shared names map differently, 100 keys occur only in Premise, 47 only in Brightpath. | Resolve against the intended SimaPro reference/library; these counts alone do not establish correctness or missing-flow counts. Both writers otherwise fall back to the original name. |
| Units | 28 mapping entries; unknown units are passed through. | 16 mapping entries; unsupported units fail validation/serialization. | Audit real scenario unit coverage and aliases. Twelve Premise keys are absent; `vehicle-kilometer` maps to `km` versus `vkm`. |
| Subcompartments | 18 entries and fallback to source spelling; resources specially blank `fossil well`. | 16 entries; unmapped subcompartments fail. | Explicit `unspecified` and `fossil well` need handling; shared entries agree. |
| Excluded flows | No equivalent SimaPro blacklist in this writer. | A blacklist omits flows such as Oxygen and turbine water; renderer emits unused-exchange warnings. | An unused warning must not silently authorize inventory loss. Define a loss policy and coverage report. |
| Metadata and identifiers | Writes Premise generator, geography, documentation URL, system description, and UUID comments. Process UUIDs are regenerated on each export. | Supports explicit SimaPro metadata and process identifiers; these Premise-specific comments/defaults are not automatically recreated. | Populate scenario provenance deliberately and decide which identifiers must remain stable. |
| Encoding and layout | Latin-1, semicolon-separated CSV, SimaPro 9.1 header; all ordinary inputs in Materials/fuels. | Same encoding and separator, SimaPro 9.5 header; energy inputs in Electricity/heat, Unicode normalization and parameter sections. | Confirm import in the target SimaPro version and compare linking across sections. |
| Input mutation and memory | Adds `used` markers and modifies water amounts; writes rows incrementally. | Preserves caller data, but renders the entire row list in memory. | Keep non-mutation and benchmark peak memory on a complete Premise scenario. |

### Mapping examples requiring review

- `Formate`: Premise maps to `Formic acid`; Brightpath maps to
  `Formic acid, thallium(1+) salt`.
- `Water, unspecified natural origin`: `Water_3.1_m3` versus
  `Water, unspecified natural origin/m3`.
- `Copper, ion`: `Copper, ion` versus `Copper`.

These are observed mapping differences, not a determination that either target
library's convention is correct.

The twelve extra Premise unit keys are `Sm3`, `cubic meter-year`, `guest night`,
`kg*day`, `kilo Becquerel`, `kilogram day`, `kilometer-year`, `litre`,
`passenger-kilometer`, `person kilometer`, `standard cubic meter`, and
`ton-kilometer`. Some are aliases; compare normalized identities before adding
new physical conversions.

## Synthetic verification

Ran both writers on a two-process inventory containing a production amount of
2, signed waste exchanges, water with normal uncertainty, and an oxygen resource.
Premise's categories were supplied to Brightpath explicitly. Brightpath also
needed `reference product` alongside exchange `product`, and activity comments.

- Production amount: Premise wrote 1; Brightpath wrote 2.
- Waste input -0.123456789: Premise wrote +0.1235; Brightpath +0.123456789.
- Waste input +0.25: Premise wrote -0.25; Brightpath +0.25. This changes sign,
  independently of numeric precision.
- Water amount 0.00123456789 m3: Premise wrote 1.235 kg; Brightpath 1.23456789 kg.
- Water normal standard deviation 0.0001 m3: Brightpath wrote the squared
  parameter as 1e-8 after converting the mean to kg. Rescaling the standard
  deviation by 1000 would give a squared parameter of 0.01 kg2.
- Premise retained Oxygen; Brightpath omitted it and emitted
  `simapro_exchange_unused`.
- `('air', 'unspecified')` failed Brightpath serialization. Reducing this to
  `('air',)` allowed the other differences to be examined.
- Giving both activities `type='process'` removed Brightpath's Waste treatment
  section, even though the waste supplier name still matched its waste heuristic.
- Premise mutated its input; Brightpath did not.

Temporary reproduction script and outputs:
`/private/tmp/compare_premise_simapro.py` and
`/private/tmp/premise-simapro-comparison/`.
These tests exercise the writers directly, not the complete NewDatabase path.
No native SimaPro import or full-scenario comparison was performed.

## Proposed implementation sequence

### First reviewed changes (2026-09-24)

Implemented in the local Brightpath working tree, without changing Premise's
export backend:

- Water-emission conversion checks the input unit, preserves kilogram inputs,
  scales normal standard deviation and distribution bounds, and retains
  dimensionless lognormal scale and signed quantities.
- Unsupported water units/distributions and unevaluated water exchange formulas
  fail explicitly. Inspection clarified that Brightpath supports calculated
  parameter formulas, but the exchange writer itself currently emits numeric
  amounts; the broader formula claim in the comparison table needs that distinction.
- Empty and explicit `unspecified` biosphere subcompartments serialize as blank;
  other unknown subcompartments still fail.
- Added 29 regression cases using Brightway's independent SimaPro uncertainty
  parser, including negative amounts, existing kilogram inputs, resource water,
  distribution bounds, non-mutation, and repeatability. Full suite: 982 passed.

The distribution interpretation was checked against
[Brightway's SimaPro CSV extractor](https://docs.brightway.dev/en/latest/_modules/bw2io/extractors/simapro_csv.html).
This establishes numeric serialization behavior, not native SimaPro acceptance.
Disputed mapping names remain pending their own evidence.

### Waste-sign follow-up (2026-09-24)

The Brightpath working tree now uses reversible sign reflection for ecoinvent
waste production and links to waste suppliers. Ordinary inputs inside waste
processes retain their signs, including credits. Uncertainty bounds are negated
and swapped; normal/lognormal spread is unchanged and location/sign metadata
follows the reflection. Unsupported reflected distributions and unevaluated
export formulas fail explicitly.

Classification uses production categories and exact included supplier
identities, with explicit exchange categories before name heuristics for
external suppliers. Import uses the CSV category and exchange section before
name heuristics. Conflicting included supplier categories fail.

Added 26 regression cases spanning cut-off and consequential contexts. These
include positive/negative uncertain waste links, neutral waste-process names,
misleading material-process names, generic Brightway process types, and a linked
inventory whose full technosphere matrix and solved supply vector survive CSV
export and re-import. The physical example requires 0.2 treatment units and
1/6 material-production units after its credits.

The UVEK writer's sign convention, external suppliers known only by name, and
native SimaPro import/LCIA remain outside this validation. Premise's exporter
has not been switched.

### Reference-flow clarification

The agreed rule distinguishes waste reference flows from links to treatment:
the reference quantity in `Waste treatment` is positive (`abs(amount)`), while
links in `Waste to treatment` are sign-flipped (`-amount`). Thus either source
reference sign gives +1 for a unit process; non-unit magnitudes are retained.
This supersedes the production-sign reflection described above. Brightpath now
applies the distinction to both ecoinvent and UVEK; ordinary signed inputs retain
their signs. The previously generated comparison CSVs predate this correction.

### Remaining sequence

1. Correct and test Brightpath's waste classification/sign handling, water
   uncertainty conversion, unsupported-distribution policy, and unit/compartment
   coverage. Make dropped exchanges explicit under the chosen loss policy.
2. Add a small Premise adapter that supplies exact technosphere/biosphere
   contexts, normalized detached records, resolved categories, and scenario
   metadata. Keep the existing writer available while validating an opt-in
   Brightpath backend. Do not run background migration during serialization.
3. Reconcile biosphere mappings and test emitted supplier-name uniqueness and
   complete internal linking. Preserve distinct scenario datasets even when
   Brightpath's market-name normalization would otherwise merge their labels.
4. Compare a complete existing scenario through both exporters: process/exchange
   counts, normalized quantities and signs, units, flow coverage, metadata,
   uncertainty, output size, and peak memory. Repeat for consequential behavior
   and the supported ecoinvent versions before claiming broad compatibility.
5. Import the new file into SimaPro, verify provider linking and selected LCIA
   results, then consider changing the default and retiring the legacy writer.

## Source locations

- Premise: `premise/new_database.py:write_db_to_simapro`,
  `premise/export.py:Export.export_db_to_simapro`, and
  `premise/data/utils/export/simapro*`.
- Brightpath: `brightpath/formats/simapro_csv.py`,
  `brightpath/profiles/simapro.py`, `brightpath/profiles/simapro_categories.py`,
  `brightpath/utils.py`, and `brightpath/data/export/simapro*`.
