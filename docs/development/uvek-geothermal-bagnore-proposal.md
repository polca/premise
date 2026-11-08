# Geothermal pilot proposal: Bagnore with native UVEK supplies

Status: **pending scientific approval**. The candidate is private; no generator
has been installed or enabled in Premise or Brightway. This proposal covers only
the UVEK 2026 cut-off / ecoinvent 3.10 biosphere electricity pilot.

The source is Tosti et al. (2020), [Complete Data Inventory of a Geothermal Power
Plant for Robust Cradle-to-Grave Life Cycle Assessment Results](https://www.mdpi.com/1996-1073/13/11/2839),
with its [public inventory supplement](https://mdpi-res.com/d_attachment/energies/energies-13-02839/article_deploy/energies-13-02839-s001.zip).
The paper specifies ecoinvent **3.5** for its original background. This proposal
curates a new native-background inventory; it does not claim general executable
Brightpath migration support for that source release.

## Concrete candidate and validation

- One allocated electricity generator, producing 1 kWh at plant, plus two
  unrolled-steel supply helpers; no pipe helpers are needed.
- All 94 published detailed inventory rows are covered, expanding compound
  material/processing references into 104 generator exchanges.
- All 43 published background-product categories have explicit native supplies
  or the two derived metal helpers, listed below.
- A linear background-demand LCI solve, combined with direct foreground flows,
  is finite. Before/after hashes verify unchanged native technosphere and
  biosphere activity/exchange contents. This is candidate feasibility evidence;
  scenario transformation, certification and export are still outstanding.
- Native input uncertainty is retained in the metal helpers. The supplement's
  input Data Quality cells are blank, and its selected quantity cells contain
  no formulas. No input distributions are inferred from the result STDV sheet.

## Modelling choices proposed for approval

The Bagnore installation is an Italian hydrothermal flash plant. Applying its
inventory to generic global IAM geothermal supply is a geographic and technology
screening assumption. Its allocated direct releases are approximately
**0.391 kg CO2 and 0.0115 kg methane per kWh**; these substantial geogenic releases
must remain visible rather than being removed to obtain a lower score.

Use the paper's **95% electricity allocation once**, with no additional heat
credit. Construction, maintenance and closure quantities follow the detailed
published rows and their reconstructed per-kWh basis. Operational releases are
already per kWh and are not divided by lifetime output again. The source boundary
excludes exploration, well testing and machinery dismantling/recycling; no
unreported geothermal-energy or direct land-occupation flows are invented.
Those boundaries limit interpretation of resource and land indicators.

The source's numerical descriptions conflict and are retained in provenance:

| Source description | Value |
| --- | ---: |
| Paper installed electric capacity | 61 MW |
| Paper annual net electricity / load factor | 544 GWh / 99% |
| Energy implied by 61 MW × 8760 h × 99% | 529.0164 GWh |
| Annual output implied by detailed commissioning summaries | 544.1004 GWh |
| Allocation-sheet annual electricity | 536.8 GWh |
| Allocation-sheet electricity factor | 95.1160824% |

The selected published inventory basis is not silently recomputed from capacity
or load factor, and the alternative allocation factor is not applied a second
time. The stated figures could involve nominal/operating-capacity or reporting
period differences; this inspection does not establish their cause.

Use complete native stainless-steel and HDPE pipe products once. The published
PE mass summary and detailed pipe length independently imply **3 kg/m**, used to
convert the metre-based PE pipe demand to the native per-kg supply. Nominal size,
manufacturing technology, delivery and vintage remain proxy limitations.

Generic raw chromium steel and low-alloyed steel use two helpers derived from
the native 37% electric / 63% converter production mixes. The helpers retain the
native producer inputs and their uncertainty, excluding only the explicit hot
rolling that belongs to the broader native rolled-product definitions.
Separately listed source forming services remain included. The specifically
hot-rolled stainless-steel demand uses the complete native rolled product.
Raw bentonite uses **Bentonite, at mine**, preserving its extraction-stage
identity rather than adding chemical activation.

Use the native 71.5%-water residual-material-landfill service for drilling waste,
the native underground-deposit service for spent hazardous sorbent, and complete
native hazardous-waste incineration for spent mineral oil. These are endpoint
screening choices: the paper's landfill wording, its generic original market
references and the native technologies do not establish identical treatment
mixes. No exact ecoinvent 3.5 technosphere market inventory is available locally;
adjacent 3.6 evidence includes landfarming and cement co-processing alternatives.
Preserve the published waste kg without moisture or heating-value correction,
and add no recovery credit beyond the complete native inventories. These output
waste masses become positive demands for positive-reference treatment services.

All nine measured air releases retain the published non-urban compartment.
Arsenic and mercury use unchanged ecoinvent 3.5/3.10 flow UUIDs despite the current
labels “Arsenic ion” and “Mercury II”; no molecular-mass correction is applied.
Geogenic carbon uses fossil-carbon labels. Four direct water inputs retain the
Italian qualifier in provenance and use the canonical generic water-resource
flow, with the exact litre-to-m³ conversion. This does not reproduce a separate
regionalized water-flow definition or establish chemical speciation measurements.

Every supply retains geography, grade, recycled-content, production/market,
allocation and vintage limitations. Matching products and reference quantities
does not establish equal original/native LCIA results. The upstream exclusions
in the imported UVEK bundle remain recorded.

## Explicit supply crosswalk

The stock helpers are proposal foreground records, not edits to the installed
UVEK database. All other entries below are existing complete native suppliers.

| Published background product | Proposed supply | Geography | Native unit |
| --- | --- | --- | --- |
| aluminium, primary, ingot | Aluminium, primary, at plant | RER | kilogram |
| bentonite | Bentonite, at mine | DE | kilogram |
| cellulose fibre, inclusive blowing in | xx Cellulose fibre, inclusive blowing in, at plant | CH | kilogram |
| cement, Portland | Portland cement, strength class Z 42.5, at plant | CH | kilogram |
| chromium steel pipe | Pipe, stainless steel | CH | kilogram |
| copper | Copper, at regional storage | RER | kilogram |
| diesel, burned in building machine | Diesel, burned in building machine, average | CH | megajoule |
| diesel, burned in diesel-electric generating set, 10MW | Diesel, burned in diesel-electric generating set | GLO | megajoule |
| drawing of pipe, steel | Drawing of pipes, steel | RER | kilogram |
| drilling waste | Disposal, drilling waste, 71.5% water, to residual material landfill | CH | kilogram |
| excavation, hydraulic digger | Excavation, hydraulic digger (proj. 500) | RER | cubic meter |
| glass fibre | Glass fibre, at plant | RER | kilogram |
| glass wool mat | Glass wool mat, at plant | CH | kilogram |
| gravel, crushed | Gravel, crushed, at mine | CH | kilogram |
| gravel, round | Gravel, round, at mine | CH | kilogram |
| hazardous waste, for underground deposit | Disposal, hazardous waste, 0% water, to underground deposit | DE | kilogram |
| hydrochloric acid, without water, in 30% solution state | Hydrochloric acid, 30% in H2O, at plant | RER | kilogram |
| iron pellet | Pellets, iron, at plant | GLO | kilogram |
| iron-nickel-chromium alloy | Iron-nickel-chromium alloy, at plant | RER | kilogram |
| lubricating oil | Lubricating oil, at plant | RER | kilogram |
| metal working, average for chromium steel product manufacturing | Chromium steel product manufacturing, average metal working | RER | kilogram |
| pentane | Pentane, at plant | RER | kilogram |
| polyethylene pipe, DN 200, SDR 41 | Polyethylen (HDPE) pipe | CH | kilogram |
| polyvinylchloride, bulk polymerised | Polyvinylchloride, bulk polymerised, at plant | RER | kilogram |
| polyvinylchloride, emulsion polymerised | Polyvinylchloride, emulsion polymerised, at plant | RER | kilogram |
| polyvinylchloride, suspension polymerised | Polyvinylchloride, suspension polymerised, at plant | RER | kilogram |
| reinforcing steel | Reinforcing steel, at plant | RER | kilogram |
| section bar extrusion, aluminium | Section bar extrusion, aluminium | RER | kilogram |
| section bar rolling, steel | Section bar rolling, steel | RER | kilogram |
| selenium | Selenium, at plant | RER | kilogram |
| sheet rolling, aluminium | Sheet rolling, aluminium | RER | kilogram |
| sheet rolling, copper | Sheet rolling, copper | RER | kilogram |
| sodium hydroxide, without water, in 50% solution state | Sodium hydroxide, 50% in H2O, production mix, at plant | RER | kilogram |
| steel, chromium steel 18/8 | steel, chromium steel 18/8, unrolled native production mix, Bagnore proposal | RER | kilogram |
| steel, chromium steel 18/8, hot rolled | Chromium steel 18/8, at plant | RER | kilogram |
| steel, low-alloyed | steel, low-alloyed, unrolled native production mix, Bagnore proposal | RER | kilogram |
| steel, unalloyed | Steel, converter, unalloyed, at plant | RER | kilogram |
| stone wool, packed | Rock wool, packed, at plant | CH | kilogram |
| sulfuric acid | Sulphuric acid, liquid, at plant | RER | kilogram |
| titanium dioxide | Titanium dioxide, chloride process, at plant | RER | kilogram |
| waste mineral oil | Disposal, used mineral oil, 10% water, to hazardous waste incineration | CH | kilogram |
| wire drawing, copper | Wire drawing, copper | RER | kilogram |
| wire drawing, steel | Wire drawing, steel | RER | kilogram |

## Pins and scope

- Public supplement SHA-256: `257dd7052e219e39fb3c2088fdbff7935dd186bf400bfeaea148c87bcd2bbed5`.
- Native bundle revision: `794d90f2c2dbb90f0c56ba8cc527fa1494aa8ef6`.
- Candidate inventory SHA-256: `06e77b5921fa2ae34638cdbbb976b415609f411ec56b9e14e46e0729f3585d86`.

The full private candidate and row-by-row review manifest are
`/private/tmp/premise-uvek-2026/geothermal_bagnore_uvek2026_candidate.json`
and `geothermal_bagnore_uvek2026_candidate_review.json` in the same directory.
Numerical feasibility and source-content hashes are in
`geothermal_bagnore_uvek2026_candidate_lci.json`.

If approved, this candidate would be registered through the existing inventory
preparation and native electricity-role paths, preserving the current user
experience. Approval would cover this pinned pilot inventory and its stated
proxies; it would not approve other pending supplier mappings, alter IAM
geothermal shares or enable conversions between system models.
