# Independent coal technology inventories

Native UVEK hard-coal and lignite records previously served both Coal PC and Coal CHP. The shared efficiency updater iterates an unordered technology intersection and adjusts each record only once. Because the two roles have different efficiency limits, whichever role ran first determined the result. An electricity portfolio could also be mistaken for a combustion generator: its purchased kWh implied an apparent efficiency near one, then the generator limit inflated its electricity purchases. The original failure and synthetic reproduction remain in `uvek-electricity-role-overlap-evidence.json`.

The correction preserves the shared Premise stages and normal user workflow. The fuel and electricity inventory models are not replaced by another UVEK engine. YAML controls two optional behaviors:

```yaml
Coal CHP:
  uvek_aliases:
    technology_copy_suffix: " [Premise Coal CHP co-generation proxy]"
    efficiency_requires_fuel_input: true
```

Coal PC also requires a declared fuel, but keeps its original supplier identities. All existing alias selectors and native proxy limitations remain. Other source families retain their default behavior; no NewDatabase argument or notebook change is required.

The existing missing-powerplant stage copies each complete native CHP template, preserving its product, unit, reference output, geography, allocation, signed exchanges, uncertainty and production weights. Production names/codes change to identify the copy. Electricity links within a copied portfolio point to the corresponding CHP copies, rather than the PC records. Matching selects these copies after creation and retains templates before creation. Repeated calls do not duplicate them. Source identity and limitations remain in exported metadata.

The suffix deliberately includes `co-generation` so the existing historical coal routine selects its CHP branch. This remains the approved historical allocated-inventory proxy, not evidence of a dedicated future CHP fleet. The normal IAM efficiency calculation follows afterwards. No separate heat credit, fuel split, allocation adjustment or invented inventory is added.

Both efficiency stages check the optional fuel requirement. Positive declared fuel inputs identify physical generators. A nonempty electricity-only portfolio of mapped suppliers remains at its original coefficients. An unmatched combustion record raises an explicit mapping error; it is not silently classified as a portfolio. This additional check is necessary because the existing efficiency calculator falls back to arbitrary MJ/kWh inputs when no declared fuel matches.

Mappings alone could not make complete, independent copies with closed country links or distinguish a portfolio from a fuelled generator. The two existing Python modules therefore receive small generic extensions: the existing proxy stage creates the declared copies, and activity matching selects them afterwards. No dedicated source-family update callback is introduced.

## Verification

Two complete REMIND SSP2-NPi 2050 five-sector diagnostic exports with Python hash seeds 1 and 2 contain 14,795 activities each. Each adds 45 intentional CHP records: 39 physical generators and six portfolios. Coal PC retains separate originals. The 256 normalized coal exchange rows have identical signatures across the two seeds. All 88 portfolio trade rows across both roles, including 352 uncertainty-field checks, retain their source values; neither historical nor IAM generator efficiencies scale those portfolios. Every exported technosphere link resolves internally. Six representative plant and EUR/World electricity demands have finite LCIs.

The focused mapping, transformation, electricity, source-axis and validation regression group passes 141 tests. Two legacy key-dependent ecoinvent 3.5 fixtures are skipped in the isolated test setup. A separate real ecoinvent 3.12 cut-off biomass/electricity/heat update completes with 29,640 activities, seven validation phases and no errors; its 85 existing warnings remain. That regression does not create a Brightway export. Black passes, and Ruff introduces no new findings; five pre-existing electricity-module findings remain. Documentation reference generation and its check pass without changing generated tables.

The five-sector exports have no validation errors; the 14 existing electricity CO2 warnings remain. Fuels still use a private diagnostic scope bypass and are not publicly enabled. Other electricity technologies, native end-use fuel-carbon coverage, remaining heat roles and other sectors still require work. The companion JSON receipt records exact export names, hash seeds, signatures, scopes and limitations.

A separate standard public four-sector build, without any scope bypass, exports `BAFU:2026-remind-SSP2-NPi-2050-four-sector-coal-copy-review-20261111-173543` with 13,882 activities and the same 45 intentional coal CHP copies. All technosphere links resolve internally; EUR/NEU/World high-voltage markets, EUR medium/low-voltage markets and a representative CHP proxy have finite LCIs. All 9 pre-export phases have zero errors; 14 existing warnings remain. This confirms the new source-family declarations work through the colleague's ordinary four-sector workflow.
