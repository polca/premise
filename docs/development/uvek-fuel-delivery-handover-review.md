# Native fuel delivery handovers

The shared fuel update now exports a five-sector UVEK scenario with 14,750 activities. The latest reviewed export is `BAFU:2026-remind-SSP2-NPi-2050-five-sector-handover-review-20261111-155713`. This remains a diagnostic build: the public fuel support guard is unchanged.

## Preserve product boundaries

Liquid fuel handovers belong at the delivery market, not at a refinery output. The YAML therefore redirects native petrol, diesel and kerosene storage identities to the existing IAM markets. Producer-specific refinery outputs and explicitly fossil-only storage remain separate. This avoids substituting a fuel blend for a specified material feedstock or fossil commodity.

The existing shared market builder still creates the mixes, converts energy shares and selects regional suppliers. Native delivery templates supply their freight, infrastructure, water, waste services and biosphere exchanges. Fuel constituents are replaced by IAM shares. The declaration identifies those constituents by name: a unit-only filter would incorrectly discard kg-based water and waste services. Original delivery auxiliaries are retained in metadata so incremental updates do not mistake a previous IAM handover for the original template. RER is the reviewed fallback outside available native coverage; grade, volatility, allocation, loss and vintage differences remain screening limitations.

## Keep gas trade outside its own mixture

Native country gas supplies trade with one another. Turning their original identities into IAM forwarders without separating that network makes a nominal fossil component consume the new mix. A first-hop check is insufficient.

The shared proxy selector identifies the country templates needed for the IAM regions. Only their closed supplier network is preserved, with distinct country names to prevent geographical relinking from changing bilateral quantities. The final build needs 33 baseline records, all referenced; 31 unnecessary copies from an earlier diagnostic were eliminated. They preserve 113 trade exchanges exactly, including 452 uncertainty-field checks. The maximum trade-quantity difference is zero. Regional fossil gas copies use that preserved network rather than mixed delivery forwarders. Original native gas MJ outputs forward using the reviewed 36 MJ/m3 basis.

These baseline records are intentional supply components. They hold the original country trade links while the public delivery identities supply the future mix. They are not an alternative sector engine.

## Why generic Python adaptations were needed

Mappings alone could not express a retained geographic forwarding identity with a different target product/unit, provide correctly filtered native market auxiliaries, or keep a coupled source trade network separate while changing its delivery identities. The adaptations extend the existing geographic-forwarding and market-building helpers; source-specific identities and decisions remain in YAML. New optional arguments preserve default behavior. No new UVEK sector module or NewDatabase argument is introduced.

Conditional native gas entries require available IAM gas data. Missing data does not create supply copies or regionalize those entries. Repeated updates retain the existing baseline rather than duplicate it. An optional converted-market handover may be absent when that helper was not prepared; required positive-share suppliers are still checked by the normal market builder.

## Evidence and remaining work

298 mapping, transformation, fuel/carbon and heat tests pass. Black passes; the modified production files introduce no Ruff findings, although pre-existing findings remain. Tests include an independent two-country trade solve, signed fuel credits, uncertainty retention, global forwarding, explicit replacement approval, unused-country exclusion and absent-IAM behavior.

The real five-sector run completes all callbacks and Brightway export. Eight representative fuel LCIs are finite and every technosphere link resolves internally. The direct native delivery/interface audit finds IAM paths for all 208 petrol, 531 diesel, 42 kerosene and 484 gas purchases in its declared scope. Explicitly fossil-only supplies do not reach the mixed fuel commodity. Outside the intentional supply handovers, the comparison finds no changed fuel-exchange quantities or uncertainty; 5,504 rows, including 5,310 uncertain rows, are preserved. Exact counts and scopes are in the accompanying verification and consumer-audit JSON files. Raw upstream production and directly specified production routes are not blanket candidates for delivered-market substitution.

## Vehicle blend follow-up

A subsequent standard five-sector diagnostic export (`BAFU:2026-remind-SSP2-NPi-2050-five-sector-handover-review-20261111-163643`) retains 14,750 activities. All internal supplier links resolve, eight representative fuel LCIs are finite, and the 33-country fossil gas baseline still preserves all 113 trade exchanges and 452 uncertainty fields exactly. Pre-export validation reports no errors; 105 fuel warnings remain visible, including origin limitations propagated to forwarded supplies. These are not silently assigned fossil or biogenic origin.

The four native CH/RER vehicle-fuel supply records now forward directly to the IAM mix instead of adding their historical biofuel fraction on top. Their nominal reference energy is calculated from their actual signed ingredient quantities and explicitly declared shared heating values. The gasoline references are 42.1814 MJ/kg (CH) and 41.956 MJ/kg (RER); diesel is 42.705 and 42.7 MJ/kg. The forwarding quantity is original reference energy divided by the target market's reference heating value. Downstream kg demands remain unchanged. Repeated updates reuse the original reference rather than compound the adjustment. Unknown ingredients fail explicitly.

The 95% ethanol convention is a dry-ethanol reference in solution, not another 0.95 dilution. The native wood documentation and the corresponding ecoinvent whey reference support that interpretation. Grade, allocation, vintage and heating-value uncertainty remain screening limitations: this calibrates nominal energy, without claiming propagation of ingredient uncertainty.

The existing shared vehicle carbon calculation now recognizes native capitalization and reads properties from declared fuel forwarders on their retained reference basis. It does not introduce another vehicle engine or apply full oxidation to industrial residual emissions. In the written database, all 526 resolved car/truck exhaust pools agree with final linked fuel properties. Their purchased quantities and uncertainty match the preceding five-sector scenario; this comparison deliberately allows the normal IAM efficiency changes relative to the original historical database. No unresolved vehicle balances occur within this classification.

235 focused fuel, carbon, handover, energy-basis and source-axis tests pass, including a hybrid-car regression that keeps unrelated electricity inputs outside fuel-provider validation. Black passes. Ruff reports only the six existing unused imports in `markets.py`; carbon and the new tests are clean. Generic Python changes are necessary to calculate the source blend's energy dynamically and carry linked carbon properties through its retained identity; source ingredients and heating-value declarations stay in YAML.

The two new JSON receipts record the actual export and comparison scope. Public fuel support remains disabled. Rail naming coverage, other transport modes, industrial combustion, converted-helper coverage, other registered sectors and multi-geography electricity prototypes remain unfinished. Connectivity, finite LCIs and the 526 vehicle checks do not prove those broader requirements.

## Next carbon coverage gate

An independent scan of the written native records finds 541 positive-CO2 datasets outside the current car/truck/train classification that purchase a declared fuel forwarder: 443 other combustion/process records, 33 rail, 31 bus, 29 aviation and five shipping records. These are review candidates, not 541 established errors: process emissions, indirect fuel use and separately specified combustion boundaries must be distinguished before changing emissions. The accompanying remaining-coverage JSON records the selection and bounded identity examples. This demonstrates why public fuel support must remain gated despite the successful vehicle checks.

## Scoped rail and bus exhaust refinement

Exact name/unit selectors in the fuel YAML identify reviewed native rail and diesel-bus operating services. Mixed electric rail with diesel shunting remains mixed traction; no transport technology is replaced. The existing rail convention is reused: preserve the recorded positive CO2 exhaust total and refresh only its carbon origin from final linked fuel purchases. This avoids imposing a new full-oxidation total on the native engine model. Fuel demand, uncertainty, negative capture credits, other emissions and infrastructure remain unchanged. Historical combustion performance and allocation remain screening limitations.

A small optional selector argument extends the existing shared carbon routine; the standard fuel callback obtains selectors from the source-family YAML. The empty default preserves ecoinvent behavior. Mappings alone could not extend the previously hard-coded car/truck/train prefix selection. No alternate sector implementation is introduced. The selectors do not by themselves operationalize the bus or train sector updates.

The new five-sector export (`BAFU:2026-remind-SSP2-NPi-2050-five-sector-handover-review-20261111-165116`) has 46 selected records: 30 bus and 15 rail exhaust balances verified, plus one without supported direct fuel left unchanged. Each verified balance preserves its previous positive exhaust total and agrees with its final linked fuel-origin fraction. Every non-link technosphere field is unchanged relative to the preceding five-sector export, including uncertainty. Representative bus and rail LCIs are finite. 237 focused regressions pass, including changed origin fractions, repeated updates, infrastructure exclusion, signed credits and family/biosphere forwarding. Black passes; the changed carbon code and tests pass Ruff, and the base module passes Ruff.

This is a bounded refinement of the earlier 541-candidate audit, not completion of transport or industrial carbon coverage. The remaining candidate groups must still be inspected, and the public fuels guard remains unchanged. See `uvek-recorded-transport-exhaust-verification.json` for exact evidence.

## Remaining conventional transport identities

The earlier name-based queue misclassified a forestry trailer and an agricultural trailer as rail. These are outside the rail refinement. Additional exact operating-service selectors now cover the other reviewed rail/locomotive, coach, diesel shipping and aircraft identities. They reuse the recorded-exhaust-total convention, preserving native engine performance, mixed traction, fuel demand and uncertainty. No manufacture or infrastructure inventory is selected from a substring. Explicit per-gram locomotive combustion records retain their own reference unit and fuel quantity.

The shared carbon reader additionally recognizes kerosene, using the linked supplier properties rather than a new heating-value assumption. This is the only further production Python change for this group. A canonical ecoinvent 3.10 high-altitude test confirms that non-fossil CO2 uses the available unspecified-air flow when an exact altitude flow is absent, with consistent categories/code.

The companion extended-transport receipt records the new five-sector export, positive exhaust balances, unchanged purchased quantities and uncertainty, preserved negative credits and other emissions, and representative finite bus, rail, shipping and aviation LCIs. 240 focused regressions pass; changed Python and tests pass Black and Ruff. All pre-export validation phases have zero errors, with warnings retained. These mappings do not operationalize separate transport-sector updates. Industrial combustion and other unreviewed identities remain open, and public fuel support remains gated.

## Captured-carbon metadata interpretation

Two published CLIC capture inventories retain the field `share biogenic CO2`, while the carbon resolver previously recognized only `share biogenic co2`. Their declared fractions are 0.06 (cement capture) and 0.6136363636363636 (municipal-waste capture). The reader now accepts both spellings, rejects conflicting values and retains finite/range validation. Published metadata, capture inventories and quantities are unchanged. A mapping cannot repair a reader that ignores the existing metadata field; this small generic Python adaptation is necessary. Unknown sources without this information still remain unresolved.

249 focused tests pass, including both spellings, agreeing duplicates, conflicting duplicates, boundary fractions, non-finite/out-of-range fractions and the absence of metadata mutation. Black and Ruff pass for the changed production file and tests. A controlled before/after five-sector run resolves all 105 previously unknown-origin fuel-property records. All 467,354 identity-normalized exchange rows have identical scientific signatures, including linked suppliers, quantities, compartments and uncertainty; exchange order and exported database/code tuples are excluded. Representative EUR/World gas LCIs are finite. Pre-export phases report no errors or fuel warnings; the 14 existing electricity CO2 warnings remain. Exact run names, hashes and declared fractions are in `uvek-capture-carbon-metadata-verification.json`.

The comparison fixes Python's hash seed only in the private diagnostic to isolate this metadata change. Uncontrolled runs exposed an independent coal-electricity alias overlap, documented in `uvek-electricity-role-overlap-evidence.json`. It is unfinished work, not corrected by imposing a seed on users.

## Reproduced electricity role overlap: next required correction

The native Coal PC and Coal CHP aliases currently select the same hard-coal/lignite records. The shared efficiency updater obtains technologies from a set, applies the first role and skips another update once a new efficiency is recorded. The roles have different maximum efficiencies (0.4 versus 0.45). Two real exports consequently assign different efficiencies to the same ENTSO-E coal portfolio. A synthetic reproduction of the existing method independently yields 2.5 versus 2.2222222222222223 kWh of portfolio input per kWh output when role order is reversed.

This also exposes a boundary problem: a country-weighted electricity portfolio should not acquire a combustion efficiency by treating its purchased kWh as fuel. The next correction must separate overlapping physical technology copies and distinguish portfolios from generators through reviewed mappings and minimal shared adaptations. Verify different role/hash orders; merely sorting roles would hide the modelling conflict. This requirement belongs to the existing technology-duplication and filter-coverage goal. The successful exports and metadata tests do not qualify full electricity reproducibility or the whole integration.
