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
