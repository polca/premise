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

This proves supplier connectivity, not final fuel completion. Native vehicle “Fuel supply” records still layer their historical blend on the new delivery mix, and their reference-energy basis must be reviewed before replacing that blend. Combustion CO2 must then follow the final linked fuel origin through the existing carbon routines. The existing 39 unresolved fuel-carbon-origin warnings remain visible. These end-use blend/carbon checks, converted-helper coverage, other registered sectors and multi-geography electricity prototypes remain unfinished. Do not enable public fuel support from connectivity or finite-LCI evidence alone.
