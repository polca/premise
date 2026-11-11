# Separate fuel supply references from carrier classification

The real standard UVEK fuel callback failed in natural-gas market annotation with `Fuel 'natural gas' requires 'cubic meter', received 'megajoule'`. The native aliases are also used to classify heat inputs; replacing them globally would remove native MJ carriers from that classification. Altering the shared natural-gas heating value would instead corrupt ecoinvent's m3 basis. Neither is a valid mapping-only solution under the existing interface.

The existing reviewed helper `fuels-312-8474b2c5b3efba4ba611` already supplies one m3 using 36 MJ of complete native high-pressure gas. It is reused, with its existing delivery-boundary, geography and vintage limitations. No new helper, scaling factor, omission or fuel-energy equation is introduced.

The minimal generic adaptation adds an optional `for_markets` selection to `InventorySet.generate_fuel_map`. Three existing fuel-stage call sites request it; the default used by heat and other classification remains unchanged. Only aliases declaring `market_suppliers` are overridden, currently UVEK natural gas. Ecoinvent has no override and retains identical filters. The exact prepared helper name includes the normal CLIC namespace, preventing a name collision with source markets.

The existing explicit `allow_generated_helpers` flag must survive both filter gates. Previously the native gate could admit a helper only for the subsequent CLIC-slug gate to discard it. The second gate now honors the same flag for generated helpers that already matched the declared physical name/product/unit filter. Ordinary unbound published CLIC datasets remain excluded. Tests exercise this distinction and exclude a wrong unit and an unreviewed helper variant.

The methane YAML lists this helper for the existing regional-copy stage. The shared copy, supplier selection, market blending, carbon tracking and strict unit/energy checks are unchanged. Heat retains its original native MJ aliases; there is no alternative UVEK sector engine.

Verification on 11 November 2026: 250 mapping, fuel energy/carbon and heat tests pass. The real REMIND SSP2-NPi 2050 standard fuel callback completes after preparing 484 records. It records 39 unresolved-carbon-origin warnings and no sector-contract error; those warnings remain visible and need component-level review. This run used a private diagnostic bypass of the public support guard and created no export. A combined five-sector export and consumer-link inspection are the next qualification steps. Passing this callback does not establish that native consumers use the new IAM fuel blends.

Lint: the modified mapping module and tests pass. The sole Ruff finding across the four touched production modules is the pre-existing unused `ws` import at line 4 of `liquid_fuels.py`; that line is unchanged. Black passes for the changed modules and tests.
