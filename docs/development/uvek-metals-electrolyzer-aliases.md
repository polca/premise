# Native electrolyzer stack filters

The native PEM, AEC and SOEC stack records each provide one positive unit of a
1 MWe stack. Their functional boundaries match the existing stack roles and
their existing `kg/MW` intensity basis. Exact `uvek_aliases` now select their
name, reference product and unit; three native-name conversion entries use
factor one. Existing ecoinvent aliases and all 62 previous conversions are
unchanged. Balance of plant, assembly, hydrogen-production services, treatment,
other products and other reference units are excluded.

251 tests pass across the mapping/conversion cases, existing metals suite and
native freight cases. No runtime Python change is needed for these aliases.
Native reference metadata and inventory hashes are recorded in the accompanying
verification JSON; complete native inventories remain private.

The actual REMIND SSP2-NPi2050 snapshot run still passes the standard diagnostic
update and exhaustive graph checks with 12,040 activities and zero errors. The
filters now match one record for each of PEMEL, AEL and HTEL - Yttrium, producing
22 compiled and executed rule decisions. This closes the previous silent
absence of these three roles; fourteen technology roles still have no alias.

All 22 decisions currently preserve source material inputs: fourteen are outside
their configured observation-year support, one lacks intensity data, six belong
to matched designs whose required suppliers cannot yet be resolved, and one
cannot resolve the titanium provider. These are reported skips, not successful
material substitutions. Metals remains unqualified.

Next: inspect the native iridium and platinum supply interfaces for the PEM
matched design; distinguish absent ceramic suppliers from missing name mappings
for SOEC; and review the titanium product-form mismatch before proposing any
substitution. In particular, titanium-zinc building sheet must not be silently
treated as pure titanium metal. The remaining native equipment filters and
their capacity/mass/area bases also require review.
