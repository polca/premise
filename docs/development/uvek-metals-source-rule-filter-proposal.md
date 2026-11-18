# Approved source-specific material-rule selection

Native wind inventories describe moving-parts assemblies rather than whole
wind-plant construction. Applying the complete whole-plant material-rule list
to those components could overlap tower/foundation materials or metals already
inside supplied components. Some native assemblies do have a direct neodymium
oxide input, which the existing per-MW neodymium rule can update correctly.

The proposed one-runtime-file change lets an alias explicitly list the existing
material-rule IDs that fit its physical boundary. It adds a small checked
selection helper and uses it in the existing Metals constructor. With no list,
all existing rules and their order remain unchanged. Unknown, duplicate, empty
or cross-technology rule lists fail before any update. Equations, supplier
selection, capacity conversion, time guards and matched-design validation are
unchanged. No background-specific engine or new inventory is introduced.

For the native moving-parts aliases, this allows the existing neodymium rule
without applying unrelated whole-plant bulk-metal totals. Assemblies that carry
neodymium inside complete magnets still require the existing preservation
policy rather than a second direct metal input. Compatible dysprosium supply
and component-specific designs remain source limitations, recorded separately.

All 328 existing metals/regional-supplier tests plus ten selection checks pass
in a private interpreter preview. The user approved the exact packet on 18 November 2026. Its source and test
bytes are applied, and all 338 checks also pass against the repository source.
The exact packet contains one runtime file and one regression file:
`proposed-metals-source-rule-filter-with-tests.patch`.
Installed native mapping coverage and public export remain separate checks.
