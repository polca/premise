# Approved: seven missing country grids inside European mineral-wool sorting

The user approved this scoped choice with warnings on 2026-11-09. The
correspondence is implemented and verified for the exact scope below. It applies
only to the inspected ecoinvent 3.9 cut-off European mineral-wool market/sorting
closure for the UVEK 2026 cut-off / ecoinvent 3.10 biosphere electricity pilot.

The source European low-voltage group purchases delivered electricity from 39
country markets. Native UVEK has matching country/voltage grids for 32. Seven
have no native low-voltage country grid: Albania (AL), Moldova (MD), Malta (MT),
Gibraltar (GI), Kosovo (XK), Montenegro (ME) and Belarus (BY). Their original
weights sum to 0.016162643621 kWh per group kWh (1.616264%). The sorting
service purchases 0.0022 kWh of this group per kg sorted. This missing-country
portion is 3.55578159657e-05 kWh/kg sorted. Full read-only source and installed
native inspection confirms a positive 1 kWh reference for the complete native
European RER low-voltage grid.

Proposed treatment: retain all 39 original signed coefficients and uncertainty.
Use complete native grids for the 32 matching countries. Use the complete RER
delivered low-voltage grid only for the seven absent countries, keeping their
entries separate. Do not normalize shares, replace the group with the Swiss
grid, or add a grid, loss factor or generation credit. Preserve the native
voltage topology, losses, infrastructure, generation/import supply and direct
emissions once.

Warnings must state the seven explicit country-to-European proxies and their
share, different regional membership and generation/trade representation,
voltage-grid losses/infrastructure, allocation, internal uncertainty, vintage
and upstream biosphere exclusions, with unquantified score effects. This is a
screening proxy, not equal national mixes or LCIA equivalence. It authorizes no
other geography fallback, isolated country mapping or source release.

The earlier matching-country utility approval explicitly excludes a new
geographic fallback, so user approval is required for this additional choice.
If it is not approved, leave the European branch refused and continue other
independent work. The source Swiss and RoW mineral-wool routes do not require
this seven-country choice.

Private full inventories: `nitrate_wool_operating_boundary_full_evidence.private.json`.
Country quantities and proposed endpoints: `nitrate_wool_missing_country_preflight.json`.

User decision: “Approve scoped proxies with warnings.” Approval covers only the
seven listed entries inside the inspected European mineral-wool sorting route;
the quantities, separate entries, other 32 countries and warnings above remain
required. No broader country mapping is authorized.

Implemented checkpoint: one exact European source-market scope, preserving all
39 country entries and the complete wool-treatment/operating graph. Focused
Brightpath/Premise tests, full resource replay, native LCI and installed-package
checks pass. Other countries, source releases and independent country-to-RER
conversions receive no approval from this decision.
