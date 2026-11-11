# Source-configured regional electricity copies

The colleague review found IAM electricity markets using European generators
where no regional generator existed. A focused reproduction confirms that a
source alias cannot opt into regional copying: the shared routine previously
read only its fixed technology list. A YAML-only change is therefore ignored.

The six-line generic adaptation extends that existing list with technologies
whose source aliases declare `regionalize: true`. It calls the same existing
`fetch_proxies`, indexing, relinking, efficiency, logging and validation paths.
It adds no source-specific engine or public argument. The built-in technology
list remains unchanged, and no ecoinvent alias opts in. The regression exercises
actual proxy creation, verifies source immutability and preservation of signed
quantities, formulas and uncertainty, and confirms unchanged ecoinvent selection.

The first six opt-ins are Biomass CHP, Gas CC, Geothermal, Solar CSP, Wind
Offshore and Wind Onshore. These contain eleven distinct plant definitions,
each with a single source geography in the inspected build. Their full
inventories are copied without added loss factors, heat credits or invented
local resource data. Standard eligible IAM efficiency adjustments still apply.
New gas/wind limitations explicitly retain the native European inventory basis.
Existing biomass/geothermal/CSP proxy limitations continue to apply.

The real REMIND SSP2-NPi 2050 four-sector export has 13,837 activities, including
143 new regional copies. Every enabled technology supplies regional high-voltage
markets from the matching region. References remain one positive kWh; no
duplicate functional identities or external technosphere links were found.
Noncombustion biosphere inventories remain unchanged in all regional copies.
Nine representative LCIs are finite. Validation reports 14 warnings, no errors.
See `uvek-electricity-regional-copy-verification.json`.

The focused mapping/electricity/transformation suite passes 106 tests. Two
credential-gated legacy tests are skipped; this is not a claim that those
licensed tests passed. Documentation reference generation and its drift check
pass without generated-file changes. Ruff finds no issues in the new test;
the electricity module retains five pre-existing unused-import/f-string findings,
with no new lint findings. Black passes on both changed Python files.

Hydro, nuclear, solar PV and conventional combustion roles with multiple
country inventories remain unfinished. Copying each country inventory separately
can duplicate regional functional identities, while selecting prototypes can
change within-technology representation. They require a separate audit before
being enabled. Regional labels alone do not establish local hydrology, land
occupation, capacity factors or emissions; existing geographic limitations
remain until supported regional data are available.

Loss coefficients, their placement in upstream electricity inputs, zero
self-consumption rows and the special Swiss electricity mixes are unchanged.
Other registered sector updates remain part of the active integration goal.
