# UVEK through the standard Premise workflow

Status: implementation and end-to-end verification in progress. The earlier
native pilot export is not evidence of standard regional-market behavior.

Baseline: Premise `release/2.6.0`, commit
`8f13df153f61687c302a460bd0584458688f3749`.

The shared biomass, electricity and heat entry points must run their normal
transformations. Regional electricity markets must include applicable REMIND
regions such as EUR and NEU, with scenario technology shares, voltage chains and
consumer links. Compact storage and the CLIC provider remain independent of
this correction. No alternate UVEK sector algorithm is an acceptable substitute.

## Necessary Python changes

- Remove the three UVEK early returns that bypass the shared transformations.
- Forward the source family and biosphere version into the existing Biomass and
  Heat constructors, mapping lookups and biosphere lookups. YAML cannot select
  UVEK aliases while the caller explicitly requests ecoinvent aliases, nor make
  release 2026 an ecoinvent biosphere version. These are parameter-routing
  adaptations; the original sector transformation sequence is retained.

Every additional Python change must be justified here by a reproduced failure
and an explanation of why mapping data alone cannot resolve it.

## Mapping decisions

2026-11-10: the shared geography mapper rejected three native labels before
biomass could start. Extend its existing missing-geography YAML; do not add a
UVEK-specific geography lookup algorithm or alter the Brightway source.

- ERCOT maps to REMIND USA.
- LBFAC32 is the exporter's deterministic alias for “Europe without Switzerland”.
  The export repair regression in brightcon-2026-material explicitly records
  this alias. Reuse the existing label's representative REMIND EUR equivalence;
  Europe outside Switzerland is not identical to REMIND EUR.
- CPA is the legacy “Centrally Planned Asia and China” region, confirmed by the
  [ecoinvent 2 methodology report](https://support.ecoinvent.org/hubfs/Knowledge%20Base/Database/Releases/overview_methodology_ecoinvent%20version%202.pdf).
  Use CHA as an explicit screening representative for its two native coal
  datasets. The legacy region spans more than one modern IAM region; no country
  production shares are available here. Keep that geographic approximation
  visible rather than claim exact boundary equivalence. Source labels,
  inventories and signed coefficients remain unchanged by this YAML mapping.

## Required verification

- Actual REMIND SSP2-NPi 2050 biomass/electricity/heat run and Brightway export.
- Inspect exported regional markets, shares, voltage chains and consumer links.
- Finite LCI checks and relevant tests; preserve the ecoinvent default route.
- Log remaining scientific limitations and all deviations from the baseline.

## Shared-path findings

- The standard biomass builder can already preserve a transient technology label.
  Use it to check the residual share by declared IAM role, rather than require
  the word “residue” in a supplier name. Keep the legacy name-based fallback for
  validation callers without that label. This keeps the share check active.
- Fuel purchases need mapping-based selection because native dry-kg wood-chip
  and pellet supplies do not use ecoinvent's “market for wood chips” names.
  Store eligible direct-consumer codes before relinking, and validate those
  consumers, including capitalized native heat labels. Indirect district-heat
  distribution services are not direct wood-fuel purchases. Preserve original
  quantities/uncertainty and remove obsolete explicit input keys on retargeting.
- The Geothermal alias names an approved native-context inventory absent from
  the UVEK source and public CLIC archive. Previously the alternate electricity
  callback loaded it. Declare its existing JSON resource in the electricity
  mapping and prepare mapping-declared inventory resources in the common
  inventory-preparation stage. Reuse the existing supplier-linking loader;
  validate its destination context, retain owned-resource provenance and include
  resource hashes in the inventory semantic fingerprint. This is inventory
  preparation, not a replacement electricity transformation or a new proxy.

The first shared electricity run creates regional markets and its supplier-vector
check refuses the missing geothermal technology. No check is disabled. The
shared ecoinvent-path regression batch currently passes 90 tests (two skipped).
Final export and graph inspection are still required.
