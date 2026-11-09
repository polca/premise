# European waste-glass treatment in the UVEK electricity pilot

The required ecoinvent 3.9 cut-off European glass-treatment group is not a
recycling handover. Its source closure contains 38 market nodes (two regional
groups and 36 country markets), twelve treatment processes and 36 country
collection exchanges. Replacing the group with UVEK's production-only recycling
handover would discard treatment burdens.

The reviewed composition preserves the original market stages, country shares,
treatment shares and collection coefficients, including marginal exchange
uncertainty. Negative source waste references become positive treatment services
through a factor of −1. Swiss collection and fly-ash-extraction routing reuse the
separately reviewed Swiss helper. Open burning, open dumping and unsanitary
landfill retain the existing source-preserving helpers and infiltration classes.
Ordinary incineration uses the previously reviewed complete native glass service.

## New landfill screening proxy

Sanitary landfill represents 29.313919% of the original European routing. The
available native glass landfill is a construction-waste landfill. Its 44 documented
waste-composition and heating-value coordinates match the source glass, including
the documented 2% water content despite the reserved 0% labels. This establishes
a material basis, not equivalence of disposal technology or environmental impact.

The source describes a 20 m sanitary landfill, 30 years of operation, collected
and treated leachate during the first 100 years, and a carbonate-phase estimate
of 5,693 years. The native glass service describes an 11 m mineral construction
landfill, 15 years of operation, extrapolation to 60,000 years and a carbonate
phase beyond that horizon. Hydrology, leachate treatment, emission timing and
speciation, land use, infrastructure, auxiliary inputs, geography, vintage,
allocation and internal uncertainty can differ materially.

Under the user's standing automatic approval for upcoming pilot decisions, the
complete native glass construction-landfill service is used as a screening proxy
per original kg for this portion only. Its inventory replaces the source
landfill's internal model. Source landfill emissions, land use, operations and
infrastructure must not be added again. No moisture factor, recovery credit or
renormalization is introduced. The impact difference is unquantified; this is not
a validated LCIA-equivalence claim. The proxy is restricted to this exact parent
and source profile, rather than approving a standalone sanitary-landfill mapping.

## Evidence and support boundary

Private read-only SQL inspection rechecked 182 complete source/native inventories
against their earlier metadata and exchange fingerprints. The public review
contains identities, fingerprints, derived target coefficients and limitations;
licensed source inventories remain outside Git. The native import remains pinned
to revision `794d90f2c2dbb90f0c56ba8cc527fa1494aa8ef6`, including its documented
2,991 upstream biosphere exclusions.

The review freezes reused recipes transitively, including the Swiss cut-off stock
bindings. Warnings and exact review evidence accompany the Premise compatibility
scope and its cold and warm cache results. No other ecoinvent release, standalone
country treatment, reverse route or cross-system-model conversion is authorized
by this mapping. It does not complete the electricity scenario, certification or
Brightway export gates.

## Verification checkpoint

The focused Brightpath tests pass (13 cases), as do the Premise tests (9 cases),
including signed quantities, lognormal and normal uncertainty, caller
non-mutation, unsupported releases, atomic replay, dependency drift refusal,
cold/warm caches and portable limitation metadata. Lint passes for the new Python
files. The existing generator import-order exception is unchanged.

An independent comparison against the read-only source inventories verified all
162 newly composed market links and 35 collection entries, plus the unchanged
Swiss helper's three fates and collection entry. All source and target treatment
routing weights agree. The original 3,385 recipes, 725 shared reviews and 395
Premise approval fingerprints remain unchanged; the new exact scope adds 39
recipes and one shared review.

The complete generator reproduces the packaged migration resource byte for byte.
A built wheel loads its own package data and converts through the public
Brightpath facade. The actual migrated graph has 53 foreground records and 138
direct biosphere terms; an augmented 12,000-row native matrix produces a finite
inventory. Independent native and augmented solves agree after reported
iterative refinement, and foreground equations pass exact rational and bounded
floating-point balance checks. Native technosphere and biosphere scientific
signatures remain unchanged. This verifies numerical behavior, not proxy impact
accuracy.

Owned build and wheel copies were removed after comparison against their source
files. Superseded harmonization payloads were removed while retaining current
audits and all lockfiles. The broader public pilot audit still refuses unresolved
correspondences; no scenario, certification or export completion is claimed.
