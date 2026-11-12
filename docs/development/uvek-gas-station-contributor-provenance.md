# Gas-station contributor provenance

An actual REMIND SSP2-NPi 2050 biomass/electricity/heat/cement/fuels diagnostic
completed its callbacks but failed exhaustive graph validation with 50 exact
duplicate supplier errors, all in regional copies of the native high-pressure
natural-gas service station. The public fuel-support guard was bypassed for the
private diagnostic; graph validation was retained and correctly refused export.

The shared geographic relinker can split a native gas purchase among multiple
historical providers. The existing reviewed station replacement then maps these
separate contributions to the same regional IAM gas provider and converts each
from MJ to m3 using 1/36. When source coefficients, comments and uncertainty are
equal, replacing supplier names and locations makes previously distinct rows
byte-equivalent. They are not additional gas purchases or disposable duplicates.

The station replacement now appends the original supplier name, reference product
and location to each existing exchange comment before replacing its identity.
This preserves contributor provenance rather than merging or deleting inputs.
The existing rescaling and uncertainty handling remain unchanged. Repetition
uses the existing regionalized-activity guard. The strict duplicate validator is
unchanged and still rejects genuinely identical records.

A static YAML entry cannot retain different original supplier identities after
runtime geographical splitting. This is why the small generic change in
`premise/fuels/biogas.py` is necessary. It adds no UVEK-specific transformation
engine or API. Ecoinvent paths without a declared upstream replacement remain
unchanged.

The new regression simulates two equal CH/RER source contributions being replaced
by the same EUR provider. Both contributions and their original notes survive,
the total remains 48/36 m3, and each normal uncertainty retains its rescaled
location and scale. Fourteen gas-station tests pass; the broader focused group
previously passed 227 tests before this additional test. Black and Ruff pass.

## Actual export verification

The repeated actual five-sector diagnostic exported 14,817 activities with zero
exhaustive graph issues. Independent comparison against the preceding allocated-
heat/carbon export verifies all 14 service-station records: their scientific
exchange multisets, including multiplicity, quantities, suppliers, signs and
uncertainty, are unchanged. Sixty-three outgoing gas contributions retain their
original supplier provenance. Comparison treats missing `negative=False` and
empty `properties`/`classifications` as their explicit defaults; nonempty metadata
and all uncertainty parameters remain compared.

The final focused suite passes 228 tests. The aggregate receipt is
`uvek-gas-station-contributor-provenance-verification.json`. This diagnostic used
the unrelated local GAINS edits and bypassed the public fuel-support guard; it is
not a clean-checkout qualification of public fuel support or every sector.
