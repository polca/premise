# Exact shared-emissions and public-admission packet

The private REMIND SSP2-NPi 2050 candidate completes all nineteen sector callbacks with zero exhaustive validation errors. It covers all 412 CLIC records with existing GAINS classifications and all 30 declared native renamed-process bindings (67 input positions), with no missing assignments or wrong operating regions. All 6,852 positive/negative LCI checks on 3,426 distinct demands in its actual output are finite and sign-linear. It includes the real vehicle-emissions refresh. All 808 focused existing and new regression checks pass. No Brightway database was written.

## Requested runtime exception

This exact packet changes two runtime Python files:

- `premise/emissions/emissions.py`: select native emitter aliases and reviewed source contexts, prepare complete copies only where needed, and call the existing shared GAINS updater. Equations, interpolation, reduction-only behaviour and uncertainty scaling remain the shared implementation. The ecoinvent path retains its defaults. The final candidate differs from the coupled-tested version only in audit labels that explicitly describe the declared mapping scope; AST comparison verifies the scientific expressions and control flow are unchanged.
- `premise/database/background.py`: add metals and emissions to the public support list and update its error text. Default selection remains biomass, electricity and heat. Unsupported external updates still fail before inventory preparation.

The first change is necessary because YAML cannot make the existing compact updater read native aliases or follow the moved burner endpoints. The second permits testing and export through the normal API. No NewDatabase arguments, database labels or alternative factor engine are introduced.

## Data and tests

The YAML extension preserves the existing caller filters and ecoinvent mappings. It adds an optional `emission_sources` child, including 35 canonical CLIC names under eleven existing GAINS classes. Thirty aliases for renamed coal CHP, fossil-supply and frozen-heat callers are already committed as a data-only resource. They reuse existing classifications without changing reference quantities, composition or upstream supply chains.

The tests cover geographic and weighted-World factors, supplier reuse, signed purchases, uncertainty, deterministic handovers, rollback and replay. Retained native combustion-component tests call that component directly; new tests cover the shared dispatch. Public-boundary tests assert metals/emissions acceptance and preserve early refusal using external updates.

## Scope and limits

The reduced construction reuses 2,408 existing input positions and adds 334 complete context copies. No new upstream inventory is built. Native selection remains restricted to air, and the four documented unsplit CHP-methane positions remain unchanged. Technology, allocation, geography and vintage differences retain screening limitations and unquantified score effects.

The other 503 emitting CLIC records have no assignment under the existing source GAINS filters and retain ordinary shared skip behaviour. This packet does not claim to classify every activity in the background database or establish impact equivalence. The native renamed audit deliberately leaves direct-drive wind parts on their already-covered supplier controls.

## Approval and next step

Nothing in the runtime packet has been applied. This combined request supersedes the earlier metals-only support-list request. Approval authorises exactly these two runtime files and the reviewed test refreshes; the mapping work is already within the authorised scope.

After approval: apply the exact packet, build/install the package, run the ordinary public nineteen-sector workflow, export once to Brightway, inspect links and sector outputs, and verify written-database LCIs. Publicly verified coverage remains 17/19 until those checks pass. The existing notebook's default workflow remains unchanged.

Exact packet: `proposed-uvek-shared-emissions-and-public-admission.patch`. Evidence: `uvek-shared-emissions-final-review-verification.json`.
