# Recognize the native reformer gas input

A real fuel-workflow audit checked eight EUR supplier templates covering six hydrogen routes with positive production in REMIND SSP2-NPi 2050. PEM, biomass, coal and converted CLIC reformers found their configured feedstocks. The native 25-bar steam-methane reformer did not: its gas demand is 157.32 MJ, while the shared efficiency parameters use cubic metres. Its logged input was therefore zero. A complete linked graph did not reveal this mismatch.

Two gas-based hydrogen parameter entries now declare the specific native delivered-gas name and its MJ unit as an additional feedstock. Their accounting factor is 1/36, using Premise's existing 36 MJ/m³ natural-gas reference. This is a nominal energy-basis conversion, not an inferred density or measured gas-composition value. The native reformer is now recognized as 4.37 m³-equivalent. Its exchanges stay in MJ. A separately purchased natural-gas heat service does not match this specific raw-gas declaration.

The existing hydrogen callback has a small generic adapter for these optional declarations. The default feedstock remains available alongside the declared alternatives; there is no background-family branch. The existing efficiency ratio and energy floor determine one scaling factor, which is applied to matched exchanges in their original units. Both cubic-metre and MJ suppliers can therefore use the same technology parameters. No new process, migration rule or helper inventory is introduced.

Three regressions failed before the correction. They verify neutral efficiency, an ordinary improvement and an improvement limited by the existing 3.5 m³ floor, equivalent to 126 MJ. They also check that the gas-heat service stays unchanged. All 236 filter, hydrogen, fuel-energy, carbon and vehicle-grade regressions pass, as do Black, Ruff and the documentation-reference check.

In the inspected real scenario, the native reformer retains its original MJ quantities for the neutral IAM change; it is no longer reported as a zero-input process. All eight audited EUR supplier templates now recognize positive feedstock demand. The standard fuel update and exhaustive graph validation pass.

Eight signed LCIs on the full 13,122-activity transformed graph are finite and reverse consistently for positive and negative functional units: the EUR and World hydrogen markets, the EUR native PEM producer, and the EUR native reformer. The matrix is held in memory; no Brightway database or activity is registered.

This verifies input recognition and unit handling, not complete fuel-sector qualification or source/native impact equivalence. Consumer boundaries, emission-origin coverage and the normal public export remain to be qualified. Eight sectors remain qualified. The complete source exchange audit is private; only bounded reference quantities and results are recorded here.
