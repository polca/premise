# Actual TIAM heat verification

The standard public `NewDatabase` workflow successfully updates heat using actual
encrypted TIAM-UCL SSP2-RCP19 2050 data, then exports 12,213 activities to Brightway.
No IAM arrays are replaced and no support guard is bypassed. The source remains
BAFU:2026-reimported with ecoinvent-3.10-biosphere in the regular Brightway project.

The source supplies eleven secondary-heat technology variables, including
geothermal and biomass, coal, coke, natural-gas and oil options with CCS. Building
and industrial end-use arrays are absent in this source. Their normal skip
behavior is retained; this is not a missing supplier disguised as an empty array.

The export has 17 secondary heat markets (16 regions plus World), each with
supplier shares summing to one. All 266 newly created activities pass positive
and negative unit-demand calculations: 532 finite LCIs with exact sign reversal.
Every technosphere link resolves inside the export, and biosphere links use the
selected database. Exhaustive graph validation has zero issues under ruleset 9;
the two recorded sector validation phases also have no issues.

Independent inspection of all 64 regional CCS heat inventories traces their
actual MJ inputs through fixed references to native allocated combustion stacks.
All retain the declared heat-input penalties and capture 90% of their attributable
stack CO2, with capture-service demand equal to the negative stack correction.
There are 16 each for natural gas, coal, wood and oil. Coke retains the explicitly
reviewed shared coal proxy. Oil's limestone-derived CO2 remains outside the
combustion capture basis. The four original source-location CCS records are
one-to-one forwarders to their regional copies, without duplicate capture credits.

These results extend controlled CCS tests to a real IAM heat run. Existing proxy,
allocation, geography, historical technology, capture-chain and uncertainty
limitations remain. This is not proof of impact equivalence, complete heat
sector decarbonization or combined-fuel carbon-origin correctness.

The combined TIAM biomass/electricity/heat/cement run stops earlier, during
inventory preparation, at 23 unreviewed correspondences in its first failing
3.12 source-profile batch. Heat alone prepares and runs successfully. Keep that
combined prerequisite frontier open; do not waive wrong-chemical matches or
infer that the four-sector workflow is qualified from this heat-only result.

The aggregate receipt is `uvek-real-TIAM-heat-verification.json`; the combined
frontier is `uvek-real-TIAM-first-preparation-frontier.json`. Runtime/mapping
contents correspond to commit 6ea6b217 and Brightpath 587bfd9, with the six
unrelated local GAINS/notebook/audit edits present. This is not a new clean-wheel
qualification. No Python or mapping change was needed for this inspection.
Combined fuels, other representative scenarios and remaining sectors stay in
the active goal.
