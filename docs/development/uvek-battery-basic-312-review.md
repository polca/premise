# Twenty complete native battery-related supplies

Twenty required ecoinvent 3.12 cut-off identities now use existing complete
BAFU processes. No Python file, manufacturing inventory or helper recipe was
added. Seventeen correspondence targets are unchanged; their inspected source
profile is now explicitly approved. Three incorrect matches are corrected:
NMC622 cell to NMC622 cell, NCA oxide to NCA active material, and electrolyte
to complete electrolyte rather than graphite anode.

The source dry cobalt-sulfate reference contains nominal 38% cobalt, consistent
with the native 0.38 kg cobalt feed per kg sulfate. Its factor remains +1; no
hydration or contained-metal multiplier is added. Native electrolyte contains
88% ethylene carbonate and 12% LiPF6; this complete mixture is an explicit
composition proxy and receives no additional solvent or salt burden.

The steel, aluminium and copper incineration references are negative in
ecoinvent and positive in BAFU. Factor -1 preserves the treatment service.
Other mappings use factor +1. Complete native inventories, including any signed
upstream credits, remain untouched. Market delivery, geography, vintage, grade,
allocation and internal uncertainty differences are explicit limitations with
unquantified impact-score effects. Only the exact inspected source profiles
are admitted; no cross-system-model conversion or new omission is authorized.

Checks passed: 42 Brightpath and 10 Premise focused tests; 160 signed caller
uncertainty cases across both native releases; fresh/warm public harmonization;
40 signed finite LCIs using an in-memory foreground; and all three normal
generator outputs matched the packaged files. Temporary generator copies were
removed. No Brightway database, activity or processed file was created.

The retry of the saved real battery graph reduced the unapproved source-3.12
identities from 28 to eight. This does not qualify a complete battery update or
a new export. The verified baseline remains fifteen of nineteen sectors.
Two historical standalone replay test failures remain separately documented.

Detailed decisions and identity/reference witnesses are packaged in Brightpath's
`uvek_battery_basic_312_review.json`. Verification is recorded in the adjoining
`uvek-battery-basic-312-verification.json`.
