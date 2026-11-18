# Final-energy support services from ecoinvent 3.8

Five exact source-3.8 cut-off correspondences are now reviewed. Mapping and
approval data change; Python modules and tests remain unchanged. Three existing
rules are newly admitted. Two profile guards gain only the inspected source
profile, while all four earlier selected profiles remain completely unchanged.
No helper, foreground omission or upstream reconstruction is introduced.

Gaseous hydrogen uses complete native Swiss steam-methane-reforming supply
per kg, with explicit refinery-versus-dedicated-production, allocation,
pressure, geography and vintage warnings. No compressor, pressure, density or
yield factor is invented. Chinese low-voltage electricity uses the complete
native grid once, retaining its losses and infrastructure without an extra
loss multiplier. Its historical portfolio replaces the source portfolio.

Bus maintenance uses the complete native historic Swiss service per unit.
Bus disposal changes negative source waste demand into positive native
service demand using factor -1. Native cut-off bulk-metal/tyre treatment and
residue routing remain; no recovery credit is added. The passenger-car inverter
uses native manufacture per kg; its documentation's 9.5 kg device and 100 kW
rating do not justify multiplying a reference that is already one kg.

All caller quantities, signs, formulas, supported uncertainty and separate
operations/emissions remain. Native internal technology, allocation, delivery,
geography, vintage and uncertainty differ; score effects are unquantified.

Checks pass: 40 signed uncertainty checks, eight direct migration cases,
fresh/warm public harmonization, ten signed finite in-memory inventories,
four earlier-profile equality checks, nine Brightpath review/manifest tests,
ten Premise approval tests and byte equality of three normal generator outputs.
Temporary generator copies were removed and no Brightway database was written.
Actual snapshot preparation clears source-3.9 and source-3.8, then reports ten
source-3.12 electronic/material/service correspondences. Final-energy update
and export remain unfinished; fifteen of nineteen sectors are qualified.
