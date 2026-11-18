# Final-energy support services from ecoinvent 3.9

Eleven exact cut-off correspondences now use complete native UVEK processes.
Only mapping and approval data change. No Python module or test changes and no
upstream manufacturing inventory is reconstructed.

The services cover locomotive manufacture, high-pressure gas-distribution
pipeline, regional oil-distribution infrastructure, aluminium arc welding,
gas boiler and chimney manufacture, LPG supply, pure phenolic resin, ENTSO-E
low-voltage electricity, and two wastewater services. Native quantities are
positive one in both UVEK releases. The two source wastewater references are
negative one: factor -1 converts waste demand into positive treatment demand.
All other factors are +1. Caller quantities, signs, formulas and uncertainty
are retained; no density, power, lifetime, loading or energy factor is invented.

These are screening proxies. Complete native inventories replace source market
portfolios, manufacture, delivery, allocation, technology, geography, vintage
and internal uncertainty. The gas-boiler records both describe a 10 kW boiler
and borrow an oil-boiler manufacturing inventory; the historical native “xxx”
label remains. Both phenolic-resin references describe pure resin, so no water
or solvent multiplier applies. Process limitations and unquantified score
changes are recorded in the packaged review and warnings.

Checks: 88 signed uncertainty checks, eight direct migration cases, fresh and
warm public harmonization, and 22 signed finite inventory calculations without
writing a database. Nine Brightpath review/manifest tests and ten Premise scoped
approval tests pass for the combined source-3.9 group. Fifteen of nineteen
sectors remain qualified; this is preparation evidence, not a sector export.
