# Recognize the LPG already present in the heat inventory

The combined workflow reached a CLIC residential LPG boiler whose native fuel
was not declared in the UVEK fuel aliases. The unchanged efficiency function
therefore found no fuel and used its fallback: the tiny pumping-electricity
input alone. That produced an implausible delivered-heat factor of about 378.

Add exact kg-based aliases for native LPG at the service station and propane/
butane at the refinery. All three installed native supplies have positive
one-kg references. Use the existing nominal LPG LHV of 45.5 MJ/kg; no fuel,
quantity, source uncertainty, supplier, efficiency or conversion multiplier is
changed. The same real source inventory then gives 1.155164835 delivered MJ per
nominal fuel MJ, within the existing condensing-boiler bounds. The pump remains
in the full inventory and does not replace the primary-fuel energy boundary.
Wrong-unit combustion/heat services are refused by the selector.

Existing ecoinvent filters, IAM labels, LHV and CO2 properties remain unchanged.
Native LPG composition, delivery, geography, allocation and vintage remain
screening differences. Seventy-nine existing heat, native-validation and
mapping tests pass. The combined workflow is being rechecked; no new sector
qualification, full-suite green claim or export follows from this local proof.
