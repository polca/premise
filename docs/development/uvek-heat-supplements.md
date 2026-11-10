# Supplemental suppliers for native UVEK heat

Current integration status: the normal UVEK heat update and export are available
for reviewed roles; see [native heat](uvek-heat.md). Gate/unfinished-work wording
below records this component's earlier development checkpoint.

`plan_heat_supplements` is an internal, read-only supplier planner. It returns
reviewed role bindings and an owned electric-boiler addition; it does not append
them, alter source inventories or enable the public heat update. Native heat
suppliers continue through their separate 28-role planner.

The six additional `uvek_aliases` cover four pinned-CLIC roles and two retained
boiler roles. The CLIC declarations use the existing family-override/root-selection
mechanism: positive IAM roles request their reviewed slugs without matching
legacy workbooks. An explicit user lockfile retains its existing root-selection
meaning. No manual helper installation or new public constructor is introduced.

The three CLIC sources retain their exact versions, IDs and SHA256 pins, original
background provenance, signed quantities, formulas, auxiliary inputs, uncertainty,
freight, infrastructure, treatments and emissions. Source/target identities,
positive references, source ownership, original fuel draw and links must agree
with the review. Standard `[CLIC slug]` collision suffixes are accepted only for
the locked heat source or locked foreground fuel; unrelated plain-name records
cannot capture a role, and native matching remains exact. The two methanol boilers also supply the existing industrial
bio-liquid role as a disclosed residential/industrial scale proxy.

Nominal final-energy conversion follows the original source bases:

| Service | Source energy basis | Nominal delivered MJ/input MJ |
| --- | --- | --- |
| Biomass and coal methanol boilers | Explicit boiler comment: 20 MJ/kg; retained approximately 0.0555556 kg/MJ | approximately 0.90 |
| Hydrogen turbine-heat proxy | Original ecoinvent 3.9 hydrogen reference property: 120.21 MJ/kg; retained 0.008947368421052631 kg/MJ | 0.9297454944044002 |
| Retained industrial electric boiler | Exact workbook draw: 0.2805836139169472 kWh/MJ; 3.6 MJ/kWh | 0.99 |

Do not substitute the generic 19.9 MJ/kg methanol or 120 MJ/kg hydrogen defaults.
The hydrogen heat record says 95% HHV in its dataset comment but 95% LHV plus
0.5% loss in its fuel-exchange comment. Neither claim reconciles exactly with
its actual fuel input and the original supplier's net heating value. Preserve
that inconsistency; do not force factor 0.95, add another loss or rescale the
inventory. The result is a nominal inventory-derived final-energy proxy, not a
validated turbine/CHP efficiency or a reconstructed engineering heat/power
balance. The 1 GW turbine inventory remains an explicit technology/scale proxy
for the IAM industrial hydrogen-boiler role. No electricity coproduct or credit
is created. Methanol pumping electricity remains in the complete LCI, outside
the fuel-only final-energy conversion boundary.

The electric boiler uses the retained workbook, verified by its byte hash, on the
complete native Swiss medium-voltage grid. Its one-MJ service and original
99%-efficiency electricity draw remain unchanged. Infrastructure is omitted in
the source workbook and remains an explicit limitation; no plant, lifetime,
capacity or recovery credit is invented. Its final-energy role uses factor 0.99;
its secondary heat-output role uses factor 1. One deterministic owned boiler
serves both roles, with safe repeated planning and collision refusal.

`uvek2026_supplement_review.json` pins the source evidence, carrier identities,
physical reference quantities, declared role bases and scientific limitations.
Changed pins, quantities, references, YAML/review disagreement, missing or
ambiguous suppliers, model-inapplicable roles, another system model and another
biosphere are refused without source mutation. Native biosphere exclusions,
upstream mapping limitations, geography, vintage and unquantified score effects
remain visible. No LCIA equivalence or impact completeness is claimed.

Public heat remains gated. The planners still need integration with all three
IAM market layers, native district delivery, consumer classification/relinking,
combined updates, certification and reproducible public exports.
