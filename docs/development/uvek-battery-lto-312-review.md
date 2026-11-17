# Lithium titanate: retain the immediate production step

The ecoinvent 3.12 lithium-titanate market previously pointed to lithium
carbonate, which is a precursor rather than the required active material.
Two mapping recipes now retain the immediate Li4Ti5O12 synthesis and its
original market freight. No remote upstream process is reconstructed and
no Python file is changed.

All fourteen non-production entries remain: ten in production and four in
the market. Production retains the original 0.22 kg lithium hydroxide,
0.90 kg titanium dioxide, 0.75 kg liquid nitrogen, 3.25 kWh electricity,
0.048 kg furnace refractory, equipment stock and treatment quantities.
The two direct air emissions retain their original amounts through the
existing ecoinvent 3.12 to 3.10 biosphere route. The public pragmatic policy
reports inferred reverse-route warnings; unresolved/ambiguous mappings,
invalid targets and incomplete coverage remain errors.

Complete native processes supply the ingredients, utilities and services.
High-aluminium-oxide refractory is an explicit proxy for the alumina saggar;
composition, forming, geometry and service-life differences remain unknown.
Chemical-factory stock is measured in kg, not whole-plant units. Native
Facilities, chemical production shares that basis, as documented in the
existing material-stock review. The source 4e-10 kg quantity is retained
without a plant-count, lifetime, throughput or plant-mass conversion.

Negative waste inputs are converted to positive native underground-disposal
and wastewater-treatment services. Complete Swiss capacity-class-1 treatment
replaces the source wastewater market; loading and untreated-route differences
remain explicit limitations. No COD, yield, stoichiometric, capacity or recovery
multiplier is introduced. All three original freight quantities remain, with
European/Swiss geography, fleet and traction proxies.

Native supply technology, production mixes, geography, allocation, delivery,
vintage, internal uncertainty and documented BAFU biosphere exclusions differ
from the source. Inventory and impact-score equivalence are not established.
No original input is omitted and no additional recovery credit is added.

Conversion checks cover signed caller quantities and uncertainty, input
non-mutation, and fresh/warm public harmonization. In-memory LCI checks cover
both signs of the consumer and both helpers, with an independent flattened
native-demand comparison. No Brightway database or activity is written.
Focused tests and generator checks are recorded in the adjoining verification
JSON. Two historical standalone replay failures remain separately disclosed.

The saved required battery graph now has three unapproved identities:
recycled HDPE, PVDF and vinylidene fluoride. This is mapping progress, not
qualification of the full battery update or a new export. The verified
baseline remains fifteen of nineteen sectors.
