# MDEA screening reconstruction for the CDR inventory

The inspected ecoinvent 3.12 cut-off European MDEA market had a heuristic
match to **2-methyl-2-butanol**, a different chemical. That match was not
approved. Rule `bp-05e536a8f677ffd7` now uses a separately reviewed MDEA
production helper and a delivery helper. This does not qualify the CDR sector.

The product remains one kilogram of pure methyldiethanolamine, CAS 105-59-9.
Its methylamine and ethylene-oxide inputs are 0.277102747869839 kg and
0.786108042926247 kg per kg product. Their molar ratio is approximately 1:2.
The quantities imply about 94.06% stoichiometric yield with the recorded atomic
weights, whereas the source documentation describes an estimated 95% yield.
Keep the inspected quantities; do not silently correct that discrepancy or
apply another yield or solution-concentration multiplier.

Keep the inspected 0.358 kWh electricity, 3.72 MJ chemical-process heat and
4e-10 factory units. Reuse the existing reviewed chemical-plant **MJ** heat
helper, including its separate water exchanges. The native fresh-steam process
is in **kg**, so using it at 3.72 kg would introduce an unsupported unit change.
Native methylamine and ethylene-oxide production have positive kg references;
their complete upstream inventories are used once.

There is no verified native MDEA plant. The remaining processing model is an
explicit screening reconstruction from native diethanolamine processing:

- Replace ammonia feed by methylamine; use the two inspected reactant quantities.
- Apply the native estimated 0.2% raw-reactant air-release assumption to methylamine
  and ethylene oxide. Scale the native EO water release by EO input, nitrogen
  releases by reactant nitrogen and carbon releases/indicators by reactant carbon.
- Preserve implied raw-material transport distances by scaling native road/rail
  freight with total reactant mass. Retain native process/cooling water per kg
  product and scale waste heat with electricity use.
- Do not retain the template's separate electricity and furnace heat in addition
  to the calibrated utilities, or add another wastewater service.
- Keep all three finished-product delivery exchanges separately, with their
  inspected quantities and uncertainty: barge, lorry and rail. These are distinct
  from raw-material freight.

These choices are assumptions, not measured MDEA emissions or a reconstruction
of industrial selectivity, recycling and distillation. The template's estimated
historical ethanolamine model and co-production allocation are limitations.
Patent descriptions support the methylamine/ethylene-oxide reaction family
and the need for separation; they do not validate the selected LCI quantities:
[Dow process description](https://patents.google.com/patent/US5395973A/en) and
[BASF separation process](https://patents.google.com/patent/US9345988B2/en).

The source-specific `Methydiethanolamine` water release of 0.00025 kg/kg has no
established definition in the selected ecoinvent 3.10 biosphere. This processing
proxy does not represent that release. It does not invent a flow, replace it by
another substance, or grant permission to omit a caller's foreground emission.
Source VOC, nutrient, wastewater, residue and toxicity coverage differ. Score
and toxicity differences are **unquantified** and remain in warnings/provenance.

Represented carbon and nitrogen releases are below their respective
feed-minus-product excesses, but residual loss fates remain unresolved. This
is not complete atom balance. DOC/TOC overlap and COD/BOD are oxygen-demand
indicators; they are not summed as separate carbon masses.

Caller signed kg, uncertainty and formulas remain unchanged. Internal retained
or scaled distributions are handled consistently, without claiming quantified
model uncertainty or correlations. One new exact 3.12 cut-off approval is added;
all 1,278 previous scopes remain unchanged. Other source versions, patches,
system models and reverse routes gain no approval.

No runtime Python changes are needed. Changes are the Brightpath mapping and
frozen review, a maintainer replay utility and its generator hook, Premise's
approval data, focused regression tests and documentation. Full licensed
source inventories remain private.

See [verification](uvek-mdea-312-verification.json) for tests and a calculation
using four transient activities, nine native supplier bindings and 15 exact
biosphere bindings, without a new Brightway database or processed file. This
checks numerical consistency of the proposed helper graph, not source-model
or LCIA equivalence, whole-sector coverage or export qualification.

The subsequent graph trace identifies the reported cycle as an identity mismatch:
232 native electricity reference-product labels remain unnormalized in the
standalone CDR graph, while its baseline uses the existing electricity aliases.
After applying those same aliases to the component identities, it is contained
in the known baseline. CDR adds no node or internal edge to the flagged component.
See [diagnostic](uvek-cdr-cycle-alias-diagnostic.json). This explains the report;
it does not yet fix standalone CDR or qualify its technology coverage.
