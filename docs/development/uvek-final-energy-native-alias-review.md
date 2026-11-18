# Native final-energy selectors

Add `uvek_aliases` to the existing mapping YAML for 302 migrated foreground or
reviewed helper roles and 40 low-voltage electricity roles. All 425 original
role definitions, ecoinvent filters, IAM labels, masks and LHVs remain unchanged.
Existing foreground names retain their original reference products; explicit
unit filters prevent matching a different output basis. Reviewed generated
helpers remain eligible only where an observed filter requires them. Published
CLIC records still require the existing explicit role binding.

Low-voltage roles select complete native grids or ordinary Premise regional
electricity markets per kWh, accepting the already declared product aliases.
They reject heat-unit and battery-capacity counterexamples. Generation,
network, loss and infrastructure inputs remain in the original inventories;
no multiplier or separate grid inventory is added by the YAML change. Actual
regionalization remains the shared Premise method.

The actual prepared-record census finds 184 nonempty REMIND roles, with suppliers
for 175 of the 203 roles having positive SSP2-NPi quantities in 2050. Twenty-eight
active direct role gaps remain. This census uses identity and provenance, not
name-only acceptance. The shared update now adds 804 regional activities and
passes exhaustive graph validation with zero errors. Forty-eight existing
mapping tests and the broader 183-test context regression run pass.

These results do not qualify the whole final-energy sector or establish its
export: the remaining direct roles and source/proxy boundaries require closure.
See `uvek-final-energy-native-alias-verification.json` and the context workflow
verification. No deep upstream reconstruction is introduced.
