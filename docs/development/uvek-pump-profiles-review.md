# Pump mapping unblocks the next carbon-removal dependency

Carbon-removal preparation used ecoinvent 3.9's global market for a 40 W pump.
The existing UVEK correspondence was reviewed for 3.6 only. Independent source
inspection confirms the same Grundfos UP 15-35x20 solar-heat circulation pump,
positive one-unit reference and manufacturing/disposal boundary in the available
reference databases. The complete native pump is retained once at factor +1,
with signed caller quantities, formulas and uncertainty unchanged. Production
energy and factory infrastructure are common explicit model exclusions; no
operation demand, capacity/lifetime factor, extra pump or recovery credit is added.

The packaged Brightpath review now covers requested cut-off profiles 3.6–3.12.
The 3.7 case uses explicitly documented 3.7.1 functional-reference evidence and
matching 3.6/3.8 manufacturer definitions. Unpatched 3.7 coefficients were not
inspected; this is a limited identity proxy, not score or direct-vector equality.
The [official patch report](https://support.ecoinvent.org/hubfs/Knowledge%20Base/Database/Releases/change%20report_v3%207%201_20201217.pdf)
is the supporting version-change evidence. The requested context is retained,
and no patch caller profile or cross-system-model route is implicitly approved.
Other versions' copper/PVC label changes and 3.12's extra disposal geographies
are documented; inspected aggregate product quantities match within 1e-12.
Native versus source upstream background, disposal, delivery, allocation,
vintage and biosphere-coverage differences remain warnings with unknown score effects.

Premise registers six additional exact source fingerprints and refreshes the
packaged-review hash for its five existing related decisions. All 1,244 prior
fingerprints remain, all 1,250 current scopes validate, and every other decision
object is unchanged except the shared review hash. The compiled migration resource
hash is identical before and after the package update: no conversion rule,
recipe, factor, native inventory or Python runtime module changes are introduced.
The dependency and colleague instructions now use Brightpath Git revision
`d1b72217feeec1420f56e81c370c4f922313cf71`.

The built wheel contains the exact reviewed JSON. Brightpath tests cover signed
units, both uncertainty families, caller ownership, scope/patch isolation and
cross-system-model refusal. Premise's fresh/cache tests cover representative
source profiles and all four other suppliers sharing the refreshed review file;
removing the 3.9 pump grant causes the expected refusal. Actual native LCIs for
+2.5 and -2.5 pumps are finite and reverse consistently; source metadata are unchanged.
The [verification receipt](uvek-pump-profiles-verification.json) separates the
completed targeted checks from an intentionally interrupted broad legacy matrix.

The real carbon-removal probe now passes the pump correspondence and stops at
spent-solvent hazardous incineration (`bp-1be0fde93275f330`). That next case is a
negative waste reference versus a positive native treatment service. It remains
under review; the public CDR gate remains closed, and no CDR update/export is
claimed by this pump correspondence check. The seven previously qualified sectors
and the full integration goal remain separate from this unfinished CDR work.
