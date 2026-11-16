# Source-3.7 car supplies and separate corrections

Twelve of the 15 refused source-3.7 correspondences now have independently
checked references and exact approvals. These inputs are used: the full
foreground dependency check found positive lightweighting demand in retained
car-manufacturing records, rather than an unused helper that could be removed.
The native references were checked in both releases.

Low-alloy steel already includes hot rolling; its native 37% electric and 63%
converter mix replaces the source mix. Aluminium metalworking excludes the
principal stock. Glider production is per kg of body/chassis, and both source
and native inventories include their respective end-of-life glider service.
Maintenance is a lifetime service unit based on the historical Golf A4 model.
No whole-car, lifetime, occupancy or road-lifetime multiplier is introduced.

Brake, road and tyre wear have negative source references and positive native
emission-service references. Their existing minus-one factors turn negative
wear demands into positive emission-service purchases. This is not pollution
cleanup or an avoided-emission credit. The native species and compartment model
replaces the source model without a new particle split or microplastic fraction.
The electronics-plastic disposal market keeps its existing sign conversion,
with explicit differences in ash extraction, recovery, allocation and delivery.
Its complete native incineration inventory replaces the source market model.

HDPE uses the complete native European eco-profile. R134a retains its chemical
identity and caller leakage; native production and delivery differ. Road and
road-maintenance services retain meter-years and their separate construction,
renewal and operation boundaries. Caller quantities, formulas and supported
uncertainty remain unchanged. Four signed conversion tests pass across both
native releases. No migration rule, new helper or runtime Python change is
included in this approval group; score differences remain unquantified.

Three cases required separate correction: the different AlMg3 alloy, the
refinery-only petrol supply and the missing exact CNG profile. All are now
resolved in the separately documented source-3.7 correction group. The actual
shared car update and the combined 14-sector run pass their graph, physical
and representative signed-LCI checks. See
`uvek-passenger-car-source37-corrections.md` for the models and limitations.

Public package admission and a real export remain to be verified. This review
writes no Brightway database.

A separate minimal Brightpath validator correction is now tested. The existing
engine supports an explicitly declared historical inventory model, but semantic
validation incorrectly compared its nested cut-off-stock authority with the
outer caller version. The correction checks the stock against the declared
model version, while retaining exact caller-profile approval, unresolved
defaults and the original stock authority digest. Twenty-one focused stock and
historical-model checks pass, including invalid versions, system models,
missing declarations and unprofiled callers. This changes only Brightpath
resource validation; no Premise Python module is changed. The three correction recipes and both diagnostic car runs now pass; public
package/export qualification remains separate.
