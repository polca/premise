# Cement and concrete grade proxies

Status: the 3.9 Swiss normal-strength concrete case is now implemented as part
of the [nine-market retained-delivery forest](uvek-concrete-delivery-findings.md).
The two 3.6 cases below remain to be implemented and verified under the current
automatic-approval instruction. All three have strength-class or binder-blend
differences that must remain explicit.

| Source profile and product | Inspected source boundary | Proposed complete native supplier |
| --- | --- | --- |
| Ecoinvent cut-off 3.6, Swiss Portland cement, `bp-9c3675cc3a3f003c` | Representative CEM I 42.5 / CEM I 52.5 R production mix; grinding and mixing, without packaging or administration | `Portland cement, strength class Z 42.5, at plant`, CH |
| Ecoinvent cut-off 3.6, Swiss normal concrete, `bp-953779a9d7354634` | Supply mix of 25% CEM II/A concrete and 75% CEM II/B concrete | `Concrete, normal, at plant`, CH |
| Ecoinvent cut-off 3.9, Swiss normal-strength concrete, `bp-67e316270a112622` | Generic production-weighted normal-strength concrete, 20–37 MPa; upstream markets account for delivery | `Concrete, normal, at plant`, CH |

The native cement is specifically labelled CEM I 42.5. Its recorded clinker
input is close to the source production mix, but that does not establish
matching strength or process performance. It cannot be presented as a verified
replacement for the source mix's 52.5 R portion.

The native concrete represents ready-mixed B 35/25 concrete with CEM I 42.5,
300 kg cement per m3 and a declared density of 2380 kg/m3. It includes mixing,
internal processes, infrastructure, effluent treatment and production waste.
Its binder differs from the 3.6 source's CEM II mix, and it does not reproduce
the 3.9 source's exact distribution of strengths, cement types and delivery.
The historical strength labels are retained as documented; no new conversion
between strength standards is inferred.

Proposed decision: use these complete native inventories as explicit screening
proxies, at factor +1 per signed kg of cement or signed m3 of concrete. Preserve
their internal burdens once and retain separately listed caller transport,
materials, construction, treatment and emissions. Do not rescale by strength,
binder/clinker content, density, yield or a presumed functional-performance
ratio, add a second grinding/mixing process, or invent missing delivery.

Warnings must identify the narrower cement strength coverage; CEM I versus
CEM II binder and clinker differences; concrete strength/distribution and
composition differences; manufacturing energy, allocation, market delivery,
background, geography and vintage differences. Inventory, mechanical-performance
and score effects are unquantified. Equal structural performance and LCIA
equivalence are not established, and the direction of the score difference is
not asserted.

Private read-only evidence pins the actual three source inventories and their
positive references: one kg of cement or one m3 of concrete. Both native
releases have the same positive reference bases and matching direct
technosphere coordinates. Candidate native LCIs are finite and nonempty,
with unchanged scientific hashes for the installed technosphere and biosphere.
Those calculations establish computability only; the prospective scenario and
export remain unverified. The original proposal introduced no helper, source database edit or approval
fingerprint; the later 3.9 implementation is linked below. Uninspected versions and inverse
or cross-system-model routes remain outside its scope.

For the 3.9 Swiss market, the implemented correspondence retains its grade-market
mixture, upstream delivery and uncertainty before using complete native concrete
production as a screening proxy. An at-plant-only replacement would have lost
that delivery. The earlier proposal enabled no mapping; see the linked final
implementation and its limits. The two 3.6 source boundaries still require their
own final implementation checks and must not inherit that exact-profile scope.
