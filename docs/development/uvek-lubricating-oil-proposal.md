# Lubricating-oil formulation proxy

Status: proposed; not approved or enabled. This is the remaining oil case from
the earlier supplier group. The five verified whole-function matches and the
closer native converter-steel correction do not authorize this formulation gap.

The actual pilot uses `bp-4f69327dbf9d3e42`, the global market for lubricating
oil, from ecoinvent cut-off 3.6, 3.9 and 3.12. Its selected European producer
explicitly includes additives and produces one kg of liquid lubricant.

The existing UVEK supplier is `Lubricating oil, at plant`, RER, per kg. Its
declared product is liquid lubricating oil; it should not be relabelled as pure
base oil. Its model describes hydrocracking, distillation and dewaxing, with
diesel, heat, electricity and chemical-plant infrastructure inputs. There are
no explicit additive-production inputs. The documentation also acknowledges
incomplete process emissions, omitted solid wastes and substantial uncertainty.
The inspected native 2025 and 2026 technosphere inputs agree; the pinned 2026
biosphere exclusions remain part of the destination's import boundary.

Proposed decision: use that complete native supplier as a screening proxy at
factor +1 per signed source kg, only for the three inspected cut-off profiles.
Keep its inventory intact and retain separately listed caller transport, oil
use, treatment and emissions. Do not infer an additive recipe, multiply the
delivered kg by its internal production yield, add diesel again or introduce
an avoided-product credit. Preserve uncertainty and formulas consistently.

Required warnings would describe the missing explicit additive formulation,
the native model's incomplete emissions and solid-waste coverage, global-market
versus European-plant delivery, allocation, geography and vintage differences.
Inventory and score effects are unquantified; this is not a claim of equivalent
lubricant grade, composition or LCIA results.

Private read-only evidence covers the actual three source profiles and their
producer definitions. A native one-kg supplier calculation is finite and
nonempty, with unchanged scientific hashes for both installed databases. This
checks the candidate's computability, not its scientific equivalence or a
prospective scenario. No approval fingerprint, helper or database mutation has
been introduced for this proposal. Uninspected versions, reverse routes and
cross-system-model conversions remain outside its scope.
