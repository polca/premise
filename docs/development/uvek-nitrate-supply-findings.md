# Nitrate-salt supply required by the UVEK electricity pilot

Status: source-boundary inspection complete; the whole salt-market mapping
remains refused. Its purge-stream dependency is now corrected as described below.

This inspection concerns `bp-cfcab3636e7a220d`, the ecoinvent 3.9 cut-off global
market for nitrate salts for solar power applications. The destination remains
UVEK 2026 cut-off with ecoinvent 3.10 biosphere. The existing generic inorganic
chemicals target does not establish the required chemical or supply boundary.

The source market combines virgin and reused salt. Its virgin mixture uses
technical-grade sodium and potassium nitrates. The reused-salt route is a
positive allocated product supply, carrying nonempty inventories from four
thermal-storage construction processes: parabolic-trough and tower designs,
each with South African and rest-of-world suppliers. Their construction,
materials, operations and disposal inputs remain part of that allocated supply.
It cannot be treated as an empty cut-off stock or replaced with virgin salts
without a new modelling decision.

The exact supplier links form an **acyclic graph**. The four allocated storage
processes buy directly from **virgin nitrate-salt production**, rather than from
the final market containing reused salts. The current recipe schema accepts
this topology. Cyclic-recipe support is not required for these inspected links.
Product names alone do not identify these suppliers; the linked activity,
reference product, geography and unit distinguish them.

The earlier approved omission of the used-salt reuse credit from one CSP
decommissioning record applies only to that caller exchange. It does not
authorize removing the reused-salt supply branch or its allocated construction
burdens from this market. Likewise, the approved complete 110 MW CSP generator
does not replace the source's four historical storage suppliers automatically.

The inspected virgin production chain includes technical-grade refining,
industrial-grade potassium nitrate, and unrefined sodium nitrate from Chilean
caliche mining. The source distinguishes these grades and includes its own
refining quantities. Preserve those process references and quantities rather
than inventing purity, yield or concentration multipliers.

The native catalog has potassium-nitrate inventories allocated separately per
kg of nitrogen and per kg of potassium oxide. Their documentation requires
joint use. Neither reference is a kg of technical-grade potassium nitrate;
selecting one endpoint or using both at one kg each would misrepresent the
reference basis. A complete sodium-nitrate supplier has not been established
from the current catalog search. These findings support further source-based
reconstruction, rather than an unsupported generic chemical match.

The private inspection freshly read 14 complete source records, retaining
280 non-production exchanges and 19 links within that graph. The unchanged
migration schema accepts the source topology. Its internal supply solve is
finite and nonnegative, with equation residual below 1.18 × 10⁻¹⁶. All nine
direct elementary flows match the ecoinvent 3.10 catalog by publisher UUID,
unit and complete receiving compartment. This verifies the inspected source
structure; it is **not a linked UVEK inventory or a native LCI result**.

The inspected graph references 125 distinct external suppliers. Of those,
25 already have exact current CLIC approval fingerprints for their source
profile. All have raw mapping entries, but a raw entry does not establish a
scientifically reviewed target. The other suppliers cover 43 reference-product
labels across multiple geographies. Some may reuse standing reviews after
inspection, while others overlap existing pending material, heat or waste
decisions. They are not 100 automatically new scientific decisions. Coverage is
checked using the public scope auditor and original packaged rule bodies;
profile-selected rule projections are not approval fingerprints.

The Chilean sodium-nitrate-rich purge-stream market is now one of those
approved suppliers. Fresh source inspection found a positive, production-only
`Recycled Content cut-off` provider and a market containing only that handover.
The two corrected correspondences preserve this allocation through source-bound
helpers, with normal stock warnings, signed kg and uncertainty. They add no
potassium-nitrate nutrient, indium-residue, fuel or avoided-production burden.
This does not extend to the four nonempty reused-salt construction suppliers.

This dependency expansion sits behind one currently blocked parent rule.
Consequently, the headline count of 35 unresolved rules / 40 source-profile
cases is the current public refusal frontier, not a complete measure of the
remaining inspection or implementation effort. The pilot still requires the
same nine roots and 55 CLIC records; no plant, storage requirement, material
input or supply share has been removed to reduce that count.

Next work is to preserve the allocated reuse branch and source production
structure, bind the external suppliers to inspected native services, retain all
signed quantities and uncertainty, and verify the resulting complete target
graph. Further mismatches must remain explicit decisions or refusals. Full
public scenario transformation, certification, Brightway export and a finite
prospective LCI remain completion gates.

Detailed licensed coefficients and source metadata remain in the private
pilot work area: `nitrate_source_routes_full_evidence.private.json`,
`nitrate_raw_production_full_evidence.private.json` and
`nitrate_source_dag_preflight.json`.
