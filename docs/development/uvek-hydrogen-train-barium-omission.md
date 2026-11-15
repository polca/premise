# Hydrogen-train screening: missing barium-carbonate supply

The six required hydrogen-train foregrounds depend on a ceramic capacitor whose
barium-titanate inventory purchases barium carbonate. No native UVEK supplier for
that chemical has been established. The suggested lithium-carbonate match is a
different chemical and is rejected. Native barite is barium sulfate; a generic
native surface-mount capacitor has a different, insufficiently established
product boundary. Neither is an approved substitute.

Apply the user's standing instruction to omit unsupported inputs with a warning
only to this exact foreground and exchange, from ecoinvent 3.12 cut-off to UVEK
2026 cut-off with the ecoinvent 3.10 biosphere. The decision is in
`premise/data/clic/approved_omissions.json`, identified by
`uvek-2026-barium-carbonate-feed-screening`. Matching uses the reviewed dataset
and exchange identities and background context; historical archive hashes are
traceability information, not an equality requirement.

This is a substantial missing feedstock **per kilogram of ceramic**: the inspected
source purchases about 0.846 kg of barium carbonate per kg of barium titanate.
The resulting ceramic inventory is incomplete and unsuitable for standalone
material assessment. A private linear solve of the 34-node CLIC foreground graph
finds approximately 6–21 nanograms of this feedstock per tonne-kilometre across
the six train services. This physical quantity is not evidence of a negligible
or bounded impact-score effect. The omitted upstream burden remains unquantified.
Do not manufacture a replacement chemical process to clear this mapping.

Keep titanium dioxide, manufacturing energy, direct emissions, all remaining
exchanges and the source reference unchanged. Add no recovery credit or
zero-burden supplier. The omission audit retains the actual signed exchange and
its uncertainty. An opt-in generic four-line annotation hook also places the
warning and actual omitted quantity in the dataset comment, so an exported
intermediate does not lose its standalone-use warning. Existing decisions do
not change unless they request that annotation explicitly.

Verification: 30 omission tests pass, including positive and negative quantities,
uncertainty retention, caller non-mutation, unchanged ecoinvent targets, unrelated
owners and visible comments. An independent check of the actual 34 foregrounds
finds exactly one changed owner and one omitted exchange; all other inputs,
direct emissions and references remain. No Brightway database was written.
Full licensed inventories and the foreground solve remain in private analysis
files. This decision alone does not qualify the train sector or establish LCIA
equivalence.
