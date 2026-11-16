# Preserve source freight identities during regional relinking

The first combined thirteen-sector run completed all updates but its final
exhaustive validation found 95 duplicate-supplier exchanges. A checked example
was a European pellet factory: six equally weighted freight inputs from IT,
AT, RER, DE, FR and BE became the same EUR train-market input. Their original
country distinction was lost when the common relinker replaced the provider.
The preceding twelve-sector checkpoint preserves the six distinct countries.

The common transport relinker now appends the original name, product and
location to the exchange comment whenever it changes that identity. It retains
all original rows, amounts, uncertainty and existing comments. It changes no
market selection, fleet mix, efficiency or loss equation. Collapsing the rows
into one uncertain coefficient would need a separate uncertainty/correlation
model, so this fix does not aggregate them or invent a distribution.

This is a necessary, generic provenance correction in the existing transport
module. Mapping-only changes cannot preserve a provider identity that the shared
relinker overwrites. No new UVEK transformation path is added. The validation rule
is unchanged: parallel contributions from distinct source suppliers remain
traceable, while exact duplicates from the same original supplier still fail.
Six rail tests pass, including both of those cases and unchanged uncertain
quantities. The combined run is being repeated; this document alone does not
qualify trains or establish a successful public workflow/export.
