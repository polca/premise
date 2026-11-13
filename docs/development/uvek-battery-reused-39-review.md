# Two existing reviews reused for the exact 3.9 battery inputs

The battery preparation now clears two more correspondences: the global copper
scrap market and the RoW chemical-industry heat service in ecoinvent 3.9 cut-off.
This extends only Premise's scoped approval data. Brightpath's existing recipes
and frozen reviews are unchanged; no runtime Python module is modified.

For copper scrap, the inspected upstream cut-off stock has one positive kg of
production and no other exchanges. Its separate allocated-stock helper retains
that boundary. This is not a zero-burden recycling process. All four original
freight coefficients remain separate and unchanged, with complete native rail,
barge, heavy-lorry and container-ship services. Global-to-regional fleet, fuel,
technology and vintage differences are explicit screening limitations. No virgin
metal burden, recovery credit, payload correction or normalization is added.

For chemical heat, both references are one MJ. The exact 3.9 helper supplies one
native chemical-plant heat service, whose documented boundary excludes make-up
water. It also retains all seven source water-provider quantities and both Water
releases, with their original air/water compartments and cubic-metre basis. The
native RER heat and CH water supplies remain geographic/technology proxies. The
3.8 approval remains unchanged; neither its water coefficients nor the rule's
default helper substitutes for the exact 3.9 recipe. No kg-steam multiplier,
additional source fuel/electricity or water correction is introduced.

Caller signs, units, formulas, supported uncertainty and separate emissions are
preserved. Existing deterministic helpers do not reconstruct internal source
uncertainty or formulas, including uncertainty on the heat water releases. This
limitation is retained in the approval warnings; stochastic or LCIA equivalence
is not claimed. Both complete helper hierarchies must be used.

Fresh and cached preparation, individual approval refusal and in-memory signed
inventory calculations pass. The two transient consumers and three helpers are
never registered in Brightway. Native-source and biosphere metadata, database
names and processed files remain unchanged. The real battery retry clears
exactly these two rules: the inspected source-3.9 cohort goes from 190 to 188
unapproved correspondences, with none added. Battery preparation and export are
still unfinished; eight of 19 sectors remain qualified for the selected scenario.

The aluminium-scrap market remains unapproved: its Chinese generic rail input
currently selects coal freight and needs a separate transport review. The next
reported battery error still concerns sodium fluoride incorrectly matched to
sodium arsenide; that chemical mismatch remains refused.

See [verification](uvek-battery-reused-39-verification.json). Changes join the next
batch publication, with Brightpath published before the Premise install pin.
