# Shared emissions integration: private feasibility result

This is a diagnostic result, not a public emissions qualification or an applied Python patch. The repository's production Python files were unchanged.

The preview starts from the exact installed eighteen-sector physical result (19,735 activities), uses the actual REMIND SSP2-NPi 2050 GAINS data, and calls Premise's existing factor preparation, pollutant update and uncertainty functions. It supplies the missing native classification and operating-region context outside those functions. It follows only burden-free, deterministic +1 handovers of the same named service; it does not explore or redesign upstream supply chains.

The existing reviewed process classes select 3,997 emitting input positions. Three further reviewed positions have no emitting inventory eligible for this preview. For generated IAM-region callers the burner copy takes the caller's IAM location; other callers retain the reviewed supplier geography and existing geography proxies. Original upstream suppliers and quantities stay unchanged. This remains a screening geography assumption; the preview does not establish geographic or impact equivalence of native templates.

The preview uses the ordinary shipped GAINS pollutant map, including the verified canonical NMVOC alias. It does not invent particle bins or convert between flow definitions. Native emission selection remains restricted to air; the four documented unsplit CHP-methane positions stay unchanged. Unclassified caller/supplier combinations are outside this reviewed subset and remain coverage work, not silently treated as new eligible mappings.

## Avoiding unnecessary datasets

The initial conservative construction made 524 complete supplier copies. Reusing an existing supplier when its unique sector and GAINS region already agree removes 190 copies. The reduced result has 334 complete copies and reuses suppliers at 2,363 input positions. It changes 1,207 pollutant exchanges in 478 activities across 25 GAINS emission categories with available reductions. It creates no new upstream process model.

An independent comparison collapses redundant identical columns and deterministic unit handovers, then checks production, technosphere and biosphere coefficients. All 19,817 checked columns match exactly. There is no coefficient tolerance relaxation. Direct numerical comparison of two differently expanded LCI solves showed cancellation differences and is not used as evidence of equivalence. All 956 positive/negative LCI checks on the 478 changed activities in the reduced graph are finite and sign-linear.

## Remaining work

The public UVEK emissions gate remains closed. The private selection/topology preparation must still become a concrete, minimal runtime proposal for review; it is not yet a tested source patch. That adapter should select native aliases, bind shared burners to their reviewed sector and operating region, and use the existing shared updater for amounts and uncertainty. The present native-only dispatch must not remain a separate factor engine.

The existing `uvek_aliases.fltr` entries are caller pools checked by the earlier native review parser; replacing them with emitter filters would break that established contract. A small generic metadata extension should preserve those pools and place the direct-emitter filters and optional context resources in an `emission_sources` child. The shared emissions class can select that child when present and keep the existing filters as its default. This would avoid another change to the native review parser. It is a proposed integration shape, not an applied or verified runtime patch.

Prepared foregrounds, renamed technologies and unclassified contexts still need a bounded coverage audit. Normal ecoinvent skip behaviour and source limitations must remain explicit. Existing regression checks, exhaustive installed-workflow validation and one inspected public Brightway export remain mandatory. No Brightway database was written for this feasibility check.

Evidence: `uvek-emissions-shared-integration-feasibility.json`. The full eighteen-sector public-support-list request for metals is separate and still awaits approval.
