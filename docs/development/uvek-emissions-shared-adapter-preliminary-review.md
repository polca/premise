# Shared emissions adapter: preliminary exact packet

Status: private proposal only. No repository runtime Python was changed. This packet is not yet ready for approval: the final prepared-foreground coverage audit is still running.

The packet changes one runtime module, `premise/emissions/emissions.py`. It selects the requested family's emitter filters, prepares complete source copies only where sector or operating region requires them, then calls the existing shared GAINS factor preparation and update functions. The ecoinvent path retains its defaults. It does not add a separate factor engine or change equations, interpolation, reduction-only behaviour or uncertainty scaling.

The data extension sits beneath existing `uvek_aliases` in an optional `emission_sources` child, leaving the reviewed caller filters and ecoinvent mappings intact. It names the direct emitters and existing reviewed context resources, scopes native selection to air, preserves the documented unsplit CHP-methane exclusions and records screening limitations. No new NewDatabase arguments or background-database labels are required.

New tests cover regional and weighted-World factors, supplier reuse, signed purchases, uncertainty, deterministic handovers, preserved uncertain handovers, rollback and exact replay. The old native combustion-component tests call that retained component directly; they are not presented as tests of the new shared dispatch. All 807 focused existing and new checks pass. On the calculation-only combined graph, the exact module matches all 589,554 physical exchange positions of the independently checked preview, passes the sector contract and leaves its source fork unchanged.

The normal private nineteen-sector callbacks and exhaustive validation have passed with zero errors. A follow-up audit used the read-only wrapper as if it were the mutable internal store and failed after validation; the corrected run now saves its physical result and lightweight CLIC classification metadata before optional audits, so an audit error cannot discard that evidence again. No Brightway database was written. Public support remains 17 sectors.

Next: finish the bounded foreground/renamed-technology audit, refine this exact packet if needed, and request the required source exception. Normal installed public execution and export still require approval. The separate metals support-list request remains pending.

Exact preliminary packet: `proposed-shared-emissions-adapter-preliminary.patch`. Evidence: `uvek-emissions-shared-adapter-preliminary-verification.json`.
