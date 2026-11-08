# Dense soda ash: approved screening proxy

The PFSA membrane dependency buys ecoinvent 3.9 cut-off's global
`market for soda ash, dense` (`bp-e1e1fd1c04f327da`). Its reference is
one positive kg of active sodium carbonate. The existing proposed counterpart,
UVEK's European `Soda, powder, at plant`, also references one positive kg of
100% sodium carbonate, but explicitly models **light soda** and excludes the
process that makes dense soda. Matching chemical identity alone does not
establish the missing processing burden.

User-approved scoped screening decision: use the complete native light-soda
inventory once per signed source kg, with factor +1. Apply no bulk-density,
solution-concentration or yield multiplier. Preserve separate caller materials,
water, transport, emissions, formulas and uncertainty. Add no invented
densification process or energy requirement.

Warnings and provenance record the missing densification, physical-form
difference, global-to-European supply proxy, source Hou-process coproduct versus
native Solvay allocation, market-delivery and vintage differences, and the
unquantified impact-score effect. Preserve the native 33% soda / 67% calcium
chloride economic allocation as already embedded; do not apply it a second time.
This covers only the inspected 3.9 cut-off market, not other grades,
versions, inverse routes or system models.

Read-only evidence pins:

| Inventory | SHA-256 of exchange-vector blobs |
| --- | --- |
| Source market, ecoinvent 3.9 cut-off | `50705a02dd0dc9993257b48857db435eda7079d2873cb6fc718898156a38ddeb` |
| Native UVEK 2025 counterpart | `65e65564999ff101d2849978d7777f9490b482f74da48d152e42289ac146f228` |
| Native UVEK 2026 counterpart | `533e398b6d071cf2870c7434172c05487b1c1a07f3777d25be0ac018ab67e948` |

The native descriptions explicitly establish the light-soda boundary and its
exclusion. The user approved “Approve light-soda proxy with warnings”. The exact 3.9
cut-off market is registered in the pilot approval ledger with packaged evidence
and explicit limitations. This decision does not certify the full prospective
scenario or authorize another profile, grade or reconstructed inventory.
