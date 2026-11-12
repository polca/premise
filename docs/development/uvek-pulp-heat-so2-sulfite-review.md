# Liquid SO₂ and sodium sulfite

Five exact ecoinvent 3.9 cut-off correspondences are now qualified for UVEK 2026 with the ecoinvent 3.10 biosphere. They cover two liquid sulphur-dioxide markets, a Chinese rail-freight market, and the sodium-sulfite market and producer. The mappings add four helper definitions and reuse 17 existing recipe definitions unchanged. They introduce no runtime Python change or exchange omission.

The SO₂ markets use UVEK's complete liquid-SO₂ production process. All 15 original outer market entries, including freight and their uncertainty, remain separate. This is a functional supply proxy. About 18.65% of the source rest-of-world SO₂ supply comes from zinc and chemical-production co-products. The native process uses recovered refinery sulphur, so it replaces those production technologies, allocations and internal uncertainty. Their score differences may be material and remain unquantified. Keeping separate outer coefficients does not preserve the original co-product production model.

Sodium sulfite has a one-kilogram Na₂SO₃ reference. UVEK's similarly named sulphite entry has an ambiguous SO₃²⁻ reference, so it is not used as a direct substitute. Instead, the source sodium-sulfite market and producer retain their 43 non-production exchanges, formulas, parameters and supported uncertainty through reviewed native supplies and the canonical biosphere. Their SO₂ input uses the explicit proxy described above. No ionic-mass, purity, extra allocation or recovery-credit adjustment is added.

The Chinese general rail-freight market uses the complete European general rail service per unchanged tonne-kilometre. The original Chinese coal-freight candidate questions its functional unit in its own documentation. The selected European proxy has a documented freight-service basis, but changes geography, traction, loads and other operating conditions. In particular, it replaces the source 28.7% electric, 1.6% steam and 69.7% diesel portfolio with the native European inventory. Its effect remains unquantified. Native operation and infrastructure are counted once.

Validation passed:

- 96 focused mapping checks, including the preceding allyl-chloride batch, and 48 shared migration checks.
- Three Premise tests covering fresh imports, cached imports and withdrawal of approvals, including the preceding two mapping batches.
- Sixteen signed finite LCI calculations. The sodium-sulfite market uses 59 native suppliers and 131 directly linked biosphere identities; its producer uses 56 suppliers. The original native database remains unchanged.
- Installation from GitHub, preserving all 1,210 earlier approval fingerprints and validating 1,215 scopes in total. New mappings retain signs, quantities, uncertainty and formulas and refuse unapproved profiles or borrowed consent.

Brightpath commit: `a680d650321f4a9899c82859847363793be7b0b8`. Premise pins that revision. See [verification receipts](uvek-pulp-heat-so2-sulfite-verification.json).

The repeated five-pulp-heat probe now reports **45 unresolved correspondences, down from 46**, with no new blocker. Sodium sulfite is the newly resolved direct input. This does not complete the pulp inventories, the industrial-heat dependency or a steel update/export. The previously recorded full-suite failures also remain unresolved; these focused results are not a full-suite pass.
