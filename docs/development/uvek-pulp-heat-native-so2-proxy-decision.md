# Liquid sulphur dioxide: use the native supply model

This is a reviewed modelling decision under the user's standing approval of forthcoming decisions. The candidate passes private conversion and calculation checks. It is not yet installed in the published mapping resources or approved by the published Premise ledger.

UVEK supplies liquid sulphur dioxide with the same chemical identity and one-kilogram reference as the two inspected ecoinvent 3.9 cut-off markets. Use that complete native production process as a screening proxy, retaining each original market quantity and freight entry. Preserve the caller's signed quantity and uncertainty. The native plant includes its own raw materials, energy and infrastructure; these are counted once.

There is an important limitation. In the source rest-of-world market, about 18.65% of sulphur dioxide is a co-product of zinc production or chemical production. The native process uses recovered refinery sulphur. This substitution changes the production technologies and their allocation of environmental burdens. Keeping the original outer market coefficients does not preserve those co-product inventories. The score difference may be material and has not been quantified. Warnings and the review record must make this visible.

The earlier investigation attempted to reproduce the source chemical network, which contains a circular supply chain. Supporting that network would require a Brightpath Python change. The selected complete native proxy avoids that requirement and better follows the instruction to solve background correspondences through mappings. Retain the investigation and test evidence; archive and withdraw the uncommitted cycle-support draft once the running test processes finish.

Sodium sulfite itself still needs its source inventory reconstructed. Its reference is one kilogram of sodium sulfite, Na₂SO₃. UVEK's similarly named sulphite entry has an ambiguous ionic reference, so it is not used as a direct one-for-one substitute. The private test converts the complete source market and producer using the reviewed native sulphur-dioxide supply. A Chinese rail-freight correspondence also needs a documented European proxy: the available Chinese coal-freight record carries an explicit functional-unit caveat in its original documentation.

Checks completed:

- Strict conversion of both source sodium-sulfite processes produces 20 linked records without unresolved correspondences, using unchanged Brightpath revision `a95a4d5`.
- The original two process identities, positive reference quantities and exchange counts remain intact.
- Fourteen positive/negative LCI calculations are finite and reverse signs exactly. The complete sodium-sulfite case uses 56 native suppliers and 131 directly linked biosphere identities.
- The native database remains unchanged. These checks do not establish equal impact scores or uncertainty distributions.

Next, freeze and test the mapping resources and exact approvals, then repeat the five-pulp conversion probe. The published backlog remains 46 direct correspondences until that integration is verified. Full steel and all-sector exports remain unfinished.

Detailed evidence: [candidate verification](uvek-pulp-heat-native-so2-proxy-preflight.json) and [retained investigation](uvek-pulp-chemical-reconstruction-preflight.json). Original licensed inventories remain outside Git.
