# Metals: five source-3.8 chemical mappings

Five ecoinvent 3.8 cut-off chemical inputs now have reviewed native UVEK
correspondences. Four market helpers retain **nine producer entries and 30
final freight entries**, separately and without renormalization. The fifth
mapping uses a complete native supply directly. Source signed quantities,
supported uncertainty, caller formulas and separate emissions remain intact.
Only these five exact source-profile permissions are added. The 17 freight
correspondences are reviewed inside the market helpers; they do not acquire
standalone global permissions.

| Source | Native model and disclosed limits |
| --- | --- |
| Ammonium chloride market | Native chlorosilane and modified-Solvay ammonium chloride. The direct ammonia/HCl branch uses Solvay as a route proxy. All three producer weights and four freight entries remain. |
| Calcium chloride market | Native Solvay and allyl-chloride-hypochlorination CaCl2. The rare-earth co-product uses Solvay as a route/allocation proxy. Three producer entries and ten freight entries remain. |
| Packed milled quicklime market | Complete native packed milled quicklime, including upstream calcination. The original source explicitly mills quicklime in pieces. One producer and twelve freight entries remain. |
| Light-soda market labelled heptahydrate | Complete native light-soda powder for both Solvay branches and all four freight entries. The source's own neutralizer converter uses 1.325 kg soda per kg NaOH-equivalent, as for dry dense soda. Preserve that implemented carbonate-function basis at factor +1; physical heptahydrate equivalence remains unverified. No hydration water or molecular-weight factor is invented. |
| Generic neutralizer, NaOH-equivalent | Complete native European caustic soda per active kg, at factor +1 per source NaOH-equivalent. This replaces the source carbonate/bicarbonate/caustic portfolio and its delivery; only neutralizing capacity is matched. Reagent-specific pH, reaction products and use-phase CO2 are not claimed equivalent. No extra reaction credit or emission is added. |

The previous LED and lithium-hydroxide matches were unrelated products. Native
CaCl2 is not rescaled using generic text about 77% de-icing flakes. Native
quicklime's documented CaO/calcination chain is not changed to hydrated lime
because of inconsistent generic CAS/synonym metadata. Native caustic is
referenced per active NaOH mass despite the 50% solution title: do not multiply
by two or add dilution water.

The caustic basis reuses the frozen active-chemical review and is rechecked in
all three current native cell technologies. The paired allocated salt-input
ratio for NaOH/chlorine is 0.997487684729064 in both releases, supporting the
active-mass convention. Current 2026 reimport raw hashes differ from the earlier
review, while its reference amount, documentation and three technology-mix
coordinates agree. This quantity-basis check does not establish equal scores or
complete inventory identity.

All correspondences are screening decisions under the user's standing
auto-approval. Source/native route, allocation, geography, fleet, vintage,
emissions and uncertainty differences are retained as warnings, with unknown
score effects. Native inventories are used complete, without stacking extra
production, infrastructure, water, losses or credits. Source background
parameter graphs and correlations are not copied without their context;
calculated coefficients and supported marginal statistics are retained.

Verification: **11 Brightpath tests**, **six Premise connector tests**, **ten
signed foreground LCI solves** and **five independent flattened native-demand
LCI comparisons** pass. Four helpers and five transient callers use a memory
datapackage; no Brightway database, registered activity or processed file is
added. Source/biosphere metadata and source records are preserved. The first
complete retry clears the five technosphere refusals and exposes three generic
Sodium-to-water flows; their separate omission review is documented in
`uvek-metals-38-sodium-omissions.md`. The shared metals sector remains unfinished:
the later retained-workbook cohort and native filters have not yet passed.

Frozen Brightpath evidence:
`export/uvek_metals_38_chemical_markets_review.json`.
