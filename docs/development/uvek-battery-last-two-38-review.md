# Resolve the remaining source-3.8 battery chemicals

The required ecoinvent 3.8 inventory group now converts successfully: its real
CLIC cohort produces 204 target activities with no audit errors. The source-3.9
group also passes, producing 567 activities. The full battery preparation retry
then reaches the source-3.12 group, where 73 correspondences remain unapproved.
This is preparation progress, not a completed battery update or new export.
Fifteen of nineteen sectors remain qualified.

The two corrected chemical markets are manganese sulfate and vinyl carbonate
(vinylene carbonate). Their former cobalt-sulfate and lithium-carbonate heuristic
candidates remain unacceptable substitutes. Since neither finished chemical has
a matching native process, the mappings retain immediate manufacturing steps
using complete native supplies and existing reviewed backgrounds. Triethylamine,
needed for vinylene carbonate, has one additional ethanol/ammonia manufacturing
step. No deeper ethanol, heat, wastewater or freight supply chain is rebuilt.

Six target helpers cover three manufacturing processes and their supply markets.
They retain 84 positions from the six selected inventories, including 34 biosphere
positions mapped by the existing strict 3.8-to-3.10 route. An independent audit
checks 405 uncertainty fields, thirteen formulas and nine negative waste inputs
converted into positive treatment demand. Available parameters and stored amounts
are retained; missing source formula-variable definitions are not invented or
used to fit new quantities. All 4,423 older recipe inventories remain unchanged.

Two complete manufacturing proxies keep the work bounded. The sulfate market's
allocated manganese-dioxide co-product branch uses dedicated sulfate manufacture;
its original co-product inventory and allocation are replaced. Both triethylamine
market producer shares use inspected European manufacture; the RoW internal
electricity/ammonia portfolio is replaced. Original supplier quantities and final
freight positions remain distinct. These choices retain no claim of production,
allocation, geography or score equivalence. Native ore-grade/battery-quality
uncertainty, utility and delivery differences, the overburden process's contradictory
prose, and BAFU biosphere exclusions remain explicit warnings. There is no new
foreground omission, recovery credit, zero-burden authority or source-code edit.

All six helpers pass twelve signed LCI solves without registration in Brightway.
Two complete consumer inventories pass four more signed solves and two independent
flattened-native comparisons; the largest refined inventory difference is
2.28e-13. Fresh/warm public harmonization and sixteen signed caller-uncertainty
checks pass. The ordinary generator reproduces all three packaged resources
byte for byte. Its temporary copies were removed, reclaiming 112,803,743 bytes.

Focused checks pass 39 Brightpath tests and ten Premise approval tests. One
individual historical DMC authoring-replay test fails on the already published
Brightpath revision `9cc231a2ffccad4c4b72516731f2f1b6d2e3c266` as well: it expects
the parent before the later, already published source-3.8 extension. This baseline
failure is recorded separately; it is not reported as passing. The complete
ordered generator and DMC runtime checks pass. Python files, including this
outdated test fixture, remain unchanged under the user's instruction.

The next work is a grouped source-3.12 review. Of its 73 error identities, 33 have
reviewed models for other releases and sixteen more have an approved equivalent
product under another identity. Check native counterparts and whether any input
is unnecessarily included before adding models for the remaining product types.
Do not interpret 73 version-specific identities as 73 new manufacturing models.

The unchanged battery branch of `premise/inventory/clic_hybrid.py` explicitly
prepares every registered battery-capacity market. Per-role native YAML choices
do not remove that procedural set. Dropping these inputs would change the
standard workflow, so the current plan keeps them and favors whole native
counterparts and existing reviewed models. Exact source-3.12 reference witnesses
were captured read-only for all 73 identities: nine have negative references,
and none is an inventory-empty handover that could simply be omitted as empty.
