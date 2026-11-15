# Native process proxies for retained metals workbooks

Seven retained inventories are allocated copies of older mining/refinery
inventories: three ruthenium/iridium records from ecoinvent 3.8 and four
germanium/rhenium records from 3.9. Their individual background purchases produced
19 and 92 correspondence refusals respectively. Some suggested matches are
physically wrong: PGM concentrate to copper production and sodium sulfide to
sodium arsenide. These correspondences remain unapproved.

Instead, `native_process_templates.yaml` describes seven complete native process
proxies. This decision uses the user's standing authorization for autonomous,
logged screening decisions. It preserves each foreground identity, positive
one-kg output, caller quantities and explicitly attributed one-kg own-metal
resource. Native technology, internal supply, allocation, geography, vintage,
emissions and internal uncertainty replace the source process model. Score
equivalence is not established; these are documented screening limitations.

## Basis and boundaries

The three PGM records use native `Platinum, primary, at refinery` in the original
ZA or RU location. Read-only comparison with the exact source-release platinum
parent finds constant factors over all eight/fourteen shared technosphere and
54 shared nonmetal biosphere coordinates. South African ruthenium has different
technosphere and nonmetal-biosphere factors; a single price ratio would be wrong.
The native direct platinum-resource exchange is removed and the source's own
ruthenium/iridium resource retained. This standardizes direct own-metal
attribution; it does not prove the original and native upstream resource balances
are equal. Do not add the original concentrate/refinery purchases again.

The native PGM documentation includes UCTE electricity assumptions and recommends
detailed modelling where PGM contributions matter. Both limitations remain
warnings. Country labels alone do not establish geographic equivalence, and
these proxies should not be presented as detailed PGM production studies.

Germanium uses complete Swedish combined-metal zinc mining/beneficiation;
rhenium uses the corresponding copper process. Native documentation explicitly
references one kg of metal contained in an unrefined product, excluding further
refining. The original source parents explicitly describe 59% Zn and **29.63%
Cu** in their concentrate products. The observed source-parent scaling therefore
multiplies these documented fractions. No guessed 25% Cu content, density,
smelting yield or recovery credit is used.

All 127 shared germanium technosphere coordinates and 23 shared coordinates per
rhenium record have constant source-parent factors. Germanium also changes three
recycled-material handovers relative to its parent; these original exchanges
remain in the audit and are replaced by the native internal model. Rhenium's own
resource is explicitly reset to one kg in the source; this resource is retained
without the mining multiplier. Native coupled-metal resource attribution is
otherwise retained for these four records. The Swedish mining route replaces
the original country/global ore and production models with explicit geographic,
technology and allocation warnings.

## Small shared preparation change

Existing mapping rules migrate purchases from a foreground; they cannot replace
that foreground's complete known process inventory. The existing native donor
filler handles unknown placeholders and deliberately refuses known nonzero
inputs. Weakening that guard would silently change its meaning.

The necessary adaptation is confined to the shared owned-inventory preparer,
`replacement_quantities.py`. It reads a separate, exact-context YAML review,
checks the source workbook, identities, positive unit references, retained
resource and donor exclusions, and preserves the original exchanges in owned
provenance. It scales copied native exchanges using the existing uncertainty
utility. Native purchases reuse existing boundary anchors; native elementary
flows reuse the existing holding mechanism. Sector methods, market construction,
allocation algorithms and the ecoinvent path are unchanged by this change.

The recipe, warnings, replaced source exchanges, excluded donor exchanges and
retained resources enter diagnostics, provenance and migration-cache identity.
No inverse route, consequential conversion or unrelated foreground is admitted.

## Verification and remaining work

37 focused native-template and existing placeholder tests pass. These cover
negative native exchanges, normal/lognormal uncertainty, separate type factors,
resource attribution, idempotence, audit preservation and refusal of changed
physical scopes. Installed-package checks pass too.

The actual REMIND SSP2-NPi2050 snapshot workflow now completes all selected metals
inventory preparation and links all seven records. Fourteen signed foreground
LCIs and seven independent native-reference calculations pass at the existing
tolerances. The coefficient vectors match the explicit native construction, and
the source/biosphere metadata and database registry remain unchanged. No new
Brightway databases, activities or processed files were created.

This is **not metals-sector qualification**. The diagnostic stops inside the
ordinary metals constructor because its freight selector requests an
ecoinvent-named global market. Native freight mappings, family/biosphere
forwarding, activity-filter coverage, market outputs, validation and export still
need verification. The public sector gate remains unchanged. Snapshot evidence
does not verify live archive freshness.
