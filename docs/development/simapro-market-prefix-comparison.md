# SimaPro comparison after market-prefix naming fix

Regenerated the full Brightpath CSV from the corrected prepared REMIND
SSP1-PkBudg1000 2050, ecoinvent 3.12 cut-off inventory, and compared it against
the unchanged latest reviewed legacy CSV. Both contain 42,770 processes.
The two Indian cottonseed energy-feed suppliers now have distinct product labels,
verified in the generated CSV itself.

| Metric | Before fix | After fix |
| --- | ---: | ---: |
| Repeated Brightpath product labels | 246 | 77 |
| Exchange rows with nonunique supplier labels | 43,858 | 31,519 |
| legacy_only_label | 242,697 | 230,361 |
| brightpath_only_label | 225,759 | 213,591 |
| amount_beyond_legacy_rounding | 515 | 515 |
| section | 29,794 | 29,880 |
| uncertainty_fields | 13,173 | 13,331 |
| multiplicity | 197 | 197 |

The remaining naming collisions include real markets with meaningful qualifiers,
such as wind-turbine markets with and without `direct drive`. Those qualifiers
are still removed by market-name normalization. This fix addresses internal
`market for` phrases only; it does not resolve every naming collision.

There remain 259 inferred classification disagreements and 1,961 explicit legacy
category fallbacks. Unit-label supplementation, empty-comment handling,
inventory-indicator omission, fossil-well handling, biosphere mappings and
blacklisting are unchanged. No SimaPro application import or LCIA comparison
was performed. Counts of unmatched labels are not counts of missing flows;
renaming and ambiguous labels affect matching.

Artifacts are under `export/simapro-comparison/market-prefix/`:

- Regenerated Brightpath CSV in `brightpath-compatibility/`
- Link to the unchanged reviewed legacy CSV in `legacy/`
- `comparison-summary.json` with file hashes
- `csv-differences.csv`, `product-label-collisions.csv`, `process-identities.csv`
- `reproduce/` scripts requiring both checkouts and the prepared local snapshot

Licensed inventory data remains excluded from Git. No commit or push was made.
