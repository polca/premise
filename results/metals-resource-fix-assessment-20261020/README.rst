Whole-row assessment of proposed resource corrections
=======================================================

Each proposal A–G was applied independently to an in-memory copy of the saved
biosphere matrix. All seven were also combined. No premise production code,
inventory workbook, saved Brightway database or characterization factor was
modified. This is an exact fixed-technosphere sensitivity assessment of the
proposals, not a rebuild with implemented premise rules.

The functional unit is one kg reference product for each of the same 284
production datasets, including country proxies. All 65 RELICS indicators are
recalculated for each case. Positive and negative off-diagonal changes are
reported, not only improvements in the producer's own-element score.

Findings
--------

A: Corrects the documented Sb content: the new route reaches 1.000, while legacy routes rise to 2.012. Only the Antimony column changes, including downstream off-diagonal use. Legacy yield/proxy review remains necessary.

B: Zinc-refining copper co-product rises from 0.013539 to 1.013543. Its other 64 indicators are unchanged. Copper extraction also increases where this co-product enters other producers' supply chains.

C: Zinc-refining silver co-product rises from 0.000175 to 1.000186. Its other 64 indicators are unchanged. Silver-column changes propagate to other supply chains.

D: Lithium electrolysis reaches 1.094125 and foil reaches 1.135702. Other indicator columns are unchanged. The retained Spodumene mineral indicator is not additional elemental lithium and must not be summed with Lithium as an independent metal quantity.

E: Arsenic rises to 1.545436, but Zinc remains 1.552068 in the 7N As row. This repairs missing As attribution only; host-metal proxy attribution remains unresolved. Hold as a complete allocation repair pending review of the zinc-based proxy.

F: All six metallurgical silicon outputs reach 1.001663–1.003107. Other columns are unchanged. Previously missing Silicon attribution propagates substantially: ZA iridium's Silicon score rises from 146.292055 to 161.865336.

G: The carbonate-branch transfer restores oxide-contained REE attribution and removes inherited co-mined REE baskets. Dy rises to 1.190680 while its Nd falls from 1.589950 to 0.000609. Non-REE columns, including Sodium and Iron, are unchanged by G.

combined: All seven resource-only sensitivities combined. The technosphere and physical supplier demand are unchanged. Effects add exactly; 20 indicator columns change. This assessment does not implement production rules or qualify proxy inventories.

With all proposals combined, 274/284 own-element results are within +/-10%
(baseline 262), and 270 are within +/-5% (baseline 261). These are descriptive,
unweighted dataset counts; they include regional copies and do not independently
validate physical extraction or inventory quality.

For dysprosium, G changes Nd 1.589950 -> 0.000609 and Pr 0.437149 -> 0.000201;
Dy 0.252891 -> 1.190680. Sodium stays 67.584313 and Iron stays 7.783409.
For arsenic, E changes As ~2.53e-7 -> 1.545436 while Zn stays 1.552068.
For lithium electrolysis, D changes Li ~6.75e-11 -> 1.094125 while Spodumene
stays 31.990875. Spodumene is a mineral indicator, not an additional elemental-Li
quantity to sum with the Lithium indicator.

Verification
------------

Every baseline row was reproduced against the saved audit; maximum absolute
score difference 1.82e-12 kg/kg. For all 19 directly assessed producer rows,
an independent normal Brightway LCIA on a patched biosphere matrix verifies
the calculated full 65-indicator vector; maximum vector difference 2.85e-14.
Combined effects equal the sum of independent effects. The saved database
fingerprint and the original matrices remain unchanged. Changed-pair reporting
uses an absolute 1e-9 kg/kg threshold. See metadata.json and numerical_checks.csv.

Lutetium is included in the complete carbonate-branch resource transfer but has
no indicator in the 65-method RELICS set. Its direct proposed changes are listed
in direct_resource_changes.csv; this score comparison cannot validate its
characterization. The corresponding metadata field records this coverage gap.

Files
-----

* report.html: offline interactive comparison; select any fix and dataset and
  inspect every before/after indicator, with optional changed-indicator filter.
* whole_row_assessment.xlsx: baseline, each individual fix, combined results,
  own-element comparisons and changed-pair tables.
* before_all_indicators.csv and A_all_indicators.csv through G_all_indicators.csv,
  plus combined_all_indicators.csv: complete 284-by-65 matrices.
* all_changed_pairs.csv: every material score change across all producers,
  including off-diagonal effects of each individual fix and the combination.
* focal_rows_all_indicators.csv: all 65 before/after indicators for producers
  directly targeted by the corresponding fix; unchanged indicators retained.
* focal_own_element.csv and focal_offdiagonal_changes.csv: compact focal tables.
* direct_resource_changes.csv: proposed direct elementary-resource adjustments.
* fix_summary.csv: affected dataset/indicator counts and score-band counts.
* reproduction_scripts.zip: exact calculation and packaging scripts under dev/.

Reproduction
------------

Use the premise environment and local Brightway project recorded in metadata.
Extract the archived scripts at the repository root, then run::

    python dev/assess_relics_resource_fixes.py results/metals-primary-relics-20261020-full results/NEW_ASSESSMENT
    python dev/report_relics_fix_assessment.py results/NEW_ASSESSMENT
