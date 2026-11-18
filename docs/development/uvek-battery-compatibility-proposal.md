# Battery compatibility proposal — not applied

All battery imports now prepare: 951 records. Current Python code then fails
twenty battery mass checks because UVEK treatment purchases are positive, while
legacy ecoinvent waste inputs are negative. Changing the physically correct
mapping signs would misrepresent disposal. Your latest instruction prohibits
Python source edits, so this proposal is supplied as an unapplied patch.

[Review the exact patch](proposed-battery-treatment-compatibility.patch).

| Python module | Necessary adaptation |
| --- | --- |
| `batteries/battery.py` | Forward the source family/biosphere context, read generated treatment suppliers' reference signs, and exclude disposal purchases from manufactured battery mass |
| `validation/validation.py` | Compare manufactured and discarded mass in the supplier's reference coordinates, retaining rejection of wrongly signed treatment credits |
| `database/new_database.py` | Forward source family/biosphere to battery, as already done for other supported sectors |
| `database/background.py` | Admit battery through the existing supported-sector list; other unfinished sectors stay refused |

No physical exchange sign or quantity is changed by this compatibility patch.
The existing density-learning rule still scales battery manufacture and disposal
together. Market-share logic, validation tolerances, missing-provider checks
and other sector transformations are unchanged. Legacy raw ecoinvent waste
coordinates remain the default; only generated, explicitly referenced
treatment suppliers supply their recorded reference sign. Invalid, zero or
ambiguous generated reference quantities remain errors.

The source context is also necessary for consistent electricity product labels
between scenario and baseline. In a controlled preview, the accounting changes
alone clear the battery callback but leave a graph-cycle flag. Forwarding the
correct UVEK context clears that flag through the existing shared normalization,
without changing graph validation or suppressing the issue.

The exact proposed behavior was executed in a private interpreter; repository
Python files were not edited. Results: 92 existing battery tests pass; all 951
inventories prepare; 29 battery activities are transformed, including 23 mass-
learning targets; and exhaustive certification of 12,898 activities reports
zero errors. The no-argument source default and refusals for other unverified
sectors remain unchanged. Source-file hashes were checked after the preview.
Formatting changes to the displayed patch were verified AST-equivalent to the
tested proposal.

This is a reviewable preview, not released support. Approval is needed before
applying the four-file exception to the no-source-change instruction. Actual
installation, the normal public battery workflow, a representative export/LCI
and combined-sector checks must follow. Fifteen sectors remain qualified;
battery, final energy, emissions and metals remain unfinished. Two historical
standalone review-replay test failures remain separately disclosed.
