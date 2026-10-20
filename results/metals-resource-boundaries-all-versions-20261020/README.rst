Resource-boundary integration matrix
====================================

Core A–G implementation, commit 5d69664b. All available supported releases
were tested with their real source databases and encrypted IMAGE SSP2-M
2020/2050 data using complete metals updates and the compact backend.

13 version/system-model combinations passed (26 scenario updates). Five
cases are blocked during default-inventory imports, before transformation;
3.5 cutoff is not locally available. These are not counted as passes.
The old-release import gaps remain outside the resource-boundary changes.

EN15804 uses its own source and premise cutoff transformation rules, matching
the existing integration-test convention. Consequential resource correction
remains disabled. Separate final-guard validation is stored in the adjacent
metals-resource-boundaries-guard-validation-20261020 directory.

Cases
-----

* 3.5 cutoff: not_available
* 3.6 cutoff: failed — Fix the unlinked exchanges before proceeding
* 3.7 cutoff: failed — Fix the unlinked exchanges before proceeding
* 3.7.1 cutoff: failed — Fix the unlinked exchanges before proceeding
* 3.8 cutoff: failed — Fix the unlinked exchanges before proceeding
* 3.8 consequential: failed — Fix the unlinked exchanges before proceeding
* 3.9 cutoff: passed
* 3.9 consequential: passed
* 3.9.1 cutoff: passed
* 3.9.1 consequential: passed
* 3.10 cutoff: passed
* 3.10 consequential: passed
* 3.10.1 cutoff: passed
* 3.10.1 consequential: passed
* 3.11 cutoff: passed
* 3.11 consequential: passed
* 3.12 cutoff: passed
* 3.12 consequential: passed
* 3.12 EN15804: passed

summary.json records configurations, source identities, code hashes,
execution counts and validation results. Individual case JSON files retain
the detailed results; raw logs remain in /private/tmp. Fixture tests cover
all 11 version labels separately and do not stand in for successful builds.

Reproduce with the archived dev/test_metals_all_versions.py script::

    python dev/test_metals_all_versions.py --output results/NEW_MATRIX
