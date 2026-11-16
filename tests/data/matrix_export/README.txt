Matrix CSV reference hashes were generated with premise commit 31780801.
The fixture is tests/test_matrix_export.py::matrix_scenario: two activities,
every packaged biosphere flow for ecoinvent 3.8–3.12, signed/zero amounts,
uncertainty fields, Unicode and CSV quoting. Hashes cover all four complete CSVs.

Keep these frozen when refactoring. If an intentional format or biosphere-table
change requires new hashes, review the full CSV differences and record the new
reference commit. Avoid regenerating them solely to make a failing test pass.
