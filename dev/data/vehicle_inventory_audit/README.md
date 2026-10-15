# Vehicle inventory audit artifacts

The CSV files preserve direct noise exchanges generated with the October 2026
passenger-car and truck inventory refreshes. Premise does not import these
exchanges.

The JSON files preserve source revisions, generation metadata, and hashes for
the installed vehicle workbooks and excluded audit artifacts. Regression tests
verify those hashes, but Premise does not read the JSON files at runtime.

None of the files in this directory are runtime package data.
