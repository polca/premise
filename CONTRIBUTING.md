# Contributing

Your contribution is welcome! Please follow the [pull request workflow](https://guides.github.com/introduction/flow/), even for minor changes.

When contributing to this repository with a major change, please first discuss the change you wish to make via issue, email, or any other method with the owners of this repository.

Please note we have a [code of conduct](https://github.com/romainsacchi/premise/blob/master/CODE_OF_CONDUCT.md), please follow it in all your interactions with the project.

## Documentation and coding standards

* [Black formatting](https://black.readthedocs.io/en/stable/)
* [Sphinx docstrings](https://sphinx-rtd-tutorial.readthedocs.io/en/latest/docstrings.html)
* [Semantic versioning](http://semver.org/)
* Data should be in text formats, e.g. JSON or CSV

## Runtime checks

For every fix, state whether a runtime path changes. For runtime changes,
compare the parent commit and candidate on identical representative inputs,
and check the expected outputs. Require output equivalence when behavior should
be unchanged. Include data and configuration changes that alter execution paths
or workload size. Warm up both implementations, alternate their measurement
order, and repeat timings without competing test/build jobs.
Report absolute times, relative change and variability; distinguish the affected
operation from total run time and state which setup or I/O costs are included.
Exercise cold and warm caches when relevant to the change.

Resolve repeatable slowdowns before accepting a fix. Differences within
measurement noise are not evidence of a speedup; report that uncertainty.
Documentation-only changes need no benchmark when runtime code is unchanged.

## Authors

* [Romain Sacchi](https://github.com/romainsacchi)
* [Alois Dirnaichner](https://github.com/Loisel)
* [Tom Mike Terlouw](https://github.com/tomterlouw)
* [Laurent Vandepaer](https://github.com/lvandepaer)
* [Chris Mutel](https://github.com/cmutel/)
* Your name here!
