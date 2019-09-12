# Coupling Brightway2 & Wurst Future Ecoinvent Toolset to the REMIND IAM.

[![Build Status](https://travis-ci.org/romainsacchi/rmnd-lca.svg?branch=master)](https://travis-ci.org/romainsacchi/rmnd-lca) [![Coverage Status](https://coveralls.io/github/romainsacchi/rmnd-lca/badge.svg)](https://coveralls.io/github/romainsacchi/rmnd-lca) [![Documentation](https://readthedocs.org/projects/rmnd-lca/badge/?version=latest)](https://rmnd-lca.readthedocs.io/en/latest/)


## Install

Download and install [miniconda](https://conda.io/miniconda.html), create and activate a new environment and run
```
conda install -y -q -c conda-forge -c cmutel -c haasad -c konstantinstadler brightway2 jupyter wurst
```

### Ecoinvent

The LCA database ecoinvent, version 3.5, is used in this project. This is commercial software and has to be purchased [here](https://www.ecoinvent.org/).

### Input Files

The following files are missing from the repository at the moment because the legal situation is not clear:
```
REMIND scenario data:

./data/Remind output files/BAU.mif
./data/Remind output files/RCP37.mif
./data/Remind output files/RCP26.mif

GAINS emission factors:

./data/GAINS emission factors.csv
```

These files will be put online asap. 

