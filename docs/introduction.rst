# Coupling Integrated Assessment Models and ecoinvent.

<p>
<a href="https://travis-ci.org/romainsacchi/rmnd-lca" rel="nofollow"><img src="https://camo.githubusercontent.com/ad7ef34aec8925f5a9de57c7442325bcc3397d5ec3b85e4e6b4dcd2e092e3204/68747470733a2f2f7472617669732d63692e6f72672f726f6d61696e7361636368692f726d6e642d6c63612e7376673f6272616e63683d6d6173746572" alt="Build Status" data-canonical-src="https://travis-ci.org/romainsacchi/rmnd-lca.svg?branch=master" style="max-width:100%;"></a>
<a href="https://coveralls.io/github/romainsacchi/rmnd-lca?branch=master" rel="nofollow"><img src="https://camo.githubusercontent.com/dc4e7f62f5512c0e8365d2a7a437d1729df3d267d72ec6b8efc2a91bc18e3b36/68747470733a2f2f636f766572616c6c732e696f2f7265706f732f6769746875622f726f6d61696e7361636368692f726d6e642d6c63612f62616467652e7376673f6272616e63683d6d6173746572" alt="Coverage Status" data-canonical-src="https://coveralls.io/repos/github/romainsacchi/rmnd-lca/badge.svg?branch=master" style="max-width:100%;"></a>
<a href="https://rmnd-lca.readthedocs.io/en/latest/" rel="nofollow"><img src="https://camo.githubusercontent.com/b3c50f02deae75a2a92509c6dedd89003dcedb85aa52822105e48158363cf79e/68747470733a2f2f72656164746865646f63732e6f72672f70726f6a656374732f726d6e642d6c63612f62616467652f3f76657273696f6e3d6c6174657374" alt="Documentation" data-canonical-src="https://readthedocs.org/projects/rmnd-lca/badge/?version=latest" style="max-width:100%;"></a>
<a href="https://badge.fury.io/py/rmnd-lca" rel="nofollow"><img src="https://camo.githubusercontent.com/ffd2ba9c77f29f90825c1d6a49241d3e01bac4dfbe76af9fc2231a0e73e82dbf/68747470733a2f2f62616467652e667572792e696f2f70792f726d6e642d6c63612e737667" alt="PyPI version" data-canonical-src="https://badge.fury.io/py/rmnd-lca.svg" style="max-width:100%;"></a>
</p>


Introduction
============

**rmnd-lca** allows to align the life cycle inventories contained in the life cycle inventory database **ecoinvent 3 cutoff**
with the output results of Integrated Assessment Models (IAM), such as **REMIND** or **IMAGE**,
in order to produce life cycle inventories under future policy scenarios for any year between 2005 and 2100.

More specifically, **rmnd-lca** will apply a series of transformation functions to ecoinvent.

In the latest version (0.1.7), the following transformation functions are available:

* **update_electricity_to_iam_data()**: alignment of regional electricity production mixes as well as efficiencies for a number of
electricity production technologies, including Carbon Capture and Storage technologies.
* **update_cars()**: fuel markets that supply transport vehicles are adjusted according to the IAM projections,
including penetration of bio- and synthetic fuels.
* **update_cement_to_iam_data()**: adjustment of technologies for cement production (dry, semi-dry, wet, with pre-heater or not),
fuel efficiency of kilns, fuel mix of kilns (including biomass and waste fuels) and clinker-to-cement ratio.
* **update_steel_to_iam_data()**: adjustment of process efficiency, fuel mix and share of secondary steel in steel markets.

However, whether or not these transformation functions can be applied will depend on the existence of the necessary variables in
the IAM file you use as input.

.. csv-table:: Availability of transformation functions
    :file: table_1.csv
    :widths: 10 10 30 10 10 10 10
    :header-rows: 1


The following REMIND IAM files come with the library:

* SSP2
    1.  **Base:** counter-factual scenario with no climate policy implemented
    2.  **NPi** (*N*ational *P*olicies *i*mplemented): scenario  describes energy,  climate  and  economic  projections for the  period  until 2030, and equivalent efforts thereafter. See [CD-LINKS modelling protocol](https://www.cd-links.org/wp-content/uploads/2016/06/CD-LINKS-global-exercise-protocol_secondround_for-website.pdf) for details.
    3.  **NDC**: All emission reductions and other mitigation commitments of the *N*ationally*D*etermined *C*ontributions under the Paris Agreement are implemented. See [CD-LINKS modelling protocol](https://www.cd-links.org/wp-content/uploads/2016/06/CD-LINKS-global-exercise-protocol_secondround_for-website.pdf) for details.
    4.  **PkBudg 1300/1100/900**: Climate policies to limit cumulative 2011-2100 CO2 emissions to 1300 / 1100 / 900 over the entire time horizon (“not-to-exceed”). Correspond to 2°, well-below 2° and 1.5° targets. Other greenhouse gases are priced with the CO2e-price using 100year global warming potentials.

The following IMAGE IAM files come with the library:

* SSP2
    1.  **Base** counter-factual scenario with no climate policy implemented

You can however use any other IAM files.

Additionally, a number of inventories for emerging technologies are added to the database.

* electricity production using various fuels (including biomass and biogas) with Carbon Capture and Storage (CCS) [Volkart et al. 2013](https://doi.org/10.1016/j.ijggc.2013.03.003)
* hydrogen production from electrolysis from different world regions,
* hydrogen production from steam methane reforming (SMR) and auto-thermal reforming (ATR) of natural gas and biogas, with and without CCS [Antonini et al. 2020](https://doi.org/10.1039/D0SE00222D)
* hydrogen production from coal gasification [Antonini et al. 2020](https://doi.org/10.1039/D0SE00222D)
* hydrogen production from woody biomass gasification, with and without CCS [Antonini et al. 2020](https://doi.org/10.1039/D0SE00222D)
* synthetic fuels from Fischer-Tropsh (diesel), Methanol-to-liquid (gasoline) and electrolchemical methanation (gas) processes,
 using direct air capture (DAC) [Zhang et al. 2019](https://doi.org/10.1039/C9SE00986H)
* passenger car inventories from the library [carculator](https://github.com/romainsacchi/carculator)
* medium and heavy duty trucks from teh library [carculator_truck](https://github.com/romainsacchi/carculator_truck)


Requirements
------------
* Python language interpreter 3.x
* License for ecoinvent 3
* Brightway2
* Some IAM output files come with the library ("REMIND_xxx.mif" for REMIND, "IMAGE_xxxx.xlsx" for IMAGE)
 and are located by default in the subdirectory "/data/iam_output_files".
 A file path can be specified to fetch IAM output files elsewhere on your computer.

How to install this package?
----------------------------

Two options:

A development version with the latest advancements (but with the risks of unseen bugs),
is available on Conda:

    conda install rmnd-lca

For a more stable and proven version, from Pypi:

    pip install rmnd-lca

will install the package and the required dependencies.

How to use it?
--------------

Examples notebook
*****************

This notebook will show you everything you need to know to use **rmnd_lca**.

