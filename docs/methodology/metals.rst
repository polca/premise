Metals
========


.. contents:: On this page
   :local:
   :depth: 1

.. raw:: html

   <span id="key-outputs"></span>

Scope and outputs
-------------------

The ``metals`` update changes selected direct material inputs in energy and
transport technologies, corrects specified resource-allocation flows, and
builds metal supply markets. It operates across different activity units;
material coefficients require explicit conversions to each reference product.

.. figure:: /_static/process-diagrams/metals.svg
   :class: process-diagram
   :alt: Metal supply and direct material inputs: Create metal supply markets; Apply in-ground resource allocation rules; Set mapped direct material inputs; Relink consumers and check.
   :align: center

Inputs and applicability
--------------------------

External material-intensity data, conversion rules and preservation policies
are stored under ``premise/data/metals``. Additional metal inventories and
production-share inputs supply market construction. These assumptions are
not automatically IAM metal-demand projections. Use ``ndb.update("metals")``
in the default sequence to retain the intended interactions with PV and other
technology updates.

Data collection and processing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Distributions for material intensities, derived from published literature, are provided in `SI_2_Material_requirements.xlsx <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/metals/SI_2_Material_requirements.xlsx>`_.
From this database, `metals_db.csv <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/metals/metals_db.csv>`_ is created,
which *premise* uses to update the material intensities for each technology.

The mapping file that associates metal intensities with datasets to be
updated can be found in `activities_mapping.yml <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/metals/activities_mapping.yml>`_.

To convert the units in `metals_db.csv <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/metals/metals_db.csv>`_
to the units used in ecoinvent (e.g., converting [kg metal/kW] to [kg metal/kg battery]), *premise* uses
the conversion factors in `technology_conversion_factors.yaml <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/metals/technology_conversion_factors.yaml>`_.

Finally, *premise* uses `metal_products.yaml <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/metals/metal_products.yaml>`_
to select the target activity, metal product and compound conversion. Each rule
has a stable identifier which is included in the structured change report.

Material preservation and conversion
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The six IEA PVPS 2026 silicon-cell names (20 geographic variants) have explicit
``preserve_source`` policies. Their source metallization pastes and other
components already include metals. Generic capacity-based material overlays
must not be applied directly to a square metre of cell. Preservation affects
these direct overrides; background scenario updates remain active.

For material updates not covered by a preservation policy, an explicit
conversion factor and matching activity unit are required. Missing conversions
stop the material update plan before application instead of defaulting to one.

The ``set_direct_amount`` operation sets a configured direct technosphere
input. It does not claim to set the total metal content of the complete supply
chain. An activity policy preserves the original material structure of
``EPR construction`` because this inventory already contains material-intensive
components. Full supply-chain metal demand can help screen for overlaps, but it
also includes energy, transport, mining and infrastructure and must not be
subtracted automatically.

.. raw:: html

   <span id="material-rules-and-validation"></span>

Transformation
----------------

Premise builds metal markets and applies the configured in-ground resource
allocation rules before updating direct material inputs and relinking.
Material intensities are interpolated within the available year range and
held at the endpoints outside it. Each direct material amount uses the
configured activity-unit conversion and compound/content factor.

Post-allocation correction
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Regarding the co-production of metals in multifunctional processes
(i.e., co-mining of metals), *premise* modifies the database to allocate
according to physical mass balance: extraction of individual elements in the
ore is fully attributed to the production of the respective metal; while other
elementary and intermediate flows follow an economic allocation, which is the
default option to deal with multi-functionality in ecoinvent. As discussed in
`Berger <https://doi.org/10.1007/s11367-020-01737-5>`__ et al. (2023), this approach ensures a correct mass balance.

For example, the amount of platinum resource included in the dataset representing
the mining of 1 kg of platinum is set to 1 kg, while the amount of other
metals (e.g., palladium, rhodium) is set to zero. For metal-bearing
intermediates, such as beryllium hydroxide, chromite ore concentrate, or
titanium dioxide products, the target resource flow is set to the target-metal
content of 1 kg of the reference product instead of being forced to 1 kg. If
no explicit content factor is known for an ore, concentrate, or mineral product,
the original target resource amount in the dataset is retained while the other
co-mined in-ground resource flows are set to zero.

The correction is data-driven and applied directly from the transformed
database. *premise* scans non-market datasets from the mining-share mapping and
additional extraction-like datasets with kilogram-scale
``natural resource::in ground`` exchanges. It then matches the in-ground
resource flow to the dataset reference product. Only these elementary resource
flows are modified: technosphere exchanges and other biosphere exchanges are
left unchanged.

The matched target flow is set to the target-metal content of the reference
product, and other co-mined in-ground resource flows are set to zero. For
mapped pure-metal suppliers that do not contain a target resource flow, the
missing in-ground metal flow is added at 1 kg per kg of reference product.
Generic carrier intermediates whose resource attribution is handled by
downstream metal-specific datasets, such as platinum group metal concentrate,
have their direct metal resource flows cleared to avoid double counting.
Explicit product-content factors are used to resolve otherwise ambiguous
metal-bearing products such as copper-cobalt ore; broader labels such as
lead-zinc concentrates remain ambiguous unless an explicit factor exists for
that product.

If the target
flow is missing or ambiguous for a mapped metal-bearing dataset that cannot be
inferred safely, *premise* raises an error instead of silently applying a
partial correction.

The markets are relinked to metals-consuming activities throughout the database.

Runtime configuration
~~~~~~~~~~~~~~~~~~~~~~~

``premise/data/metals/metal_products.yaml`` is the authoritative material-rule
file. ``technology_conversion_factors.yaml`` contains activity unit conversion
factors. Invalid selectors, factors or allocation groups stop the update before
material amounts are applied.

``set_direct_amount`` replaces matching direct technosphere inputs. Biosphere
exchanges are never selected by this operation. Providers are matched by exact
name and reference product, with the location preference ``World``, ``GLO``,
then ``RoW``. If no configured provider exists, the rule is skipped and a
validation warning and the reason for the choice are recorded.

For ecoinvent 3.11 and 3.12, the gallium rule selects
``gallium, high-grade`` for the configured semiconductor-grade application.
Provider name and reference product must match the source database version.

Markets and downstream links
------------------------------

For cut-off markets, eligible secondary-supply exchanges are copied from an
existing source market when their positive total does not exceed one. Primary
supply fills the remaining share; transport additions are weighted by that
primary share. Without usable secondary inputs the market uses primary supply
only. Consequential markets exclude this copied secondary supply. Market
creation is skipped if no primary suppliers can be constructed.

Mining and refining markets creation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

*premise* builds global supply markets for several mined and refined metals.
In these markets, the contribution of different mining and refining regions corresponds
to their *current* market shares. Following this approach, the supply from different
regions for a specific metal will be directly proportional to the country-level
contributions to the global market. These shares are derived from various sources,
mainly `BGS <https://www2.bgs.ac.uk/mineralsuk/statistics/worldStatistics.html>`_
and `USGS <https://doi.org/10.3133/mcs2023>`_, in addition to data from
`van den Brink <https://doi.org/10.1016/j.resconrec.2022.106586>`__ et al. (2022) for Antimony refining.
For certain markets where data was available, *premise* incorporates projections
from `BNEF <https://about.bnef.com/>`_ regarding the development of future mining and refining projects to
forecast the market shares' evolution up to 2030.

The file used to build the global supply markets for mined and refined metals is `mining_shares_mapping.xlsx <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/metals/mining_shares_mapping.xlsx>`_.

Additionally, global metal supply markets modeled account for the average transport
distances and modes of transport for the different metals from producer
to consumer. These data are retrieved from `UNCTAD <https://unctad.org/system/files/official-document/ser-rp-2022d5_en.pdf>`_.

Average transport distance and modes of transport for each producer can be found under: `transport_markets_data <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/metals/transport_markets_data.csv>`_.

Assumptions and limitations
-----------------------------

Direct metal inputs and total supply-chain metal requirements are different
quantities. Source-preservation policies prevent specified overlays; they do
not freeze upstream suppliers. Unknown ore/compound content and unavailable
providers have the fallback behavior documented below. A corrected resource
mass balance is not an independent validation of the whole mining inventory.

.. raw:: html

   <span id="dataset-wise-comparison"></span>

Worked example and checks
---------------------------

For an illustrative intensity of 2 kg metal/kW and a mapped 3 kW reference
installation, the intended direct input is 6 kg only if the rule and activity
unit authorize that conversion. A square metre of PV cell cannot use this
capacity conversion by default. Inspect ``Key Changes`` for direct amounts
and ``Methodology`` for applied or preserved rule IDs. Check background metal
demand separately to avoid double counting component inventories.

EPR construction
~~~~~~~~~~~~~~~~~~

EPR construction and the specified silicon-cell activities keep their direct
source material structure. Their components already carry material inputs;
Premise does not apply the generic plant- or capacity-level metal additions
to these activities. Upstream suppliers can still change.

.. raw:: html

   <span id="source-inventories-graphite"></span>

Sources and inventory details
-------------------------------

Inventories
~~~~~~~~~~~~~

*premise* provides inventories for the following metals:

* `Cobalt <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-cobalt.xlsx>`_.
* `Germanium <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-germanium.xlsx>`_, as a co-product from zinc mine operation, based on the unallocated dataset in ecoinvent.
* `Graphite <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-graphite.xlsx>`_.
* `Iridium <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-PGM.xlsx>`_, as a co-product from PGM mine operation, based on the unallocated dataset in ecoinvent.
* `Lithium <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-lithium.xlsx>`_.
* `Rhenium <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-rhenium.xlsx>`_, as a co-product from copper mine operation, based on the unallocated dataset in ecoinvent.
* `Ruthenium <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-PGM.xlsx>`_, as a co-product from PGM mine operation, based on the unallocated dataset in ecoinvent.
* and `Vanadium <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-batteries-vanadium.xlsx>`_.

The inventories are provided under `premise/data/additional_inventories <https://github.com/polca/premise/tree/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories>`_

Graphite
~~~~~~~~~~

*premise* includes new inventories for:

* natural graphite, from `Engels <https://doi.org/10.1016/j.jclepro.2022.130474>`__ et al. 2022,
* synthetic graphite, from `Surovtseva <https://doi.org/10.1111/jiec.13234>`__ et al. 2022,

forming a new market for graphite, with the following datasets:

 ===================================== =========== ===========
  Activity                              Location
 ===================================== =========== ===========
  market for graphite, battery grade                1.0
  graphite, natural                     CN          0.8
  graphite, synthetic                   CN          0.2
 ===================================== =========== ===========

to represent a 80:20 split between natural and synthetic graphite,
according to `Surovtseva <https://doi.org/10.1111/jiec.13234>`__ et al, 2022.

These inventories can be found here: `LCI_graphite <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-graphite.xlsx>`__.

Cobalt
~~~~~~~~

New inventories of cobalt are added, from the work of Dai, Kelly and `Elgowainy <https://greet.es.anl.gov/publication-update_cobalt>`__, 2018.
They are available under the following datasets:

=================================================================================== ===========
Activity                                                                             Location
=================================================================================== ===========
cobalt sulfate production, from copper mining, economic allocation                   CN
cobalt sulfate production, from copper mining, energy allocation                     CN
cobalt metal production, from copper mining, via electrolysis, economic allocation   CN
cobalt metal production, from copper mining, via electrolysis, energy allocation     CN
=================================================================================== ===========

These cobalt inventories can be selected when
by the Cobalt Development Institute (CDI) since ecoinvent 3.7, which seem to lack transparency.

These inventories can be found here: `LCI_cobalt <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-cobalt.xlsx>`__.

Lithium
~~~~~~~~~

New inventories for lithium extraction are also added,
from the work of `Schenker <https://doi.org/10.1016/j.resconrec.2022.106611>`__ et al., 2022.
They cover lithium extraction from five different locations in Chile, Argentina and China.
They are available under the following datasets for battery production:

=================================================================================== ===========
Activity                                                                             Location
=================================================================================== ===========
market for lithium carbonate, battery grade                                          GLO
market for lithium hydroxide, battery grade                                          GLO
=================================================================================== ===========

These inventories can be found here: `LCI_lithium <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-lithium.xlsx>`__.

.. raw:: html

   <span id="source-provenance-and-currency"></span>

Sources and data dates
~~~~~~~~~~~~~~~~~~~~~~~~

.. include:: /reference/generated/source-metals.inc
