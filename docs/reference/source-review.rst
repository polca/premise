Scientific sources and review status
======================================

This register distinguishes sources used by the implementation from newer
comparison candidates. Review date: 7 September 2026. Older publication dates
do not by themselves invalidate an inventory. Newer results are not directly
comparable unless functional units, boundaries, allocation and backgrounds match.

The review covers every documented sector, but **is not a completed full-text
review of every citation or supplement**. The individual citation ledger marks
the remaining work. Publisher access failures, missing source years and abstract-only
checks are not counted as coefficient validation.

Download the :download:`structured source register <source-register.yaml>` and
:download:`citation review ledger </development/citation-review.json>`.
The :download:`literature search log </development/literature-search-log.json>`
records queries, inclusion criteria and limits. Sources labelled as context
may be methodological guidance rather than an imported inventory; the
structured register distinguishes that role.
See :doc:`/development/scientific-update-backlog` for proposed modelling work.

Photovoltaics
---------------

.. list-table::
   :header-rows: 1

   * - Property
     - Assessment
   * - Implemented scope
     - Single-Si TOPCon/PERC, multi-Si and CdTe; manufacturing through electricity and end of life; country extensions use inherited yield assumptions.
   * - Data years
     - 2022–2025 factory observations for the 2026 report; other inputs vary
   * - Applicable configurations
     - 2026 core: ecoinvent 3.12 cut-off only; legacy core otherwise.
   * - Review outcome
     - Reviewed and retained; provenance gaps remain

The official July 2026 release confirms the new TOPCon/PERC and CdTe coverage. The bundled report and inventory integration remain the authoritative coefficients. Country yields and the degradation convention still need provenance clarification.

Sources: `source context <https://iea-pvps.org/key-topics/t12-lci-pv-systems-2026/>`__; `comparison candidate <https://iea-pvps.org/key-topics/t12-lci-pv-systems-2026/>`__.

See :doc:`/methodology/electricity/photovoltaics`.

Other electricity generation and storage
------------------------------------------

.. list-table::
   :header-rows: 1

   * - Property
     - Assessment
   * - Implemented scope
     - Generation per kWh; CCS adds capture/storage; storage service differs from battery manufacturing.
   * - Data years
     - Sources include 2013 CCS, 2016 geothermal and 2019 plant corrections; underlying observations vary
   * - Applicable configurations
     - Supported configurations; additional workbook selection is version dependent.
   * - Review outcome
     - Newer alternative identified

Existing plant-efficiency corrections must retain their source year. WindTrace provides a newer parametric onshore inventory with supporting workbooks; Swiss/European wind inventory updates offer another regional candidate. Neither is a drop-in update for all generators.

Sources: `source context <https://www.nature.com/articles/s41893-019-0221-6>`__; `comparison candidate <https://doi.org/10.1111/JIEC.70114>`__.

See :doc:`/methodology/electricity/generation`.

Heat and end-use heating
--------------------------

.. list-table::
   :header-rows: 1

   * - Property
     - Assessment
   * - Implemented scope
     - Delivered heat, with secondary supply, buildings and industry kept separate; final-energy conversion is not a new measured boiler or heat-pump inventory.
   * - Data years
     - Ecoinvent source years vary; IAM years and reporting conventions are model dependent
   * - Applicable configurations
     - Model-specific layer coverage; final energy only regionalizes heating activities.
   * - Review outcome
     - Modelling review needed; candidate full text unverified

A 2025 residential-heating assessment is a comparison candidate, not evidence that the current industrial proxies are invalid. Refrigerant, seasonal performance and heat-temperature boundaries need explicit matching before transferring results.

Sources: `source context <https://doi.org/10.1016/j.buildenv.2025.113931>`__; `comparison candidate <https://doi.org/10.1016/j.buildenv.2025.113931>`__.

See :doc:`/methodology/heat`.

Biomass
---------

.. list-table::
   :header-rows: 1

   * - Property
     - Assessment
   * - Implemented scope
     - Biomass fuel supply; residue treatment excludes some forestry burdens but retains collection/processing/transport. Allocation does not establish carbon neutrality.
   * - Data years
     - Inherited forestry/residue inventories; reference years vary and are not all recorded
   * - Applicable configurations
     - IAM mapped purpose-grown/residual shares; attributional and consequential interpretations differ.
   * - Review outcome
     - Modelling review needed; DOE PDF fetch blocked

The DOE 2025 guidance cautions against automatically classifying forest residues as waste. A US Forest Service 2025 study compares energy and product uses. Counterfactual use, timing and allocation must be assessed separately from the supply coefficients.

Sources: `source context <https://www.energy.gov/sites/default/files/2025-01/BiCRS%20Best%20Practices%20for%20LCA.pdf>`__; `comparison candidate <https://research.fs.usda.gov/treesearch/69595>`__.

See :doc:`/methodology/biomass`.

Hydrogen
----------

.. list-table::
   :header-rows: 1

   * - Property
     - Assessment
   * - Implemented scope
     - kg hydrogen with production, conditioning, transport and losses; plant-gate and delivered hydrogen are different functional units.
   * - Data years
     - Source publications mainly 2011–2022; reference/projection years vary by route
   * - Applicable configurations
     - IAM efficiency projections where mapped, external assumptions otherwise.
   * - Review outcome
     - Newer alternative identified

R&D GREET 2025 Rev.1 is a newer comparison source. Regulatory 45VH2-GREET uses a specified well-to-gate boundary and is not an interchangeable full LCI. Compare methane leakage, electricity sourcing and compression boundaries before using its GWP as validation.

Sources: `source context <https://pubs.rsc.org/en/content/articlelanding/2020/se/d0se00222d>`__; `comparison candidate <https://greet.anl.gov/>`__.

See :doc:`/methodology/fuels/hydrogen`.

Biofuels
----------

.. list-table::
   :header-rows: 1

   * - Property
     - Assessment
   * - Implemented scope
     - Crop/residue cultivation and conversion; land-use assumptions, co-products and biogenic carbon can dominate comparisons.
   * - Data years
     - 2006–2022 cited sources, including a 2020 GREET update
   * - Applicable configurations
     - Mapped fuels with region-specific feedstocks; inherited agricultural inventory limitations remain.
   * - Review outcome
     - Newer alternative identified; original full text inaccessible

The cited Cavalett/Cherubini DOI fetch returned 403. JEC v5 provides primary European pathway workbooks; R&D GREET has subsequent updates. These sources require harmonized allocation and land-use boundaries, not just comparison of reported GWP values.

Sources: `source context <https://doi.org/10.1002/bbb.2395>`__; `comparison candidate <https://joint-research-centre.ec.europa.eu/welcome-jec-website/jec-publications/jec-version-5-2020_en>`__.

See :doc:`/methodology/fuels/biofuels`.

Synthetic fuels
-----------------

.. list-table::
   :header-rows: 1

   * - Property
     - Assessment
   * - Implemented scope
     - Fischer–Tropsch and methanol routes; CO2 supply, hydrogen, heat integration and allocation are part of the boundary.
   * - Data years
     - 2013–2019 route sources; future efficiency projections are separate
   * - Applicable configurations
     - Fuel family and system-model constraints apply.
   * - Review outcome
     - Newer alternative identified; detailed LCI comparison pending

JEC v5 supplies pathway-level energy data; later Power-to-X studies cover different plant integration. A newer integrated system result cannot replace a unit-process coefficient without separating heat credits and co-products.

Sources: `source context <https://doi.org/10.1039/C9SE00658C>`__; `comparison candidate <https://doi.org/10.1016/j.jclepro.2025.145713>`__.

See :doc:`/methodology/fuels/synthetic`.

Ammonia
---------

.. list-table::
   :header-rows: 1

   * - Property
     - Assessment
   * - Implemented scope
     - Ammonia production routes; hydrogen production, nitrogen supply and Haber–Bosch synthesis.
   * - Data years
     - 2021 and 2024 cited studies; source observations vary
   * - Applicable configurations
     - Imported inventories and mapped fuel-market use are distinct.
   * - Review outcome
     - Reviewed at abstract level; newer regional alternative identified

The existing 2024 prospective study covers global ammonia across 26 regions. A 2025 South African facility study distinguishes vented versus sold oxygen, illustrating why its allocation cannot be assumed equivalent to cut-off modelling. The PMC full-text endpoint required CAPTCHA.

Sources: `source context <https://doi.org/10.1016/j.heliyon.2024.e27547>`__; `comparison candidate <https://journal.hep.com.cn/fie/EN/10.1007/s11708-025-1013-5>`__.

See :doc:`/methodology/fuels/ammonia`.

Mobile and stationary batteries
---------------------------------

.. list-table::
   :header-rows: 1

   * - Property
     - Assessment
   * - Implemented scope
     - Battery mass/capacity manufacture is separate from lifetime electricity throughput, losses and replacements.
   * - Data years
     - Lithium-ion sources include 2019/2021; emerging chemistries include 2023/2024 studies
   * - Applicable configurations
     - Supplemental imports depend on ecoinvent version; chemistry and energy-density projections have separate sources.
   * - Review outcome
     - Newer alternative identified

The SLU source confirms Zhang et al. 2024 as a prospective sodium-ion study, not a mature global-market average. Argonne documents 2025 changes to assembly energy, material composition, graphite and recycling. Treat these as distinct candidate updates.

Sources: `source context <https://research.slu.se/en/publications/future-climate-impacts-of-sodium-ion-batteries/>`__; `comparison candidate <https://greet.anl.gov/list.php>`__.

See :doc:`/methodology/batteries`.

Transport
-----------

.. list-table::
   :header-rows: 1

   * - Property
     - Assessment
   * - Implemented scope
     - Passenger-km and tonne-km, vehicle manufacture, use and end of life; occupancy/payload and fleet weights matter.
   * - Data years
     - Vehicle source generations vary; several inventories originate around 2020–2021
   * - Applicable configurations
     - Six update modes exist; mapped fleet availability differs by model and file.
   * - Review outcome
     - Newer alternative identified; workbook generation provenance incomplete

The carculator release page documents later inventories and an ecoinvent 3.10 migration. This does not identify the generation used to export every packaged workbook. R&D GREET offers payload-specific truck comparisons with different geographical assumptions.

Sources: `source context <https://pubs.acs.org/doi/abs/10.1021/acs.est.0c07773>`__; `comparison candidate <https://carculator.psi.ch/>`__.

See :doc:`/methodology/transport/overview`.

Steel
-------

.. list-table::
   :header-rows: 1

   * - Property
     - Assessment
   * - Implemented scope
     - Primary and secondary steel routes, hydrogen/electrification and host-process CCS; slag and co-product boundaries matter.
   * - Data years
     - Harpprecht et al. 2025; some CCS support assumptions originate in 2013
   * - Applicable configurations
     - Available technologies depend on IAM and mappings.
   * - Review outcome
     - Reviewed at repository abstract level; retained provisionally

The author repository confirms the 2025 study covers emerging steel technologies and non-climate burden shifts. Its recent publication does not update every supporting CCS unit process automatically. Publisher full-text fetch failed; repository metadata supports scope, not all coefficients.

Sources: `source context <https://elib.dlr.de/219882/>`__; `comparison candidate <https://elib.dlr.de/219882/>`__.

See :doc:`/methodology/steel`.

Cement
--------

.. list-table::
   :header-rows: 1

   * - Property
     - Assessment
   * - Implemented scope
     - Clinker versus cement, calcination versus fuel CO2, capture modules versus host kiln.
   * - Data years
     - Müller et al. 2024; older BREF/CCS sources remain
   * - Applicable configurations
     - IMAGE technology-specific routes differ from aggregate capture-share representations.
   * - Review outcome
     - Reviewed at repository abstract level; retained provisionally

The author repository confirms the IMAGE-based prospective clinker study. Premise documents deliberate adaptations to its fuel mix and capture inputs. These differences must remain explicit; matching the paper title does not establish identical inventories.

Sources: `source context <https://repository.tudelft.nl/record/uuid%3Abb754595-882f-484e-9099-621205785b7c>`__; `comparison candidate <https://repository.tudelft.nl/record/uuid%3Abb754595-882f-484e-9099-621205785b7c>`__.

See :doc:`/methodology/cement`.

Metals
--------

.. list-table::
   :header-rows: 1

   * - Property
     - Assessment
   * - Implemented scope
     - Material intensity and regional mining/refining markets; ore, concentrate and refined-metal units differ.
   * - Data years
     - Mixed production inventories; mineral statistics include a 2023 edition
   * - Applicable configurations
     - Co-mining allocation and version-specific background links remain important.
   * - Review outcome
     - Newer alternative identified

USGS MCS 2026 and BGS World Mineral Production 2020–2024 provide newer production statistics. Statistics are not life-cycle inventories: changing country shares does not update ore grade, energy use or allocation.

Sources: `source context <https://doi.org/10.3133/mcs2023>`__; `comparison candidate <https://pubs.usgs.gov/publication/mcs2026>`__.

See :doc:`/methodology/metals`.

Mining and tailings
---------------------

.. list-table::
   :header-rows: 1

   * - Property
     - Assessment
   * - Implemented scope
     - Tailings treatment, reprocessing and recovered materials; leaching prevention and substitution are modelling assumptions.
   * - Data years
     - Studies span 2008–2023; regional uptake assumptions are mixed estimates
   * - Applicable configurations
     - Treatment topology and adoption shares vary by region.
   * - Review outcome
     - Modelling review needed; newer case study identified

The author-hosted 2023 study explicitly treats prospective reprocessing of sulfidic copper tailings; energy and resource inputs can offset recovery benefits. A 2025 iron-tailings thesis is a different ore/site and is not a global replacement.

Sources: `source context <https://www.research-collection.ethz.ch/bitstream/20.500.11850/598085/4/1-s2.0-S004896972300654X-main.pdf>`__; `comparison candidate <https://odr.chalmers.se/items/d2953f0d-f05d-42ce-a1c1-760d3beb410c/full>`__.

See :doc:`/methodology/mining`.

Carbon capture and removal
----------------------------

.. list-table::
   :header-rows: 1

   * - Property
     - Assessment
   * - Implemented scope
     - Captured CO2, stored CO2 and net removal are distinct; energy supply and permanence determine net outcomes.
   * - Data years
     - Qiu et al. 2022 plus route-specific sources
   * - Applicable configurations
     - DAC, biomass and mineral routes have different carbon-accounting boundaries.
   * - Review outcome
     - Reviewed and retained; new route is not a direct replacement

Qiu et al. models solvent/sorbent DAC with IMAGE 3.2, learning and heat-supply alternatives. New solar-driven DAC-to-fuel work is utilization, not equivalent permanent removal. Preserve the distinction when comparing scores.

Sources: `source context <https://www.nature.com/articles/s41467-022-31146-1>`__; `comparison candidate <https://www.nature.com/articles/s41467-025-67977-x>`__.

See :doc:`/methodology/cdr`.

Air emissions
---------------

.. list-table::
   :header-rows: 1

   * - Property
     - Assessment
   * - Implemented scope
     - Selected non-CO2 biosphere flows scaled using activity/region factors.
   * - Data years
     - 2020 normalization; precise bundled GAINS release needs additional provenance
   * - Applicable configurations
     - CLE/MFR options; Premise application is reduction-only.
   * - Review outcome
     - Documentation corrected; release provenance unresolved

Official GAINS documentation defines CLE and maximum technically feasible reduction scenarios. The scenario name alone does not identify the legislative cutoff or factor release. Documentation now distinguishes GAINS modelling from Premise reduction-only application.

Sources: `source context <https://gains.docs.iiasa.ac.at/model/scenario/scenario_type.html>`__; `comparison candidate <https://gains.docs.iiasa.ac.at/model/scenario/components.html>`__.

See :doc:`/methodology/emissions`.

Consequential modelling
-------------------------

.. list-table::
   :header-rows: 1

   * - Property
     - Assessment
   * - Implemented scope
     - Marginal suppliers from production trends and constraints; market boundaries differ from attributional averages.
   * - Data years
     - Maes et al. 2023; scenario trajectories are release specific
   * - Applicable configurations
     - Consequential ecoinvent 3.8+; sector-specific exclusions apply.
   * - Review outcome
     - Reviewed at abstract level; method retained

The original paper supports IAM-based marginal-supplier identification. A 2026 seaweed competitiveness study illustrates a different market-specific method; it does not supersede the energy-market algorithm. Sensitivity to interval, foresight and supplier constraints remains necessary.

Sources: `source context <https://www.sciencedirect.com/science/article/pii/S1364032123006871>`__; `comparison candidate <https://link.springer.com/article/10.1007/s11367-026-02683-4>`__.

See :doc:`/methodology/system-models`.


Citation-level findings and access limits
-------------------------------------------

The :download:`source access audit </development/source-access-audit.json>`
records 91 individual retrieval attempts in addition to the sector searches.
A successful retrieval is not a full-text or coefficient validation. Several
DOI endpoints returned access errors; alternative author repositories were
used for the steel, cement, sodium-ion and copper-tailings scope checks.

Specific findings include a 2017 publication date for the linked Task 39 report,
a superseded cement/lime BREF, a 2021 author correction to the tailings-facility
survey, and short project-summary PDFs that do not expose complete biofuel or
geothermal inventories. See the citation ledger for the corresponding URLs.
The remaining full-text and supplementary-data gaps are explicit review
limitations, not evidence that the scientific claims are false.
