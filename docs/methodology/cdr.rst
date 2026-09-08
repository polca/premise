Carbon capture and carbon dioxide removal
===========================================

.. raw:: html

   <span id="carbon-dioxide-removal"></span>

.. contents:: On this page
   :local:
   :depth: 1

.. raw:: html

   <span id="key-outputs"></span>

Scope and outputs
-------------------

The ``cdr`` update regionalizes carbon-removal and supporting capture/storage
activities and creates removal markets. Mapped modules describe additional
capture and storage requirements per kilogram; they must be distinguished
from complete host processes and from net life-cycle removal.

.. figure:: /_static/process-diagrams/cdr.svg
   :class: process-diagram
   :alt: Carbon dioxide removal: Prepare support activities and route volumes; Regionalize removal activities; Adjust electricity and heat/fuel inputs; Build removal markets and relink.
   :align: center

Inputs and applicability
--------------------------

The update is skipped when ``cdr_technology_mix`` is absent.

Additional inventories define process boundaries. ``carbon_dioxide_removal.yaml``
maps production and carrier-specific energy variables; ``data/cdr/cdr_activities.yaml``
selects support activities. Technology coverage depends on both available
inventories and mapped scenario data. Use ``ndb.update("cdr")`` in the default
sequence for prospective energy suppliers.

Using this update
~~~~~~~~~~~~~~~~~~~

After the setup in :doc:`/getting_started/first-scenario`:

.. code-block:: python

   ndb.update("cdr")

Transformation
----------------

Process context
~~~~~~~~~~~~~~~~~

Carbon capture can reduce releases from a host process; carbon dioxide removal
requires removal of atmospheric or biogenic carbon with the relevant storage
boundary. Capturing fossil carbon is not itself atmospheric removal.

This page describes capture/removal modules and their scenario deployment.
Electricity, steel and cement pages retain the host-process inventories and
their capture integration. Read the module boundaries below before combining
them with a host process to avoid counting that process twice.

Mapped technologies can include DACCS, BECCS, biochar, enhanced weathering,
ocean liming, biomass fermentation CCS, synthetic-fuel CCS and industrial
non-fossil cement CO2 capture, where variables and inventories are available.

When ``energy_use_aliases`` are present, electricity aliases scale electricity
exchanges. Other mapped final-energy carriers are grouped as heat/fuel and
receive a separate inverse-efficiency factor bounded to 0.5–1.5. Missing,
zero or non-finite efficiency signals give a factor of one. DAC-specific
energy floors are applied afterwards. Materials, sorbents, solvents, water,
waste treatment and biosphere flows are not scaled by that energy adjustment.

Wood-power BECCS, biomethane-SMR CCS, fermentation CCS and non-fossil cement
capture modules exclude the host production plant. They contain the additional
capture, compression, transport and storage system for the stated kilogram
of atmospheric or biogenic CO2. CDR-specific heat support is handled within
``carbon_dioxide_removal.py``, not a separate CDR branch of the heat update.

Markets and downstream links
------------------------------

When several CDR routes share the same IAM production alias, Premise divides
that production among them using their mapped carrier-energy amounts. If the
energy total is zero, it uses equal shares. Afforestation routes are further
restricted to regions assigned the matching eucalyptus or poplar/willow
feedstock; world totals are recomputed from the allowed regions.

Regional and world removal markets use mapped production volumes. Imported
capture-only activities are not necessarily suppliers of those markets.
Host-sector CCS remains integrated through the electricity, cement and steel
transformations; check that a CDR module is not added a second time to a host
process already containing its capture requirements.

Assumptions and limitations
-----------------------------

DAC energy floors are expressed in MJ per kilogram: sorbent routes use
0.5 electricity, 3.5 heat and 4.0 total; solvent routes use 0.7 electricity,
5.3 heat and 6.0 total. For heat-pump routes, the heat floor is converted to
an additional electricity requirement using COP 3. These energy floors are
applied after the 0.5–1.5 efficiency-factor bounds, so the final exchange
factor can differ from the initial bounded factor.

A kilogram captured or stored is not automatically a kilogram of net removal
after upstream burdens. Fossil capture is not atmospheric removal. Energy
carrier grouping, technology availability and storage boundaries constrain
interpretation. The direct biosphere flows are not rescaled by the CDR energy
efficiency step.

Worked example and checks
---------------------------

For illustration, separate energy factors of 0.9 and 0.8 change a module's
1 kWh electricity and 2 MJ heat to 0.9 kWh and 1.6 MJ, subject to implemented
bounds. They do not change 0.01 kg of sorbent through that callback. Inspect
these exchange classes, the storage link and host-process exclusions, then
assess net life-cycle removal using the complete supply chain.

Sources and inventory details
-------------------------------

Source inventories: carbon capture
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Two sets of inventories for Direct Air Capture (DAC) are available in *premise*:
one for a solvent-based system and one for a sorbent-based system. The
inventories were developed by `Qiu <https://doi.org/10.1038/s41467-022-31146-1>`__ and are available in the `LCI_DAC <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-carbon-capture.xlsx>`__
spreadsheet. For each, a variant including the subsequent compression,
transport and storage of the captured CO2 is also available.

They can be consulted here: `LCI_DAC <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-carbon-capture.xlsx>`__.

Additionally, two datasets for carbon capture at point sources are available:
one at a cement plant from `Meunier <https://doi.org/10.1016/j.renene.2019.07.010>`__ et al, 2020, and another one at a
municipal solid waste incineration plant (MSWI) from `Bisinella <https://doi.org/10.1016/j.wasman.2021.04.046>`__ et al, 2021.

They introduce the following datasets:

 =============================================================================================================== ===========
  Activity                                                                                                         Location
 =============================================================================================================== ===========
  carbon dioxide, captured, with a sorbent-based direct air capture system, 100ktCO2                               RER
  carbon dioxide, captured and stored, with a sorbent-based direct air capture system, 100ktCO2                    RER
  carbon dioxide, captured, with a solvent-based direct air capture system, 1MtCO2                                 RER
  carbon dioxide, captured and stored, with a solvent-based direct air capture system, 1MtCO2                      RER
  carbon dioxide, captured at municipal solid waste incineration plant, for subsequent reuse                       RER
  carbon dioxide, captured at cement production plant, for subsequent reuse                                        RER
 =============================================================================================================== ===========

The same workbook also contains harmonized capture-only modules for biogenic
and industrial CDR routes:

* ``carbon dioxide, captured and stored, at wood burning power plant, pipeline
  200km, storage 1000m`` excludes the host power plant and represents the
  additional capture and storage system. The energy penalty is modelled as
  3.565 MJ/kg CO2 of wood-derived industrial heat from
  ``heat and power co-generation, wood chips, 6667 kW``.
* ``carbon dioxide, captured and stored, from a hydrogen production plant using
  steam methane reforming of biomethane`` excludes the host SMR plant. It uses
  methyldiethanolamine (MDEA), 0.11598 kWh/kg CO2 of low-voltage electricity
  and 0.948 MJ/kg CO2 of biomethane-based heat inferred from `Antonini <https://pubs.rsc.org/en/content/articlelanding/2020/se/d0se00222d>`__ et al.
  The final CO2 compression electricity is omitted to avoid double counting
  with the common storage module.
* ``carbon dioxide, captured and stored, from a biomass fermentation plant``
  excludes the host fermentation plant. Because the fermentation stream is
  already high-purity CO2, no solvent, sorbent, regeneration heat or additional
  capture electricity is included; the dataset uses the common compression,
  transport and storage module.

Using the transformation function ``update("cdr")``, *premise* regionalizes the
CDR support activities listed in ``premise/data/cdr/cdr_activities.yaml`` and
then regionalizes the CDR technologies mapped in
``carbon_dioxide_removal.yaml``. Regionalized DAC support variants include
industrial steam heat, high-temperature heat-pump heat and waste heat where the
source inventory supports them:

 ======================================================================================================================================================= ==================
  name                                                                                                                                                      location
 ======================================================================================================================================================= ==================
  carbon dioxide, captured and stored, with a solvent-based direct air capture system, 1MtCO2, with industrial steam heat, and grid electricity            all IAM regions
  carbon dioxide, captured and stored, with a solvent-based direct air capture system, 1MtCO2, with heat pump heat, and grid electricity                   all IAM regions
  carbon dioxide, captured and stored, with a sorbent-based direct air capture system, 100ktCO2, with waste heat, and grid electricity                     all IAM regions
  carbon dioxide, captured and stored, with a sorbent-based direct air capture system, 100ktCO2, with industrial steam heat, and grid electricity          all IAM regions
  carbon dioxide, captured and stored, with a sorbent-based direct air capture system, 100ktCO2, with heat pump heat, and grid electricity                 all IAM regions
 ======================================================================================================================================================= ==================

Only solid sorbent DAC can use waste heat, as the heat requirement
for liquid solvent DAC is too high (~900 C). CDR efficiency adjustments, when
provided by the IAM scenario, scale electricity exchanges separately from heat
and fuel exchanges; material inputs and biosphere flows are not scaled.

.. raw:: html

   <span id="source-provenance-and-currency"></span>

Sources and data dates
~~~~~~~~~~~~~~~~~~~~~~~~

.. include:: /reference/generated/source-cdr.inc
