Cement production
===================


.. contents:: On this page
   :local:
   :depth: 1

.. raw:: html

   <span id="key-outputs"></span>

Scope and outputs
-------------------

The ``cement`` update regionalizes clinker production and cement supply
chains, adjusts clinker fuel use and integrates scenario-supported capture.
Clinker production and cement blending have separate kilogram reference
products. An efficiency change in the kiln does not itself change the
clinker-to-cement ratio.

.. figure:: /_static/process-diagrams/cement.svg
   :class: process-diagram
   :alt: Clinker and cement supply: Prepare clinker and capture routes; Set energy target and adjust coal; Set pollutants and capture flows; Build clinker and cement markets; Relink consumers and check.
   :align: center

Inputs and applicability
--------------------------

The update is skipped when ``cement_technology_mix`` is absent.

The starting point is the source database plus imported capture inventories.
Mapped IAM production, efficiency and capture data determine the regional
updates; Capture-route availability follows the model-specific technology mappings. Use ``ndb.update("cement")`` after upstream
energy transformations, or run the default update sequence.

Using this update
~~~~~~~~~~~~~~~~~~~

After the setup in :doc:`/getting_started/first-scenario`:

.. code-block:: python

   ndb.update("cement")

Transformation
----------------

Existing ammonia inputs are set to 0.005 kg/kg clinker, or 0.00662 for the
MEA route. NOx is set to 7.6e-4 kg/kg generally, 3.8e-4 for MEA and 1.22e-5
for efficient direct-separation and oxyfuel routes. The three efficient CCS
routes multiply mercury and sulfur-dioxide emissions by 0.001. These fixed
route assumptions are separate from the coal-energy adjustment.

Dataset proxies
~~~~~~~~~~~~~~~~~

*premise* duplicates clinker production datasets in ecoinvent (called
"clinker production") so as to create a proxy dataset for each IAM region.
The location of the proxy datasets used for a given IAM region is a location
included in the IAM region. If no valid dataset is found, *premise* resorts
to using a rest-of-the-world (RoW) dataset to represent the IAM region.

*premise* changes the location of these duplicated datasets and fill
in different fields, such as that of *production volume*.

Efficiency adjustment
~~~~~~~~~~~~~~~~~~~~~~~

Premise computes visible fuel energy and adds a nonnegative secondary-fuel
allowance so that the accounted starting energy is at least 3.4 MJ/kg clinker.
The allowance represents energy already implicit in source emissions; it is
not added as a burdened fuel input.

The requested thermal target is **3.4 MJ/kg multiplied by the inverse IAM
efficiency change**, clipped to 3.1–5.0 MJ/kg for ordinary routes. Routes
whose technology name begins ``cement, dry feed rotary kiln, efficient``
receive a **fixed 3.0 MJ/kg target** in this finite-factor branch.

The difference between target and accounted starting energy is applied to
hard coal first and distributed proportionally across all coal inputs.
Coal cannot become negative. If coal is absent or insufficient, the target
is not fully reached; the remaining energy difference is recorded. Thus the
target and the energy represented by the resulting exchanges can differ.
Fossil CO2 changes with the applied coal-energy change. This efficiency step
does not independently reset calcination or non-fossil CO2 flows.

Carbon Capture and Storage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Capture is attached to the mapped CCS kiln routes. Direct separation captures
``0.543 × 0.95 = 0.51585 kg CO2/kg clinker`` and subtracts that amount from
fossil CO2, with a zero lower limit. MEA and oxyfuel capture 90% of the
activity's total fossil and non-fossil CO2 and reduce those releases
proportionally. Stored biogenic CO2 is also represented as an atmospheric
CO2 uptake flow. The capture input links to the appropriate regional add-on
module and its compression, transport and storage chain.

The scenario determines production shares of the mapped routes. The current
routine does not implement a separate general allocation of sector-wide
sequestered emissions across all non-IMAGE clinker activities.

Capture integration and source adaptations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When choosing IMAGE, scenarios include the emergence of a new, more efficient kiln, as well
as kilns fitted with three types of carbon capture technologies:

* using monoethanolamine (MEA) as a solvent,
* using oxyfuel combustion,
* using Direct Separation (Leilac process).

The implementation of the corresponding datasets for these new kiln
technologies is based on the work of `Muller <https://doi.org/10.1016/j.jclepro.2024.141884>`__ et al., 2024.

The capture inventories are treated as add-on modules to the transformed
clinker datasets. The clinker dataset keeps the host kiln, its fuel use and its
direct emissions. The capture module represents the additional capture,
conditioning, compression, transport and storage requirements per kilogram of
CO2 captured.

The capture modules use the following assumptions:

* the kiln fuel mix remains the one from ecoinvent and is adjusted through the
  clinker efficiency update, rather than being replaced by the IMAGE
  non-metallic-minerals final-energy mix;
* MEA capture uses the source inventory heat requirement of 4.0556 MJ/kg CO2
  captured, represented as an industrial heat input, together with low-voltage
  electricity, MEA make-up, NaOH, tap water and spent-solvent treatment;
* the oxyfuel inventory uses an oxygen input of 0.313 kg O2/kg CO2 captured,
  based on the CEMCAP oxygen demand after correction for the CO2 capture rate;
* all three capture routes use the same downstream CO2 compression, transport
  and storage module.

In a nutshell, *premise*:

* makes copies of the ``clinker production`` dataset,
* adjusts the fuel consumption and related CO2 emissions,
* adjusts specific hot pollutant emissions removed by the carbon capture process (Mercury, NOx, SOx),
* adds an input from the carbon capture process, based on a capture efficiency share,
* and removes a corresponding amount from the outgoing CO2 emissions.

The Direct Separation process captures process/calcination emissions only,
using 95% of the 0.543 kg/kg process-emission basis. The MEA and oxyfuel routes capture process and fuel
CO2, using a 90% capture share.

Markets and downstream links
------------------------------

Cement markets
~~~~~~~~~~~~~~~~

When clinker production datasets are created for each IAM region,
*premise* duplicates cement production datasets for each IAM region
as well. These cement production datasets link the newly created
clinker production dataset, corresponding to their IAM region.

Original market datasets
~~~~~~~~~~~~~~~~~~~~~~~~~~

Market datasets originally present in the ecoinvent LCI database are cleared
from any inputs. Instead, an input from the newly created regional market
is added, depending on the location of the dataset.

The table below shows the example of the clinker market
for South Africa, which now only includes an input from the "SAF"
regional market, which "includes" it in terms of geography.


 ============================================ =========== ================ ===========
  Output                                       _           _                _
 ============================================ =========== ================ ===========
  producer                                     amount      unit             location
  market for clinker                           1.00E+00    kilogram         **ZA**
  Input                                        _           _                _
  supplier                                     amount      unit             location
  market for clinker                           1.00E+00    kilogram         ***SAF**
 ============================================ =========== ================ ===========

Relinking
~~~~~~~~~~~

Once cement production and market datasets are created, *premise*
re-links cement-consuming activities to the new regional markets for
cement. The regional market it re-links to depends on the location
of the consumer.

Assumptions and limitations
-----------------------------

The fuel adjustment, calcination emissions and capture operation have
separate accounting rules. The inferred secondary-fuel energy ledger is not
an added burdened fuel exchange. The practical fuel floors below constrain
accounted kiln energy; they do not establish plant-specific performance.
Externally projected reductions in the clinker-to-cement ratio are not
applied by this update.

Clinker-to-cement ratio
~~~~~~~~~~~~~~~~~~~~~~~~~

Cement composition and clinker content remain those of the matched source
activities; this update does not apply an external clinker-ratio trajectory.

Worked example and checks
---------------------------

For an illustrative inverse-efficiency factor of 0.8, the ordinary-kiln
target is clipped from 3.4 × 0.8 = 2.72 to 3.1 MJ/kg. If its accounted starting
energy is 3.4 MJ/kg but only 0.1 MJ/kg is coal, removing all coal leaves
3.3 MJ/kg: the 3.1 target is not fully attainable through this operation.
Inspect target and achieved energy, direct pollutant changes, and the selected
capture route separately. Confirm the regional clinker-to-cement links.

.. raw:: html

   <span id="source-age-clarification"></span>

Sources and inventory details
-------------------------------

Source inventories: cement
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

*premise* introduces inventories for capturing carbon dioxide at cement
production plants using three prospective technologies:

* Post-combustion capture using monoethanolamine (MEA)
* Direct separation
* Oxyfuel combustion

These inventories represent the gate-to-gate capture of 1 kg of CO2 and
include upstream material and energy inputs as well as transport and storage
of the captured CO2. They are from `Muller <https://doi.org/10.1016/j.jclepro.2024.141884>`__ et al. (2024). They can be found
here: `LCI_cement <https://github.com/polca/premise/blob/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/additional_inventories/lci-carbon-capture.xlsx>`__.

==============================================================================  ==========
Carbon capture at cement production plants                                       location
==============================================================================  ==========
carbon dioxide, captured, at cement production plant, using monoethanolamine     RER
carbon dioxide, captured, at cement production plant, using direct separation    RER
carbon dioxide, captured, at cement production plant, using oxyfuel              RER
==============================================================================  ==========

A fourth cement-related activity is available for CDR accounting rather than
ordinary cement transformation:
``carbon dioxide, captured and stored, at cement production plant, from
non-fossil carbon dioxide, using monoethanolamine``. It is scaled to 1 kg of
non-fossil CO2 stored, includes a 1 kg ``Carbon dioxide, in air`` input, uses
the MEA cement capture inputs and the common CO2 compression, transport and
storage module, and intentionally ignores fossil CO2 co-captured from the
cement flue gas.

Monoethanolamine (MEA)
^^^^^^^^^^^^^^^^^^^^^^^^

Represents conventional post-combustion carbon capture using MEA solvents,
based on the CEMCAP study (Voldsund, 2019). The dataset includes heat and
electricity demand for regeneration and compression, solvent losses, chemical
pretreatment (NaOH), and incineration of spent solvents. The source inventory
uses 4.0556 MJ/kg CO2 of industrial heat for solvent regeneration and
low-voltage electricity for fans, cooling and CO2 processing.

Direct separation
^^^^^^^^^^^^^^^^^^^

Models CO₂ capture via a separate calciner (as in the LEILAC project),
allowing for nearly pure CO₂ stream separation without additional chemical
solvents. Includes extra electricity consumption for calciner operation
and CO₂ compression.

Oxyfuel combustion
^^^^^^^^^^^^^^^^^^^^

Simulates complete fuel combustion in a controlled O₂/CO₂ atmosphere.
The resulting flue gas has high CO₂ purity, reducing downstream separation
needs. Liquid oxygen is supplied via an air separation unit (ASU), and waste
heat is recovered to offset some electricity needs. Emissions of SOₓ, NOₓ, CO,
and Hg are significantly reduced.

All three capture routes include subsequent CO2 compression, transport, and
storage via the carbon dioxide compression, transport and storage dataset
from *premise*.

.. raw:: html

   <span id="source-provenance-and-currency"></span>

Sources and data dates
~~~~~~~~~~~~~~~~~~~~~~~~

.. include:: /reference/generated/source-cement.inc
