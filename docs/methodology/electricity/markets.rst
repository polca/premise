Markets
=========


.. contents:: On this page
   :local:
   :depth: 1

Scope and outputs
-------------------

Regional electricity markets combine generation, transmission/distribution
requirements and applicable storage inputs for 1 kWh at the stated voltage.
They are built within ``ndb.update("electricity")`` after generation suppliers
are prepared.

.. figure:: /_static/process-diagrams/electricity-markets.svg
   :class: process-diagram
   :alt: Electricity market construction: Read generation shares and source losses; Construct high-voltage supply; Construct medium- and low-voltage supply; Redirect eligible electricity consumers.
   :align: center

Inputs and applicability
--------------------------

IAM production series mapped in ``electricity.yaml`` determine technology
weights. Source inventories supply voltage conversions, losses and available
technology proxies. Average and marginal shares depend on the selected
system model. Detailed PV installation recipes are separate from IAM
technology shares; see :doc:`photovoltaics`.

Transformation
----------------

The builder creates high-, medium- and low-voltage markets in that order.
Residential PV is excluded from the high-voltage technology mix and handled
at low voltage. Nonzero within-technology supplier shares below 0.001 are
pruned and the retained supplier weights renormalized. Network losses use
production-weighted source-database coefficients.

Cut-off builds also create 20-, 40- and 60-year period mixes; consequential
builds create the requested-year mix. Special-purpose electricity families,
including certified products, designated industry supplies and waste-heat
reuse services, are excluded from the general market rewrite. Aluminium
supplies have their separate location-specific adjustment.

Premise constructs the voltage layers described below, assigns generation
suppliers and connects lower-voltage supply to the upstream layers. Storage
operation and capacity requirements enter through their designated exchanges;
they should be inspected separately from generation shares.

Markets and downstream links
------------------------------

Regional electricity markets
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

High voltage regional markets
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

*premise* creates high, medium and low-voltage electricity markets for each IAM region.
It starts by creating high-voltage markets and define the share of each supplying technology
by their respective production volumes in respect to the total volume produced.

High voltage supplying technologies are all technologies besides:

* residential photovoltaic power (low voltage)
* waste incineration co-generating powerplants (medium voltage)

Several datasets can qualify for a given technology, in a given IAM region.
To define to which extent a given dataset should be supplying in the market,
*premise* uses the current production volume of the dataset.

For example, if coal-fired powerplants are to supply 25% of the high voltage
electricity in the IAM region "Europe", *premise* fetches the production volumes
of all coal-fired powerplants which ecoinvent location is *included* in the
IAM region "Europe" (e.g., DE, PL, LT, etc.), and allocates to each of those
a supply share based on their respective production volume in respect to the
total production volume of coal-fired powerplants.

For example, the table below shows the contribution of biomass-fired CHP powerplants
in the regional high voltage electricity market for IMAGE's "WEU" region
(Western Europe). The biomass CHP technology represents 2.46% of the supply mix.
Biomass CHP datasets included in the region "WEU" are given a supply share
corresponding to their respective current production volumes.


 ============== =========================================== ==================== ================================== =====================
  energy type    Supplier name                               Supplier location    Contribution within energy type    Final contribution
 ============== =========================================== ==================== ================================== =====================
  Biomass CHP    heat and power co-generation, wood chips    FR                   3.80%                              0.09%
  Biomass CHP    heat and power co-generation, wood chips    AT                   2.87%                              0.07%
  Biomass CHP    heat and power co-generation, wood chips    NO                   0.06%                              0.00%
  Biomass CHP    heat and power co-generation, wood chips    FI                   7.65%                              0.19%
  Biomass CHP    heat and power co-generation, wood chips    SE                   9.04%                              0.22%
  Biomass CHP    heat and power co-generation, wood chips    IT                   8.27%                              0.20%
  Biomass CHP    heat and power co-generation, wood chips    BE                   4.59%                              0.11%
  Biomass CHP    heat and power co-generation, wood chips    DE                   12.53%                             0.31%
  Biomass CHP    heat and power co-generation, wood chips    LU                   0.05%                              0.00%
  Biomass CHP    heat and power co-generation, wood chips    DK                   6.60%                              0.16%
  Biomass CHP    heat and power co-generation, wood chips    GR                   0.01%                              0.00%
  Biomass CHP    heat and power co-generation, wood chips    CH                   1.81%                              0.04%
  Biomass CHP    heat and power co-generation, wood chips    ES                   5.10%                              0.13%
  Biomass CHP    heat and power co-generation, wood chips    PT                   1.34%                              0.03%
  Biomass CHP    heat and power co-generation, wood chips    IE                   0.77%                              0.02%
  Biomass CHP    heat and power co-generation, wood chips    NL                   2.32%                              0.06%
  Biomass CHP    heat and power co-generation, wood chips    GB                   33.18%                             0.81%
  _              _                                           Sum                  100.00%                            2.46%
 ============== =========================================== ==================== ================================== =====================


Transformation losses are added to the high-voltage market datasets.
Transformation losses are the result of weighting country-specific
high voltage losses (provided by ecoinvent) of countries included in the
IAM region with their respective current production volumes (also provided by
ecoinvent). This is not ideal as it supposes that future country-specific
production volumes will remain the same in respect to one another.

High voltage regional markets for aluminium smelters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Aluminium production is a significant consumer of electricity.
In the ecoinvent database, aluminium smelters are represented by
specific electricity markets. Conversely, Integrated Assessment Models
(IAM) scenarios aggregate the electricity consumption of aluminium
smelters with that of other electricity consumers.

To improve accuracy, it is necessary to align the electricity markets
of aluminium producers with regional electricity markets. However,
certain aluminium electricity markets have already achieved substantial
decarbonization, primarily due to the use of hydroelectric power in
some smelters.

Therefore, premise integrates aluminium smelters into regional electricity
markets only for those regions that have not yet undergone significant
decarbonization. The regions affected are:

* Rest of World (RoW)
* IAI Area, Africa
* China (CN)
* IAI Area, South America
* United Nations Oceania (UN-OCEANIA)
* IAI Area, Asia excluding China and Gulf Cooperation Council (GCC)
* IAI Area, Gulf Cooperation Council (GCC)

Meanwhile, *premise* maintains the current decarbonized electricity markets
for aluminium smelters in the following regions:

* IAI Area, Russia & Rest of Europe excluding EU27 & EFTA
* Canada (CA)
* IAI Area, EU27 & EFTA

Although the future development of aluminium-specific electricity markets
remains uncertain, it is reasonable to hypothesize that these markets
will follow the decarbonization trends of their respective regions.
Consequently, aligning the carbon-intensive electricity markets of
aluminium smelters with regional electricity markets is likely more
accurate than retaining the current setup.

In fact, such approach has been used by the International Aluminium Industry
association itself, in their `Aluminium Sector Greenhouse Gas Pathways to 2050 Roadmap <https://international-aluminium.org/resource/aluminium-sector-greenhouse-gas-pathways-to-2050-2021/>`_, where
they connected the electricity consumption of aluminium smelters to future
regional mixes defined by the International Energy Agency (IEA).

.. _electricity-storage-operation:

Storage
^^^^^^^^^

Storage can enter the high-voltage mix through the mapped ``Storage, Battery``
and ``Storage, Hydrogen`` production technologies when their scenario shares
are positive. Their operating inventories supply electricity and contain the
associated charging, equipment and conversion requirements. The market
builder does not add a separate generic battery-capacity exchange solely
because renewables have a large share.

The operating activities include vanadium-redox-flow electricity supply and
hydrogen-based electricity storage. Losses and equipment lifetimes belong to
those inventories; inspect their input coefficients when calculating delivered
storage electricity. The :doc:`/methodology/battery-stationary` update changes
capacity markets separately. An available stationary capacity market is not
evidence that an electricity market consumes it.

Medium voltage regional markets
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The workflow is not too different from that of high voltage markets.
There are however only two possible providers of electricity in medium
voltage markets: the high voltage market, as well as waste incineration
powerplants.

High-to-medium transformation losses are added as an input of the medium voltage
market to itself. Distribution losses are modelled the same way as for
high voltage markets and are added to the input from high voltage market.

Low voltage regional markets
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Low voltage regional markets receive an input from the medium voltage
market, as well as from residential photovoltaic power.

Medium-to-low transformation losses are added as an input from the low voltage
market to itself. Distribution losses are modelled the same way as
for high and medium voltage markets, and are added to the input
from the medium voltage market.

The table below shows the example of a low voltage market for the IAM IMAGE
regional "WEU".

 ============================================================== ============== ================ =========== ==================================================
  supplier                                                       amount         unit             location    description
 ============================================================== ============== ================ =========== ==================================================
  market group for electricity, medium voltage                   1.023880481    kilowatt hour    WEU         input from medium voltage + distribution losses
  market group for electricity, low voltage                      0.025538286    kilowatt hour    WEU         transformation losses (2.55%)
  electricity production, photovoltaic, residential              0.00035691     kilowatt hour    DE
  electricity production, photovoltaic, residential              0.000143875    kilowatt hour    IT
  electricity production, photovoltaic, residential              9.38E-05       kilowatt hour    ES
  electricity production, photovoltaic, residential              9.03E-05       kilowatt hour    GB
  electricity production, photovoltaic, residential              7.82E-05       kilowatt hour    FR
  electricity production, photovoltaic, residential              6.80E-05       kilowatt hour    NL
  electricity production, photovoltaic, residential              3.76E-05       kilowatt hour    BE
  electricity production, photovoltaic, residential              2.16E-05       kilowatt hour    GR
  electricity production, photovoltaic, residential              2.08E-05       kilowatt hour    CH
  electricity production, photovoltaic, residential              1.48E-05       kilowatt hour    AT
  electricity production, photovoltaic, residential              9.44E-06       kilowatt hour    SE
  electricity production, photovoltaic, residential              8.66E-06       kilowatt hour    DK
  electricity production, photovoltaic, residential              6.83E-06       kilowatt hour    PT
  electricity production, photovoltaic, residential              2.60E-06       kilowatt hour    FI
  electricity production, photovoltaic, residential              1.30E-06       kilowatt hour    LU
  electricity production, photovoltaic, residential              1.01E-06       kilowatt hour    NO
  electricity production, photovoltaic, residential              2.40E-07       kilowatt hour    IE
  distribution network construction, electricity, low voltage    8.74E-08       kilometer        RoW
  market for sulfur hexafluoride, liquid                         2.99E-09       kilogram         RoW
  sulfur hexafluoride                                            2.99E-09       kilogram                     transformer emissions
 ============================================================== ============== ================ =========== ==================================================

.. note::

    You can check the electricity supply mixes assumed
    in your scenarios by generating a scenario summary report.

.. code-block:: python

    ndb.generate_scenario_report()

Long-term regional electricity markets
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Long-term (i.e., 20, 40 and 60 years) regional markets are created
for modelling the lifetime-weighted burden associated to electricity supply
for systems that have a long lifetime (e.g., battery electric vehicles, buildings).

These long-term markets contain a period-weighted electricity supply
mix. For example, if the scenario year is 2030 and the period considered
is 20 years, the supply mix represents the supply mixes between 2030 and 2050,
with an equal weight given to each year.

The rest of the modelling is similar to that of regular regional electricity
markets described above.

Original market datasets
~~~~~~~~~~~~~~~~~~~~~~~~~~

Market datasets originally present in the ecoinvent LCI database are cleared
from any inputs. Instead, an input from the newly created regional market
is added, depending on the location of the dataset.

The table below shows the example of the low voltage electricity market
for Great Britain, which now only includes an input from the "WEU"
regional market, which "includes" it in terms of geography.


 ============================================ =========== ================ ===========
  Output                                       _           _                _
 ============================================ =========== ================ ===========
  *producer*                                   *amount*    *unit*           *location*
  market for electricity, low voltage          1.00E+00    kilowatt hour    **GB**
  **Input**                                    _           _                _
  *supplier*                                   *amount*    *unit*           *location*
  market group for electricity, low voltage    1.00E+00    kilowatt hour    **WEU**
 ============================================ =========== ================ ===========

Relinking
~~~~~~~~~~~

Once the new markets are created, *premise* re-links all electricity-consuming
activities to the new regional markets. The regional market it re-links to
depends on the location of the consumer.

PV products and market layers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The residential solar-PV technology category supplies the IAM low-voltage
market; other PV categories enter the high-voltage technology mix. These
categories are selected by the technology mapping, not by a universal 3 kWp
capacity threshold. A supplier's reference-product voltage is distinct from
the IAM market layer consuming it: the new commercial PV inventories have a
medium-voltage reference product. See :doc:`photovoltaics` for the inventory
variants and their regionalization.

Storage operation above describes electricity delivered after storage losses.
The :doc:`/methodology/batteries` chapter separately documents battery
manufacturing inventories and chemistry assumptions.

Assumptions and limitations
-----------------------------

A supplier's reference-product voltage and the market consuming it are
separate properties. Sum generation shares within their intended layer;
do not include transmission, infrastructure or storage-capacity exchanges
in that sum. Country PV recipe assumptions do not become IAM observations
when suppliers are aggregated to regions.

Worked example and checks
---------------------------

For an illustrative 1 kWh generation mix with shares 0.7 and 0.3, verify
those two production contributions before inspecting losses and ancillary
inputs. A lower-voltage market may require more than 1 kWh upstream due to
losses. Trace one low-voltage consumer through the layers and check that
storage losses and equipment are not counted twice.

Sources and inventory details
-------------------------------

See the source inventories and mappings linked above, and
:doc:`/reference/inventories` for constructor import selection.
