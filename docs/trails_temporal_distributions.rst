TRAILS temporal distributions
=============================

TRAILS stores its canonical temporal metadata under
``premise/data/trails/temporal/``. The validated YAML files separate sources,
reusable profiles, selector assignments, exact legacy mappings, and explicitly
resolved duplicate identities. The file
``premise/data/trails/temporal_distributions.csv`` is a deterministically
generated compatibility artifact for external consumers and regression checks.
TRAILS runtime loading reads the YAML registry directly. Do not edit the CSV by
hand.

The canonical files are:

``sources.yaml``
    Structured source metadata, including publisher, URI, source version,
    publication or update date, retrieval date, and coverage.

``profiles.yaml``
    Reviewed temporal quantities with an explicit reference event,
    distribution, applicability, assumptions, evidence status, source IDs, and
    activation review. Empirical profiles also state their population,
    weighting, reference period, censoring, sample size, and quality flags.

``assignments.yaml``
    Declarative dataset selectors and exact overrides. Operational assignments
    are intentionally separate from evidence profiles: an observed fleet-age
    distribution is not automatically a service-life distribution. Only
    reviewed, behavior-preserving assignments are active.

``legacy_profiles.yaml`` and ``legacy_mappings.yaml``
    The resolved compatibility model. Reusable legacy parameter and assumption
    records are referenced by compact groups of exact dataset identities.

``legacy_conflicts.yaml``
    Every duplicate identity found during migration, the differing fields, the
    selected source row, and any field-level resolution needed to preserve the
    old loader's effective behavior.

The parameters are applied to exchanges before export. This allows the
resulting database or matrix export to keep information about when an exchange
occurs relative to the activity reference year.

The temporal reference point is year ``0`` of the activity. Negative offsets
represent exchanges that happened before the reference product is supplied, for
example crop carbon uptake before harvest. Positive offsets represent future
exchanges, for example forest regrowth after harvest, maintenance, end-of-life
treatment, or long-term emissions.


How the table is used
---------------------

``TrailsDataPackage`` loads ``legacy_profiles.yaml`` and
``legacy_mappings.yaml`` directly, then applies the resolved records with
different matching rules depending on ``temporal_tag``:

``stock_asset``
    The row identifies a durable supplier activity by ``name`` and
    ``reference product``. Its temporal-distribution parameters are copied to the
    matching technosphere exchange.

``maintenance``
    The supplier is assigned a uniform distribution from year ``0`` to the
    lifetime of the calling dataset.

``end_of_life``
    The supplier is assigned a discrete one-pulse distribution at
    ``dataset lifetime`` years. No additional year is added.

``biomass_growth``
    The row identifies an activity by ``name`` and ``reference product``. Its
    temporal-distribution parameters are copied to the biosphere exchange named
    ``Carbon dioxide, in air`` in that activity.

``long_term_emission``
    The row identifies biosphere exchanges using a flow-name selector and a
    ``reference product`` selector of the form
    ``compartment|subcompartment|unit``. The generated discrete profile is
    copied to matching biosphere exchanges.

``throughput_process``
    These rows mainly provide lifetime context for activities that call
    maintenance or end-of-life suppliers. They are not directly assigned to an
    exchange by the current ``add_temporal_distributions()`` pass.

When an exchange cannot be assigned unambiguously, TRAILS writes audit files
under ``trails_temp/``. The most useful files are
``temporal_distribution_faulty_exchanges.csv`` and
``long_term_biosphere_matches.csv``. Selector-backed assignments also write
``temporal_selector_matches.csv`` with the selected assignment and profile.


Temporal quantities and reference events
----------------------------------------

The YAML schema distinguishes quantities that were conflated in the original
table:

``service_life``
    Duration between commissioning and retirement of a newly produced asset.

``fleet_age``
    Distribution of commissioning dates among assets operating in a stated
    reference year. Offsets are non-positive relative to current service.

``construction_timing``
    Timing of an asset-supplying technosphere exchange relative to the activity
    reference event.

``retirement_timing``
    Timing of retirement or treatment relative to a declared commissioning,
    current-service, or lifecycle event.

``biomass_growth``
    Timing of biogenic carbon uptake relative to harvest or current output.

``long_term_release``
    Timing of a biosphere release relative to current output.

Profiles declare ``reference_event`` rather than relying on the sign of their
offsets alone. This prevents, for example, an observed age of operating assets
from being mistaken for an empirically observed lifetime.


Dataset selectors
-----------------

Assignments can reuse one profile for many datasets. Predicates support
``equals``, ``contains``, ``startswith``, ``in``, and ``glob`` on dataset fields
and classifications. A selector has ``all`` and ``any`` inclusion groups plus
optional exclusions. Selectors can be restricted to named ecoinvent versions
and can declare expected minimum and maximum match counts for coverage checks.

Supplier selectors use ``reference product`` for the exchanged infrastructure
product and ``name`` for the supplying activity name. These can differ: for
example, ``wind turbine construction, ...`` supplies ``wind turbine, ...``.
The audit resolves supplier activity names through the linked dataset so it
uses the same identity convention as Premise exports. Fuel-specific custom
vehicle and other explicitly named-activity selectors retain ``name``.

Supplier and biosphere assignments may also combine ``caller_match`` with
``exchange_match``. This makes the relationship explicit: a construction
profile can depend on the technology and geography of the electricity-producing
caller as well as the identity of its infrastructure exchange. Matching only
the supplier is insufficient when a globally supplied asset is used by fleets
with different regional age compositions.

Resolution is deterministic: exact overrides outrank version-specific direct
selectors, which outrank general direct selectors, country-proxy selectors, and
generic fallbacks. A ``proxy`` assignment therefore applies only when no direct
selector matches. Priority and selector specificity break ties.
Equal-precedence matches that select different profiles raise an error instead
of silently choosing one. Cement and iron/steel relationships also have explicit world-profile proxies
for IAM region labels that are not covered by a direct country or regional
selector. Direct selectors retain precedence. These fallbacks do not imply
that world fleet composition describes each IAM region.

The audit API reports raw and selected match counts,
uncovered records, and expected-count violations.

The selector operators mirror the stable subset of dataset-filter behavior used
elsewhere in premise while remaining backend-independent and serialisable.

The active cereal selectors are audited against installed cutoff inventories
with ``dev/audit_trails_temporal_selectors.py``. The independent candidate set
requires a direct positive ``Carbon dioxide, in air`` natural-resource input
and either a cereal crop-production classification or a canonical
``biomass_growth`` mapping. This separates genuine pre-harvest uptake from
similarly named conversion processes such as ethanol production and seed
processing. Run the audit from the repository root when the corresponding
licensed Brightway projects are available:

.. code-block:: console

   PYTHONPATH=. python dev/audit_trails_temporal_selectors.py

The default project and database names are
``ecoinvent-{version}-cutoff`` for versions 3.8, 3.9.1, 3.10, 3.11, and 3.12.
Both templates and the version list can be overridden on the command line. The
selectors also accept premise's equivalent ``3.9`` and ``3.10.1`` source-version
strings.


Migration audit
---------------

The source table contained 9,678 rows but only 9,293 unique
``(name, reference product)`` identities. The migration ledger records 378
duplicated identities (385 excess rows). Of those, 134 differed in an
operational timing field, principally ``lifetime``; the remainder differed only
in annotations or classifications. The canonical compatibility model contains
960 profiles because distinct assumptions are retained even when their numeric
timing is identical, and 2,774 classification/mapping groups covering all 9,293
identities.

The old loader's effective outputs were compared with a compiled canonical CSV:
965 stock-asset parameter records, 170 end-of-life suppliers, 431 biomass-growth
records, 63 maintenance suppliers, 15 long-term biosphere selectors, 7,145
dataset lifetimes, and all 9,293 lifecycle-reference records were identical.
One duplicate needed an explicit field-level resolution: its final row had a
blank lifetime, while the historical loader retained an earlier nonblank value.
That decision is recorded in ``legacy_conflicts.yaml`` rather than remaining an
implicit iteration-order effect.


Evidence pilots
---------------

The registry contains non-operational evidence pilots for both fleet age and
service life:

* Eurostat 2024 road-stock age bands for passenger cars, buses, heavy lorries,
  road tractors, and light commercial vehicles. Only countries reporting every
  requested age band are aggregated. The open-ended over-20-years band is
  retained; no unsupported upper age or annual interpolation is introduced.
* EIA Form 860 (2025) operating-generator ages and observed retirement ages for
  selected U.S. electricity technologies. Operating ages are weighted by net
  summer capacity. Retirement observations from 2002 onward are documented as
  observed retirements, not uncensored survival curves.
* European Commission passenger-car and light-commercial-vehicle survival
  curves derived from UK MOT inspection records. The source applies the UK data
  as a European proxy. Four percent remain at the final reported age, so the
  registry preserves a right-censored tail rather than inventing a maximum life.

Empirical survival profiles and the raw banded fleet observations remain
deliberately unassigned. Applying survival evidence requires a cohort model that
handles censoring. Reviewed construction-timing profiles may, however, derive a
runtime approximation from an observed fleet when the whole-asset relationship
is unambiguous. The raw evidence and operational approximation remain separate
profiles so fleet age cannot be mistaken for prospective lifetime.

For European buses, lorries over 3.5 tonnes, and light commercial vehicles, the
2024 Eurostat stock bands are now applied to the single whole-vehicle exchange
inside each corresponding transport-service dataset. Closed bands use their
arithmetic midpoints (1, 3.5, 7.5, and 15 years); the open-ended over-20 band is
represented at 22.5 years following the EEA convention. These are
vehicle-count-weighted construction-vintage proxies, not distance-weighted or
fully attribution-based profiles. European callers receive direct reviewed
assignments. Other geographies receive lower-precedence proxy assignments until
better regional evidence is available. The road-tractor profile remains
``eligible`` but unassigned because the installed inventories contain no
distinct road-tractor service-to-vehicle relationship.

Passenger-car transport now uses nine fuel-specific profiles from the UK
Department for Transport's VEH1111 table, published 29 April 2026. These
profiles describe licensed cars at the end of 2025 by year of first use.
They distinguish petrol, diesel, battery electric, fuel-cell electric, gas,
petrol and diesel hybrids, and petrol and diesel plug-in hybrids. Whole-car
exchanges in passenger-transport callers receive the appropriate fuel profile;
UK callers use direct assignments and other locations use explicit geographic
proxies. This avoids applying an all-powertrain fleet age to young electric
and hybrid fleets. The Eurostat all-car observation remains separate evidence.

Known-year vehicle-count-weighted mean ages are 10.36 years for petrol, 10.66
for diesel, 2.04 for battery electric, 5.61 for fuel-cell electric, 13.71 for
gas, 3.08 and 3.46 for petrol and diesel hybrids, and 2.68 and 4.51 for petrol
and diesel plug-in hybrids. Integer ages are 2025 minus year of first use;
within-year registration dates are not modelled. Unknown first-use dates are
excluded and their share is recorded. Known ages cover 90.61% of gas cars
and 97.61%--99.99% of the other fuel populations. Fuel-cell evidence comprises
only 106 known-age cars and is explicitly flagged as a small fuel fleet.

These are fuel-specific, count-weighted stock proxies. They do not describe
age-specific mileage, lifetime service attribution, or separate size and Euro
emission-standard cohorts. Current fuel labels do not identify powertrain
conversion dates. The source's GAS category combines fuels and is an explicit
proxy for both compressed-gas and LPG inventories. Future-year use retains
the observed age shape rather than projecting fuel-specific market growth.
All 201 passenger-car identities retained in the legacy catalogue have a
fuel-aware service mapping; all 74 passenger-car service activities in the
packaged workbook expose exactly one selected whole-car exchange.

Construction substeps
~~~~~~~~~~~~~~~~~~~~

Some remaining legacy curves belonged to components of new assets, rather
than the operating assets themselves. Selected tanks, fuel-cell systems,
gearboxes, retarders, suspension and wheels in explicit new-vehicle assembly
now occur in the parent construction year. Selected electrical-installation
and same-product market links within PV installation, and construction-work
inputs within CHP construction, follow the same convention. This is a fixed
zero offset at annual resolution, labelled as an assumption rather than
empirical fleet evidence. It prevents an additional whole-fleet age shift
inside construction that has already been dated by a service-to-asset link.
Manufacturing lead times remain unresolved.

The general construction-event selectors exclude operating-factory capital inputs,
EV chargers, maintenance and replacement callers. A separate, explicit convention
now covers chargers allocated inside new-truck production, as explained below.
Existing exchange amounts and service lifetimes are preserved. Separate fleet profiles now cover the
reviewed factory and direct-service charger relationships described below.
Factory-capital markets pass through the construction event without adding
another stock-age shift.

Use ``dev/audit_trails_temporal_basis.py`` to reproduce the effective fleet,
construction-event and legacy counts. Its denominator is matched capital-input
exchange occurrences, including assembly and markets; it is not impact-weighted
coverage or a census of all infrastructure. The legacy catalogue remains a
compatibility layer, so counting its retained profiles does not measure how
often those profiles still win at runtime.

Coverage gaps reviewed in November 2026
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Low-voltage electricity networks now use the SIGEIF 2023 electricity concession
control report, printed page 19, Table P8. It reports 5,409,052 metres of operating
low-voltage lines across 66 municipalities, split into six age bands. The weighting
is explicitly line length, resolving an ambiguity in the earlier Eurelectric
share-range evidence. Five closed integer-age bands use midpoints; the open
pre-1973 band uses a modelled representative age of 72.55 years that reproduces
the separately reported rounded overall mean of 42 years. This is not a measured
oldest-cohort mean or maximum. The older Eurelectric ranges remain held as
separate evidence.
Calibration assumes that the reported overall mean and Table P8 use the same
population and length weighting; a different aggregation would require a
different tail assumption.

The profile applies only from low-voltage electricity services to their complete
low-voltage network input, in kilometres. The urban/suburban French concession
is an explicit geographic proxy elsewhere. The bundled inventory contains more
than lines, so transformer, meter and communications-equipment ages are not
separately established. Medium- and high-voltage networks are excluded. Latest
aggregate means are not combined with an earlier cohort histogram.

Stationary battery electricity supply now receives an energy-capacity-weighted
profile from EIA-860 2025 Schedule 3_4. Its 981 operating battery generators have
119,654.9 MWh of known-year energy capacity and a weighted mean age of 1.31 years.
Lithium-ion systems account for 96.45% of that capacity. Stored energy, rather
than discharge power, weights the construction cohorts. The source is a U.S.
utility-scale fleet; its geography, size and mixed chemistry are explicit proxies
for the inventory's whole stationary-capacity portfolio. The inventory's chemistry
mix is preserved. The age distribution is attached once, from electricity supply
to the complete capacity portfolio, and not again to cells, packs, controls or
disposal. Later cell replacements and augmentation are not inferred from original
commissioning dates. Standalone chemistry-specific, home and mobile batteries
do not receive this profile.

Factory building capital now uses the 2023 Great Britain National Buildings
Database, Factory Summary Table 3_16, published by DESNZ in 2026. The ``Factories
etc`` subgroup FA_B provides floor-area-weighted construction-period shares;
the 0.3% unknown-age share is excluded. Closed periods use midpoint years;
pre-1919 is represented by 1900. The approximate mean age is 49.82 years.
This is a factory-building proxy for shell-dominated photovoltaic-cell-factory
capital and metal-coating facilities represented by building halls. The PV-factory
inventory also contains machinery: dating that input with the shell is an explicit
whole-facility approximation, not evidence of machinery construction or replacement
age. Whole chemical process plants remain excluded. Caller aliases include wafer,
computer and printer manufacturing, coating, enamelling and bitumen-seal production
where the inventory explicitly uses these same factory products.

Plug-in charging equipment now uses the German Bundesnetzagentur register
snapshot dated 12 November 2026. Only operating fast-charging equipment rated
150--250 kW is selected, bracketing the 200 kW inventory design. All 12,625
selected physical equipment IDs have valid commissioning dates; their count-weighted
mean age is 2.41 years. Counts are by equipment ID rather than connector count.
Public German equipment supplies a documented use-class and geographic proxy
for private overnight bus-depot chargers. The source records equipment commissioning,
unlike an AFDC date that can reflect first appearance in a network feed. Maintenance
status, normal charging, future dates and other power bands are excluded.

The German plug-in mapping is restricted to direct road-transport-service inputs.
Pantograph opportunity chargers now have a separate, explicitly modelled Swiss
site-age proxy. BERNMOBIL documents three operating terminus sites: Weiermatt
(line 17, regular service from 2019), Stuckishaus (line 21, 2023) and Blinzern
(line 19, 2025). At the 2026 reference year, ages 7, 3 and 1 receive equal site
weights, giving a mean of 3.67 years. Planned Elfenau infrastructure is excluded.
ABB identifies the original Weiermatt pilot as 450 kW and dates its initial
service to December 2018; the operator's 2019 regular-service convention carries
up to one year of uncertainty. These are three documented sites, not a
representative national fleet or equipment-module census. Later-site power
ratings, usage weights and equipment replacement dates are unknown. The profile
therefore records small-sample, geographic and 450 kW rating-transfer limitations,
and has a low-priority global proxy assignment restricted to bus-service inputs.

The fourteen plug-in charger links inside new-truck production now use a separate
construction-event assumption: zero additional offset from truck construction.
The source workbook allocates fractions of shared chargers to new vehicles;
this convention preserves that placement and all quantities. Freight-service
fleet timing dates the truck and its allocated charger together, without adding
a second fleet-age offset inside production. It is not evidence that shared
chargers are physically manufactured with each truck, or an independently
observed charger fleet. Independent commissioning, utilisation and replacement
would require a different shared-infrastructure service allocation. That remains
a modelling limitation, but these links no longer use a legacy lifetime-shaped
curve against the truck-construction reference event.

.. list-table:: Remaining reviewed limitations
   :header-rows: 1
   :widths: 30 70

   * - Asset or relationship
     - Status
   * - Pantograph charging and chargers inside truck production
     - Four pantograph-service inputs use the small-sample Swiss site proxy. Fourteen new-truck charger inputs use an explicit construction-year assumption. Neither is presented as a representative independent charger fleet.
   * - Medium/high-voltage grids and separate transformers/meters
     - The new length-weighted evidence is low-voltage only. Broader network and component profiles need compatible observations.
   * - Ammonia and methanol process units
     - GEM site ages remain evidence only; industrial-site founding dates do not date current synthesis units.
   * - Coal heating stoves
     - The available study groups coal boilers and furnaces; a stove-specific mapping remains unreviewed.
   * - High-speed/urban rail and dedicated cargo aircraft
     - Existing mixed-fleet observations do not establish subtype-specific composition. Current conventional-rail and passenger/belly-freight mappings remain restricted.
   * - Residential buildings and road tractors
     - Building or tractor stock data exist, but no compatible reviewed service-to-whole-asset relationship is activated in the audited inventories.

The effective-basis audit reports no remaining legacy construction curves in the
audited base ecoinvent databases or the audited bus, truck, passenger-car and
battery-capacity workbooks across ecoinvent 3.8--3.12. This is a count of capital
exchanges that receive timing,
not a claim that every infrastructure asset has acquired fleet evidence. The
large legacy catalogue is retained for compatibility and unreviewed inventories.

For seagoing freight, the 2024 UNCTAD world-fleet bands are applied to the
whole-vessel exchange in bulk-carrier, container-ship, petroleum-tanker, ferry,
liquefied-gas-tanker, and other-liquid-tanker transport datasets. Profiles are
separated into bulk carriers, container ships, oil tankers, and UNCTAD's
``other ship types`` category. The bands are weighted by deadweight tonnage,
which is a better fit for tonne-kilometre service than vessel counts. Closed
bands use their midpoints. The representative age of the open over-20 band is
calibrated to 30.214 years so the published global bands reproduce UNCTAD's
12.5-year deadweight-tonnage-weighted fleet mean. This common tail is an
explicit approximation because within-band ages by ship type are not published.
The evidence is global, so no geographic proxy tier is needed; inland barges
and fishing vessels remain outside these selectors. Bulk-carrier and ferry
relationships are available throughout ecoinvent 3.8-3.12; container-ship and
petroleum-tanker service datasets enter the audited inventories in 3.11.

LNG freight now overrides the broad ``other ship types`` profile with IGU's
2026 World LNG Report Appendix 3. All 804 active-vessel rows are parsed and
delivery-year ages checked against the report's 2026 age reference. Conventional,
Q-Flex, Q-Max and icebreaking carriers contribute 734 vessels and 123,624,533
cubic metres of cargo capacity. Their cargo-capacity-weighted mean age is
10.15 years. The remaining 70 vessels are storage or regasification units,
small- or mid-scale ships, or bunkering vessels. The orderbook is excluded.
Only LNG transport-to-whole-tanker exchanges receive this profile; the broad
UNCTAD profile continues to cover the other reviewed ship types. Delivery year
approximates original construction, and cargo volume does not capture distance,
utilisation or hull mass. The fleet observation is end-2025 with ages referenced
to 2026. Independent inventory scans find one LNG transport-to-whole-tanker
relationship in each of ecoinvent 3.11 and 3.12, and none in 3.8, 3.9.1 or 3.10.

European commercial aircraft now use Eurostat's 2024 EU27 stock-age bands.
All 27 countries report all five bands, representing 6,096 aircraft. Closed
bands are represented at ages 2, 7, 12, and 17 years; the open 20-years-or-over
band is represented at 22.5 years. The resulting count-weighted mean is 11.90
years, close to the independently reported EASA European fleet mean of 11.8
years for 2023. The raw Eurostat bands are retained separately as an
``evidence_only`` profile so the open oldest band is not silently converted to
a point estimate outside runtime use.

The runtime selector applies only to the whole passenger-aircraft input of
passenger service and belly-freight datasets. Belly freight shares the profile
because it is carried by passenger aircraft. Dedicated freighters are excluded:
Eurostat combines commercial-aircraft categories, while EASA reports a
substantially older cargo fleet. Aircraft maintenance, airports, and aircraft
components are also excluded. The distribution is aircraft-count-weighted, not
seat-, payload-, flight-, passenger-kilometre-, or tonne-kilometre-weighted, and
therefore remains a surviving-stock construction-vintage proxy rather than an
attribution-based production profile. Since all compatible ecoinvent callers
are ``GLO``, EU27 evidence is explicitly marked as a geographic proxy. The
independent audits find eight passenger or belly-freight relationships in each
of ecoinvent 3.8, 3.9.1, 3.10, 3.11, and 3.12, with no missed or unintended
matches.

Rhine inland freight vessels use annual commissioning-year counts recovered
from the vector points in Figure 12 of the CCNR 2024 Market Observation report.
Separate profiles cover 8,319 dry-cargo vessels and 1,827 tanker vessels with
plotted construction years. Their count-weighted mean ages in 2024 are 50.80
and 27.57 years respectively; 80.72% of the dry-cargo fleet and 37.60% of the
tanker fleet were commissioned before 2000. The figure's ``Until 1880`` point
is zero for both fleets, so no open-tail age is imposed.

The selectors require an inland-freight service caller and the exact whole
``barge`` or ``barge tanker`` exchange. RER callers use direct reviewed
assignments; RoW uses the same Rhine evidence only as a lower-precedence proxy
until stronger regional evidence is available. Vessel counts are not weighted
by capacity, payload, distance, or tonne-kilometres. The source also warns that
IVR may retain inactive old vessels, reports 135 dry-cargo vessels with unknown
construction years, and does not report an equivalent unknown-year tanker
count. Major rebuilding, repowering, and conversions are not observed as age
resets. Independent audits find four relevant relationships in every ecoinvent
version from 3.8 through 3.12, with no missed or unintended matches.

Conventional passenger rail uses Figure 3.2 of ORR's October 2026 rolling-stock
maintenance review. It reports GB mainline vehicle shares of 19%, 23%, 3%, 11%,
19% and 26% in the 0--5, 6--10, 11--15, 16--20, 21--25 and 26-plus age groups.
The rounded values sum to 101% and are normalised. The raw evidence profile
retains the open oldest band. The runtime profile uses label midpoints for
closed bands and an explicit 30-year representative age for the oldest band,
giving an approximate mean age of 16.69 years. Changing that oldest point to
26 or 40 years changes the mean to 15.66 or 19.26 years respectively; none of
these oldest-band point values is an observed tail mean.

The review's age figure excludes TARA fleets representing 6% of vehicles and
does not give the figure's observation date or vehicle count. GB's mixed
mainline population is used only as a documented geographic and service-class
proxy for conventional long-distance and regional whole-train exchanges.
High-speed and urban callers, trams, freight, maintenance and construction are
excluded. Vehicle counts do not account for train length, structural mass or
passenger-kilometres. All compatible ecoinvent callers are outside GB, so the
assignment is explicitly lower-priority ``proxy`` evidence throughout.
Independent inventory checks find eight relevant whole-train relationships in
each of ecoinvent 3.8, 3.9.1, 3.10, 3.11 and 3.12.

European rail freight now uses the 2019 freight-wagon manufacturing-year
composition in Figure 15 of the European Commission railway-guidelines study.
The figure is based on European Centralised Virtual Vehicle Register data and
reports approximately 648,000 wagons in known year bands plus 38,000 without a
manufacturing year. The raw profile retains the open ``before 1960`` band. The
runtime construction profile places the closed bands at their midpoints and
represents the open band at age 64 years (manufacturing year 1955), reproducing
an approximate count-weighted mean age of 31 years. The open-tail point and the
digitised thousand-wagon bar heights are explicit approximations.

The selector applies only from a freight-train transport service to the whole
``goods wagon`` exchange. It excludes wagon maintenance, locomotives, track,
passenger trains, and markets. European callers use the profile directly;
other caller locations use it as a lower-precedence proxy until stronger local
evidence is available. Wagon counts are not payload- or tonne-kilometre-weighted,
so this remains a surviving-stock construction-vintage proxy rather than a
fully attribution-based production profile. The relevant freight-train
inventories exist in ecoinvent 3.8--3.10 and are absent from 3.11--3.12; the
assignment applicability records that boundary explicitly.
Independent inventory audits found 20 whole-wagon relationships in each of
ecoinvent 3.8, 3.9.1, and 3.10, and none in 3.11 or 3.12, with no missed or
unintended matches.

European non-residential buildings use the MODERATE Horizon Europe 2020 EU27
stock, with a separate hotel-and-restaurant profile and a general service-sector
profile for hall and multi-storey building inputs. Construction-period shares
are weighted by constructed floor area. Closed periods use their midpoints; the
open pre-1945 band is represented at 104.542 years using the Eurostat 2021
occupied-dwelling split between pre-1919 and 1919-1945, with 1900 recorded as
the explicit representative year for the remaining open tail. This residential
tail refinement is labelled as a proxy for non-residential buildings.

European callers receive direct assignments and other regions receive the same
profiles only as lower-precedence European proxies. Generic building mappings
exclude markets, activities whose names contain ``construction``, and callers
producing ``unit`` assets. This prevents a current-stock age profile from moving
the building components of newly constructed factories, machines, or other
capital assets into the past. Renovation does not reset original construction
age because neither source resolves major refurbishment timing separately.

European building-heating appliances use the 2012 EU28 installed-capacity age
bands published by the European Commission/Fraunhofer heating and cooling
mapping study. Separate profiles cover non-condensing natural-gas and fuel-oil
boilers, biomass furnaces, biomass stoves, aerial heat pumps, and ground heat
pumps. The original ``before 1992``, ``1992-2002``, and ``after 2002`` shares
are retained in evidence-only fleet-age profiles. Runtime construction profiles
represent those bands at ages 30, 15, and 4.5 years respectively. The 30-year
point for the open oldest band is an explicit approximation and can understate
the age of much older appliances. The evidence is installed-capacity weighted,
not heat-output or utilisation weighted, and combines empirical observations
with bottom-up gap filling. Its 2012 reference year is therefore flagged as
stale rather than silently treated as a current European stock estimate.

Selectors apply only from a heat-production activity to a complete appliance.
They exclude condensing boilers, industrial or district-scale furnaces,
``state-of-the-art`` and ``future`` datasets, the generic 30 kW heat pump, and
the borehole portion of a ground-source system. The air-water inventory happens
to use a supplier labelled ``heat pump, brine-water, 10kW``; caller context is
therefore required to select the aerial rather than ground profile. EU28 caller
locations receive direct assignments. Switzerland, ``RoW``, and other regions
receive the same European evidence only through lower-precedence proxy
assignments. The coal boiler/furnace profile remains on hold because the source
category is broader than ecoinvent's coal-stove relationship and its open
oldest band contains most of the stock. Independent audits found 41 reviewed
heating-appliance relationships in each of ecoinvent 3.8, 3.9.1, 3.10, 3.11,
and 3.12, with no missed or unintended matches.

Prospective appliance life is kept separate. The 2026 European Commission/VHK
market model supplies Weibull assumptions for condensing and non-condensing gas
boilers, jet-burner boilers, and hydronic heat pumps. These are stored as
``service_life`` evidence-only profiles and do not generate construction
vintages or runtime assignments.

Electricity distribution grids remain a partial-feasibility case. A 2021
Eurelectric/E.DSO study reports 2020 low-voltage-line shares only as broad
ranges: 30-35% under 20 years, 25-35% at 20-40 years, and 35-40% over 40 years.
The registry preserves these ranges in a ``hold`` fleet-age profile rather than
choosing convenient midpoints. The source does not establish whether the
shares are route-length, circuit-length, or line-count weighted, while
ecoinvent's kilometre-based low-voltage distribution-network product bundles
network infrastructure and is consumed through electricity-market datasets.
It also provides no comparable composition for medium- and high-voltage lines,
transformers, substations, or digital equipment. The inventory relationship is
technically selectable: ecoinvent 3.8, 3.9.1, 3.10, 3.11, and 3.12 contain 169,
205, 206, 207, and 207 low-voltage electricity-market-to-network links
respectively. Activation nevertheless waits for a point distribution with
compatible length weighting. European data may then serve as a documented
fallback elsewhere.

Cement-plant infrastructure uses the July 2026 Global Energy Monitor Global
Cement and Concrete Tracker summary of operating cement capacity by plant-age
band. Profiles are capacity-weighted, matching output-based cement and clinker
activities more closely than plant counts. Country-specific profiles cover
Brazil, Canada, Colombia, Ecuador, India, Peru, South Africa, Switzerland,
Tunisia, and the United States. ``Europe without Switzerland`` uses an aggregate
of the other reported European countries, while ``RoW`` uses the published
world distribution; European data are therefore not substituted where the same
source provides more relevant geographic evidence.

The selectors apply only from ``cement production`` or ``clinker production``
callers to their whole ``cement factory`` or ``integrated cement plant`` input.
They do not match factory construction, markets, concrete production, or plant
components. Closed ten-year bands use their midpoints. The open
``50 years and over`` band is represented at 55 years, making the resulting
mean a conservative lower estimate for old fleets. Capacity with unknown start
dates is excluded and the known-age bands are renormalised. The tracker age is
the plant start date rather than the vintage of each kiln or grinding line, so
later additions, major rebuilds, and refurbishment remain explicit unresolved
limitations rather than being treated as observed replacement timing.

Iron- and steelmaking furnaces use the September 2026 V1 Global Energy Monitor
Global Iron and Steel Tracker API snapshot. Separate capacity-weighted profiles
cover blast furnaces, basic oxygen furnaces, electric arc furnaces, and direct
reduced iron furnaces. The runtime relationship is technology-aware: pig-iron
production receives the blast-furnace profile, converter steel receives the
basic-oxygen profile, electric and cast-iron production receive the electric-arc
profile, and Indian sponge-iron production receives the DRI profile even though
ecoinvent represents its capital input with a supplier named ``blast furnace``.
This caller-aware distinction prevents the supplier label from overriding the
actual production route.

Europe, India, Canada, and world profiles are used where the corresponding
ecoinvent locations exist. Exact country or region evidence takes precedence;
``RoW`` uses the world fleet rather than a European proxy. Profiles use reported
furnace-unit age and current unit capacity, not plant age or furnace counts.
Operating and operating-pre-retirement units are included; planned, retired,
and cancelled units are excluded. Units without a numeric unit age are excluded
and known-age capacity is renormalised. Coverage varies materially: known ages
represent more than 81% of blast-furnace capacity in every activated geography,
but only 22% of European and 41% of world BOF capacity. These low-coverage BOF
profiles are explicitly flagged, as are samples below 30 units.

Furnace relining and refurbishment do not reset the source's construction age.
This is especially important for old European blast furnaces and means the
profiles represent original unit vintages, not the timing of refractory,
shell, or auxiliary-equipment replacement. The selectors therefore apply only
to whole-furnace exchanges in operating iron and steel activities; furnace
construction, markets, rolling, and other downstream processes remain outside
the mapping.

Long-distance natural-gas and petroleum transport now use operating-pipeline
vintages from Global Energy Monitor. Natural-gas transmission phases come from
the November 2025 Global Gas Infrastructure Tracker, while oil and NGL phases
come from the June 2026 Global Oil Infrastructure Tracker. The profiles are
weighted by merged reported or route-estimated pipeline length because the
ecoinvent whole-pipeline exchanges are measured in kilometres. European
callers use the tracker's European start-region population. Other caller
locations use the corresponding world population; a European proxy is not
needed where the same source provides stronger global evidence.

Only operating transmission phases with positive length and a valid start year
are used. Known start years cover 71% of represented European gas-pipeline
length and 81% globally, and 90% of European oil/NGL length and 77% globally;
excluded unknown-year length is reported in each profile and the remaining
weights are renormalised. When a phase reports several start years, the
earliest valid year anchors original construction because the tracker does not
allocate phase length among later expansions or recommissioning dates. Repairs,
replacement sections, reversals, and refurbishment are therefore not inferred
as new construction unless the source records a separate phase.

The selectors require a ``transport, pipeline`` caller for natural gas or
petroleum and one of the exact whole-transmission-pipeline exchanges used by
ecoinvent. They exclude pipeline construction, markets, CO2 transport,
hydrogen, and distribution or gathering networks. The profiles describe the
vintage composition of surviving installed length, not pipeline service life
or throughput-weighted attribution to current transport. Independent inventory
audits found 15, 49, 51, 31, and 31 matching relationships in ecoinvent 3.8,
3.9.1, 3.10, 3.11, and 3.12 respectively, with no missed or unintended matches.

Coal-mine infrastructure now uses the May 2026 Global Energy Monitor Global
Coal Mine Tracker. Four profiles separate surface and underground mines for
Europe and the world. Operating mines are weighted first by reported annual
production; positive capacity is used only when production is unavailable.
Mines with no valid opening year are excluded and their weight is reported
before the known-age distribution is renormalised. Known opening years cover
97.6% of represented European surface-mine weight, 100% of European underground
weight, 93.5% of world surface weight, and 96.7% of world underground weight.

Selectors connect hard-coal or lignite mine operations only to the exact whole
``mine infrastructure`` exchange for open-cast or underground mining. Markets,
mine construction, preparation-only activities, and mine components are not
selected. European callers take the European profile where available; other
locations use the corresponding world profile because the same tracker offers
better global evidence than a European proxy. Opening year is the original
mine vintage: expansions, equipment replacement, and redevelopment are not
observed as age resets. The profiles therefore describe the surviving
production-weighted mine stock, not prospective mine life.
Independent inventory audits found 21 whole-mine-infrastructure relationships
in each of ecoinvent 3.8, 3.9.1, and 3.10, and 24 in each of 3.11 and 3.12,
with no missed or unintended matches. The European underground selector has no
direct relationship in 3.10; the corresponding world profile supplies the
reviewed fallback for that version.

The first active selectors also use a behavior-preserving reviewed assumption: annual
barley, maize, oat, rye, spelt, triticale, and wheat production use uniform
pre-harvest uptake over ``[-1, 0]``. Fifteen selectors cover version-specific
crop names, maize silage, farm-grown maize and oat seed, cereal co-products,
and regional feed-wheat production. They cover 144 canonical identities in the
3.12 compatibility registry and are regression-checked against the legacy
mappings. Direct inventory audits found 81, 91, 116, 214, and 214 relevant
activities in the cutoff inventories for ecoinvent 3.8, 3.9.1, 3.10, 3.11,
and 3.12 respectively. Consequential inventories contained 61, 71, 97, 150,
and 150 matches. Neither system model had missed candidates, out-of-scope
matches, or selected activities lacking uptake. The reproducible profile
derivation is ``dev/generate_trails_empirical_profiles.py``.

Profiles have an explicit ``review.activation_status``. Only profiles marked
``active`` may be referenced by an operational assignment; ``eligible`` marks a
source-backed candidate still awaiting selector review, ``evidence_only`` keeps
descriptive observations out of runtime behavior, and ``hold`` records a known
blocker. The registry validator rejects assignments to any non-active profile.

Four source-backed technical-life assumptions are active as caller context:
wind and solar PV at 30 years, hydropower at 100 years, and nuclear at 60 years.
They use the NREL ATB definition of prospective technical life and apply only to
``electricity production, ...`` activities selected by name, electricity
reference product, and kilowatt-hour unit. The selectors were audited against
ecoinvent 3.8, 3.9.1, 3.10, 3.11, and 3.12 cutoff databases. They do not replace
the separate construction-vintage distributions attached to stock-asset
exchanges. The NREL 15-year stationary-battery assumption remains ``eligible``
but unassigned because the installed inventories do not expose an equally clean
cross-version activity family for stationary storage.

Relationship-aware construction-timing selectors apply the capacity-weighted
EIA 2025 operating-fleet age profiles to onshore wind, solar PV, conventional
hydropower, pumped storage, nuclear, steam-coal/lignite, natural-gas combined
cycle, natural-gas combustion-turbine, natural-gas steam-turbine, and geothermal
infrastructure exchanges. U.S. regional callers use direct ``reviewed-us-*``
assignments where matching U.S. datasets exist. Other countries use explicit
``proxy-*`` assignments backed by the same U.S. evidence only while no better
country-specific selector exists. The 10 MW gas-turbine family is proxy-only
because ecoinvent has no U.S. caller dataset in that family. The selector audit
reports direct and proxy match counts separately. Offshore wind remains
evidence-only in the EIA source because that profile contains only three
generators; broader GEM evidence now supports fixed structures as described
below. U.S. wood biomass is active for the core furnace and ORC generation
unit in U.S. wood-chip CHP callers; dust collectors are excluded because their
retrofit and replacement timing may differ. That U.S. profile retains precedence
for U.S. callers. European and world wood profiles now extend the core-equipment
mapping elsewhere, as described below. Stationary batteries remain unassigned because
the installed inventories expose manufacture and capacity products, but no clean
stationary-storage operating-service relationship.

Wood CHP outside the United States and energy-recovering municipal incineration
use the Global Energy Monitor Bioenergy Power Tracker API snapshot retrieved
14 November 2026. Its 4,535 records are preserved as the input snapshot; API
coverage is not asserted identical to the latest release advertised on the
tracker website. Only operating units with exactly the dedicated wood/other
solid-biomass fuel category or the municipal/industrial-refuse category are
selected. Mixed-fuel units, fossil co-firing, landfill gas, sewage-only units,
and any unit flagged as converted or replaced are excluded. Original start
years define the construction vintages; conversion years never reset age.

The European dedicated-wood profile has 121 known-year units, covering 99.15%
of eligible electrical capacity, with a 17.85-year capacity-weighted mean age.
The world counterpart has 344 known-year units, 96.72% coverage, and a
14.98-year mean. European refuse has 145 known-year units, 93.62% coverage,
and a 22.16-year mean; world refuse has 1,353 known-year units, 96.97% coverage,
and an 11.59-year mean. Unknown-year capacity is excluded and known-year weights
are renormalised. European callers use European profiles; other callers use
lower-priority world proxies, while the existing EIA U.S. wood profile retains
precedence.

These are pragmatic technology and size proxies. The utility-scale source
incompletely covers small facilities, including the small ORC wood CHP systems
represented by ecoinvent. Weighting uses electrical generating capacity because
comparable heat capacity and waste-treatment throughput are unavailable. The
refuse source mixes municipal and industrial waste. These limitations are stored
in each profile rather than implying that the source directly observes small
wood CHP fleets or throughput-weighted incinerator ages.

Wood selectors cover only core furnace and ORC generation-unit exchanges from
wood-chip CHP operation, excluding state-of-the-art and future scenarios from
the new GEM assignments. Dust collectors and other retrofit equipment are not
selected. Waste selectors require a municipal-incineration treatment caller and
the exact whole municipal-waste-incineration facility input. Callers explicitly
without energy recovery, future scenarios, hazardous-waste facilities, markets,
and construction-only activities are excluded.
Independent inventory checks find 80, 160, 164, 164 and 164 newly mapped wood
CHP core-equipment relationships in ecoinvent 3.8, 3.9.1, 3.10, 3.11 and 3.12
respectively, plus 158, 218, 275, 321 and 321 municipal-incineration facility
relationships. Existing U.S. EIA wood relationships are additional to these
new GEM matches.

Concentrating solar power now uses the February 2026 Global Energy Monitor
Global Solar Power Tracker. Operating solar-thermal phases are weighted by
reported nameplate capacity and grouped by commissioning year. Country-specific
profiles cover Spain (53 known-year phases and a 14.34-year capacity-weighted
mean age), the United States (12 phases; 13.02 years), and South Africa (seven
phases; 8.00 years). A world profile supplies the reviewed fallback elsewhere:
121 of 127 operating phases have a valid start year, representing 88.30% of
reported operating capacity and a 9.38-year mean age. Unknown-year capacity is
excluded and the known-year distribution is renormalised.

The selector applies only from ``electricity production, solar thermal
parabolic trough`` callers to the complete 50 MW concentrated-solar plant
exchange. It excludes collector fields, thermal storage, heat-transfer systems,
power blocks, plant construction, and markets when those datasets are not the
electricity-service caller. Spain, U.S. regions, and South Africa take their
country profiles; other locations take the world profile. The tracker groups
parabolic trough, tower, Fresnel, solar chimney, and other solar-thermal designs,
so the operating technology family is an explicit proxy for ecoinvent's
parabolic-trough plant. Original phase start years are not reset for repowering,
storage replacement, or refurbishment. Independent inventory audits find five
complete-plant relationships in every ecoinvent version from 3.8 through 3.12,
with no missed or unintended matches.


Offshore oil and gas platforms use the Norwegian Offshore Directorate's complete
facility and status-history exports dated 14 November 2026. The profile includes
73 Norwegian surface facilities currently in service, in positive water depth,
with production, processing, drilling or wellhead functions. Subsea templates,
wind turbines, onshore facilities, storage-only vessels, risers, flares and
accommodation-only facilities are excluded. All selected facilities have a known
first-service date; their count-weighted mean age in 2026 is 25 years.

The earliest recorded in-service date for each unique facility ID is compared
with the current startup date. This avoids treating the restart of Njord A in
2022 or Jotun FPSO in 2025 as new original construction: their histories retain
1997 and 1999 respectively. Dates remain commissioning proxies: fabrication lead
time and construction before a vessel conversion are unavailable. Later upgrades
and replacement equipment do not inherit a fresh whole-platform vintage.

Selectors apply only from offshore petroleum-and-gas production activities to
their exact whole petroleum or natural-gas platform exchange. Onshore production
is excluded even where ecoinvent uses an offshore-platform input as a proxy.
Norwegian callers use the reviewed profile directly; other regions use it as an
explicit lower-priority fallback until stronger local evidence is available.
The profile combines oil and gas facilities and several structural designs,
including production vessels. Count weighting does not capture differences in
structural mass, processing capacity or utilisation, which remain limitations of
this pragmatic construction-vintage proxy.

Inventory audits identify 10 whole-platform relationships in ecoinvent 3.8,
50 in 3.9.1, and 72 in each of 3.10, 3.11 and 3.12. Each version has two direct
Norwegian relationships; the remaining relationships receive the Norwegian
proxy. The selectors cover both ``off-shore`` and ``offshore`` caller names.


Fixed offshore wind structures now use the February 2026 GEM wind-farm map
snapshot. Only operating fixed-bottom phases are included, with 343 phases
globally and 130 in Europe; all have valid start years. Country profiles cover
Britain (44 phases), Germany (32) and China (156), taking precedence over the
European and world fallbacks. Capacity-weighted mean ages are 8.21 years for
Europe, 5.83 globally, 8.92 for Britain, 8.54 for Germany and 4.33 for China.
These phase dates approximate original fixed-structure construction. Floating
and unknown-mount phases are excluded, and transfer from a modern fleet with
larger turbines to ecoinvent's 2 MW design remains an explicit approximation.
Selectors apply only to the exact ``offshore, fixed parts`` input in offshore
wind electricity production. Moving parts, maintenance and replacement timing
are not inferred from these original phase dates.
Independent inventory scans identify 22 complete fixed-structure relationships
in ecoinvent 3.8 and 26 in each of 3.9.1, 3.10, 3.11 and 3.12, with no missed
or unintended matches.

Methanol and ammonia were investigated using the November 2026 GEM Chemicals
Tracker public dashboard. Four capacity-weighted site-vintage profiles preserve
European and world evidence for sites reporting natural-gas feedstock without
coal, but remain ``evidence_only`` with no runtime assignments. The dates refer
to industrial sites, which can substantially predate current synthesis units.
For example, Yara's own Sluiskil history identifies 1929 as the beginning of
ammonium-sulphate production, followed by later fuel changes and major
investments. Assigning that date to all current ammonia production equipment
would conflate site history with unit construction.

The European site-age means of 52.38 years for methanol and 68.33 for ammonia
therefore do not establish current production-unit ages. Existing capacity of
each chemical is used separately; developing capacity is excluded and
unknown-age capacity renormalised. Unit-specific commissioning, expansions and
rebuilds must be resolved before activation. Chemical profiles are shown in the
PDF in a separate evidence-only category, retaining the limitation visibly.


CSV columns
-----------

.. list-table::
   :header-rows: 1
   :widths: 24 76

   * - Column
     - Meaning
   * - ``name``
     - Activity name, or biosphere flow name for ``long_term_emission`` rows.
   * - ``reference product``
     - Activity reference product. For ``long_term_emission`` rows this is the
       ``compartment|subcompartment|unit`` selector.
   * - ``temporal_tag``
     - Controls how the row is matched and applied.
   * - ``tag_confidence``
     - Confidence in the tag assignment.
   * - ``tag_notes``
     - Short explanation of why the tag applies.
   * - ``lifetime``
     - Lifetime or rotation time in years. This is also used as context for
       maintenance and end-of-life timing.
   * - ``age distribution type``
     - Numeric temporal-distribution code copied to the exchange as
       ``temporal_distribution``.
   * - ``loc``, ``scale``
     - Distribution parameters used by lognormal, normal, and triangular rows.
   * - ``minimum``, ``maximum``
     - Distribution bounds used by uniform and triangular rows.
   * - ``offsets``, ``weights``
     - Explicit time offsets and associated weights for discrete distributions.
       Values are separated with ``|``.
   * - ``param_confidence``
     - Confidence in the timing parameters.
   * - ``param_notes``
     - Sources, assumptions, profile names, priorities, and other audit notes.


Distribution codes
------------------

The CSV uses the same numeric convention that is copied to exchange field
``temporal_distribution``.

.. list-table::
   :header-rows: 1
   :widths: 16 24 60

   * - Code
     - Distribution
     - Required parameters
   * - ``2``
     - Lognormal
     - ``loc`` and ``scale``. Used by some stock-asset rows.
   * - ``3``
     - Normal
     - ``loc`` and ``scale``. Present only in a small number of legacy rows.
   * - ``4``
     - Uniform
     - ``minimum`` and ``maximum``.
   * - ``5``
     - Triangular
     - ``loc``, ``minimum``, and ``maximum``.
   * - ``6``
     - Discrete
     - ``offsets`` and ``weights``. Weights should sum to ``1`` after parsing.

Rows with type ``6`` are used when the timing profile is known explicitly, or
when TRAILS generates a profile from a named long-term-emission profile.


Biomass growth
--------------

Rows tagged ``biomass_growth`` are applied only to biosphere exchanges named
``Carbon dioxide, in air``. The amount of the CO2 exchange is not changed. Only
its timing metadata is changed.

Forestry and woody-biomass uptake represents regeneration following harvest.
The harvested biomass is assigned the next growth cycle's CO2 uptake, with no
additional delay for replanting. This replaces the previous allocation of
uptake before harvest; the existing uptake amount is counted once. The rule
also covers the tagged forest-residue, debarking, imported-wood, cork, bamboo,
and short-rotation willow growth profiles. It changes only the uptake exchange,
without shifting the biomass supply or other forestry exchanges.

The reference point is the supplying activity's harvest year. For biomass used
in the same year, regrowth therefore follows use. If another exchange specifies
storage or a processing delay, that timing remains separate; uptake is not
re-anchored to the final combustion year.

Agricultural crop profiles retain their existing pre-harvest timing. The
forestry rows for the matched species below use a nonlinear annual profile
derived from the ALIGNED T1.2 forest carbon flux model.

The profile follows the cumulative biomass-growth curve used in the ALIGNED
spreadsheet model, based on De Rosa et al. (2017):

.. code-block:: text

    C(t) = 1 / (1 + 100 ** ((T / 2 - t) / (T / 2)))

where ``T`` is the rotation time and ``t`` is the stand age. Annual weights are
computed from year-to-year differences in ``C(t)``, then normalized to sum to
``1``. Offsets are annual stand-age midpoints after harvest. Relative to the
previous pre-harvest profiles, each offset is increased by ``T``; the weights
and their order are preserved. For example, a 30-year rotation has offsets from
``0.5`` to ``29.5`` years and peaks around the middle of the rotation. Spruce's
83-year profile runs from ``0.5`` to ``82.5`` years. Fractional rotations retain
their shorter final bin.

Uniform forestry profiles are likewise shifted from ``[-T, 0]`` to ``[0, T]``.
The triangular bamboo profile's bounds and mode are shifted by its two-year
rotation, preserving its original one-year support width: bounds ``[0, 1]``
and mode ``1``. The rotation estimates and profile shapes are retained; the
post-harvest placement is a modelling assumption, documented in ``param_notes``.

The current forestry update uses rotation-time values from the ALIGNED managed
forest database and the growth profile from the ALIGNED T1.2 LCA Carbon Flux
model, as documented by Lancz et al. (2026). Source notes in
``param_notes`` also cite De Rosa et al. (2017) and the underlying rotation-time
sources from the ALIGNED database.

.. list-table::
   :header-rows: 1
   :widths: 46 18 18 18

   * - ALIGNED mapping
     - CSV rows
     - Rotation time
     - Discrete bins
   * - ``Lophira alata`` in Cameroon
     - 5
     - 30 y
     - 30
   * - ``Fagus sylvatica`` in Europe
     - 11
     - 115 y
     - 115
   * - ``Betula`` species in Europe
     - 19
     - 61 y
     - 61
   * - ``Eucalyptus`` in Australia
     - 7
     - 33 y
     - 33
   * - ``Quercus`` species in Europe
     - 11
     - 123.5 y
     - 124
   * - ``Araucaria angustifolia`` in Brazil
     - 3
     - 27.5 y
     - 28
   * - ``Pinus sylvestris`` in Europe
     - 17
     - 100.5 y
     - 101
   * - ``Picea abies`` in Europe
     - 17
     - 83 y
     - 83

This profile is a timing profile only. Parameters such as mean annual increment,
wood density, and carbon fraction affect the amount of carbon in the ALIGNED
model, but they cancel out when the annual uptake curve is normalized into
TRAILS timing weights.


Wood-chip suppliers
-------------------

Sawmill chip production, wood-manufacturing chip co-products, drying, and
post-consumer chip recovery are tagged ``throughput_process``. This tag does not
assign a delay to the biomass supply exchange. In the reviewed ecoinvent
3.10--3.12 cutoff inventories, the matched processing activities have no direct
``Carbon dioxide, in air`` exchange. For sawmill chips, uptake occurs upstream
in forestry activities, where the ``biomass_growth`` rules apply.

The table retains both the older ``wet`` sawmill-chip product names and the
ecoinvent 3.12 ``green`` names. Post-consumer chip recovery likewise has entries
with and without ``dry`` in the product name. These are separate exact-match
aliases; the loader does not rewrite product names. The legacy spruce-chip
drying entry is retained, although it was not found in the reviewed databases.

Unused growth distributions and unsupported 15-year lifetimes have been removed
from these processing rows. No storage or drying duration is inferred from an
asset lifetime. A blank lifetime also means that the row supplies no lifetime
context for maintenance or end-of-life inputs; none of the matched processing
activities in the reviewed databases had inputs selected by those rules.

Willow short-rotation coppice is a cultivation activity with direct CO2 uptake.
Both its older product name and its ecoinvent 3.12 dry-mass product name use
``biomass_growth``, with uniform regrowth uptake from year ``0`` to ``2``. This
is the same two-year rotation assumption for both aliases, not a delay applied
to the willow supply exchange.


Long-term emissions
-------------------

Rows tagged ``long_term_emission`` use type ``6`` discrete distributions. The
CSV row gives a biosphere-flow selector and the profile name is stored in
``param_notes`` as ``profile:<name>``. A selector priority can be stored as
``priority:<number>``.

The long-term profiles use 32 midpoint bins from 100 to 1000 years:

* 100 to 200 years: 10-year bins.
* 200 to 500 years: 25-year bins.
* 500 to 1000 years: 50-year bins.

Available profile names are:

``uniform_100_1000``
    Generic fallback for long-term groundwater and soil selectors.

``front_loaded_long_term``
    Higher weights in the earliest long-term bins. Used for more degradable
    organic flows.

``ammonium_plateau``
    Broad plateau followed by slow decline.

``conservative_washout``
    Soluble conservative leaching with a persistent tail.

``mobile_metal``
    Earlier long-term weights than sorbed metals.

``sorbed_metal``
    Later and more persistent tail for strongly sorbed metals.

``persistent_tail``
    Defined in code for highly persistent emissions, but not currently selected
    by a CSV row.


Maintaining the registry
------------------------

Edit the YAML registry, not ``temporal_distributions.csv``. Keep sources and
assumptions as structured fields, give every profile an evidence status, and
state geography and reference-year applicability when the evidence is not
global. For discrete profiles, offsets and weights must be aligned, finite, and
normalised.

After editing legacy profiles or mappings, regenerate the compatibility table:

.. code-block:: bash

    PYTHONPATH=. conda run -n premise python \
        dev/compile_trails_temporal_csv.py

``dev/migrate_trails_temporal_csv.py`` documents the one-time migration and
conflict-resolution procedure. It is not the normal authoring direction.

Useful checks before committing changes:

.. code-block:: bash

    conda run -n premise pytest tests/test_temporal_legacy.py \
        tests/test_temporal_specs.py tests/test_temporal_evidence.py \
        tests/test_trails_temporal.py tests/test_lifecycle_timing.py
    git diff --check
    make -C docs html


References
----------

* Lancz, K., Ghose, A., and Pizzol, M. (2026). Extension and validation of
  forest carbon flux model for dynamic Life Cycle Assessment. International
  Journal of Life Cycle Assessment, 31:130. doi:10.1007/s11367-026-02695-0.
* De Rosa, M., Schmidt, J., Brandao, M., and Pizzol, M. (2017). A flexible
  parametric model for a balanced account of forest carbon fluxes in LCA.
  International Journal of Life Cycle Assessment, 22, 172-184.
  doi:10.1007/s11367-016-1148-z.

Finding IAM stock-vintage packages
----------------------------------

``TrailsDataPackage`` looks for the annual build-year results for the
selected IAM scenario. Place the ZIP in ``premise/data/iam_vintage_files/``
in a source checkout, or pass ``vintage_package_dir`` to the class. Name the file
``<model>_<pathway>_stock_vintages_v2.zip`` using a lowercase model name
and the pathway as recorded in the manifest. Put the three files from
iam-prep-for-premise at the root of the ZIP.

.. code-block:: text

   remind_SSP2-EU21-NPi2025_stock_vintages_v2.zip
     datapackage.json
     vintages.parquet
     validation.json

Once a versioned Zenodo record contains the ZIP, pass its numeric record ID
to ``TrailsDataPackage`` as ``vintage_zenodo_record_id``. The local file takes
priority. On a local miss, TRAILS checks the cache for that record and downloads
the ZIP if needed. It checks the scenario and file checksums before saving
the download. The verified ZIP is available as ``vintage_package_path`` on
the instance. With the default folder and no record ID, a missing package
leaves that attribute set to ``None``.
