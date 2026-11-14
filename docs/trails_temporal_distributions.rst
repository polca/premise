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
of silently choosing one. The audit API reports raw and selected match counts,
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

The empirical fleet and survival profiles remain deliberately unassigned.
Applying them requires a reviewed geography/technology mapping and, for
retirement timing, a cohort or survival model that handles censoring. The first
active selectors instead use a behavior-preserving reviewed assumption: annual
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
hydropower, pumped storage, and nuclear infrastructure exchanges. U.S. regional
callers use direct ``reviewed-us-*`` assignments. Other countries use explicit
``proxy-*`` assignments backed by the same U.S. evidence only while no better
country-specific selector exists. The selector audit reports direct and proxy
match counts separately. Offshore wind remains evidence-only because its EIA
operating-fleet profile contains only three generators.


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
