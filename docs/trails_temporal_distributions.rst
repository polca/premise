TRAILS temporal distributions
=============================

TRAILS stores temporal-distribution metadata in
``premise/data/trails/temporal_distributions.csv``. The table is loaded by
``TrailsDataPackage`` and the parameters are applied to exchanges before export.
This allows the resulting database or matrix export to keep information about
when an exchange occurs relative to the activity reference year.

The temporal reference point is year ``0`` of the activity. Negative offsets
represent exchanges that happened before the reference product is supplied, for
example biomass carbon uptake before forest harvest. Positive offsets represent
future exchanges, for example maintenance, end-of-life treatment, or long-term
emissions.


How the table is used
---------------------

``TrailsDataPackage.add_temporal_distributions()`` applies the CSV rows with
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
    ``dataset lifetime + 1`` years.

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
``long_term_biosphere_matches.csv``.


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

Most agricultural rows still use simple uniform uptake over the crop or
perennial lifetime. The forestry rows for the matched species below use a
nonlinear annual profile derived from the ALIGNED T1.2 forest carbon flux model.

The profile follows the cumulative biomass-growth curve used in the ALIGNED
spreadsheet model, based on De Rosa et al. (2017):

.. code-block:: text

    C(t) = 1 / (1 + 100 ** ((T / 2 - t) / (T / 2)))

where ``T`` is the rotation time and ``t`` is the stand age. Annual weights are
computed from year-to-year differences in ``C(t)``, then normalized to sum to
``1``. Offsets are annual midpoints shifted by ``-T`` so that uptake occurs
before harvest. For example, a 30-year rotation has offsets from ``-29.5`` to
``-0.5`` years and peaks around the middle of the rotation.

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


Maintaining the table
---------------------

When updating ``temporal_distributions.csv``:

* Keep ``tag_notes`` focused on why the row is tagged.
* Keep ``param_notes`` focused on timing sources and assumptions.
* Use ``tag_confidence`` for the row classification and ``param_confidence`` for
  the timing parameters.
* For type ``6`` rows, keep ``offsets`` and ``weights`` the same length and make
  sure weights sum to ``1``.
* Prefer explicit source citations in ``param_notes`` when values come from a
  paper, dataset, or reviewed model.

Useful checks before committing changes:

.. code-block:: bash

    conda run -n premise pytest tests/test_trails_temporal.py
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
