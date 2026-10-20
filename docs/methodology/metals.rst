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
The loader retains the intensity unit and checks it against the conversion.
Missing metal/technology pairs remain missing; they are not converted to zero.
A specified zero range removes the corresponding direct input. Every compiled
rule records an applied or preserved outcome, including unavailable intensities.

EV intensities in the runtime CSV are in kg metal/kg motor. The three motor
activities therefore use a conversion of one, rather than 0.1 MW/53 kg.
For example, the 2020 aluminium intensity is 0.2252697267 kg/kg motor.

The EV mapping selects the vehicle-motor and electric-scooter production
families. The asynchronous rail traction motor has its own engineering
composition and is outside this road-vehicle mapping. Its production name
must not trigger a generic EV material overlay or require a road-motor
conversion merely because it starts with ``electric motor production``.

The PEMFC mapping selects the production and reference-product names of the
configured 1 kWe proton-exchange-membrane fuel-cell stack. Its conversion is
0.001 MW per stack. Adhesive films, membranes, system assemblies and other
activities mentioning fuel cells or PEM are outside this stack-level mapping.

The AEL and HTEL mappings likewise select the configured 1 MWe AEC and SOEC
stack activities and reference products. Both conversions are one MW per
stack. Motor-drive assemblies, balance-of-plant equipment and glass seals
retain their own inventories rather than receiving stack-level intensities.

The c-Si mapping requires a silicon-cell reference product as well as a
matching activity name. Substrate-preparation services and scrap co-products
therefore do not receive cell-level material intensities.

Unit compatibility alone does not establish component scope. The EV summary
includes whole-vehicle observations from Cullbrand and Magnusson's
`The Use of Potentially Critical Materials in Passenger Cars
<https://odr.chalmers.se/items/0926d689-2b39-4acf-950d-673f8d6660b7>`_,
Table 8 (printed pages 33–34). Its 6256.55 g lithium total belongs to a hybrid
car, predominantly its battery. Dividing by a 53 kg motor reproduces the CSV's
0.1180481 kg/kg; it does not turn this into a motor composition.

Rules using these mixed-scope EV summaries now have an explicit
``preserve_reason`` and leave source inputs intact, at every year. The
unverified scandium component boundary is also preserved. The shared boron and
neodymium magnet rules are preserved: their 53/1.98 conversion belongs to one
reference motor and cannot be imposed on the shared magnet supplier. These
decisions remain in the change report; rules are not silently removed.
Component-resolved observations and distinct suppliers are needed to re-enable
these overlays. Battery rules remain separate; motor lithium is not moved to
a battery without a validated battery composition and functional unit.

The shared passenger-car engine is explicitly preserved. Its competing Pt/Pd
factors, 1/0.260129659643436 and 1/0.3051750380517504, derive from the petrol
and diesel car engine inputs respectively in ecoinvent 3.12. Both vehicles use
the same engine provider, so a global overwrite cannot satisfy both mass bases.
Separate component inventories are needed before applying these vehicle-level
intensities to the engine. Other overlapping rules that write the same activity
and input product are rejected before material updates begin.

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
Material intensities are interpolated within each metal/technology pair's
supported year range. Outside that range, the source input is preserved and
a decision records the temporal restriction (``year_outside_support`` for
individual rules, ``design_incomplete`` for a matched design).
A 2020-only aluminium observation does not become a 2050 projection because
neodymium has a 2050 observation. Each applied direct material amount uses the
configured activity-unit conversion and compound/content factor.

Existing-provider fallback for PEM iridium
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``pemel-iridium-iridium`` rule requests ``market for iridium``. Market
creation can be skipped because PGM freight observations are absent or the
mining-share series does not cover the requested year. Previously, that also
prevented application of the separate, supported material-intensity series:
the PEM stack kept its original 0.8 kg iridium input at every year.

The rule now explicitly declares ``existing_provider_fallback`` for
``platinum group metal, extraction and refinery operations`` producing
``iridium``. This supplier comes from ``lci-PGM.xlsx``; it is not assumed to
exist as a native ecoinvent iridium market. The existing stack uses its
South African activity. When the requested market is absent, retain that
same supplier and geography instead of constructing a new mix. A valid
configured market remains preferred when available.

The fallback requires exactly one existing nonnegative kilogram iridium
technosphere input, the declared supplier identity, no custom exchange formula,
and one active supplier at its existing location in the current database.
The supplier must produce one kg of the same reference product. Missing,
duplicate, changed-product or incompatible-unit matches preserve the input
and record an explicit diagnostic. A wrong unit on a present configured
market raises a configuration error. Other material rules do not opt into
this fallback; no producer is selected for an absent source input.

The conversion file identifies the activity as one **1 MWe stack per unit**,
so its conversion is one MW/unit: kg/MW becomes kg per stack, without a
factor of 1,000 or a conversion to kg/kWh. Hydrogen-output, stack lifetime,
replacement and operating-energy coefficients are retained. The source hydrogen
recipe's stack demand already includes replacements; changing kg/stack does
not add another lifetime factor. Provenance records the requested market,
actual refinery, retained location, selection mode and converted target.
Direct-metal comparisons include the declared fallback provider.

Matched PEM catalyst reference
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The previous PEMEL CSV independently combined medians from ten Ir and Pt
observations with a Ru loading from one experimental Ru-Ir catalyst. This
mix did not represent a documented electrode design. The ``Stahl_2016``
workbook citation also actually identifies Staffell and Ingram's 2010 alkaline
fuel-cell CHP paper; its true numerical source remains unresolved. The
historical workbook and all 25 source observations are preserved, but its
catalyst observations no longer determine the runtime coefficients.

Use the matched 2022 baseline in Badgett et al. (2024),
`NREL/TP-6A20-87625 <https://docs.nlr.gov/docs/fy24osti/87625.pdf>`_,
Table 1 (printed page 5), Figure 3 and section 3. The anode contains
2 mg Ir/cm² and the cathode 1 mg Pt/cm² at 2 A/cm² and 1.9 V/cell:

.. math::

   A_{sum} = \frac{10^6\ W}{2\ A/cm^2 \times 1.9\ V}
             = 263157.895\ cm^2

   m_{Ir} = A_{sum} \times 2\ mg/cm^2 \times 10^{-6}\ kg/mg
             = 0.526315789\ kg/MW

   m_{Pt,catalyst} = A_{sum} \times 1\ mg/cm^2 \times 10^{-6}\ kg/mg
             = 0.263157895\ kg/MW

This pure-Ir anode has zero Ru. That zero is specific to the chosen design;
it does not describe every PEM catalyst. Loadings refer to contained metal,
not oxide mass. The summed area already includes every cell: multiplying by
300 cells again would double-count the stack. The report's rounded
300 × 877 cm² geometry gives 999,780 W, 0.022% below 1 MW. Normalize to
exactly 1 MW of nominal stack DC terminal power; AC-system power and delivered
hydrogen energy have different boundaries. Treating the existing ``1MWe``
activity as this DC stack reference is an explicit capacity-basis proxy.

For the default **prospective** case, hold the 2022 baseline over 2020–2022
(the 2020 value is a backcast proxy). From 2022 to 2050, reduce both loadings
linearly to the report's paired very-low-PGM case in Table A-7 (printed
page 65): 0.4 mg Ir/cm² and 0.1 mg Pt/cm². This gives **0.105263158 kg Ir/MW**
and **0.026315789 kg cathode-catalyst Pt/MW** in 2050, reductions of 80% and
90% respectively for electrode catalysts. Add hardware coating Pt separately
as described below; 0.026315789 kg/MW is not the total Pt requirement.
Ru remains zero throughout. Intermediate years follow the same common
calendar fraction; their Ir/Pt ratio evolves with the paired loadings rather
than combining independent sample medians.

The report does **not** assign a calendar date to this case. Reaching it in
2050 and interpolating linearly are explicit scenario assumptions, not a
published forecast or achieved observations. Section 3.5 and Figure 25 treat
these as theoretical isolated changes. Use only the loading-reduction case;
do not also credit the report's separate 5 A/cm² current-density case.
Retain 2 A/cm² and 1.9 V/cell (3.8 W/cm²), the same nominal DC capacity,
hydrogen productivity and replacement schedule. Thus only catalyst loading
changes: there is no additional reduction from higher power density,
efficiency or longer life. The common 2022 knot prevents improvement being
credited before the reference's status year.

Lower loading at unchanged performance and durability is an assumption.
The report explicitly warns of degradation and efficiency trade-offs,
especially under dynamic operation; this is not a demonstrated commercial
trajectory. The unchanged lifetime allocation remains a separate proxy,
as described below. Evaluate the constant-loading sensitivity alongside the
prospective case when assessing how this assumption affects metal demand.

Equal CSV mean/median/min/max values specify a deterministic scenario, not
zero engineering uncertainty. Outside the 2020–2050 support, preserve source
inputs. The ``design_group`` on the three catalyst rules requires complete
configured membership, supported finite intensities, and valid kilogram
suppliers for both positive inputs before changing any member. A missing or
incompatible supplier cannot leave a partially changed catalyst set. No Ru
market is required to apply its explicit zero. Provenance identifies the
matched group, requested year, selected intensities and temporal support.

The reference inputs, projection assumptions, previous constant coefficients,
historical observations, citation audit and limits are packaged in
``data/metals/pem_catalyst_reference.json``. Run
``python scripts/update_pem_catalyst_reference.py`` to reproduce only the nine
PEMEL Ir/Pt/Ru CSV rows at 2020, 2022 and 2050. Pt rows contain the combined
cathode-catalyst and selected hardware-coating quantity. The updater guards recognized
source values, preserves unrelated records including embedded newlines,
and is byte-idempotent.

The **fixed-baseline** sensitivity retains 0.526315789 kg Ir/MW and
0.263157895 kg cathode-catalyst Pt/MW over the same years. With the default
hardware coatings, its total Pt is 0.545034298 kg/MW throughout. Export a separate full intensity
CSV without changing runtime data:

.. code-block:: console

   python scripts/update_pem_catalyst_reference.py --scenario fixed-baseline --output /tmp/metals-fixed-baseline.csv

For an intentional sensitivity build, omit ``--output`` to select the
constant case in the local runtime CSV, then rebuild scenarios. Run the
updater with its default ``--scenario prospective`` to restore the default.
Restart Python before rebuilding after a switch, because the material-data
loader caches its array within a Python process.
An alternative CSV export alone does not alter an existing build or point
premise to the exported file. Both cases retain the same non-catalyst inputs;
this switch affects only the matched PEMEL catalyst series.

Platinum hardware coatings
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The original stack has a direct 0.75 kg Pt input labelled cathode catalyst;
it has no separate hardware-coating input or coating-service activity.
NREL's Table 1 separately specifies Pt-coated anode PTLs and bipolar plates.
Including cathode catalyst alone therefore leaves the selected reference
incomplete. The runtime Pt input now replaces the old direct input with
**cathode catalyst + PTL coating + bipolar-plate coating**, once. These
components are calculated separately in the offline updater and reference
JSON; they are not appended on top of the old 0.75 kg recipe.

The user-selected coating reference assumes a **100 nm Pt film on both
nominal flat faces of each anode PTL and bipolar plate**. The report directly
specifies 100 nm only for the PTL (printed page 30). Applying that thickness
to bipolar plates and choosing both faces are explicit engineering
assumptions. Figure 3 gives an 877 cm² active cell area, PTL area plus a 10%
processing buffer, and 35 × 35 cm (1225 cm²) plate footprint. Use these as
nominal coated footprints: 1.1 times active area for the PTL, and the full
plate plane for bipolar plates, without subtracting ports or masked seals.
Do not apply another 10% manufacturing-loss multiplier. The earlier
`Mayyas report <https://docs.nlr.gov/docs/fy19osti/72740.pdf>`_ used a gold
coating; its gold is not silently interpreted as additional platinum.

Using `NIST's Pt density <https://physics.nist.gov/cgi-bin/Star/compos.pl?matno=078>`_
of 21.45 g/cm³ (21,450 kg/m³):

.. math::

   N_{equiv} = \frac{263157.895\ cm^2}{877\ cm^2/cell}

   m_{Pt,PTL} = \frac{263157.895}{10^4}\ m^2
                 \times 1.1 \times 2 \times 100\times10^{-9}\ m
                 \times 21450\ kg/m^3
                 = 0.124184211\ kg/MW

   m_{Pt,BPP} = N_{equiv} \times \frac{1225}{10^4}\ m^2
                 \times 2 \times 100\times10^{-9}\ m
                 \times 21450\ kg/m^3
                 = 0.157692192\ kg/MW

Exact 1 MW normalization uses about 300.066 cell equivalents; the source's
rounded 300-cell assembly is 999,780 W. Two bipolar-plate face equivalents
per cell represent the internal shared faces and terminal-facing surfaces;
do not add two further whole, two-sided end plates. These are projected
flat-film areas, not measured microscopic fibre or pore surface areas.

The resulting coating reference is **0.281876403 kg Pt/MW**. With unchanged
coating geometry at all years, total Pt declines from **0.545034298 kg/MW**
in 2020–2022 to **0.308192192 kg/MW** in 2050. Electrode catalyst retains
its 90% reduction; the total declines by about 43.5% because the coating
has no separately established future-thrifting trajectory. Coatings use
the existing replacement-inclusive stack allocation: four stacks imply
four coating sets, not another lifetime or replacement multiplier.

Export explicit sensitivities without changing the runtime CSV:

.. code-block:: console

   python scripts/update_pem_catalyst_reference.py --coating-case one-face --output /tmp/metals-one-face.csv
   python scripts/update_pem_catalyst_reference.py --coating-case thin-film --output /tmp/metals-thin-film.csv
   python scripts/update_pem_catalyst_reference.py --coating-case active-area --output /tmp/metals-active-area.csv
   python scripts/update_pem_catalyst_reference.py --coating-case none --output /tmp/metals-catalyst-only.csv

``one-face`` halves the nominal coated-face count. ``thin-film`` uses 20 nm
on both components, one fifth of reference coating mass. A
`manufacturer reports 20–100 nm for PTLs <https://mds.umicore.com/en/electroplating/news-media/news/ultra-thin-uniform-platinum-coating-for-ptls-in-pem-electrolysers/>`_,
but that does not validate a 20 nm bipolar plate or this complete system.
``active-area`` coats only active-area planes, omitting the PTL buffer and
outer plate footprint. ``none`` reproduces the previous catalyst-only
comparison; it deliberately omits coverage and is not evidence of zero
physical coating. Combine any coating case with either catalyst scenario.
These are engineering sensitivities, not statistical bounds or demonstrated
commercial designs. The default is ``--coating-case reference``.

Retained catalyst and nominal flat-film coating masses assume 100%
incorporation at the direct metal-purchase boundary. Real pore/fibre area,
coating penetration, flow-channel surface, masks, discontinuity, deposition
utilization, manufacturing rejects/recovery, process energy and treatment
remain unresolved. No factory-line cost yield is interpreted as a measured
metal recovery yield. The overall inventory remains a reference model with
these gaps; the coating addition does not validate the entire stack.

Other hardware remains unchanged. In particular, the potassium and titanium
observations linked to ``Stahl_2016`` still need separate review. The retained
hydrogen recipe uses 54 kWh/kg H₂, 20 years, 8,000 hours/year, 18.52 kg H₂/h,
and three replacements (four stacks total); its specifications list 45,000
hours nominal stack life. Thus demand is four stacks per 2,963,200 kg H₂,
not another lifetime multiplier on catalyst mass. NREL separately reports
51.1 stack + 4.2 balance-of-plant kWh/kg H₂. This targeted change does not claim
that the full hydrogen system matches NREL's performance, hardware or lifetime.
No primary/secondary share or supply mix is changed. Rebuild scenarios to
apply it; source inventories, saved databases, indicators and dashboards are
not rewritten.

Matched SOEC ceramic reference
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The SOEC stack previously retained 37.7 kg of mixed Sm/Eu/Gd oxide while the
metals update added explicit yttrium oxide. Gerloff (2021),
`section 2.2.3 <https://doi.org/10.1016/j.est.2021.102759>`_, used mixed
rare-earth concentrate as an environmental-impact proxy for unavailable
yttrium data. It was not a measured physical ingredient. Retaining both
representations introduced overlapping component supply chains; this was
not direct double-counting of measured yttrium by a yttrium indicator.
Concentrate masses and purity labels also cannot be treated as assays of
the pure oxides.

The reference in ``soec_ceramic_reference.json`` reconciles just the YSZ
electrolyte and YDC blocking-layer package. It uses 0.425 kg YSZ and
0.262 kg YDC per source kW, and retains Gerloff's existing 0.88 capacity
exponent from 1 kW to 1 MW. This gives 185.519229 kg YSZ and 114.367148 kg
YDC per nominal 1 MW stack. The reference compositions are:

* YSZ: 8 mol% Y₂O₃ among the binary oxides, with 92 mol% ZrO₂.
* YDC: Ce₀.₈Y₀.₂O₁.₉, with 20% Y on cation sites. Its oxide mixture
  contains 0.1 mole Y₂O₃ per 0.8 mole CeO₂.

These are explicit reference compositions, not 8% and 20% mass fractions.
Pure-oxide inputs are calculated with the documented molar masses:

.. math::

   w_{Y_2O_3,YSZ} = \frac{0.08 M_{Y_2O_3}}
      {0.08 M_{Y_2O_3} + 0.92 M_{ZrO_2}}

   w_{Y_2O_3,YDC} = \frac{0.1 M_{Y_2O_3}}
      {0.1 M_{Y_2O_3} + 0.8 M_{CeO_2}}

The resulting purchases are 41.612603 kg Y₂O₃, 160.019911 kg ZrO₂ and
98.253863 kg CeO₂ per stack, summing to 299.886377 kg ceramic. The two
obsolete proxy exchanges are replaced by the separately calculated Y₂O₃
contributions; this is not a one-for-one mass substitution. Both the source
Excel inventory and runtime CSV use the same oxide basis. The three rules
share a ``design_group``: missing suppliers, incompatible units, missing
coefficients or unsupported years prevent a partial ceramic update.
Within the support, direct oxides are replaced once, not added on top.

.. list-table:: Contained-metal coefficients, kg metal per nominal MW stack
   :header-rows: 1

   * - Year
     - Y
     - Zr
     - Ce
   * - 2020
     - 32.767592
     - 118.466316
     - 79.987324
   * - 2050, prospective assumption
     - 6.016672
     - 21.752377
     - 14.686997

Source access and interpretation are recorded explicitly. The original
Gerloff supplement Figure 5 and Häfele (2016) supplement Figure S1/Table S2
were inspected. The latter identifies YDC, not GDC, and alternative air
electrodes. The Häfele main-article numerical bill of materials was not
directly accessible. The 0.425/0.262 kg source component masses are
reconstructed from Gerloff's rounded entries and the attributed Häfele
observations in the packaged ``SI_2_Material_requirements.xlsx``. Without
the capacity exponent, the reference chemistry reproduces its separately
attributed Y, Zr and Ce quantities within 0.001 kg/MW. Primary experimental
sources in the JSON support the chemical conventions; they do not validate
this manufacturer's complete stack. Historical workbook observations remain
unchanged and prior runtime rows are retained in the reference JSON.

Future treatment and sensitivities
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The user-selected prospective assumption retains the existing 2050 central
target of 6.016672 kg Y/MW. All three oxide inputs decline linearly together
from 2020 to 2050, maintaining ceramic composition and component ratio.
This assumes an 81.64% reduction in ceramic mass per MW. It is not an
independently demonstrated future design, performance improvement or
durability guarantee. Steel, non-ceramic electrode inputs and manufacturing
energy are not automatically reduced with this package; existing hydrogen
efficiency and lifetime assumptions are retained. YSZ in any separately
omitted fuel-electrode support is not covered by this calculation.

The old 2050 lower and upper limits combined different designs. They are
not retained as independent uncertainty bounds on a matched recipe.
Reference rows are deterministic (mean = median = min = max), with
explicit scenario sensitivities instead:

.. code-block:: console

   python scripts/update_soec_ceramic_reference.py
   python scripts/update_soec_ceramic_reference.py --scenario fixed-baseline --output /tmp/metals-soec-fixed.csv
   python scripts/update_soec_ceramic_reference.py --scenario linear-capacity --output /tmp/metals-soec-linear.csv

``fixed-baseline`` holds the corrected ceramic package constant through
2050. ``linear-capacity`` scales the source component masses linearly to
1 MW, increasing the 2020 package by a factor of 2.290868 relative to the
default. It retains the same absolute 2050 Y target, so its stronger
subsequent reduction is also an assumption. These exports leave runtime
data and the source workbook unchanged. The original sublinear capacity
exponent is retained for continuity, not established as correct physical
area scaling. Use the linear case to assess this material sensitivity.

The workbook always contains the corrected baseline reference. The default
updater reproduces this workbook and the prospective runtime rows. An
alternative CSV alone does not select it in premise. Restart Python and
rebuild scenario databases to apply changed runtime coefficients.

Oxide purchases assume complete incorporation. Production losses,
deposition rejects/recovery and additional ceramic fabrication remain gaps.
Alternative air-electrode recipes, uncertain La/Sr amounts, the glass-sealant
silicone proxy, Ni/YSZ support, steel and whole-stack manufacture/lifetime
require separate review. Existing SOEC specifications allocate one initial
stack plus seven replacements over 3,776,000 kg lifetime H₂ output. Those
eight stacks already multiply each stack's oxide recipe; kg/MW must not be
multiplied by eight again. The correction does not validate all system-level
SOEC metal demand. Primary/secondary shares and saved results are unchanged.

Post-allocation correction
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This correction applies to attributional databases only. Consequential
databases retain their source resource exchanges and substitution credits.
Applying an economic-allocation correction there can both add extraction at
refining and remove resource burdens associated with displaced coproducts.
These operations conflict with the source model's substitution convention; see
ecoinvent's `system-model documentation <https://support.ecoinvent.org/system-models>`_.

Regarding the co-production of metals in multifunctional processes
(i.e., co-mining of metals), *premise* modifies the database to allocate
according to physical mass balance: extraction of individual elements in the
ore is fully attributed to the production of the respective metal; while other
elementary and intermediate flows follow an economic allocation, which is the
default option to deal with multi-functionality in ecoinvent. As discussed in
`Berger <https://doi.org/10.1007/s11367-020-01737-5>`__ et al. (2020), this is a
resource-attribution convention. It does not validate physical recovery yields,
ore grades or the allocation of the remaining inventory.

For example, the amount of platinum resource included in the dataset representing
the mining of 1 kg of platinum is set to 1 kg, while the amount of other
metals (e.g., palladium, rhodium) is set to zero. For metal-bearing
intermediates, such as beryllium hydroxide, chromite ore concentrate, or
titanium dioxide products, the target resource flow is set to the target-metal
content of 1 kg of the reference product instead of being forced to 1 kg. If
no explicit content factor is known for an ore, concentrate, or mineral product,
the original target resource amount in the dataset is retained while the other
co-mined metal resource flows are set to zero. Non-target non-metal resource
flows, including gangue, are preserved.

Stibnite concentrate uses its documented 70 wt% Sb2S3 content, or approximately
0.501807 kg Sb/kg concentrate. This replaces a factor fitted to the legacy
refining recipe. The explicit iron-precipitation recipe requires approximately
1 kg Sb resource/kg output; the legacy lead-smelting proxy requires about
2.012 kg/kg with its retained 4.009 kg concentrate input. The latter is an
inventory/yield limitation, not a reason to reduce the concentrate's Sb content.

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
missing in-ground metal flow is added at 1 kg per kg of reference product only
with primary-route evidence and when it is not already supplied upstream.
Markets are traversed when checking the upstream chain. A missing supplier,
cycle or depth limit is unresolved evidence, not proof that extraction is absent.
Such cases preserve the source and emit a diagnostic. Waste treatment,
recovery and recycling activities never acquire virgin extraction flows through
this correction. Activities with non-kilogram reference units are excluded.
Primary mines retain their own extraction when they consume metal-containing
reagents. For other producers, upstream presence can remove a duplicate direct
resource only when the positive target-bearing feed mass covers the target
amount. This is a conservative screening condition, not a recovery-yield model.

The material-path check uses kilogram inputs and their reference product, using
the supplier name only when a product is missing. Energy and service exchanges
with industry names such as ``electricity, cobalt industry`` are not material
feeds. Carrier suppliers must still resolve uniquely before traversal stops at
their downstream-attribution boundary.

``primary zinc production from concentrate`` has an explicit primary extraction
boundary for the ``zinc`` output fed by ``zinc concentrate``. Its small zinc and
zinc-oxide process inputs can return through the zinc market to the same primary
producers; those loops do not replace extraction from the concentrate. All
positive target-bearing suppliers must resolve uniquely, the concentrate feed
must weigh at least 1 kg per kg output, and the other target-bearing inputs must
total less than 1 kg even assuming pure metal. These are applicability checks
for the existing attribution convention, not inferred grades or yields. The
regional boundaries are planned together before checking downstream consumers,
so one producer's extraction cannot conceal missing attribution at others.
The zinc and zinc-oxide input amounts and their supply-chain burdens are retained.
Cycles without such an explicit boundary remain unresolved.

Generic carrier intermediates whose resource attribution is handled by
downstream metal-specific datasets, such as platinum group metal concentrate,
have their direct metal resource flows cleared to avoid double counting.
Explicit product-content factors are used to resolve otherwise ambiguous
metal-bearing products such as copper-cobalt ore; broader labels such as
lead-zinc concentrates remain ambiguous unless an explicit factor exists for
that product.

Ambiguous or duplicate targets in strict correction calls raise errors. Other
unresolved activity roles and upstream chains produce preservation diagnostics.
Intentional zero resource flows are deterministic and lose their former
uncertainty parameters. Positive corrected flows retain proportionally rescaled
distribution parameters; this propagates source uncertainty, rather than
estimating uncertainty in the attribution convention itself.

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
only. Consequential markets retain their source marginal suppliers: historical
average country shares are not evidence of future marginal supply. If a configured
primary route is unavailable, the entire source market is preserved and the
missing route is recorded; the available routes are not renormalized silently.
Regional proxies retain existing suppliers and use a recorded geographical
fallback when needed. Unresolved or duplicate final suppliers raise an error.

All positive producer shares are retained, including shares below one percent.
Transport includes all available modes and sums production routes within each
country. All providers must be freight services in tonne-kilometres; aircraft
manufacturing markets measured in aircraft units are explicitly excluded.
A primary input of 0.75 kg travelling 1000 weighted km contributes
0.75 tonne-km. Before creating proxies or relinking a market, all positive
primary origins must have transport observations. Otherwise the source market
is preserved, with the missing origins and their fraction of primary supply
recorded. A recorded zero distance is distinguished from absent data. Negative,
non-finite distances and unknown modes are rejected. The market validator checks the applied supplier
shares and retained secondary fraction, not the unused split YAML assumptions.

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

Replacement markets are built only within the years declared in this workbook
(currently 2020–2030). Interpolation uses the actual year labels. Outside that
range, source markets are preserved instead of carrying the 2030 shares
forward implicitly. Zero-share routes do not require a production provider.

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
Unverified EV component overlays are now preserved as described above.
In particular, these fixes do not supply a dynamic scrap-stock model, future
ore-grade or recovery-yield projections, endogenous metal demand, or marginal
supplier projections for consequential modelling. Preserving source data
prevents unsupported overrides; it does not make inherited inventories current
or establish the physical feasibility of a future scenario.

The scenario mapping at ``mapping["metals"]["methodology"]`` records these
limitations and decision counts. ``material decisions``, ``market diagnostics``
and ``resource diagnostics`` give activity-level evidence. Passing numerical
validation does not mean that all configured markets or materials were updated.
For example, a 2050 build now preserves all markets whose share data end in 2030.

Removing the remaining methodological barriers requires independently sourced
inputs: component bills of materials; grades and recovery yields with consistent
allocation boundaries; stocks, lifetimes, collection and recycling losses; and
scenario-specific mining capacities and marginal supplier choices. These need
sensitivity analysis and external validation, not calibration of RELICS scores
to 1 kg/kg. Zinc's explicit attribution boundary remains a convention.

RELICS indicators can compare extraction by element in kg per functional unit
alongside climate change. They should be reported separately by element and
with biosphere-mapping coverage. They do not measure scarcity, supply risk,
installed metal content or the plausibility of a future supply scenario.

The October 2026 correctness audit and its reproducible commands are recorded
in ``results/metals-correctness/README.rst``. It includes paired resource and
climate scores, biosphere coverage, compatibility builds and backend checks.

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
