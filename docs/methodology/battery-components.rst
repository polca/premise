Battery materials and components
================================

Use this page to look up how a particular battery component is represented.
It covers electronics, process materials, pack manufacture and cooling. Each subsection states its own
chemistry and reference quantity; the assumptions do not apply to all batteries.
For scenario updates, see :doc:`battery-mobile` and :doc:`battery-stationary`.
Complete-pack and cell-capacity bases are explained in
:doc:`battery-inventories`.

.. contents:: On this page
   :local:
   :depth: 2

Electronics and controls
------------------------

Battery-control resistor supplies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC dataset pages:** `resistor assembly production, battery monitoring board, 128 resistors <https://clic.psi.ch/datasets/resistor-assembly-production-battery-monitoring-board-128-resistors/>`__.

The three Li-S control-electronics references
use the existing GLO ``market for resistor, surface-mounted`` as a qualified
whole-component manufacturing proxy. The 128-part monitoring-board bundle
purchases **0.00053595 kg**: its **111 nonzero resistors** retain a 0.0004893 kg
mass reference and its **17 zero-ohm jumpers** retain 0.00004665 kg as separate
functional/count provenance. The central-board allowance remains
**0.006122448979591837 kg/kg board**, equivalent to 3 g per 490 g reference;
the standalone interface remains **0.005 kg/kg interface**. The bundle's
part counts are not transferred to those independent mass allowances.

Native whole-resistor supply replaces the custom film, lead-glass and
ruthenium manufacturing chain for these purchases. It does not establish
exact Vishay/Yageo composition, jumper electrical equivalence or complete
Ru/Pd coverage. Native supplier uncertainty remains its own distribution;
equivalence to the former joint uncertainty is not claimed. Other components,
parent purchases, board masses and existing retirement ownership remain
unchanged, with no additional end-of-life purchase or recovery credit.
Existing unused source helpers remain preserved outside these new purchase
paths. Technosphere and biosphere source declarations both stay **3.9**.

Battery-monitor capacitor families
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC dataset pages:** `capacitor assembly production, battery monitoring board, 134 capacitors <https://clic.psi.ch/datasets/capacitor-assembly-production-battery-monitoring-board-134-capacitors/>`__; `capacitor production, c0g, 0805 <https://clic.psi.ch/datasets/capacitor-production-c0g-0805/>`__; `capacitor production, c0g, 0603 <https://clic.psi.ch/datasets/capacitor-production-c0g-0603/>`__; `capacitor production, c0g, 0402 <https://clic.psi.ch/datasets/capacitor-production-c0g-0402/>`__; `capacitor production, flexible, class 2, 0805 <https://clic.psi.ch/datasets/capacitor-production-flexible-class-2-0805/>`__; `capacitor production, class 2, 1206 <https://clic.psi.ch/datasets/capacitor-production-class-2-1206/>`__; `capacitor production, class 2, 0805 <https://clic.psi.ch/datasets/capacitor-production-class-2-0805/>`__; `capacitor production, class 2, 0603 <https://clic.psi.ch/datasets/capacitor-production-class-2-0603/>`__; `capacitor production, class 2, 0402 <https://clic.psi.ch/datasets/capacitor-production-class-2-0402/>`__.

The 134-capacitor bundle contains
51 Class 2 0402, 25 Class 2 0603, five Class 2 0805, six Class 2 1206,
one flexible 0805, 11 C0G 0402, two C0G 0603 and 33 C0G 0805. The two
KEMET 0402 parts retain their inherited 1.06 mg allowances; the other nine
C0G 0402 parts use the 1.2 mg family reference. These allowances do not
establish the exact fitted-part masses or assays.

The capacitor subtotal is **0.98098 g** per 70 g monitoring board.
Unspecified component mass remains in its separate coverage allowance.

The two C0G 0603/0805 helpers retain the qualified source-family compositions,
95% known-feed retention, utilities and mixed-waste treatment assumptions.
Unknown retained fractions and dopants remain unresolved rather than
renormalized into known ingredients. Existing calcium-zirconate production is
reused once. Both source declarations remain **3.9**.

Control-package wafer allocation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC dataset pages:** `semiconductor package production, cc2662r1ftwrgzrq1 <https://clic.psi.ch/datasets/semiconductor-package-production-cc2662r1ftwrgzrq1/>`__; `semiconductor package production, lm5168fqddarq1 <https://clic.psi.ch/datasets/semiconductor-package-production-lm5168fqddarq1/>`__; `semiconductor package production, txu0204qpwrq1 <https://clic.psi.ch/datasets/semiconductor-package-production-txu0204qpwrq1/>`__; `battery monitoring integrated circuit production, copper wire bonding <https://clic.psi.ch/datasets/battery-monitoring-integrated-circuit-production-copper-wire-bonding/>`__.

The CC2662R1FTWRGZRQ1, LM5168FQDDARQ1 and TXU0204QPWRQ1 package references,
and the battery-monitoring ASIC proxy, convert their unchanged silicon masses
to fabricated-wafer area using **270 micrometres** and **2330 kg/m3**:
``area = silicon mass / (2330 * 270e-6)``. The reference thickness is an allocation assumption rather than measured
fitted-die geometry.

Native fabrication and yields are purchased once. Non-wafer inputs, package
counts, parent purchases, board budgets, assembly and retirement remain
unchanged. The monitoring ASIC retains the imperfect BQ79616 material proxy
for the fitted BQ79718 role; this is not a functional replacement claim.
Package chemistry, fused-silica manufacture and unmodelled phosphorus gaps
remain outside this correction. Both original source declarations stay
**3.9**.

Battery-control connector material reference
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC dataset pages:** `connector production, battery control board, plated contacts <https://clic.psi.ch/datasets/connector-production-battery-control-board-plated-contacts/>`__.

The connector recipe uses a qualified material reference based on **five
Molex headers, 36 contacts and 8.594 g**. The additional Samtec geometric
estimate is excluded from this material reference, without asserting that
the header is absent from fitted hardware or that all headers are covered.
The reference is scaled by mass to the unchanged **20 g/module-board** and
**100 g/central-board** allowances.

Plating volumes are subtracted from the finished-contact volume before the
remaining alloy volume is converted to mass. The housing residual then closes
to the fixed reference mass. The thin-layer surface approximation, geometry,
alloy proportions, plating thickness and 70/30 nylon/glass housing remain
engineering assumptions. The **95%** net handling reference is not a measured
plating-deposition efficiency or manufacturing-loss rate.

Nominal rejects remain **0.05263157894736836 kg/kg finished connector**.
Their composition, destination and treatment burdens are unassigned; the
unsupported hazardous-waste incineration purchase is removed. This does not
establish zero physical loss, zero impact or an environmental improvement.
Assembly remains **1 kWh/kg**, with existing board masses, connector doses
and retirement ownership preserved. Both source declarations remain **3.9**.

Additional fitted monitoring-board packages
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC dataset pages:** `semiconductor package production, tps3436bdadaddfrq1 <https://clic.psi.ch/datasets/semiconductor-package-production-tps3436bdadaddfrq1/>`__; `semiconductor package production, tps71533qdckrq1 <https://clic.psi.ch/datasets/semiconductor-package-production-tps71533qdckrq1/>`__; `semiconductor package production, tpd6e004rser <https://clic.psi.ch/datasets/semiconductor-package-production-tpd6e004rser/>`__; `semiconductor package production, iso7741fqdwrq1 <https://clic.psi.ch/datasets/semiconductor-package-production-iso7741fqdwrq1/>`__.

The 70 g monitoring-board reference itemizes one each of TPS3436BDADADDFRQ1,
TPS71533QDCKRQ1, TPD6E004RSER and ISO7741FQDWRQ1. Their combined full declared
package mass is **0.716853378 g**. Deducting that mass from the current
post-capacitor unresolved allowance changes it from **5.913802956 g** to
**5.196949578 g**, preserving the 70 g board, 134 capacitors, 128 resistors,
existing package doses, parent counts, assembly and retirement.

First-variant material declarations are qualified package-material proxies,
not exact fitted-lot assays. The deduction includes chemically unresolved
additives within each full package mass. Known ingredients are not
renormalized to fill those gaps and missing chemical forms are not invented.
The 270 micrometre wafer-allocation reference and single native fabrication
and yield purchase remain in place. The **1 kWh/kg** package-assembly input
is an engineering allowance. The separate CC2662R1FRGZT and two ESD2CAN24DBZR
devices remain unquantified within residual coverage.

Standalone interface and embedded residual boundaries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC dataset pages:** `battery auxiliary interface and power conditioning unit production <https://clic.psi.ch/datasets/battery-auxiliary-interface-and-power-conditioning-unit-production/>`__.

The standalone auxiliary interface replaces **0.24 kg glass fibre plus
0.16 kg epoxy** with **0.12987012987012989 m2** of native unmounted Pb-free
through-hole board per kg interface. This uses a qualified **40% finished-board
fraction** and **3.08 kg/m2** two-layer board proxy. Its existing copper
allowance now denotes external component stock rather than known board
tracks; the actual component/wiring partition remains unverified. Other
standalone component allowances and **2 kWh/kg** assembly remain unchanged.

The already detailed 490 g central and 70 g module control boards no longer
purchase the complete auxiliary-interface recipe for unidentified components.
Their unchanged residual masses instead buy direct copper/ferrite/aluminium/
nylon stock at fractions **0.3461538462 / 0.2884615385 / 0.1923076923 /
0.1730769231**, with **2 kWh/kg residual** for component preparation. This
coverage reference adds no second PCB laminate or already itemized capacitors,
resistors, ICs or connectors. It is not measured residual composition and does
not establish zero missing precious metals or complete fabrication coverage.

VRFB controller materials and moulding
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC dataset pages:** `system controller production, for home storage system <https://clic.psi.ch/datasets/system-controller-production-for-home-storage-system/>`__.

Per kg controller, retained PP housing **0.15210236949152542 kg**, LCD
**0.5750483805084746 kg** and electronics **0.27284925 kg** total **1 kg**.
Using the **0.994** moulding-retention reference once, external
PP feed and injection-moulding service each purchase **0.15302049244620264 kg**.
Native auxiliaries, embedded stock, recorded waste and uncertainty remain
intact; unallocated polymer-residual fate remains unknown.

The inherited **15-inch LCD** is a mass proxy for a **7-inch device**;
dimensional equivalence and complete component manufacture remain unverified.
Suppliers, freight, output and consumer amounts stay unchanged. The parent
**13 kg**, **91% monitoring / 9% BMS** allocation remains an assumption.
Both original source declarations stay **3.9**. Existing profile-specific
supplier mapping and regional PP shares for later targets are preserved.

Central-control fasteners and mass
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC dataset pages:** `printed circuit board production, battery central control, mounted <https://clic.psi.ch/datasets/printed-circuit-board-production-battery-central-control-mounted/>`__.

The central-control material reference uses **2.8 g** fasteners, retaining
**0.49 kg** mounted board, **2.8 kg** HV equipment and **1.2 kg** LV equipment.
Its nominal mass is **4.4928 kg**. This remains a
qualified material reference, not a verified manufacturer BOM, actual circuit
topology or a literal fractional installed-controller count.

Li-O2 keeps its **4 kg** central-control allowance by purchasing
``4 / 4.4928`` reference units, or ``4 / 4.4928 / 333.85`` per kg complete
battery. Its **88.07 kg** auxiliary retirement, final-assembly electricity
and detailed RER cooling input remain unchanged.

The buffer-pack references retain **44.6 kg/car pack**, **238 kg/truck pack**,
the **70%** cell share, controller counts and total retirement. Existing
residual-hardware proportions distribute the freed **25.2 g** per pack,
including corresponding working services. This preserves the existing
engineering allocation; it does not establish measured residual composition.
Indirect ASSB controller counts and the historical ``143 - 6.538`` hardware
partition remain intact; removed fastener mass is not reassigned to ASSB
materials.

Sixteen-module monitoring reference
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC dataset pages:** `printed circuit board production, battery module control, mounted <https://clic.psi.ch/datasets/printed-circuit-board-production-battery-module-control-mounted/>`__; `battery monitoring integrated circuit production, copper wire bonding <https://clic.psi.ch/datasets/battery-monitoring-integrated-circuit-production-copper-wire-bonding/>`__.

The 16-module control assembly adopts **16 monitoring boards at 70 g each**,
purchasing **1.12 kg** of the existing mounted-board recipe. Actual board masses, control topology and the
complete fitted BOM remain qualified engineering assumptions.

Li-O2 module-monitor manufacturing reference
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC dataset pages:** `battery module system production <https://clic.psi.ch/datasets/battery-module-system-production/>`__.

The Li-O2 module-packaging reference purchases the existing GLO mounted
battery-module monitor board recipe in place of the generic mounted-board
market. The quantity remains **0.67 / 19.51 = 0.03434136340338288 kg per kg**
of module hardware. This is a mass-based manufacturing proxy; it does not
establish a verified Li-O2 circuit, manufacturer BOM, topology or integer
board count. The board is a manufacturing proxy for the module control function.

Process materials
-----------------

Magnesium precursor wet-feed water accounting
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC dataset pages:** `magnesium hydroxide production <https://clic.psi.ch/datasets/magnesium-hydroxide-production/>`__.

The active sodium-ion precursor route keeps synthetic magnesium chloride in
solution before hydrated-lime precipitation. Its MgCl2 reference is a solute
equivalent; acid-carrier and reaction water count toward the downstream brine
water requirement. Per kg magnesium hydroxide, **3.0709019328959926 kg** of
acid-carrier water and **0.3251626038638667 kg** of reaction water meet part
of the existing **104.07159280039637 kg** solution-water requirement.
Additional tap water is therefore **100.67552826363651 kg**, or
**0.9516544877931421 kg/kg brine** at the unchanged **105.79 kg** brine dose.
The corresponding reaction-water emission from the MgCl2 helper is removed.

This is a qualified wet-feed assumption, not measured plant operation or
complete industrial water accounting. Reagents, utility allowances, waste
treatment, precursor quantities and filtrate throughput remain unchanged.
The disconnected legacy recipe and separate land-occupation question remain
outside this correction. Both original source declarations stay **3.9**.

Magnesium precipitation land boundary
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The active synthetic magnesium-hydroxide precipitation stage removes its
inherited **1.63 square meter-year/kg** mineral-extraction occupation.
Upstream supplier inventories retain their land burdens. Own-stage
chemical-plant land remains unquantified, not zero; no substitute factory area
or land transformation is added.

The disconnected legacy recipe and its land exchange, the **0.0275 kg**
NMMT precursor dose, wet-feed water accounting, reagents, utilities, treatment
and uncertainty remain unchanged. Both original source declarations stay
**3.9**.

Nickel-on-alumina catalyst preparation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC dataset pages:** `catalyst production, nickel on aluminium oxide, hydrogenation <https://clic.psi.ch/datasets/catalyst-production-nickel-on-aluminium-oxide-hydrogenation/>`__.

The qualified hydrogenation-catalyst reference includes carrier water for
its two assumed **50% nitric-acid and caustic stocks**. The unchanged
**4.131754633808913 kg** dilution-water allowance plus
**1.4205358314581416 kg** carrier water requires **5.552290465267054 kg**
deionised water per kg catalyst. A calculated **0.05738652942776854 kg**
oxygen outlet to generic air completes the nominal manufacturing mass balance,
without an oxygen sale or recovery credit.

Stock concentrations, catalyst loading and retention, utilities and NOx
capture remain engineering assumptions. The oxygen outlet is calculated,
not measured factory emissions. Existing water, nitrogen and NOx outlets,
other chemicals, residue treatment, uncertainty and consumer quantities stay
unchanged. Both original source declarations remain **3.9**.

4-HO-TEMPO peroxide-carrier water
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC dataset pages:** `production of 4-HO-TEMPO <https://clic.psi.ch/datasets/production-of-4-ho-tempo/>`__.

The 4-HO-TEMPO batch counts **0.065 kg carrier water** accompanying its
unchanged **0.065 kg active peroxide**, supplied as **0.130 kg of 50% solution**.
Together with the existing **0.080 kg** initial water, the deionised-water
purchase is **0.145 kg/batch** at the same grade and supplier. Product output
remains **0.108673330176 kg**, and all other exchanges, uncertainty and
consumer quantities remain unchanged.

The added **0.065 kg/batch** retains an unassigned outlet. Evaporation,
recovery, wastewater fate and complete reaction/workup mass closure remain
unresolved. This input correction does not add a new outlet or treatment.
Both original source declarations stay **3.9**.

TMATEMPO oxidation carrier water
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC dataset pages:** `TMATEMPO quantitative production <https://clic.psi.ch/datasets/tmatempo-quantitative-production/>`__.

The TMATEMPO oxidation reference counts the full carrier water for its
documented **30% peroxide feed**. The unchanged **0.407459375 kg** active
peroxide carries **0.9507385416666666 kg** water per kg product. Together
with the existing **1.0027 kg** initial-water allowance, total deionised
water is **1.9534385416666666 kg**, replacing the partial-carrier total
of **1.5459791666666667 kg**.

The extra **0.407459375 kg/kg product** retains an unassigned outlet.
Evaporation, recovery, discharge and complete reaction/workup mass closure
remain unresolved. Output, active peroxide, existing neat-caustic and
initial-water basis, pH adjustment, other chemicals, utilities, waste treatment,
uncertainty and consumer quantities remain unchanged. Both original source
declarations stay **3.9**.

Dry zinc-chloride carrier-water accounting
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC dataset pages:** `zinc chloride (ZnCl2) production, electroactive RFBs <https://clic.psi.ch/datasets/zinc-chloride-zncl2-production-electroactive-rfbs/>`__.

The retained **1.363 kg** nominal dry-salt batch uses **0.72916 kg**
pure-equivalent HCl. Under the adopted **30 wt%** stock-preparation convention,
add **1.7013733333333334 kg** deionised water from Europe without Switzerland:
``0.72916 * (1 / 0.30 - 1)``. Carrier water is purchased explicitly in addition to the dry-salt reagents.

All nine existing exchanges stay unchanged, including **0.0017013733333333333
m3** water to air as liquid-volume-equivalent evaporation accounting,
**0.02 kg** hydrogen, **8.189967723809524 MJ** heat and **0.1363 kWh** electricity.
The dry phase, stock concentration and evaporation/utility allowances remain
engineering assumptions; industrial assay, losses and actual energy demand
are unverified. Preserve the nominal batch rounding without invented closure
or duplicate wastewater. Reagents, freight, output and all consumers remain
unchanged. Both original source declarations remain **3.9**.

Lithium-foil internal manufacturing return
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC dataset pages:** `lithium foil production, for LiS battery <https://clic.psi.ch/datasets/lithium-foil-production-for-lis-battery/>`__.

Per kg finished foil, the adopted source convention purchases **1 kg net
external lithium** from lithium-chloride electrolysis and returns manufacturing
offcuts internally. The separate **0.020408163265306145 kg** external hazardous
waste incineration purchase is removed. Actual losses remain unverified;
this convention does not establish measured zero loss or a recovery credit.

The **4.95 kg** density-adjusted rolling-service coefficient and assumed
**0.5 kWh/reference-kg** intensity are preserved, giving **2.475 kWh**.
The coefficient does not establish measured throughput or pass count.
General electricity **2.1 kWh**, cooling **4.2 MJ**, heat **5 MJ**, output and
all consumers remain unchanged. Additional internal handling, remelting,
atmosphere and lubrication burdens remain unquantified. No duplicate factory
is added. Both original source declarations remain **3.9**.

Pack manufacture and cooling
----------------------------

Flow-battery aluminium end-plate losses
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC dataset pages:** `end plate production, for RFB <https://clic.psi.ch/datasets/end-plate-production-for-rfb/>`__.

The plate reference buys **1 kg** generic wrought alloy,
**1 kg** aluminium rolling service and the unchanged **1 kg** average
working service per kg finished plate. The unsupported extra foreground
95% retention allowance and its **0.05263157894736836 kg** incineration
purchase are removed. Native embedded processing stock, scrap, losses,
treatment and uncertainty remain at their version-specific values.

No fixed native-stock cancellation is applied. The reference's
**0.00298 kg/kg** stock-minus-scrap diagnostic residual remains unallocated;
no new waste, emission or recovery credit is assigned. Alloy-product kilograms
are nominal stock, while service kilograms are process purchases.

Generic alloy and forming remain engineering proxies: the source specifies
**10 mm** plate, whereas native rolling describes **0.2–6 mm** sheet. Geometry
and processing equivalence remain unverified. Plate output, installed parent
quantities and consumers are unchanged. Both original source declarations
stay **3.9**.

Li-S factory allocation per nominal capacity
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC dataset pages:** `battery cell assembly, Li-S <https://clic.psi.ch/datasets/battery-cell-assembly-li-s/>`__; `battery cell production, Li-S <https://clic.psi.ch/datasets/battery-cell-production-li-s/>`__.

The static reference allocates **5e-7 operating-factory units per
nominal kWh**. At the retained **150 Wh/kg cell**, this is **7.5e-8 factory
units/kg cell**: ``5e-7 * 0.15``.

This is a qualified allocation convention; it does not establish measured
industrial productivity or authorize future learning or automatic resizing.
The **47.319 g** ingredient-sum cell basis, specific energy, cell assembly,
formation electricity, factory construction and utility recipe, output and
all consumers remain unchanged. Both original source declarations remain
**3.9**.

Li-O2 final-assembly electricity
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC dataset pages:** `battery production, Li-O2 <https://clic.psi.ch/datasets/battery-production-li-o2/>`__.

Apply **0.41 MJ** of final-assembly electricity once per complete **333.85 kg**
manufacturing reference. The corresponding US medium-voltage electricity
purchase is **0.00034113790291714506 kWh/kg battery**, equivalent within rounding
to ``0.41 / 3.6 / 333.85``. This final-system assembly allowance is separate
from existing cell processing and casing manufacture. It is not a per-cell
allowance or a charge per delivered or lifetime kWh.

The detailed component sum remains an adopted engineering reference; the
source's **321 kg system / 267 kg pack** headline discrepancy remains explicit.
The US grid is the retained source proxy. All eight existing exchanges,
including the zero duplicate-retirement fragment, output, upstream recipes and
consumers stay unchanged. Both original source declarations remain **3.9**.
Cooling and control differences from curated CLIC remain separate; this
addition does not establish complete recipe equivalence or authorize future
learning.

Standalone Li-O2 cooling supplier
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC dataset pages:** `cooling system production, for Li-O2 battery <https://clic.psi.ch/datasets/cooling-system-production-for-li-o2-battery/>`__.

Standalone battery manufacture reuses the existing **RER** `cooling system production, for Li-O2 battery <https://clic.psi.ch/datasets/cooling-system-production-for-li-o2-battery/>`__ reference. It includes radiator, manifolds,
clamps, pipe fitting, thermal pad, glycol and freight. The purchase remains
**0.08707503369776846 kg/kg battery**, representing **29.07 kg** cooling per
**333.85 kg** system; its formula and cached value remain unchanged.

The RER
composition remains an inherited engineering proxy; battery-specific thermal
design and performance are unverified. Its **0.99996 kg** nominal material sum
retains source rounding without invented closure.

Radiator and manifold aluminium feed
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC dataset pages:** `radiator production, for Li-O2 battery <https://clic.psi.ch/datasets/radiator-production-for-li-o2-battery/>`__; `manifolds production, for Li-O2 battery <https://clic.psi.ch/datasets/manifolds-production-for-li-o2-battery/>`__.

The existing RER radiator and manifold references each purchase **1 kg** from
``market for aluminium, wrought alloy`` (GLO), replacing the mixed-metal-scrap
input. The former input represented scrap rather than an established finished
metal supply before forming. The wrought-alloy market is a qualified stock
proxy; actual alloy grade and recycled content remain unverified. No claim of
**100% secondary aluminium** is made.

All stock quantities, outputs, forming, freight, infrastructure, retirement,
other exchanges, formulas and caches remain unchanged. No additional remelting
process, recovery credit or yield correction is added. Native processing stock,
losses and uncertainty remain untouched. Original technosphere and biosphere
declarations both remain **3.9**. This feed correction does not certify the
other inherited thermal, forming or equipment assumptions.

Sixteen-module detailed cooling reference
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC dataset pages:** `battery control and cooling system production, 16 modules <https://clic.psi.ch/datasets/battery-control-and-cooling-system-production-16-modules/>`__.

The 16-module control assembly purchases **10 kg** of the existing detailed RER Li-O2 cooling reference,
including the corrected wrought-alloy radiator and manifold inputs. The GLO
unit cooler remains available for its other consumers. The inherited RER
forming services, regional inputs, freight and infrastructure remain qualified
engineering assumptions; thermal performance, alloy grade and procurement are
not established measurements. The full pad/pipe injection-service convention
is retained without changing native yields or processing coefficients.

Retirement is **15.6128 kg**: **4.4928 kg** central control, **1.12 kg** monitoring
and **10 kg** nominal cooling purchase. Listed cooler materials sum to
**9.9996 kg**; the **0.0004 kg** gap remains documented as source rounding.
No filler, yield correction or recovery credit is introduced. Supplier recipes,
other consumers, module mass, lifetime and capacity purchases are preserved.

Additional battery components
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The three Schmidt-derived multilayer-pouch references purchase **0.5 kg**
aluminium and retain **0.5 kg** aluminium rolling per kilogram of pouch.
Their listed polymers total **0.503 kg**, so the **1.003 kg** material total
retains source rounding. No extra yield, normalization or recycling credit
is inferred. The complete NMC532 battery market purchases **one kilogram**
of complete pack manufacture per kilogram sold; its cell share belongs in
pack manufacture, not in the market a second time. The BMS production
reference product matches its existing parent declaration.

The three all-solid-state pack references retain their existing cell fraction
and electricity purchase while using the shared passive-material
composition. This is a qualified engineering reference, not a measured
chemistry-specific pack bill of materials. No controller mass is reassigned.

Home-storage modules retain their sea and road delivery purchases and disable
the additional unsupported rail purchase. The
`NMC811 home-storage route <https://clic.psi.ch/datasets/home-electricity-storage-system-195-kwh-nmc811-battery/>`__
retains its GLO cell-market reference. Whole-system road delivery follows the
manufactured component budgets: **88.36128 tonne-km** for LFP and
**84.86574428571429 tonne-km** for NMC811. Existing nominal capacities,
160/280 Wh/kg cell references, endpoint-mean replacement convention and
assembly electricity remain unchanged; these are engineering assumptions,
not newly measured geometry or component survival.
