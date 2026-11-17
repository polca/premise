.. Generated from premise/data/clic/commercial_replacements.yaml; do not edit.

This registry contains 57 whole-activity rules and 10 background-supplier rules.

Whole-activity replacements
---------------------------

.. _commercial-mapping-onboard-charger:

onboard-charger
~~~~~~~~~~~~~~~

**CLIC activity names**

* ``charger production, road vehicle, onboard``

:Reference unit: ``kilogram``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One kilogram of native onboard charger per kilogram of requested charger.

Passenger-car charger used for other road-vehicle classes; power rating, design year and material composition can differ. Manufacturing only; does not add use-phase electricity.

Base native quantity: 1 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``charger production, for electric passenger car``; ``charger, electric passenger car``; ``kilogram``
     - ``GLO``
     - medium
     - Inherits the rule's limitations and sources.

**Sources and assumptions**

* Native ecoinvent 3.12 dataset documentation: charger production, for electric passenger car (GLO), functional unit kilogram.
* https://support.ecoinvent.org/licensing

.. _commercial-mapping-auxiliary-dc-converter:

auxiliary-dc-converter
~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``converter production, road vehicle, auxiliary dc``

:Reference unit: ``kilogram``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One kilogram of native automotive converter per kilogram of requested converter.

Native reference is a 4.5 kg device suitable for a 100 kW electric drivetrain; auxiliary converter voltage, rating and design may differ. Mass-normalized manufacturing proxy; no use-phase efficiency is inferred.

Base native quantity: 1 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``converter production, for electric passenger car``; ``converter, for electric passenger car``; ``kilogram``
     - ``GLO``
     - medium
     - Inherits the rule's limitations and sources.

**Sources and assumptions**

* Native ecoinvent 3.12 dataset documentation: converter production, for electric passenger car (GLO), functional unit kilogram.
* https://support.ecoinvent.org/licensing

.. _commercial-mapping-pv-residential-electricity:

pv-residential-electricity
~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``electricity production, photovoltaic, residential``

:Reference unit: ``kilowatt hour``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One native kWh per requested kWh; native installation, lifetime, yield and maintenance inputs are retained.

A conventional single-Si roof generator replaces the reported residential technology mix. Uses the native local generator where available, otherwise RoW then CH including that proxy yield; preserving the requested location for IAM mapping does not establish local solar yield. Advanced module detail and updated national mixes are lost.

Base native quantity: 1 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``electricity production, photovoltaic, 3kWp slanted-roof installation, single-Si, panel, mounted``; ``electricity, low voltage``; ``kilowatt hour``
     - ``same`` → ``RoW`` → ``CH``
     - medium
     - Inherits the rule's limitations and sources.

**Sources and assumptions**

* Native ecoinvent 3.12 inventory and reference-product metadata: electricity production, photovoltaic, 3kWp slanted-roof installation, single-Si, panel, mounted.
* https://ecoinvent.org/ecoinvent-v3-12/
* https://support.ecoinvent.org/hubfs/Comms/2025%20v3.12%20Release/ecoinvent%20Introduction%20to%20version%203.12%20Webinar%20Q%26A.pdf

.. _commercial-mapping-pv-10-kwp-facade-multicrystalline-silicon-laminate-integrated-at-building:

pv-10-kwp-facade-multicrystalline-silicon-laminate-integrated-at-building
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``photovoltaic installation construction, 10 kwp, facade, multicrystalline silicon, laminate, integrated, at building``

:Reference unit: ``unit``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One requested 10 kWp installation = 10/3 = 3.33333333333 native installations. This is a capacity-based engineering proxy, not a measured bill of materials.

All native 3 kWp material, inverter and installation inputs scale linearly with rated capacity. No economies of scale or updated PERC/TOPCon manufacturing are inferred. Native manufacturing/supply geography is retained, with RoW/CH proxies when no local installation is available.

Base native quantity: 3.3333333333333335 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``photovoltaic facade installation, 3kWp, multi-Si, laminated, integrated, at building``; ``photovoltaic facade installation, 3kWp, multi-Si, laminated, integrated, at building``; ``unit``
     - ``same`` → ``RoW`` → ``GLO`` → ``CH``
     - medium
     - Inherits the rule's limitations and sources.

**Additional replacement metadata**

.. code-block:: yaml

   pv technology: multi-Si
   pv capacity kwp: 10.0

**Sources and assumptions**

* Native ecoinvent 3.12 inventory and reference-product metadata: photovoltaic facade installation, 3kWp, multi-Si, laminated, integrated, at building.
* https://ecoinvent.org/ecoinvent-v3-12/
* https://support.ecoinvent.org/hubfs/Comms/2025%20v3.12%20Release/ecoinvent%20Introduction%20to%20version%203.12%20Webinar%20Q%26A.pdf

.. _commercial-mapping-pv-10-kwp-facade-multicrystalline-silicon-panel-mounted-at-building:

pv-10-kwp-facade-multicrystalline-silicon-panel-mounted-at-building
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``photovoltaic installation construction, 10 kwp, facade, multicrystalline silicon, panel, mounted, at building``

:Reference unit: ``unit``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One requested 10 kWp installation = 10/3 = 3.33333333333 native installations. This is a capacity-based engineering proxy, not a measured bill of materials.

All native 3 kWp material, inverter and installation inputs scale linearly with rated capacity. No economies of scale or updated PERC/TOPCon manufacturing are inferred. Native manufacturing/supply geography is retained, with RoW/CH proxies when no local installation is available.

Base native quantity: 3.3333333333333335 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``photovoltaic facade installation, 3kWp, multi-Si, panel, mounted, at building``; ``photovoltaic facade installation, 3kWp, multi-Si, panel, mounted, at building``; ``unit``
     - ``same`` → ``RoW`` → ``GLO`` → ``CH``
     - medium
     - Inherits the rule's limitations and sources.

**Additional replacement metadata**

.. code-block:: yaml

   pv technology: multi-Si
   pv capacity kwp: 10.0

**Sources and assumptions**

* Native ecoinvent 3.12 inventory and reference-product metadata: photovoltaic facade installation, 3kWp, multi-Si, panel, mounted, at building.
* https://ecoinvent.org/ecoinvent-v3-12/
* https://support.ecoinvent.org/hubfs/Comms/2025%20v3.12%20Release/ecoinvent%20Introduction%20to%20version%203.12%20Webinar%20Q%26A.pdf

.. _commercial-mapping-pv-10-kwp-facade-single-crystalline-silicon-laminate-integrated-at-building:

pv-10-kwp-facade-single-crystalline-silicon-laminate-integrated-at-building
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``photovoltaic installation construction, 10 kwp, facade, single-crystalline silicon, laminate, integrated, at building``

:Reference unit: ``unit``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One requested 10 kWp installation = 10/3 = 3.33333333333 native installations. This is a capacity-based engineering proxy, not a measured bill of materials.

All native 3 kWp material, inverter and installation inputs scale linearly with rated capacity. No economies of scale or updated PERC/TOPCon manufacturing are inferred. Native manufacturing/supply geography is retained, with RoW/CH proxies when no local installation is available.

Base native quantity: 3.3333333333333335 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``photovoltaic facade installation, 3kWp, single-Si, laminated, integrated, at building``; ``photovoltaic facade installation, 3kWp, single-Si, laminated, integrated, at building``; ``unit``
     - ``same`` → ``RoW`` → ``GLO`` → ``CH``
     - medium
     - Inherits the rule's limitations and sources.

**Additional replacement metadata**

.. code-block:: yaml

   pv technology: single-Si
   pv capacity kwp: 10.0

**Sources and assumptions**

* Native ecoinvent 3.12 inventory and reference-product metadata: photovoltaic facade installation, 3kWp, single-Si, laminated, integrated, at building.
* https://ecoinvent.org/ecoinvent-v3-12/
* https://support.ecoinvent.org/hubfs/Comms/2025%20v3.12%20Release/ecoinvent%20Introduction%20to%20version%203.12%20Webinar%20Q%26A.pdf

.. _commercial-mapping-pv-10-kwp-facade-single-crystalline-silicon-panel-mounted-at-building:

pv-10-kwp-facade-single-crystalline-silicon-panel-mounted-at-building
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``photovoltaic installation construction, 10 kwp, facade, single-crystalline silicon, panel, mounted, at building``

:Reference unit: ``unit``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One requested 10 kWp installation = 10/3 = 3.33333333333 native installations. This is a capacity-based engineering proxy, not a measured bill of materials.

All native 3 kWp material, inverter and installation inputs scale linearly with rated capacity. No economies of scale or updated PERC/TOPCon manufacturing are inferred. Native manufacturing/supply geography is retained, with RoW/CH proxies when no local installation is available.

Base native quantity: 3.3333333333333335 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``photovoltaic facade installation, 3kWp, single-Si, panel, mounted, at building``; ``photovoltaic facade installation, 3kWp, single-Si, panel, mounted, at building``; ``unit``
     - ``same`` → ``RoW`` → ``GLO`` → ``CH``
     - medium
     - Inherits the rule's limitations and sources.

**Additional replacement metadata**

.. code-block:: yaml

   pv technology: single-Si
   pv capacity kwp: 10.0

**Sources and assumptions**

* Native ecoinvent 3.12 inventory and reference-product metadata: photovoltaic facade installation, 3kWp, single-Si, panel, mounted, at building.
* https://ecoinvent.org/ecoinvent-v3-12/
* https://support.ecoinvent.org/hubfs/Comms/2025%20v3.12%20Release/ecoinvent%20Introduction%20to%20version%203.12%20Webinar%20Q%26A.pdf

.. _commercial-mapping-pv-10-kwp-flat-roof-multicrystalline-silicon-panel-mounted-on-roof:

pv-10-kwp-flat-roof-multicrystalline-silicon-panel-mounted-on-roof
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``photovoltaic installation construction, 10 kwp, flat-roof, multicrystalline silicon, panel, mounted, on roof``

:Reference unit: ``unit``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One requested 10 kWp installation = 10/3 = 3.33333333333 native installations. This is a capacity-based engineering proxy, not a measured bill of materials.

All native 3 kWp material, inverter and installation inputs scale linearly with rated capacity. No economies of scale or updated PERC/TOPCon manufacturing are inferred. Native manufacturing/supply geography is retained, with RoW/CH proxies when no local installation is available.

Base native quantity: 3.3333333333333335 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``photovoltaic flat-roof installation, 3kWp, multi-Si, on roof``; ``photovoltaic flat-roof installation, 3kWp, multi-Si, on roof``; ``unit``
     - ``same`` → ``RoW`` → ``GLO`` → ``CH``
     - medium
     - Inherits the rule's limitations and sources.

**Additional replacement metadata**

.. code-block:: yaml

   pv technology: multi-Si
   pv capacity kwp: 10.0

**Sources and assumptions**

* Native ecoinvent 3.12 inventory and reference-product metadata: photovoltaic flat-roof installation, 3kWp, multi-Si, on roof.
* https://ecoinvent.org/ecoinvent-v3-12/
* https://support.ecoinvent.org/hubfs/Comms/2025%20v3.12%20Release/ecoinvent%20Introduction%20to%20version%203.12%20Webinar%20Q%26A.pdf

.. _commercial-mapping-pv-10-kwp-flat-roof-single-crystalline-silicon-panel-mounted-on-roof:

pv-10-kwp-flat-roof-single-crystalline-silicon-panel-mounted-on-roof
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``photovoltaic installation construction, 10 kwp, flat-roof, single-crystalline silicon, panel, mounted, on roof``

:Reference unit: ``unit``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One requested 10 kWp installation = 10/3 = 3.33333333333 native installations. This is a capacity-based engineering proxy, not a measured bill of materials.

All native 3 kWp material, inverter and installation inputs scale linearly with rated capacity. No economies of scale or updated PERC/TOPCon manufacturing are inferred. Native manufacturing/supply geography is retained, with RoW/CH proxies when no local installation is available.

Base native quantity: 3.3333333333333335 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``photovoltaic flat-roof installation, 3kWp, single-Si, on roof``; ``photovoltaic flat-roof installation, 3kWp, single-Si, on roof``; ``unit``
     - ``same`` → ``RoW`` → ``GLO`` → ``CH``
     - medium
     - Inherits the rule's limitations and sources.

**Additional replacement metadata**

.. code-block:: yaml

   pv technology: single-Si
   pv capacity kwp: 10.0

**Sources and assumptions**

* Native ecoinvent 3.12 inventory and reference-product metadata: photovoltaic flat-roof installation, 3kWp, single-Si, on roof.
* https://ecoinvent.org/ecoinvent-v3-12/
* https://support.ecoinvent.org/hubfs/Comms/2025%20v3.12%20Release/ecoinvent%20Introduction%20to%20version%203.12%20Webinar%20Q%26A.pdf

.. _commercial-mapping-pv-10-kwp-slanted-roof-cadmium-telluride-laminate-integrated-on-roof:

pv-10-kwp-slanted-roof-cadmium-telluride-laminate-integrated-on-roof
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``photovoltaic installation construction, 10 kwp, slanted-roof, cadmium telluride, laminate, integrated, on roof``

:Reference unit: ``unit``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One requested 10 kWp installation = 10/3 = 3.33333333333 native installations. This is a capacity-based engineering proxy, not a measured bill of materials.

All native 3 kWp material, inverter and installation inputs scale linearly with rated capacity. No economies of scale or updated PERC/TOPCon manufacturing are inferred. Native manufacturing/supply geography is retained, with RoW/CH proxies when no local installation is available. The CdTe panel request is represented by the available integrated laminate, so the mounting boundary differs.

Base native quantity: 3.3333333333333335 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``photovoltaic slanted-roof installation, 3kWp, CdTe, laminated, integrated, on roof``; ``photovoltaic slanted-roof installation, 3kWp, CdTe, laminated, integrated, on roof``; ``unit``
     - ``same`` → ``RoW`` → ``GLO`` → ``CH``
     - medium
     - Inherits the rule's limitations and sources.

**Additional replacement metadata**

.. code-block:: yaml

   pv technology: CdTe
   pv capacity kwp: 10.0

**Sources and assumptions**

* Native ecoinvent 3.12 inventory and reference-product metadata: photovoltaic slanted-roof installation, 3kWp, CdTe, laminated, integrated, on roof.
* https://ecoinvent.org/ecoinvent-v3-12/
* https://support.ecoinvent.org/hubfs/Comms/2025%20v3.12%20Release/ecoinvent%20Introduction%20to%20version%203.12%20Webinar%20Q%26A.pdf

.. _commercial-mapping-pv-10-kwp-slanted-roof-cadmium-telluride-panel-mounted-on-roof:

pv-10-kwp-slanted-roof-cadmium-telluride-panel-mounted-on-roof
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``photovoltaic installation construction, 10 kwp, slanted-roof, cadmium telluride, panel, mounted, on roof``

:Reference unit: ``unit``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One requested 10 kWp installation = 10/3 = 3.33333333333 native installations. This is a capacity-based engineering proxy, not a measured bill of materials.

All native 3 kWp material, inverter and installation inputs scale linearly with rated capacity. No economies of scale or updated PERC/TOPCon manufacturing are inferred. Native manufacturing/supply geography is retained, with RoW/CH proxies when no local installation is available. The CdTe panel request is represented by the available integrated laminate, so the mounting boundary differs.

Base native quantity: 3.3333333333333335 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``photovoltaic slanted-roof installation, 3kWp, CdTe, laminated, integrated, on roof``; ``photovoltaic slanted-roof installation, 3kWp, CdTe, laminated, integrated, on roof``; ``unit``
     - ``same`` → ``RoW`` → ``GLO`` → ``CH``
     - medium
     - Inherits the rule's limitations and sources.

**Additional replacement metadata**

.. code-block:: yaml

   pv technology: CdTe
   pv capacity kwp: 10.0

**Sources and assumptions**

* Native ecoinvent 3.12 inventory and reference-product metadata: photovoltaic slanted-roof installation, 3kWp, CdTe, laminated, integrated, on roof.
* https://ecoinvent.org/ecoinvent-v3-12/
* https://support.ecoinvent.org/hubfs/Comms/2025%20v3.12%20Release/ecoinvent%20Introduction%20to%20version%203.12%20Webinar%20Q%26A.pdf

.. _commercial-mapping-pv-10-kwp-slanted-roof-multicrystalline-silicon-laminate-integrated-on-roof:

pv-10-kwp-slanted-roof-multicrystalline-silicon-laminate-integrated-on-roof
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``photovoltaic installation construction, 10 kwp, slanted-roof, multicrystalline silicon, laminate, integrated, on roof``

:Reference unit: ``unit``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One requested 10 kWp installation = 10/3 = 3.33333333333 native installations. This is a capacity-based engineering proxy, not a measured bill of materials.

All native 3 kWp material, inverter and installation inputs scale linearly with rated capacity. No economies of scale or updated PERC/TOPCon manufacturing are inferred. Native manufacturing/supply geography is retained, with RoW/CH proxies when no local installation is available.

Base native quantity: 3.3333333333333335 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``photovoltaic slanted-roof installation, 3kWp, multi-Si, laminated, integrated, on roof``; ``photovoltaic slanted-roof installation, 3kWp, multi-Si, laminated, integrated, on roof``; ``unit``
     - ``same`` → ``RoW`` → ``GLO`` → ``CH``
     - medium
     - Inherits the rule's limitations and sources.

**Additional replacement metadata**

.. code-block:: yaml

   pv technology: multi-Si
   pv capacity kwp: 10.0

**Sources and assumptions**

* Native ecoinvent 3.12 inventory and reference-product metadata: photovoltaic slanted-roof installation, 3kWp, multi-Si, laminated, integrated, on roof.
* https://ecoinvent.org/ecoinvent-v3-12/
* https://support.ecoinvent.org/hubfs/Comms/2025%20v3.12%20Release/ecoinvent%20Introduction%20to%20version%203.12%20Webinar%20Q%26A.pdf

.. _commercial-mapping-pv-10-kwp-slanted-roof-multicrystalline-silicon-panel-mounted-on-roof:

pv-10-kwp-slanted-roof-multicrystalline-silicon-panel-mounted-on-roof
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``photovoltaic installation construction, 10 kwp, slanted-roof, multicrystalline silicon, panel, mounted, on roof``

:Reference unit: ``unit``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One requested 10 kWp installation = 10/3 = 3.33333333333 native installations. This is a capacity-based engineering proxy, not a measured bill of materials.

All native 3 kWp material, inverter and installation inputs scale linearly with rated capacity. No economies of scale or updated PERC/TOPCon manufacturing are inferred. Native manufacturing/supply geography is retained, with RoW/CH proxies when no local installation is available.

Base native quantity: 3.3333333333333335 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``photovoltaic slanted-roof installation, 3kWp, multi-Si, panel, mounted, on roof``; ``photovoltaic slanted-roof installation, 3kWp, multi-Si, panel, mounted, on roof``; ``unit``
     - ``same`` → ``RoW`` → ``GLO`` → ``CH``
     - medium
     - Inherits the rule's limitations and sources.

**Additional replacement metadata**

.. code-block:: yaml

   pv technology: multi-Si
   pv capacity kwp: 10.0

**Sources and assumptions**

* Native ecoinvent 3.12 inventory and reference-product metadata: photovoltaic slanted-roof installation, 3kWp, multi-Si, panel, mounted, on roof.
* https://ecoinvent.org/ecoinvent-v3-12/
* https://support.ecoinvent.org/hubfs/Comms/2025%20v3.12%20Release/ecoinvent%20Introduction%20to%20version%203.12%20Webinar%20Q%26A.pdf

.. _commercial-mapping-pv-10-kwp-slanted-roof-single-crystalline-silicon-laminate-integrated-on-roof:

pv-10-kwp-slanted-roof-single-crystalline-silicon-laminate-integrated-on-roof
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``photovoltaic installation construction, 10 kwp, slanted-roof, single-crystalline silicon, laminate, integrated, on roof``

:Reference unit: ``unit``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One requested 10 kWp installation = 10/3 = 3.33333333333 native installations. This is a capacity-based engineering proxy, not a measured bill of materials.

All native 3 kWp material, inverter and installation inputs scale linearly with rated capacity. No economies of scale or updated PERC/TOPCon manufacturing are inferred. Native manufacturing/supply geography is retained, with RoW/CH proxies when no local installation is available.

Base native quantity: 3.3333333333333335 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``photovoltaic slanted-roof installation, 3kWp, single-Si, laminated, integrated, on roof``; ``photovoltaic slanted-roof installation, 3kWp, single-Si, laminated, integrated, on roof``; ``unit``
     - ``same`` → ``RoW`` → ``GLO`` → ``CH``
     - medium
     - Inherits the rule's limitations and sources.

**Additional replacement metadata**

.. code-block:: yaml

   pv technology: single-Si
   pv capacity kwp: 10.0

**Sources and assumptions**

* Native ecoinvent 3.12 inventory and reference-product metadata: photovoltaic slanted-roof installation, 3kWp, single-Si, laminated, integrated, on roof.
* https://ecoinvent.org/ecoinvent-v3-12/
* https://support.ecoinvent.org/hubfs/Comms/2025%20v3.12%20Release/ecoinvent%20Introduction%20to%20version%203.12%20Webinar%20Q%26A.pdf

.. _commercial-mapping-pv-10-kwp-slanted-roof-single-crystalline-silicon-panel-mounted-on-roof:

pv-10-kwp-slanted-roof-single-crystalline-silicon-panel-mounted-on-roof
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``photovoltaic installation construction, 10 kwp, slanted-roof, single-crystalline silicon, panel, mounted, on roof``

:Reference unit: ``unit``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One requested 10 kWp installation = 10/3 = 3.33333333333 native installations. This is a capacity-based engineering proxy, not a measured bill of materials.

All native 3 kWp material, inverter and installation inputs scale linearly with rated capacity. No economies of scale or updated PERC/TOPCon manufacturing are inferred. Native manufacturing/supply geography is retained, with RoW/CH proxies when no local installation is available.

Base native quantity: 3.3333333333333335 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``photovoltaic slanted-roof installation, 3kWp, single-Si, panel, mounted, on roof``; ``photovoltaic slanted-roof installation, 3kWp, single-Si, panel, mounted, on roof``; ``unit``
     - ``same`` → ``RoW`` → ``GLO`` → ``CH``
     - medium
     - Inherits the rule's limitations and sources.

**Additional replacement metadata**

.. code-block:: yaml

   pv technology: single-Si
   pv capacity kwp: 10.0

**Sources and assumptions**

* Native ecoinvent 3.12 inventory and reference-product metadata: photovoltaic slanted-roof installation, 3kWp, single-Si, panel, mounted, on roof.
* https://ecoinvent.org/ecoinvent-v3-12/
* https://support.ecoinvent.org/hubfs/Comms/2025%20v3.12%20Release/ecoinvent%20Introduction%20to%20version%203.12%20Webinar%20Q%26A.pdf

.. _commercial-mapping-pv-10-mw-ground-mounted-cadmium-telluride-panel-on-open-ground:

pv-10-mw-ground-mounted-cadmium-telluride-panel-on-open-ground
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``photovoltaic installation construction, 10 mw, ground-mounted, cadmium telluride, panel, on open ground``

:Reference unit: ``unit``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: low
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One requested 10000 kWp installation = 10000/570 = 17.5438596491 native installations. This is a capacity-based engineering proxy, not a measured bill of materials.

Linear installed-capacity scaling of the native 570 kWp multi-Si ground plant, including land and balance of system. Economies of scale and modern plant layouts are not resolved; single-Si and CdTe requests use a multi-Si technology proxy.

Base native quantity: 17.54385964912281 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``photovoltaic plant construction, 570kWp, multi-Si, on open ground``; ``photovoltaic plant, 570kWp, multi-Si, on open ground``; ``unit``
     - ``GLO``
     - low
     - Inherits the rule's limitations and sources.

**Additional replacement metadata**

.. code-block:: yaml

   pv technology: multi-Si
   pv capacity kwp: 10000.0

**Sources and assumptions**

* Native ecoinvent 3.12 inventory and reference-product metadata: photovoltaic plant construction, 570kWp, multi-Si, on open ground.
* https://ecoinvent.org/ecoinvent-v3-12/
* https://support.ecoinvent.org/hubfs/Comms/2025%20v3.12%20Release/ecoinvent%20Introduction%20to%20version%203.12%20Webinar%20Q%26A.pdf

.. _commercial-mapping-pv-10-mw-ground-mounted-multicrystalline-silicon-panel-on-open-ground:

pv-10-mw-ground-mounted-multicrystalline-silicon-panel-on-open-ground
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``photovoltaic installation construction, 10 mw, ground-mounted, multicrystalline silicon, panel, on open ground``

:Reference unit: ``unit``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One requested 10000 kWp installation = 10000/570 = 17.5438596491 native installations. This is a capacity-based engineering proxy, not a measured bill of materials.

Linear installed-capacity scaling of the native 570 kWp multi-Si ground plant, including land and balance of system. Economies of scale and modern plant layouts are not resolved; single-Si and CdTe requests use a multi-Si technology proxy.

Base native quantity: 17.54385964912281 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``photovoltaic plant construction, 570kWp, multi-Si, on open ground``; ``photovoltaic plant, 570kWp, multi-Si, on open ground``; ``unit``
     - ``GLO``
     - medium
     - Inherits the rule's limitations and sources.

**Additional replacement metadata**

.. code-block:: yaml

   pv technology: multi-Si
   pv capacity kwp: 10000.0

**Sources and assumptions**

* Native ecoinvent 3.12 inventory and reference-product metadata: photovoltaic plant construction, 570kWp, multi-Si, on open ground.
* https://ecoinvent.org/ecoinvent-v3-12/
* https://support.ecoinvent.org/hubfs/Comms/2025%20v3.12%20Release/ecoinvent%20Introduction%20to%20version%203.12%20Webinar%20Q%26A.pdf

.. _commercial-mapping-pv-10-mw-ground-mounted-single-crystalline-silicon-panel-on-open-ground:

pv-10-mw-ground-mounted-single-crystalline-silicon-panel-on-open-ground
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``photovoltaic installation construction, 10 mw, ground-mounted, single-crystalline silicon, panel, on open ground``

:Reference unit: ``unit``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One requested 10000 kWp installation = 10000/570 = 17.5438596491 native installations. This is a capacity-based engineering proxy, not a measured bill of materials.

Linear installed-capacity scaling of the native 570 kWp multi-Si ground plant, including land and balance of system. Economies of scale and modern plant layouts are not resolved; single-Si and CdTe requests use a multi-Si technology proxy.

Base native quantity: 17.54385964912281 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``photovoltaic plant construction, 570kWp, multi-Si, on open ground``; ``photovoltaic plant, 570kWp, multi-Si, on open ground``; ``unit``
     - ``GLO``
     - medium
     - Inherits the rule's limitations and sources.

**Additional replacement metadata**

.. code-block:: yaml

   pv technology: multi-Si
   pv capacity kwp: 10000.0

**Sources and assumptions**

* Native ecoinvent 3.12 inventory and reference-product metadata: photovoltaic plant construction, 570kWp, multi-Si, on open ground.
* https://ecoinvent.org/ecoinvent-v3-12/
* https://support.ecoinvent.org/hubfs/Comms/2025%20v3.12%20Release/ecoinvent%20Introduction%20to%20version%203.12%20Webinar%20Q%26A.pdf

.. _commercial-mapping-pv-250-kwp-flat-roof-multicrystalline-silicon-panel-mounted-on-roof:

pv-250-kwp-flat-roof-multicrystalline-silicon-panel-mounted-on-roof
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``photovoltaic installation construction, 250 kwp, flat-roof, multicrystalline silicon, panel, mounted, on roof``

:Reference unit: ``unit``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One requested 250 kWp installation = 250/3 = 83.3333333333 native installations. This is a capacity-based engineering proxy, not a measured bill of materials.

All native 3 kWp material, inverter and installation inputs scale linearly with rated capacity. No economies of scale or updated PERC/TOPCon manufacturing are inferred. Native manufacturing/supply geography is retained, with RoW/CH proxies when no local installation is available.

Base native quantity: 83.33333333333333 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``photovoltaic flat-roof installation, 3kWp, multi-Si, on roof``; ``photovoltaic flat-roof installation, 3kWp, multi-Si, on roof``; ``unit``
     - ``same`` → ``RoW`` → ``GLO`` → ``CH``
     - medium
     - Inherits the rule's limitations and sources.

**Additional replacement metadata**

.. code-block:: yaml

   pv technology: multi-Si
   pv capacity kwp: 250.0

**Sources and assumptions**

* Native ecoinvent 3.12 inventory and reference-product metadata: photovoltaic flat-roof installation, 3kWp, multi-Si, on roof.
* https://ecoinvent.org/ecoinvent-v3-12/
* https://support.ecoinvent.org/hubfs/Comms/2025%20v3.12%20Release/ecoinvent%20Introduction%20to%20version%203.12%20Webinar%20Q%26A.pdf

.. _commercial-mapping-pv-250-kwp-flat-roof-single-crystalline-silicon-panel-mounted-on-roof:

pv-250-kwp-flat-roof-single-crystalline-silicon-panel-mounted-on-roof
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``photovoltaic installation construction, 250 kwp, flat-roof, single-crystalline silicon, panel, mounted, on roof``

:Reference unit: ``unit``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One requested 250 kWp installation = 250/3 = 83.3333333333 native installations. This is a capacity-based engineering proxy, not a measured bill of materials.

All native 3 kWp material, inverter and installation inputs scale linearly with rated capacity. No economies of scale or updated PERC/TOPCon manufacturing are inferred. Native manufacturing/supply geography is retained, with RoW/CH proxies when no local installation is available.

Base native quantity: 83.33333333333333 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``photovoltaic flat-roof installation, 3kWp, single-Si, on roof``; ``photovoltaic flat-roof installation, 3kWp, single-Si, on roof``; ``unit``
     - ``same`` → ``RoW`` → ``GLO`` → ``CH``
     - medium
     - Inherits the rule's limitations and sources.

**Additional replacement metadata**

.. code-block:: yaml

   pv technology: single-Si
   pv capacity kwp: 250.0

**Sources and assumptions**

* Native ecoinvent 3.12 inventory and reference-product metadata: photovoltaic flat-roof installation, 3kWp, single-Si, on roof.
* https://ecoinvent.org/ecoinvent-v3-12/
* https://support.ecoinvent.org/hubfs/Comms/2025%20v3.12%20Release/ecoinvent%20Introduction%20to%20version%203.12%20Webinar%20Q%26A.pdf

.. _commercial-mapping-pv-250-kwp-slanted-roof-multicrystalline-silicon-laminate-integrated-on-roof:

pv-250-kwp-slanted-roof-multicrystalline-silicon-laminate-integrated-on-roof
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``photovoltaic installation construction, 250 kwp, slanted-roof, multicrystalline silicon, laminate, integrated, on roof``

:Reference unit: ``unit``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One requested 250 kWp installation = 250/3 = 83.3333333333 native installations. This is a capacity-based engineering proxy, not a measured bill of materials.

All native 3 kWp material, inverter and installation inputs scale linearly with rated capacity. No economies of scale or updated PERC/TOPCon manufacturing are inferred. Native manufacturing/supply geography is retained, with RoW/CH proxies when no local installation is available.

Base native quantity: 83.33333333333333 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``photovoltaic slanted-roof installation, 3kWp, multi-Si, laminated, integrated, on roof``; ``photovoltaic slanted-roof installation, 3kWp, multi-Si, laminated, integrated, on roof``; ``unit``
     - ``same`` → ``RoW`` → ``GLO`` → ``CH``
     - medium
     - Inherits the rule's limitations and sources.

**Additional replacement metadata**

.. code-block:: yaml

   pv technology: multi-Si
   pv capacity kwp: 250.0

**Sources and assumptions**

* Native ecoinvent 3.12 inventory and reference-product metadata: photovoltaic slanted-roof installation, 3kWp, multi-Si, laminated, integrated, on roof.
* https://ecoinvent.org/ecoinvent-v3-12/
* https://support.ecoinvent.org/hubfs/Comms/2025%20v3.12%20Release/ecoinvent%20Introduction%20to%20version%203.12%20Webinar%20Q%26A.pdf

.. _commercial-mapping-pv-250-kwp-slanted-roof-multicrystalline-silicon-panel-mounted-on-roof:

pv-250-kwp-slanted-roof-multicrystalline-silicon-panel-mounted-on-roof
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``photovoltaic installation construction, 250 kwp, slanted-roof, multicrystalline silicon, panel, mounted, on roof``

:Reference unit: ``unit``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One requested 250 kWp installation = 250/3 = 83.3333333333 native installations. This is a capacity-based engineering proxy, not a measured bill of materials.

All native 3 kWp material, inverter and installation inputs scale linearly with rated capacity. No economies of scale or updated PERC/TOPCon manufacturing are inferred. Native manufacturing/supply geography is retained, with RoW/CH proxies when no local installation is available.

Base native quantity: 83.33333333333333 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``photovoltaic slanted-roof installation, 3kWp, multi-Si, panel, mounted, on roof``; ``photovoltaic slanted-roof installation, 3kWp, multi-Si, panel, mounted, on roof``; ``unit``
     - ``same`` → ``RoW`` → ``GLO`` → ``CH``
     - medium
     - Inherits the rule's limitations and sources.

**Additional replacement metadata**

.. code-block:: yaml

   pv technology: multi-Si
   pv capacity kwp: 250.0

**Sources and assumptions**

* Native ecoinvent 3.12 inventory and reference-product metadata: photovoltaic slanted-roof installation, 3kWp, multi-Si, panel, mounted, on roof.
* https://ecoinvent.org/ecoinvent-v3-12/
* https://support.ecoinvent.org/hubfs/Comms/2025%20v3.12%20Release/ecoinvent%20Introduction%20to%20version%203.12%20Webinar%20Q%26A.pdf

.. _commercial-mapping-pv-250-kwp-slanted-roof-single-crystalline-silicon-laminate-integrated-on-roof:

pv-250-kwp-slanted-roof-single-crystalline-silicon-laminate-integrated-on-roof
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``photovoltaic installation construction, 250 kwp, slanted-roof, single-crystalline silicon, laminate, integrated, on roof``

:Reference unit: ``unit``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One requested 250 kWp installation = 250/3 = 83.3333333333 native installations. This is a capacity-based engineering proxy, not a measured bill of materials.

All native 3 kWp material, inverter and installation inputs scale linearly with rated capacity. No economies of scale or updated PERC/TOPCon manufacturing are inferred. Native manufacturing/supply geography is retained, with RoW/CH proxies when no local installation is available.

Base native quantity: 83.33333333333333 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``photovoltaic slanted-roof installation, 3kWp, single-Si, laminated, integrated, on roof``; ``photovoltaic slanted-roof installation, 3kWp, single-Si, laminated, integrated, on roof``; ``unit``
     - ``same`` → ``RoW`` → ``GLO`` → ``CH``
     - medium
     - Inherits the rule's limitations and sources.

**Additional replacement metadata**

.. code-block:: yaml

   pv technology: single-Si
   pv capacity kwp: 250.0

**Sources and assumptions**

* Native ecoinvent 3.12 inventory and reference-product metadata: photovoltaic slanted-roof installation, 3kWp, single-Si, laminated, integrated, on roof.
* https://ecoinvent.org/ecoinvent-v3-12/
* https://support.ecoinvent.org/hubfs/Comms/2025%20v3.12%20Release/ecoinvent%20Introduction%20to%20version%203.12%20Webinar%20Q%26A.pdf

.. _commercial-mapping-pv-250-kwp-slanted-roof-single-crystalline-silicon-panel-mounted-on-roof:

pv-250-kwp-slanted-roof-single-crystalline-silicon-panel-mounted-on-roof
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``photovoltaic installation construction, 250 kwp, slanted-roof, single-crystalline silicon, panel, mounted, on roof``

:Reference unit: ``unit``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One requested 250 kWp installation = 250/3 = 83.3333333333 native installations. This is a capacity-based engineering proxy, not a measured bill of materials.

All native 3 kWp material, inverter and installation inputs scale linearly with rated capacity. No economies of scale or updated PERC/TOPCon manufacturing are inferred. Native manufacturing/supply geography is retained, with RoW/CH proxies when no local installation is available.

Base native quantity: 83.33333333333333 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``photovoltaic slanted-roof installation, 3kWp, single-Si, panel, mounted, on roof``; ``photovoltaic slanted-roof installation, 3kWp, single-Si, panel, mounted, on roof``; ``unit``
     - ``same`` → ``RoW`` → ``GLO`` → ``CH``
     - medium
     - Inherits the rule's limitations and sources.

**Additional replacement metadata**

.. code-block:: yaml

   pv technology: single-Si
   pv capacity kwp: 250.0

**Sources and assumptions**

* Native ecoinvent 3.12 inventory and reference-product metadata: photovoltaic slanted-roof installation, 3kWp, single-Si, panel, mounted, on roof.
* https://ecoinvent.org/ecoinvent-v3-12/
* https://support.ecoinvent.org/hubfs/Comms/2025%20v3.12%20Release/ecoinvent%20Introduction%20to%20version%203.12%20Webinar%20Q%26A.pdf

.. _commercial-mapping-battery-cell-lfp:

battery-cell-lfp
~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``battery cell production, Li-ion, LFP``

:Reference unit: ``kilogram``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

1 native kilogram per requested kilogram; manufacturing geography remains that of the selected native supplier.

Native cell manufacture per kg replaces the source-specific formulation, manufacturing energy and geography; any separate capacity-to-mass conversion remains unchanged.

Base native quantity: 1 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``battery cell production, Li-ion, LFP``; ``battery cell, Li-ion, LFP``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - medium
     - Inherits the rule's limitations and sources.
   * - 2
     - ``battery cell production, Li-ion``; ``battery cell, Li-ion``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - low
     - The older release has no requested chemistry; use legacy generic Li-ion (LMO) cell manufacture per kg. Electrodes, energy use and material burdens differ; retain the consumer's own density and cycle-life assumptions. Native ecoinvent 3.8, 3.9.1 and 3.10 background inventories and reference-unit metadata; selected release and supplier are recorded at runtime.

**Sources and assumptions**

* Native ecoinvent 3.12 dataset documentation and reference-unit inventory: battery cell production, Li-ion, LFP.
* https://ecoinvent.org/ecoinvent-v3-12/

.. _commercial-mapping-battery-cell-market-lfp:

battery-cell-market-lfp
~~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``market for battery cell, Li-ion, LFP``
* ``market for battery cell, Li-ion, LFP, curated supply``

:Reference unit: ``kilogram``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

1 native kilogram per requested kilogram; manufacturing geography remains that of the selected native supplier.

Native cell manufacture per kg replaces the source-specific formulation, manufacturing energy and geography; any separate capacity-to-mass conversion remains unchanged. Uses the native market supply mix rather than the curated source mix.

Base native quantity: 1 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``market for battery cell, Li-ion, LFP``; ``battery cell, Li-ion, LFP``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - medium
     - Inherits the rule's limitations and sources.
   * - 2
     - ``market for battery cell, Li-ion``; ``battery cell, Li-ion``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - low
     - The older release has no requested chemistry; use legacy generic Li-ion (LMO) cell manufacture per kg. Electrodes, energy use and material burdens differ; retain the consumer's own density and cycle-life assumptions. Uses the available native market mix. Native ecoinvent 3.8, 3.9.1 and 3.10 background inventories and reference-unit metadata; selected release and supplier are recorded at runtime.

**Sources and assumptions**

* Native ecoinvent 3.12 dataset documentation and reference-unit inventory: market for battery cell, Li-ion, LFP.
* https://ecoinvent.org/ecoinvent-v3-12/

.. _commercial-mapping-battery-pack-lfp:

battery-pack-lfp
~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``battery pack production, lithium-ion, lfp, without electronics``

:Reference unit: ``kilogram``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

Remove native BMS purchases or direct electronic-board inputs as present in that release, and renormalize all remaining inputs to one kg of pack excluding those electronics; actual removed mass and multiplier are reported.

Native cell manufacture per kg replaces the source-specific formulation, manufacturing energy and geography; any separate capacity-to-mass conversion remains unchanged. The native BMS purchase is removed, and all remaining native inputs are divided by one minus its kg share. Separate control-unit purchases in the consuming model are retained. Pack architecture and thermal design are represented by the native reference.

**Native inputs excluded before scaling**

* ``market for battery management system, for Li-ion battery``
* ``battery management system production, for Li-ion battery``
* ``market for printed wiring board, surface mounted, unspecified, Pb containing``
* ``market for printed wiring board, surface mounted, unspecified, Pb free``

Remaining native inputs are renormalized to a kilogram excluding the removed electronics.

Base native quantity: 1 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``battery production, Li-ion, LFP, rechargeable``; ``battery, Li-ion, LFP, rechargeable``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - medium
     - Inherits the rule's limitations and sources.
   * - 2
     - ``battery production, Li-ion, LFP, rechargeable, prismatic``; ``battery, Li-ion, LFP, rechargeable, prismatic``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - medium
     - Older prismatic-pack name for native LFP manufacture; the manufacturing process and reference kg, rather than the dataset spelling, define the replacement. Native ecoinvent 3.8, 3.9.1 and 3.10 background inventories and reference-unit metadata; selected release and supplier are recorded at runtime.
   * - 3
     - ``battery production, Li-ion, rechargeable, prismatic``; ``battery, Li-ion, rechargeable, prismatic``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - low
     - The pre-3.9 generic Li-ion pack is an LMO manufacturing proxy. LFP requests therefore lose chemistry-specific manufacturing detail; electronic boards are removed and remaining mass is renormalized. Native ecoinvent 3.8, 3.9.1 and 3.10 background inventories and reference-unit metadata; selected release and supplier are recorded at runtime.

**Sources and assumptions**

* Native ecoinvent 3.12 dataset documentation and reference-unit inventory: battery production, Li-ion, LFP, rechargeable.
* https://ecoinvent.org/ecoinvent-v3-12/

.. _commercial-mapping-battery-cell-limn2o4:

battery-cell-limn2o4
~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``battery cell production, Li-ion, LiMn2O4``

:Reference unit: ``kilogram``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

1 native kilogram per requested kilogram; manufacturing geography remains that of the selected native supplier.

Native cell manufacture per kg replaces the source-specific formulation, manufacturing energy and geography; any separate capacity-to-mass conversion remains unchanged.

Base native quantity: 1 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``battery cell production, Li-ion, LiMn2O4``; ``battery cell, Li-ion, LiMn2O4``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - medium
     - Inherits the rule's limitations and sources.
   * - 2
     - ``battery cell production, Li-ion``; ``battery cell, Li-ion``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - medium
     - Legacy generic Li-ion cell manufacture is the older LMO reference; source documentation contains inherited wording, so identity and purchasing structure were checked together. Native ecoinvent 3.8, 3.9.1 and 3.10 background inventories and reference-unit metadata; selected release and supplier are recorded at runtime.

**Sources and assumptions**

* Native ecoinvent 3.12 dataset documentation and reference-unit inventory: battery cell production, Li-ion, LiMn2O4.
* https://ecoinvent.org/ecoinvent-v3-12/

.. _commercial-mapping-battery-pack-limn2o4:

battery-pack-limn2o4
~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``battery pack production, lithium-ion, limn2o4, without electronics``

:Reference unit: ``kilogram``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

Remove native BMS purchases or direct electronic-board inputs as present in that release, and renormalize all remaining inputs to one kg of pack excluding those electronics; actual removed mass and multiplier are reported.

Native cell manufacture per kg replaces the source-specific formulation, manufacturing energy and geography; any separate capacity-to-mass conversion remains unchanged. The native BMS purchase is removed, and all remaining native inputs are divided by one minus its kg share. Separate control-unit purchases in the consuming model are retained. Pack architecture and thermal design are represented by the native reference.

**Native inputs excluded before scaling**

* ``market for battery management system, for Li-ion battery``
* ``battery management system production, for Li-ion battery``
* ``market for printed wiring board, surface mounted, unspecified, Pb containing``
* ``market for printed wiring board, surface mounted, unspecified, Pb free``

Remaining native inputs are renormalized to a kilogram excluding the removed electronics.

Base native quantity: 1 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``battery production, Li-ion, LiMn2O4, rechargeable``; ``battery, Li-ion, LiMn2O4, rechargeable``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - medium
     - Inherits the rule's limitations and sources.
   * - 2
     - ``battery production, Li-ion, LiMn2O4, rechargeable, prismatic``; ``battery, Li-ion, LiMn2O4, rechargeable, prismatic``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - medium
     - Older prismatic-pack name for native LiMn2O4 manufacture; the manufacturing process and reference kg, rather than the dataset spelling, define the replacement. Native ecoinvent 3.8, 3.9.1 and 3.10 background inventories and reference-unit metadata; selected release and supplier are recorded at runtime.
   * - 3
     - ``battery production, Li-ion, rechargeable, prismatic``; ``battery, Li-ion, rechargeable, prismatic``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - medium
     - The pre-3.9 generic Li-ion pack is an LMO manufacturing proxy. LFP requests therefore lose chemistry-specific manufacturing detail; electronic boards are removed and remaining mass is renormalized. Native ecoinvent 3.8, 3.9.1 and 3.10 background inventories and reference-unit metadata; selected release and supplier are recorded at runtime.

**Sources and assumptions**

* Native ecoinvent 3.12 dataset documentation and reference-unit inventory: battery production, Li-ion, LiMn2O4, rechargeable.
* https://ecoinvent.org/ecoinvent-v3-12/

.. _commercial-mapping-battery-cell-nca:

battery-cell-nca
~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``battery cell production, Li-ion, NCA``

:Reference unit: ``kilogram``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

1 native kilogram per requested kilogram; manufacturing geography remains that of the selected native supplier.

Native cell manufacture per kg replaces the source-specific formulation, manufacturing energy and geography; any separate capacity-to-mass conversion remains unchanged.

Base native quantity: 1 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``battery cell production, Li-ion, NCA``; ``battery cell, Li-ion, NCA``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - medium
     - Inherits the rule's limitations and sources.

**Sources and assumptions**

* Native ecoinvent 3.12 dataset documentation and reference-unit inventory: battery cell production, Li-ion, NCA.
* https://ecoinvent.org/ecoinvent-v3-12/

.. _commercial-mapping-battery-pack-nca:

battery-pack-nca
~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``battery pack production, lithium-ion, nca, retained existing cell, without electronics``

:Reference unit: ``kilogram``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

Remove native BMS purchases or direct electronic-board inputs as present in that release, and renormalize all remaining inputs to one kg of pack excluding those electronics; actual removed mass and multiplier are reported.

Native cell manufacture per kg replaces the source-specific formulation, manufacturing energy and geography; any separate capacity-to-mass conversion remains unchanged. The native BMS purchase is removed, and all remaining native inputs are divided by one minus its kg share. Separate control-unit purchases in the consuming model are retained. Pack architecture and thermal design are represented by the native reference.

**Native inputs excluded before scaling**

* ``market for battery management system, for Li-ion battery``
* ``battery management system production, for Li-ion battery``
* ``market for printed wiring board, surface mounted, unspecified, Pb containing``
* ``market for printed wiring board, surface mounted, unspecified, Pb free``

Remaining native inputs are renormalized to a kilogram excluding the removed electronics.

Base native quantity: 1 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``battery production, Li-ion, NCA, rechargeable``; ``battery, Li-ion, NCA, rechargeable``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - medium
     - Inherits the rule's limitations and sources.
   * - 2
     - ``battery production, Li-ion, NCA, rechargeable, prismatic``; ``battery, Li-ion, NCA, rechargeable, prismatic``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - medium
     - Older prismatic-pack name for native NCA manufacture; the manufacturing process and reference kg, rather than the dataset spelling, define the replacement. Native ecoinvent 3.8, 3.9.1 and 3.10 background inventories and reference-unit metadata; selected release and supplier are recorded at runtime.
   * - 3
     - ``battery production, NCA, Li-ion, rechargeable, prismatic``; ``battery, Li-ion, NCA, rechargeable, prismatic``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - medium
     - Older prismatic-pack name for native NCA manufacture; the manufacturing process and reference kg, rather than the dataset spelling, define the replacement. This naming order is used in ecoinvent 3.8. Native ecoinvent 3.8, 3.9.1 and 3.10 background inventories and reference-unit metadata; selected release and supplier are recorded at runtime.

**Sources and assumptions**

* Native ecoinvent 3.12 dataset documentation and reference-unit inventory: battery production, Li-ion, NCA, rechargeable.
* https://ecoinvent.org/ecoinvent-v3-12/

.. _commercial-mapping-battery-cell-nmc111:

battery-cell-nmc111
~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``battery cell production, Li-ion, NMC111``

:Reference unit: ``kilogram``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

1 native kilogram per requested kilogram; manufacturing geography remains that of the selected native supplier.

Native cell manufacture per kg replaces the source-specific formulation, manufacturing energy and geography; any separate capacity-to-mass conversion remains unchanged.

Base native quantity: 1 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``battery cell production, Li-ion, NMC111``; ``battery cell, Li-ion, NMC111``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - medium
     - Inherits the rule's limitations and sources.

**Sources and assumptions**

* Native ecoinvent 3.12 dataset documentation and reference-unit inventory: battery cell production, Li-ion, NMC111.
* https://ecoinvent.org/ecoinvent-v3-12/

.. _commercial-mapping-battery-pack-nmc111:

battery-pack-nmc111
~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``battery pack production, lithium-ion, nmc111, retained existing cell, without electronics``

:Reference unit: ``kilogram``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

Remove native BMS purchases or direct electronic-board inputs as present in that release, and renormalize all remaining inputs to one kg of pack excluding those electronics; actual removed mass and multiplier are reported.

Native cell manufacture per kg replaces the source-specific formulation, manufacturing energy and geography; any separate capacity-to-mass conversion remains unchanged. The native BMS purchase is removed, and all remaining native inputs are divided by one minus its kg share. Separate control-unit purchases in the consuming model are retained. Pack architecture and thermal design are represented by the native reference.

**Native inputs excluded before scaling**

* ``market for battery management system, for Li-ion battery``
* ``battery management system production, for Li-ion battery``
* ``market for printed wiring board, surface mounted, unspecified, Pb containing``
* ``market for printed wiring board, surface mounted, unspecified, Pb free``

Remaining native inputs are renormalized to a kilogram excluding the removed electronics.

Base native quantity: 1 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``battery production, Li-ion, NMC111, rechargeable``; ``battery, Li-ion, NMC111, rechargeable``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - medium
     - Inherits the rule's limitations and sources.
   * - 2
     - ``battery production, Li-ion, NMC111, rechargeable, prismatic``; ``battery, Li-ion, NMC111, rechargeable, prismatic``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - medium
     - Older prismatic-pack name for native NMC111 manufacture; the manufacturing process and reference kg, rather than the dataset spelling, define the replacement. Native ecoinvent 3.8, 3.9.1 and 3.10 background inventories and reference-unit metadata; selected release and supplier are recorded at runtime.

**Sources and assumptions**

* Native ecoinvent 3.12 dataset documentation and reference-unit inventory: battery production, Li-ion, NMC111, rechargeable.
* https://ecoinvent.org/ecoinvent-v3-12/

.. _commercial-mapping-battery-cell-nmc532:

battery-cell-nmc532
~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``battery cell production, Li-ion, NMC532``

:Reference unit: ``kilogram``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

1 native kilogram per requested kilogram; manufacturing geography remains that of the selected native supplier.

Native cell manufacture per kg replaces the source-specific formulation, manufacturing energy and geography; any separate capacity-to-mass conversion remains unchanged.

Base native quantity: 1 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``battery cell production, Li-ion, NMC532``; ``battery cell, Li-ion, NMC532``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - medium
     - Inherits the rule's limitations and sources.
   * - 2
     - ``battery cell production, Li-ion, NMC111``; ``battery cell, Li-ion, NMC111``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - low
     - The older release has no requested chemistry; use NMC111 cell manufacture per kg. Electrodes, energy use and material burdens differ; retain the consumer's own density and cycle-life assumptions. Native ecoinvent 3.8, 3.9.1 and 3.10 background inventories and reference-unit metadata; selected release and supplier are recorded at runtime.

**Sources and assumptions**

* Native ecoinvent 3.12 dataset documentation and reference-unit inventory: battery cell production, Li-ion, NMC532.
* https://ecoinvent.org/ecoinvent-v3-12/

.. _commercial-mapping-battery-pack-nmc532:

battery-pack-nmc532
~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``battery pack production, lithium-ion, nmc532, without electronics``

:Reference unit: ``kilogram``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

Remove native BMS purchases or direct electronic-board inputs as present in that release, and renormalize all remaining inputs to one kg of pack excluding those electronics; actual removed mass and multiplier are reported.

Native cell manufacture per kg replaces the source-specific formulation, manufacturing energy and geography; any separate capacity-to-mass conversion remains unchanged. The native BMS purchase is removed, and all remaining native inputs are divided by one minus its kg share. Separate control-unit purchases in the consuming model are retained. Pack architecture and thermal design are represented by the native reference.

**Native inputs excluded before scaling**

* ``market for battery management system, for Li-ion battery``
* ``battery management system production, for Li-ion battery``
* ``market for printed wiring board, surface mounted, unspecified, Pb containing``
* ``market for printed wiring board, surface mounted, unspecified, Pb free``

Remaining native inputs are renormalized to a kilogram excluding the removed electronics.

Base native quantity: 1 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``battery production, Li-ion, NMC532, rechargeable``; ``battery, Li-ion, NMC532, rechargeable``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - medium
     - Inherits the rule's limitations and sources.
   * - 2
     - ``battery production, Li-ion, NMC111, rechargeable, prismatic``; ``battery, Li-ion, NMC111, rechargeable, prismatic``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - low
     - Older prismatic-pack name for native NMC111 manufacture; the manufacturing process and reference kg, rather than the dataset spelling, define the replacement. NMC532 manufacture is approximated by NMC111; chemistry-specific material burdens are not preserved. Native ecoinvent 3.8, 3.9.1 and 3.10 background inventories and reference-unit metadata; selected release and supplier are recorded at runtime.

**Sources and assumptions**

* Native ecoinvent 3.12 dataset documentation and reference-unit inventory: battery production, Li-ion, NMC532, rechargeable.
* https://ecoinvent.org/ecoinvent-v3-12/

.. _commercial-mapping-battery-cell-nmc622:

battery-cell-nmc622
~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``battery cell production, Li-ion, NMC622``

:Reference unit: ``kilogram``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

1 native kilogram per requested kilogram; manufacturing geography remains that of the selected native supplier.

Native cell manufacture per kg replaces the source-specific formulation, manufacturing energy and geography; any separate capacity-to-mass conversion remains unchanged.

Base native quantity: 1 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``battery cell production, Li-ion, NMC622``; ``battery cell, Li-ion, NMC622``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - medium
     - Inherits the rule's limitations and sources.
   * - 2
     - ``battery cell production, Li-ion, NMC811``; ``battery cell, Li-ion, NMC811``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - low
     - The older release has no requested chemistry; use NMC811 cell manufacture per kg. Electrodes, energy use and material burdens differ; retain the consumer's own density and cycle-life assumptions. Native ecoinvent 3.8, 3.9.1 and 3.10 background inventories and reference-unit metadata; selected release and supplier are recorded at runtime.

**Sources and assumptions**

* Native ecoinvent 3.12 dataset documentation and reference-unit inventory: battery cell production, Li-ion, NMC622.
* https://ecoinvent.org/ecoinvent-v3-12/

.. _commercial-mapping-battery-cell-market-nmc622:

battery-cell-market-nmc622
~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``market for battery cell, Li-ion, NMC622``
* ``battery cell supply reference, Li-ion, NMC622, native global market``

:Reference unit: ``kilogram``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

1 native kilogram per requested kilogram; manufacturing geography remains that of the selected native supplier.

Native cell manufacture per kg replaces the source-specific formulation, manufacturing energy and geography; any separate capacity-to-mass conversion remains unchanged. Uses the native market supply mix rather than the curated source mix.

Base native quantity: 1 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``market for battery cell, Li-ion, NMC622``; ``battery cell, Li-ion, NMC622``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - medium
     - Inherits the rule's limitations and sources.
   * - 2
     - ``market for battery cell, Li-ion, NMC811``; ``battery cell, Li-ion, NMC811``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - low
     - The older release has no requested chemistry; use NMC811 cell manufacture per kg. Electrodes, energy use and material burdens differ; retain the consumer's own density and cycle-life assumptions. Uses the available native market mix. Native ecoinvent 3.8, 3.9.1 and 3.10 background inventories and reference-unit metadata; selected release and supplier are recorded at runtime.

**Sources and assumptions**

* Native ecoinvent 3.12 dataset documentation and reference-unit inventory: market for battery cell, Li-ion, NMC622.
* https://ecoinvent.org/ecoinvent-v3-12/

.. _commercial-mapping-battery-pack-nmc622:

battery-pack-nmc622
~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``battery pack production, lithium-ion, nmc622, without electronics``

:Reference unit: ``kilogram``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

Remove native BMS purchases or direct electronic-board inputs as present in that release, and renormalize all remaining inputs to one kg of pack excluding those electronics; actual removed mass and multiplier are reported.

Native cell manufacture per kg replaces the source-specific formulation, manufacturing energy and geography; any separate capacity-to-mass conversion remains unchanged. The native BMS purchase is removed, and all remaining native inputs are divided by one minus its kg share. Separate control-unit purchases in the consuming model are retained. Pack architecture and thermal design are represented by the native reference.

**Native inputs excluded before scaling**

* ``market for battery management system, for Li-ion battery``
* ``battery management system production, for Li-ion battery``
* ``market for printed wiring board, surface mounted, unspecified, Pb containing``
* ``market for printed wiring board, surface mounted, unspecified, Pb free``

Remaining native inputs are renormalized to a kilogram excluding the removed electronics.

Base native quantity: 1 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``battery production, Li-ion, NMC622, rechargeable``; ``battery, Li-ion, NMC622, rechargeable``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - medium
     - Inherits the rule's limitations and sources.
   * - 2
     - ``battery production, Li-ion, NMC811, rechargeable, prismatic``; ``battery, Li-ion, NMC811, rechargeable, prismatic``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - low
     - Older prismatic-pack name for native NMC811 manufacture; the manufacturing process and reference kg, rather than the dataset spelling, define the replacement. NMC622 manufacture is approximated by NMC811; chemistry-specific material burdens are not preserved. Native ecoinvent 3.8, 3.9.1 and 3.10 background inventories and reference-unit metadata; selected release and supplier are recorded at runtime.
   * - 3
     - ``battery production, NMC811, Li-ion, rechargeable, prismatic``; ``battery, Li-ion, NMC811, rechargeable, prismatic``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - low
     - Older prismatic-pack name for native NMC811 manufacture; the manufacturing process and reference kg, rather than the dataset spelling, define the replacement. NMC622 manufacture is approximated by NMC811; chemistry-specific material burdens are not preserved. This naming order is used in ecoinvent 3.8. Native ecoinvent 3.8, 3.9.1 and 3.10 background inventories and reference-unit metadata; selected release and supplier are recorded at runtime.

**Sources and assumptions**

* Native ecoinvent 3.12 dataset documentation and reference-unit inventory: battery production, Li-ion, NMC622, rechargeable.
* https://ecoinvent.org/ecoinvent-v3-12/

.. _commercial-mapping-battery-cell-nmc811:

battery-cell-nmc811
~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``battery cell production, Li-ion, NMC811``

:Reference unit: ``kilogram``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

1 native kilogram per requested kilogram; manufacturing geography remains that of the selected native supplier.

Native cell manufacture per kg replaces the source-specific formulation, manufacturing energy and geography; any separate capacity-to-mass conversion remains unchanged.

Base native quantity: 1 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``battery cell production, Li-ion, NMC811``; ``battery cell, Li-ion, NMC811``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - medium
     - Inherits the rule's limitations and sources.

**Sources and assumptions**

* Native ecoinvent 3.12 dataset documentation and reference-unit inventory: battery cell production, Li-ion, NMC811.
* https://ecoinvent.org/ecoinvent-v3-12/

.. _commercial-mapping-battery-pack-nmc811:

battery-pack-nmc811
~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``battery pack production, lithium-ion, nmc811, retained existing cell, without electronics``

:Reference unit: ``kilogram``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

Remove native BMS purchases or direct electronic-board inputs as present in that release, and renormalize all remaining inputs to one kg of pack excluding those electronics; actual removed mass and multiplier are reported.

Native cell manufacture per kg replaces the source-specific formulation, manufacturing energy and geography; any separate capacity-to-mass conversion remains unchanged. The native BMS purchase is removed, and all remaining native inputs are divided by one minus its kg share. Separate control-unit purchases in the consuming model are retained. Pack architecture and thermal design are represented by the native reference.

**Native inputs excluded before scaling**

* ``market for battery management system, for Li-ion battery``
* ``battery management system production, for Li-ion battery``
* ``market for printed wiring board, surface mounted, unspecified, Pb containing``
* ``market for printed wiring board, surface mounted, unspecified, Pb free``

Remaining native inputs are renormalized to a kilogram excluding the removed electronics.

Base native quantity: 1 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``battery production, Li-ion, NMC811, rechargeable``; ``battery, Li-ion, NMC811, rechargeable``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - medium
     - Inherits the rule's limitations and sources.
   * - 2
     - ``battery production, Li-ion, NMC811, rechargeable, prismatic``; ``battery, Li-ion, NMC811, rechargeable, prismatic``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - medium
     - Older prismatic-pack name for native NMC811 manufacture; the manufacturing process and reference kg, rather than the dataset spelling, define the replacement. Native ecoinvent 3.8, 3.9.1 and 3.10 background inventories and reference-unit metadata; selected release and supplier are recorded at runtime.
   * - 3
     - ``battery production, NMC811, Li-ion, rechargeable, prismatic``; ``battery, Li-ion, NMC811, rechargeable, prismatic``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - medium
     - Older prismatic-pack name for native NMC811 manufacture; the manufacturing process and reference kg, rather than the dataset spelling, define the replacement. This naming order is used in ecoinvent 3.8. Native ecoinvent 3.8, 3.9.1 and 3.10 background inventories and reference-unit metadata; selected release and supplier are recorded at runtime.

**Sources and assumptions**

* Native ecoinvent 3.12 dataset documentation and reference-unit inventory: battery production, Li-ion, NMC811, rechargeable.
* https://ecoinvent.org/ecoinvent-v3-12/

.. _commercial-mapping-battery-cell-nmc955:

battery-cell-nmc955
~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``battery cell production, Li-ion, NMC955``

:Reference unit: ``kilogram``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: low
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

1 native kilogram per requested kilogram; manufacturing geography remains that of the selected native supplier.

Native cell manufacture per kg replaces the source-specific formulation, manufacturing energy and geography; any separate capacity-to-mass conversion remains unchanged. NMC955 is approximated by NMC811 manufacturing per kg; chemistry-specific electrode materials and their burdens are not reproduced. This does not substitute the proxy chemistry energy density or cycle life into the consuming model.

Base native quantity: 1 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``battery cell production, Li-ion, NMC811``; ``battery cell, Li-ion, NMC811``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - low
     - Inherits the rule's limitations and sources.

**Sources and assumptions**

* Native ecoinvent 3.12 dataset documentation and reference-unit inventory: battery cell production, Li-ion, NMC811.
* https://ecoinvent.org/ecoinvent-v3-12/

.. _commercial-mapping-battery-pack-nmc955:

battery-pack-nmc955
~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``battery pack production, lithium-ion, nmc955, without electronics``

:Reference unit: ``kilogram``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: low
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

Remove native BMS purchases or direct electronic-board inputs as present in that release, and renormalize all remaining inputs to one kg of pack excluding those electronics; actual removed mass and multiplier are reported.

Native cell manufacture per kg replaces the source-specific formulation, manufacturing energy and geography; any separate capacity-to-mass conversion remains unchanged. NMC955 is approximated by NMC811 manufacturing per kg; chemistry-specific electrode materials and their burdens are not reproduced. This does not substitute the proxy chemistry energy density or cycle life into the consuming model. The native BMS purchase is removed, and all remaining native inputs are divided by one minus its kg share. Separate control-unit purchases in the consuming model are retained. Pack architecture and thermal design are represented by the native reference.

**Native inputs excluded before scaling**

* ``market for battery management system, for Li-ion battery``
* ``battery management system production, for Li-ion battery``
* ``market for printed wiring board, surface mounted, unspecified, Pb containing``
* ``market for printed wiring board, surface mounted, unspecified, Pb free``

Remaining native inputs are renormalized to a kilogram excluding the removed electronics.

Base native quantity: 1 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``battery production, Li-ion, NMC811, rechargeable``; ``battery, Li-ion, NMC811, rechargeable``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - low
     - Inherits the rule's limitations and sources.
   * - 2
     - ``battery production, Li-ion, NMC811, rechargeable, prismatic``; ``battery, Li-ion, NMC811, rechargeable, prismatic``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - low
     - Older prismatic-pack name for native NMC811 manufacture; the manufacturing process and reference kg, rather than the dataset spelling, define the replacement. NMC955 manufacture is approximated by NMC811; chemistry-specific material burdens are not preserved. Native ecoinvent 3.8, 3.9.1 and 3.10 background inventories and reference-unit metadata; selected release and supplier are recorded at runtime.
   * - 3
     - ``battery production, NMC811, Li-ion, rechargeable, prismatic``; ``battery, Li-ion, NMC811, rechargeable, prismatic``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - low
     - Older prismatic-pack name for native NMC811 manufacture; the manufacturing process and reference kg, rather than the dataset spelling, define the replacement. NMC955 manufacture is approximated by NMC811; chemistry-specific material burdens are not preserved. This naming order is used in ecoinvent 3.8. Native ecoinvent 3.8, 3.9.1 and 3.10 background inventories and reference-unit metadata; selected release and supplier are recorded at runtime.

**Sources and assumptions**

* Native ecoinvent 3.12 dataset documentation and reference-unit inventory: battery production, Li-ion, NMC811, rechargeable.
* https://ecoinvent.org/ecoinvent-v3-12/

.. _commercial-mapping-battery-cell-lto:

battery-cell-lto
~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``battery cell production, Li-ion, LTO``

:Reference unit: ``kilogram``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: low
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

1 native kilogram per requested kilogram; manufacturing geography remains that of the selected native supplier.

Native cell manufacture per kg replaces the source-specific formulation, manufacturing energy and geography; any separate capacity-to-mass conversion remains unchanged. LTO is approximated by LFP manufacturing per kg; chemistry-specific electrode materials and their burdens are not reproduced. This does not substitute the proxy chemistry energy density or cycle life into the consuming model.

Base native quantity: 1 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``battery cell production, Li-ion, LFP``; ``battery cell, Li-ion, LFP``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - low
     - Inherits the rule's limitations and sources.
   * - 2
     - ``battery cell production, Li-ion``; ``battery cell, Li-ion``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - low
     - The older release has no requested chemistry; use legacy generic Li-ion (LMO) cell manufacture per kg. Electrodes, energy use and material burdens differ; retain the consumer's own density and cycle-life assumptions. Native ecoinvent 3.8, 3.9.1 and 3.10 background inventories and reference-unit metadata; selected release and supplier are recorded at runtime.

**Sources and assumptions**

* Native ecoinvent 3.12 dataset documentation and reference-unit inventory: battery cell production, Li-ion, LFP.
* https://ecoinvent.org/ecoinvent-v3-12/

.. _commercial-mapping-battery-cell-market-lto:

battery-cell-market-lto
~~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``market for battery cell, Li-ion, LTO``

:Reference unit: ``kilogram``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: low
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

1 native kilogram per requested kilogram; manufacturing geography remains that of the selected native supplier.

Native cell manufacture per kg replaces the source-specific formulation, manufacturing energy and geography; any separate capacity-to-mass conversion remains unchanged. LTO is approximated by LFP manufacturing per kg; chemistry-specific electrode materials and their burdens are not reproduced. This does not substitute the proxy chemistry energy density or cycle life into the consuming model. Uses the native market supply mix rather than the curated source mix.

Base native quantity: 1 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``market for battery cell, Li-ion, LFP``; ``battery cell, Li-ion, LFP``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - low
     - Inherits the rule's limitations and sources.
   * - 2
     - ``market for battery cell, Li-ion``; ``battery cell, Li-ion``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - low
     - The older release has no requested chemistry; use legacy generic Li-ion (LMO) cell manufacture per kg. Electrodes, energy use and material burdens differ; retain the consumer's own density and cycle-life assumptions. Uses the available native market mix. Native ecoinvent 3.8, 3.9.1 and 3.10 background inventories and reference-unit metadata; selected release and supplier are recorded at runtime.

**Sources and assumptions**

* Native ecoinvent 3.12 dataset documentation and reference-unit inventory: market for battery cell, Li-ion, LFP.
* https://ecoinvent.org/ecoinvent-v3-12/

.. _commercial-mapping-battery-controls-per-unit:

battery-controls-per-unit
~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``battery monitoring and central control system production, lithium-ion, lfp``
* ``battery monitoring and central control system production, lithium-ion, limn2o4``
* ``battery monitoring and central control system production, lithium-ion, nca``
* ``battery monitoring and central control system production, lithium-ion, nmc111``
* ``battery monitoring and central control system production, lithium-ion, nmc532``
* ``battery monitoring and central control system production, lithium-ion, nmc622``
* ``battery monitoring and central control system production, lithium-ion, nmc811``
* ``battery monitoring and central control system production, lithium-ion, nmc955``
* ``battery central control and protection unit production``
* ``battery control and cooling system production, 16 modules``

:Reference unit: ``unit``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: low
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

Four kg of native battery-management electronics per requested control assembly. This is an explicit engineering approximation to one vehicle-pack control system, not a kg-to-unit identity conversion.

A single 4 kg BMS represents the complete requested control assembly regardless of module count or chemistry. The native NMC622 23.5 kWh/165 kg reference pack has about 4.3 kg of BMS, motivating this rounded approximation. Cooling hardware, split central/monitoring designs and modern solid-state electronics are not resolved.

Base native quantity: 4 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``battery management system production, for Li-ion battery``; ``battery management system, for Li-ion battery``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - low
     - Inherits the rule's limitations and sources.

**Sources and assumptions**

* Native ecoinvent 3.12 dataset documentation and reference-unit inventory: battery management system production, for Li-ion battery.
* https://ecoinvent.org/ecoinvent-v3-12/

.. _commercial-mapping-battery-controls-per-kg:

battery-controls-per-kg
~~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``battery management system production, for Li-ion battery``
* ``battery auxiliary interface and power conditioning unit production``

:Reference unit: ``kilogram``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

1 native kilogram per requested kilogram; manufacturing geography remains that of the selected native supplier.

Native BMS electronics per kg; auxiliary power-conditioning hardware is represented by the generic battery-control assembly. Specific voltage, cooling and semiconductor architecture are not resolved.

Base native quantity: 1 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``battery management system production, for Li-ion battery``; ``battery management system, for Li-ion battery``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - medium
     - Inherits the rule's limitations and sources.

**Sources and assumptions**

* Native ecoinvent 3.12 dataset documentation and reference-unit inventory: battery management system production, for Li-ion battery.
* https://ecoinvent.org/ecoinvent-v3-12/

.. _commercial-mapping-battery-lto-pack-market:

battery-lto-pack-market
~~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``market for battery, Li-ion, LTO``

:Reference unit: ``kilogram``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: low
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

1 native kilogram per requested kilogram; manufacturing geography remains that of the selected native supplier.

An LFP pack supply proxy represents LTO pack manufacture per kg. It includes native controls; detailed LTO electrodes, cycle life, energy density and the source pack architecture are not reproduced or substituted into the consuming capacity conversion.

Base native quantity: 1 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``market for battery, Li-ion, LFP, rechargeable``; ``battery, Li-ion, LFP, rechargeable``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - low
     - Inherits the rule's limitations and sources.
   * - 2
     - ``market for battery, Li-ion, LFP, rechargeable, prismatic``; ``battery, Li-ion, LFP, rechargeable, prismatic``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - low
     - Older native LFP pack supply name; LTO remains a chemistry proxy with no inferred change to consumer performance. Native ecoinvent 3.8, 3.9.1 and 3.10 background inventories and reference-unit metadata; selected release and supplier are recorded at runtime.
   * - 3
     - ``market for battery, Li-ion, rechargeable, prismatic``; ``battery, Li-ion, rechargeable, prismatic``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - low
     - Pre-3.9 generic LMO pack supply is used where LFP is unavailable. The LTO manufacturing chemistry is not reproduced. Native ecoinvent 3.8, 3.9.1 and 3.10 background inventories and reference-unit metadata; selected release and supplier are recorded at runtime.

**Sources and assumptions**

* Native ecoinvent 3.12 dataset documentation and reference-unit inventory: market for battery, Li-ion, LFP, rechargeable.
* https://ecoinvent.org/ecoinvent-v3-12/

.. _commercial-mapping-powertrain-electric-motor:

powertrain-electric-motor
~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``traction powertrain production, permanent magnet motor``

:Reference unit: ``kilogram``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

1 native kilogram per requested kilogram; manufacturing geography remains that of the selected native supplier.

Native electric-passenger-car traction motor manufacture per kg substitutes the hybrid permanent-magnet motor material reference. Motor type, power rating, magnet content, design year and vehicle class can differ. Uses the motor-only inventory to avoid counting the separately supplied charger, converter and controller again; no use-phase efficiency is inferred.

Base native quantity: 1 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``electric motor production, vehicle (electric powertrain)``; ``electric motor, electric passenger car``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - medium
     - Inherits the rule's limitations and sources.

**Sources and assumptions**

* Native ecoinvent 3.12 dataset documentation and reference-unit inventory: electric motor production, vehicle (electric powertrain).
* https://ecoinvent.org/ecoinvent-v3-12/

.. _commercial-mapping-powertrain-motorcycle:

powertrain-motorcycle
~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``mechanical powertrain production, motorcycle, gasoline``

:Reference unit: ``kilogram``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: low
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

1 native kilogram per requested kilogram; manufacturing geography remains that of the selected native supplier.

A passenger-car engine per kg is a manufacturing proxy for the motorcycle mechanical powertrain. Gearbox, starter and clutch detail and motorcycle-specific manufacturing are not resolved; no fuel consumption or efficiency is inferred.

Base native quantity: 1 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``internal combustion engine production, passenger car``; ``internal combustion engine, passenger car``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``CN`` → ``RER``
     - low
     - Inherits the rule's limitations and sources.

**Sources and assumptions**

* Native ecoinvent 3.12 dataset documentation and reference-unit inventory: internal combustion engine production, passenger car.
* https://ecoinvent.org/ecoinvent-v3-12/

.. _commercial-mapping-vehicle-auxiliary-battery:

vehicle-auxiliary-battery
~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``auxiliary battery production and attributed treatment, passenger car, historical burden proxy``

:Reference unit: ``kilogram``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: low
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One kg of the native manufactured component per requested kg of historical component.

Native stationary VRLA lead-acid manufacture per kg approximates an automotive auxiliary battery. Starting performance, lifetime and design are not substituted into the vehicle. The original attributed-treatment convention is not reproduced; no additional recycling credit or remelting burden is invented under the cutoff background.

Base native quantity: 1 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``battery production, lead acid, rechargeable, stationary``; ``battery, lead acid, rechargeable, stationary``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``RER`` → ``US``
     - low
     - Inherits the rule's limitations and sources.

**Sources and assumptions**

* Native ecoinvent 3.12 documentation and inventory: battery production, lead acid, rechargeable, stationary.
* https://ecoinvent.org/ecoinvent-v3-12/

.. _commercial-mapping-vehicle-dry-combustion-equipment:

vehicle-dry-combustion-equipment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``dry combustion equipment lifecycle, historical Notter engineering reference``

:Reference unit: ``kilogram``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: low
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One kg of the native manufactured component per requested kg of historical component.

Native passenger-car engine manufacture per kg represents the historical combustion-equipment reference. Exhaust, transmission and allocated end-of-life burdens can differ; this proxy covers manufacturing and its fabrication wastes, without inferring fuel consumption or adding an unsupported historical recycling allocation.

Base native quantity: 1 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``internal combustion engine production, passenger car``; ``internal combustion engine, passenger car``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``RER`` → ``US``
     - low
     - Inherits the rule's limitations and sources.

**Sources and assumptions**

* Native ecoinvent 3.12 documentation and inventory: internal combustion engine production, passenger car.
* https://ecoinvent.org/ecoinvent-v3-12/

.. _commercial-mapping-vehicle-mechanical-electric-car:

vehicle-mechanical-electric-car
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``mechanical powertrain production and attributed treatment, electric passenger car, historical reference``

:Reference unit: ``kilogram``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: low
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One kg of the native manufactured component per requested kg of historical component.

A kg of native glider manufacture approximates the steel/aluminium mechanical assembly (gearbox/cooling), not a whole additional car. Body materials and fabrication substitute mechanical machining and cooling detail. Excludes traction motor and power electronics, which remain separately supplied; the original attributed-treatment allocation is not reproduced.

Base native quantity: 1 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``glider production, passenger car``; ``glider, passenger car``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``RER`` → ``US``
     - low
     - Inherits the rule's limitations and sources.

**Sources and assumptions**

* Native ecoinvent 3.12 documentation and inventory: glider production, passenger car.
* https://ecoinvent.org/ecoinvent-v3-12/

.. _commercial-mapping-vehicle-mechanical-electric-two-wheeler:

vehicle-mechanical-electric-two-wheeler
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``mechanical equipment production, electric two-wheeler, historical steel-aluminium proxy``

:Reference unit: ``kilogram``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: low
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One kg of the native manufactured component per requested kg of historical component.

Native electric-scooter glider manufacture per kg replaces the unverified historical steel/aluminium mechanical-equipment proxy. Body materials and fabrication approximate the smaller mechanical assembly; detailed gearbox and cooling components are not resolved. Motor and battery are excluded from the native supplier and remain separate.

Base native quantity: 1 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``glider production, electric scooter``; ``glider, for electric scooter``; ``kilogram``
     - ``same`` → ``GLO`` → ``RoW`` → ``RER`` → ``US``
     - low
     - Inherits the rule's limitations and sources.

**Sources and assumptions**

* Native ecoinvent 3.12 documentation and inventory: glider production, electric scooter.
* https://ecoinvent.org/ecoinvent-v3-12/

.. _commercial-mapping-electrolyzer-pfsa-membrane:

electrolyzer-pfsa-membrane
~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``perfluorosulfonic acid membrane production, ew1100, engineering reference``

:Reference unit: ``kilogram``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: low
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One kg of native PTFE resin manufacture per requested kg of PFSA membrane: a fluoropolymer manufacturing-burden proxy, not a functional membrane substitution.

PFSA has a fluorinated backbone and ionic sulfonic-acid side chains; PTFE alone is not proton-conducting. The proxy omits side-chain chemistry, equivalent-weight specificity, reinforcement and membrane forming. Operational conductivity, lifetime and electrolyzer efficiency remain those of the consuming model. Manufacturing and fluorinated-emission impacts can be substantially biased; this is a low-confidence material proxy.

Base native quantity: 1 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``polytetrafluoroethylene production, emulsion polymerization``; ``polytetrafluoroethylene``; ``kilogram``
     - ``GLO``
     - low
     - Inherits the rule's limitations and sources.
   * - 2
     - ``tetrafluoroethylene production``; ``tetrafluoroethylene``; ``kilogram``
     - ``same`` → ``RoW`` → ``RER`` → ``GLO``
     - low
     - Earlier releases lack the native PTFE polymer inventory. One kg of its tetrafluoroethylene monomer manufacture is used as a fluorinated-feedstock burden proxy; polymerization, membrane forming and ionic side chains are omitted. This is a lower-bound material approximation, not functional PFSA manufacture. Native ecoinvent 3.8, 3.9.1 and 3.10 background inventories and reference-unit metadata; selected release and supplier are recorded at runtime.

**Sources and assumptions**

* https://www.nafion.com/en/products/sulfonic-membranes
* Native ecoinvent 3.12 PTFE production inventory (GLO), per kg.
* The 1:1 resin mass mapping is a documented engineering assumption, not a reported PFSA inventory.

.. _commercial-mapping-electrolyzer-power-conversion:

electrolyzer-power-conversion
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``power conversion system production, electrolysis, transformer and rectifier, 1 mw``

:Reference unit: ``unit``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: low
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

Two native 500 kW inverter assemblies represent the manufacturing burden of a 1 MW electrolyzer power-conversion assembly.

Uses legacy photovoltaic DC/AC inverter manufacturing as a capacity-normalized proxy for AC/DC conversion. Native bulk metals, oil/cooling, electronics and documented end-of-life flows are retained. A separately rated grid transformer, modern SiC semiconductor architecture, harmonics and voltage levels are not resolved. No operational efficiency is inferred from the inverter. This is a low-confidence assembly proxy that replaces the complete restricted component chain.

Base native quantity: 2 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``inverter production, 500kW``; ``inverter, 500kW``; ``unit``
     - ``same`` → ``RER`` → ``RoW``
     - low
     - Inherits the rule's limitations and sources.

**Sources and assumptions**

* Native ecoinvent 3.12 inverter production, 500kW (RER/RoW): roughly 3000 kg assembly including fabrication and disposal inputs.
* https://ecoinvent.org/ecoinvent-v3-12/
* Two 500 kW units per 1 MW is an explicit capacity-based engineering approximation, not a supplier bill of materials.

.. _commercial-mapping-semiconductor-sic-wafer:

semiconductor-sic-wafer
~~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``silicon carbide wafer production, physical vapour transport``

:Reference unit: ``kilogram``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: low
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

A 350 micrometre SiC wafer at 3210 kg/m3 has an areal mass of 1.1235 kg/m2; one requested kg is represented by 1/1.1235 = 0.8900756564 m2 of native electronic-grade single-Si wafer manufacturing.

Electronic-grade silicon substrate manufacture is a wafer-fabrication burden proxy, not a SiC material recipe. Crystal-growth route, wafer material, thickness, yield and polishing can differ substantially; SiC-specific high-temperature PVT and epitaxy are not reproduced. The representative 350 micrometre thickness is an assumption for the requested kg reference. Operational semiconductor performance is not changed.

Base native quantity: 0.8900756564307967 native reference units per requested output, before electronics mass renormalization where listed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``single-Si wafer production, for electronics``; ``single-Si wafer, for electronics``; ``square meter``
     - ``same`` → ``RoW`` → ``RER`` → ``GLO``
     - low
     - Inherits the rule's limitations and sources.

**Sources and assumptions**

* https://www.sicrystal.de/index.php/en/properties-en
* https://assets.wolfspeed.com/uploads/2026/02/Wolfspeed_Materials_Catalog.pdf
* Native ecoinvent 3.12 single-Si wafer production, for electronics, per square metre.

.. _commercial-mapping-methane-pyrolysis-published-reference:

methane-pyrolysis-published-reference
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**CLIC activity names**

* ``hydrogen production, gaseous, 100 bar, from methane pyrolysis``

:Reference unit: ``kilogram``
:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11, 3.12
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: low
:Used inventory license: CC-BY-4.0

**Conversion and boundary**

Per kg H2, use the independently published Table 1 values: 5.7 m3 natural gas, 10.3 kWh net electricity including purification/compression, and 0.04 kg fugitive methane. No quantities from the disputed CLIC recipe are reused.

The published service is H2 at 200 bar; its full energy demand is retained as a conservative proxy for the requested 100 bar reference, without a pressure credit. Uses the available global background markets rather than reproducing the authors' specific background. All burdens are assigned to H2; the 3 kg solid-carbon coproduct is recorded as an uncredited boundary output, without a carbon-black displacement credit. Plant construction/decommissioning and downstream carbon fate are excluded. Foreground values are deterministic; no unsupported uncertainty parameters are invented.

**Independent purchase 1: 5.7 cubic meter per reference output**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``market group for natural gas, high pressure``; ``natural gas, high pressure``; ``cubic meter``
     - ``same`` → ``GLO``
     - low
     - Inherits the rule's limitations and sources.
   * - 2
     - ``market for natural gas, high pressure``; ``natural gas, high pressure``; ``cubic meter``
     - ``same`` → ``RoW`` → ``CH``
     - low
     - Inherits the rule's limitations and sources.

**Independent purchase 2: 10.3 kilowatt hour per reference output**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``market group for electricity, medium voltage``; ``electricity, medium voltage``; ``kilowatt hour``
     - ``same`` → ``GLO`` → ``RER``
     - low
     - Inherits the rule's limitations and sources.
   * - 2
     - ``market for electricity, medium voltage``; ``electricity, medium voltage``; ``kilowatt hour``
     - ``same`` → ``CH``
     - low
     - Inherits the rule's limitations and sources.

**Independent foreground emissions per reference output**

.. list-table::
   :header-rows: 1

   * - Flow
     - Compartment
     - Unit
     - Amount
   * - Methane, fossil
     - air
     - kilogram
     - 0.04

**Additional replacement metadata**

.. code-block:: yaml

   uncredited solid carbon output kg per kg H2: 3.0
   proxy source pressure bar: 200
   proxy requested pressure bar: 100
   classifications:
   - - ISIC rev.4 ecoinvent
     - 3520:Manufacture of gas; distribution of gaseous fuels through mains
   - - CPC
     - '34210: Hydrogen, nitrogen, oxygen, carbon dioxide and rare gases; inorganic oxygen
       compounds of non-metals n.e.c.'

Independent premise adaptation of Zhang et al. (2026), When does methane pyrolysis become competitive? A life-cycle and total cost assessment of hydrogen pathways, Table 1. Pressure label and background suppliers changed; carbon remains uncredited.

Publication: https://doi.org/10.1016/j.ijhydene.2026.154768

**Sources and assumptions**

* https://doi.org/10.1016/j.ijhydene.2026.154768
* https://api.crossref.org/works/10.1016/j.ijhydene.2026.154768
* Crossref version-of-record license: https://creativecommons.org/licenses/by/4.0/ (effective 2026-03-28). Table 1 and Sections 2.1-2.2 give the foreground and no-credit boundary.

Background-supplier replacements
--------------------------------

.. _commercial-mapping-background-nmc622-pack:

background-nmc622-pack
~~~~~~~~~~~~~~~~~~~~~~

**Original background supplier identities**

.. list-table::
   :header-rows: 1

   * - Name
     - Product
     - Location
     - Unit
   * - market for battery, Li-ion, NMC622
     - battery, Li-ion, NMC622
     - GLO
     - kilogram
   * - market for battery, Li-ion, NMC622, rechargeable
     - battery, Li-ion, NMC622, rechargeable
     - GLO
     - kilogram

:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: low
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One kg of the reviewed native supplier per kg of the unavailable reference; purchasing amounts and consumer operating assumptions are retained.

The older target has no native NMC622 pack. A complete NMC811 pack, including its own controls, represents manufacturing per kg; this does not change the consuming vehicle's capacity, density or lifetime parameters.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``market for battery, Li-ion, NMC811, rechargeable, prismatic``; ``battery, Li-ion, NMC811, rechargeable, prismatic``; ``kilogram``
     - ``same`` → ``GLO`` → ``Europe without Switzerland`` → ``RoW`` → ``RER`` → ``CN`` → ``CH``
     - low
     - Inherits the rule's limitations and sources.

**Sources and assumptions**

* Native ecoinvent 3.8, 3.9.1, 3.10 and 3.11 target metadata and actual background-link validation.
* Engineering manufacturing-burden proxy; chemical or grade equivalence is not asserted.

.. _commercial-mapping-background-nmc532-pack:

background-nmc532-pack
~~~~~~~~~~~~~~~~~~~~~~

**Original background supplier identities**

.. list-table::
   :header-rows: 1

   * - Name
     - Product
     - Location
     - Unit
   * - market for battery, Li-ion, NMC532
     - battery, Li-ion, NMC532
     - GLO
     - kilogram
   * - market for battery, Li-ion, NMC532, rechargeable
     - battery, Li-ion, NMC532, rechargeable
     - GLO
     - kilogram

:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: low
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One kg of the reviewed native supplier per kg of the unavailable reference; purchasing amounts and consumer operating assumptions are retained.

Native NMC111 complete-pack manufacture approximates NMC532 per kg where that chemistry is absent. Manufacturing cathode composition differs; no operating-property substitution is inferred.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``market for battery, Li-ion, NMC111, rechargeable, prismatic``; ``battery, Li-ion, NMC111, rechargeable, prismatic``; ``kilogram``
     - ``same`` → ``GLO`` → ``Europe without Switzerland`` → ``RoW`` → ``RER`` → ``CN`` → ``CH``
     - low
     - Inherits the rule's limitations and sources.

**Sources and assumptions**

* Native ecoinvent 3.8, 3.9.1, 3.10 and 3.11 target metadata and actual background-link validation.
* Engineering manufacturing-burden proxy; chemical or grade equivalence is not asserted.

.. _commercial-mapping-background-lfp-pack:

background-lfp-pack
~~~~~~~~~~~~~~~~~~~

**Original background supplier identities**

.. list-table::
   :header-rows: 1

   * - Name
     - Product
     - Location
     - Unit
   * - market for battery, Li-ion, LFP, rechargeable, prismatic
     - battery, Li-ion, LFP, rechargeable, prismatic
     - GLO
     - kilogram
   * - market for battery, Li-ion, LFP, rechargeable
     - battery, Li-ion, LFP, rechargeable
     - GLO
     - kilogram

:Registry target versions: 3.8
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: low
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One kg of the reviewed native supplier per kg of the unavailable reference; purchasing amounts and consumer operating assumptions are retained.

The pre-3.9 generic LMO complete-pack inventory is a low-confidence manufacturing proxy for unavailable LFP packs; native controls are included as in the requested complete-pack boundary.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``market for battery, Li-ion, rechargeable, prismatic``; ``battery, Li-ion, rechargeable, prismatic``; ``kilogram``
     - ``same`` → ``GLO`` → ``Europe without Switzerland`` → ``RoW`` → ``RER`` → ``CN`` → ``CH``
     - low
     - Inherits the rule's limitations and sources.

**Sources and assumptions**

* Native ecoinvent 3.8, 3.9.1, 3.10 and 3.11 target metadata and actual background-link validation.
* Engineering manufacturing-burden proxy; chemical or grade equivalence is not asserted.

.. _commercial-mapping-background-ptfe:

background-ptfe
~~~~~~~~~~~~~~~

**Original background supplier identities**

.. list-table::
   :header-rows: 1

   * - Name
     - Product
     - Location
     - Unit
   * - market for polytetrafluoroethylene
     - polytetrafluoroethylene
     - GLO
     - kilogram

:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: low
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One kg of the reviewed native supplier per kg of the unavailable reference; purchasing amounts and consumer operating assumptions are retained.

Earlier releases have no native PTFE polymer. Tetrafluoroethylene monomer manufacture represents fluorinated feedstock burden per kg; polymerization and downstream forming are omitted. This is a lower-bound manufacturing proxy.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``tetrafluoroethylene production``; ``tetrafluoroethylene``; ``kilogram``
     - ``same`` → ``GLO`` → ``Europe without Switzerland`` → ``RoW`` → ``RER`` → ``CN`` → ``CH``
     - low
     - Inherits the rule's limitations and sources.

**Sources and assumptions**

* Native ecoinvent 3.8, 3.9.1, 3.10 and 3.11 target metadata and actual background-link validation.
* Engineering manufacturing-burden proxy; chemical or grade equivalence is not asserted.

.. _commercial-mapping-background-vinylidene-fluoride:

background-vinylidene-fluoride
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Original background supplier identities**

.. list-table::
   :header-rows: 1

   * - Name
     - Product
     - Location
     - Unit
   * - market for vinylidene fluoride
     - vinylidene fluoride
     - GLO
     - kilogram

:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1, 3.11
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: low
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One kg of the reviewed native supplier per kg of the unavailable reference; purchasing amounts and consumer operating assumptions are retained.

Tetrafluoroethylene is a different fluoromonomer and approximates the unavailable vinylidene-fluoride manufacturing burden per kg. Fluorine content and process chemistry differ; consuming polymerization operations are retained, with no additional polymer product added.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``tetrafluoroethylene production``; ``tetrafluoroethylene``; ``kilogram``
     - ``same`` → ``GLO`` → ``Europe without Switzerland`` → ``RoW`` → ``RER`` → ``CN`` → ``CH``
     - low
     - Inherits the rule's limitations and sources.

**Sources and assumptions**

* Native ecoinvent 3.8, 3.9.1, 3.10 and 3.11 target metadata and actual background-link validation.
* Engineering manufacturing-burden proxy; chemical or grade equivalence is not asserted.

.. _commercial-mapping-background-mdea:

background-mdea
~~~~~~~~~~~~~~~

**Original background supplier identities**

.. list-table::
   :header-rows: 1

   * - Name
     - Product
     - Location
     - Unit
   * - market for methyldiethanolamine
     - methyldiethanolamine
     - RER
     - kilogram

:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: low
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One kg of the reviewed native supplier per kg of the unavailable reference; purchasing amounts and consumer operating assumptions are retained.

Use the same amine from an available native market if present, otherwise diethanolamine as a related amine-manufacturing proxy. Methylation chemistry, toxicity and solvent properties differ; capture efficiency and solvent consumption are not inferred from the proxy.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``market for methyldiethanolamine``; ``methyldiethanolamine``; ``kilogram``
     - ``same`` → ``GLO`` → ``Europe without Switzerland`` → ``RoW`` → ``RER`` → ``CN`` → ``CH``
     - low
     - Inherits the rule's limitations and sources.
   * - 2
     - ``market for diethanolamine``; ``diethanolamine``; ``kilogram``
     - ``same`` → ``GLO`` → ``Europe without Switzerland`` → ``RoW`` → ``RER`` → ``CN`` → ``CH``
     - low
     - Inherits the rule's limitations and sources.

**Sources and assumptions**

* Native ecoinvent 3.8, 3.9.1, 3.10 and 3.11 target metadata and actual background-link validation.
* Engineering manufacturing-burden proxy; chemical or grade equivalence is not asserted.

.. _commercial-mapping-background-zirconium-sponge:

background-zirconium-sponge
~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Original background supplier identities**

.. list-table::
   :header-rows: 1

   * - Name
     - Product
     - Location
     - Unit
   * - market for zirconium sponge, nuclear-grade
     - zirconium sponge, nuclear-grade
     - GLO
     - kilogram

:Registry target versions: 3.8
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: low
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One kg of the reviewed native supplier per kg of the unavailable reference; purchasing amounts and consumer operating assumptions are retained.

Titanium sponge approximates the unavailable zirconium-metal manufacturing burden as a related sponge-metallurgy reference per kg. Zirconium ore chemistry and nuclear-grade hafnium separation are not represented; physical alloy or reactor performance is not changed.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``market for titanium sponge``; ``titanium sponge``; ``kilogram``
     - ``same`` → ``GLO`` → ``Europe without Switzerland`` → ``RoW`` → ``RER`` → ``CN`` → ``CH``
     - low
     - Inherits the rule's limitations and sources.

**Sources and assumptions**

* Native ecoinvent 3.8, 3.9.1, 3.10 and 3.11 target metadata and actual background-link validation.
* Engineering manufacturing-burden proxy; chemical or grade equivalence is not asserted.

.. _commercial-mapping-background-xanthate:

background-xanthate
~~~~~~~~~~~~~~~~~~~

**Original background supplier identities**

.. list-table::
   :header-rows: 1

   * - Name
     - Product
     - Location
     - Unit
   * - market for sodium ethyl xanthate
     - sodium ethyl xanthate
     - GLO
     - kilogram

:Registry target versions: 3.8
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: low
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One kg of the reviewed native supplier per kg of the unavailable reference; purchasing amounts and consumer operating assumptions are retained.

No native xanthate collector is available. Generic organic-chemical manufacture is a coarse burden proxy per kg; sulfur/sodium-specific production and toxicity are not represented. Flotation dosage and recovery assumptions remain in the consumer.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``market for chemical, organic``; ``chemical, organic``; ``kilogram``
     - ``same`` → ``GLO`` → ``Europe without Switzerland`` → ``RoW`` → ``RER`` → ``CN`` → ``CH``
     - low
     - Inherits the rule's limitations and sources.

**Sources and assumptions**

* Native ecoinvent 3.8, 3.9.1, 3.10 and 3.11 target metadata and actual background-link validation.
* Engineering manufacturing-burden proxy; chemical or grade equivalence is not asserted.

.. _commercial-mapping-background-lfp-active-material:

background-lfp-active-material
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Original background supplier identities**

.. list-table::
   :header-rows: 1

   * - Name
     - Product
     - Location
     - Unit
   * - market for lithium iron phosphate
     - lithium iron phosphate
     - RoW
     - kilogram

:Registry target versions: 3.8
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: low
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One kg of the reviewed native supplier per kg of the unavailable reference; purchasing amounts and consumer operating assumptions are retained.

A native LMO cathode manufacturing reference approximates the unavailable LFP active material. It includes an electrode-composite boundary and differs in chemistry; this is a low-confidence burden proxy without changing capacity or cycle-life assumptions.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``market for cathode, LiMn2O4, for lithium-ion battery``; ``cathode, LiMn2O4, for lithium-ion battery``; ``kilogram``
     - ``same`` → ``GLO`` → ``Europe without Switzerland`` → ``RoW`` → ``RER`` → ``CN`` → ``CH``
     - low
     - Inherits the rule's limitations and sources.

**Sources and assumptions**

* Native ecoinvent 3.8, 3.9.1, 3.10 and 3.11 target metadata and actual background-link validation.
* Engineering manufacturing-burden proxy; chemical or grade equivalence is not asserted.

.. _commercial-mapping-background-recycled-hdpe:

background-recycled-hdpe
~~~~~~~~~~~~~~~~~~~~~~~~

**Original background supplier identities**

.. list-table::
   :header-rows: 1

   * - Name
     - Product
     - Location
     - Unit
   * - polyethylene, high density, pellets, recycled to generic market for polyethylene, high density, granulate
     - polyethylene, high density, granulate
     - RER
     - kilogram

:Registry target versions: 3.8, 3.9, 3.9.1, 3.10, 3.10.1
:Registry system models: cutoff, EN15804
:Review status: reviewed
:Confidence: medium
:Used inventory license: ecoinvent (separate usage rights required)

**Conversion and boundary**

One kg of the reviewed native supplier per kg of the unavailable reference; purchasing amounts and consumer operating assumptions are retained.

Preserve recycled HDPE supply rather than accept a generic virgin-granulate alias. Europe without Switzerland represents the unavailable RER market where possible; regional recycling mix and pellet grade can differ.

**Native supplier candidates, in selection order**

.. list-table::
   :header-rows: 1

   * - Priority
     - Native supplier identity
     - Location preference
     - Confidence
     - Qualification
   * - 1
     - ``market for polyethylene, high density, granulate, recycled``; ``polyethylene, high density, granulate, recycled``; ``kilogram``
     - ``Europe without Switzerland`` → ``RoW`` → ``CH`` → ``GLO``
     - medium
     - Inherits the rule's limitations and sources.

**Sources and assumptions**

* Native ecoinvent 3.8, 3.9.1, 3.10 and 3.11 target metadata and actual background-link validation.
* Engineering manufacturing-burden proxy; chemical or grade equivalence is not asserted.
