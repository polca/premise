Configuring sector updates
============================

After constructing ``ndb``, run all default updates with:

.. code-block:: python

   ndb.update()

This is the usual choice. Earlier updates prepare suppliers needed by later
ones. Updates use the data available for each scenario; selecting a sector
does not guarantee that its required variables are present.

Choose part of the workflow
----------------------------

You can request one sector or an ordered list:

.. code-block:: python

   ndb.update("electricity")
   # Alternative: start a fresh build and run a selected sequence.
   # ndb.update(["biomass", "electricity", "fuels", "heat"])

A list runs in the order you give it. A single-sector call does not also run
its suppliers' updates. Use the full workflow unless you have a reason to
keep part of the background unchanged.

.. list-table:: Accepted update names
   :header-rows: 1
   :widths: 25 75

   * - Name
     - What it changes
   * - ``biomass``
     - :doc:`Biomass supply </methodology/biomass>` and regional markets.
   * - ``electricity``
     - :doc:`Generation and markets </methodology/electricity/index>`, including PV adjustments.
   * - ``cement``, ``steel``
     - :doc:`Cement </methodology/cement>` and :doc:`steel </methodology/steel>` production, efficiencies and markets.
   * - ``fuels``
     - :doc:`Fuel supply chains and markets </methodology/fuels/index>`, including hydrogen.
   * - ``renewable``
     - Wind-turbine inventories; electricity supply is handled by ``electricity``.
   * - ``metals``
     - :doc:`Material inputs and metal supply markets </methodology/metals>`.
   * - ``mining``
     - :doc:`Sulfidic-tailings treatment </methodology/mining>` and its regional shares.
   * - ``heat``
     - :doc:`Delivered-heat supply and end-use markets </methodology/heat>`.
   * - ``cdr``
     - :doc:`Carbon removal and supporting capture/storage processes </methodology/cdr>`.
   * - ``battery``
     - Mass inputs and chemistry shares in existing :doc:`battery capacity markets </methodology/batteries>`.
   * - ``cars``, ``two_wheelers``, ``trucks``, ``ships``, ``buses``, ``trains``
     - :doc:`Transport inventories and available fleets </methodology/transport/index>`, selected by mode.
   * - ``final energy``
     - :doc:`End-use heating and transport energy-use activities </methodology/final-energy>`.
   * - ``external``
     - :doc:`Custom scenario changes <external-scenarios>`.
   * - ``emissions``
     - :doc:`Mapped air-pollutant emissions </methodology/emissions>` using GAINS data.

The default order is biomass, electricity, cement, steel, fuels, renewable,
metals, mining, heat, cdr, battery, cars, two_wheelers, trucks, ships, buses,
trains, final energy, external, emissions. In particular, custom scenario
changes run before the final emissions update.

Next, :doc:`reports` explains how to review the changes before using an export.
To inspect or edit individual activities, see :doc:`inventories`.
