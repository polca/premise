Shared modelling principles
=============================

Sector transformations share a workflow: interpret scenario data, modify
inventory activities, construct supply markets and relink consumers. The
details and exceptions belong to the sector chapters.

Scenario years and interpolation
----------------------------------

A generated database represents the requested scenario year. When that year
falls between available observations, the relevant data loader interpolates
the scenario series. Reference years, extrapolation and missing-data handling
depend on the variable and transformation; they are not universal settings.
Check the sector assumptions before interpreting an interpolated value as a
directly reported IAM result.

Efficiency changes
--------------------

Relative efficiency adjustment applies the scenario's efficiency change to
the inventory's starting efficiency. Absolute adjustment targets the scenario
efficiency itself. For example, a 10% relative improvement of a 40% efficient
process gives 44%; an absolute target of 50% gives 50%.

For a fixed output, the corresponding energy-input scaling factor is the old
efficiency divided by the new efficiency. Exchange selection, technology bounds
and emissions corrections remain sector-specific. See
:doc:`electricity/generation` for the electricity implementation and
:doc:`electricity/photovoltaics` for PV area and efficiency assumptions.

Relative efficiency series prepared by the general IAM loader are normalized
to 2020 (or the nearest available reference year), converted from specific
energy consumption where needed, and constrained to non-decreasing efficiency.
Ratios after 2020 cannot fall below one; those before 2020 cannot exceed one.
The final ratio range is 0.5–2. Missing ratios are filled along the technology
variable dimension where possible, then with one. These processed series can
therefore differ from raw IAM observations. Absolute-efficiency series and
external PV/battery trajectories use their own preparation rules.

The exchanges affected by an efficiency change depend on the caller:
electricity's general efficiency step scales all non-production exchanges;
transport scales selected energy inputs and all biosphere exchanges; steel
selects energy inputs and fossil CO2; CDR selects energy carriers and excludes
biosphere flows. Consult the sector chapter before interpreting a common
scaling factor as the same physical adjustment.

Markets and production weights
--------------------------------

Scenario production volumes determine technology shares where mapped data are
available. When several inventory suppliers represent one technology,
inventory production volumes can distribute its share among those suppliers.
These are separate weighting steps: an inventory production volume does not
necessarily represent future country-level production.

Units and market boundaries matter. Heat, for example, converts final-energy
inputs to delivered heat before calculating shares. Consequential markets use
production trajectories to identify marginal suppliers, as explained in the
final chapter, :doc:`system-models`.

Regionalization and relinking
-------------------------------

Regionalization creates geographical variants of mapped activities. Relinking
connects their exchanges and downstream consumers to the appropriate available
suppliers. Product, unit, activity identity and geography constrain matching;
regionalization alone does not make an inventory representative of local
technology or resource conditions. See :doc:`regionalization` for fallback
rules and :doc:`/user_guide/updates` for execution dependencies.
