Vehicle and component provenance
--------------------------------

The refreshed car and truck workbooks preserve ``source``, ``model``,
``model version``, ``model publication`` and ``vehicle inventory provenance``
through Excel import and Brightway export. The structured provenance is JSON
in the workbook. It identifies the original model and shared-library version,
recorded source revisions and file hashes, baseline year, climate, cycle,
usage assumptions, proxy decisions, generation date and metadata-update date.
The publication describes the model; recent corrections are identified by
source hashes and are not claimed to have appeared in that publication.

The passenger-car model reference is Sacchi et al. (2022),
`When, where and how can the electrification of passenger cars reduce greenhouse
gas emissions? <https://doi.org/10.1016/j.rser.2022.112475>`_. The truck model
reference is Sacchi, Bauer and Cox (2021),
`Does Size Matter? <https://doi.org/10.1021/acs.est.0c07773>`_. Bundled components
retain their own activity-specific sources. A model citation does not replace
the literature source for a fuel cell, tank or material recipe. Missing
component references are stated explicitly instead of being inferred.

Original-model specification comments and ``baseline vehicle parameters``
describe **2020 vehicles**. After transformation, ``premise vehicle provenance``
separately records the IAM, pathway, scenario year, actual dataset location,
reference production, final operating-energy purchases and their supplier
locations/units, efficiency adjustments and any scenario battery specifications.
Forwarding services are identified as such and do not claim direct fuel use.
The original provenance stays unchanged on regional clones. Combustion and
hydrogen tanks retain their baseline design under the current policy.

FCEV baseline metadata records total vehicle power, gross fuel-cell power,
its gross/net multiplier and complementary battery power, in kW. The car
pack uses the corrected cell fraction of total pack mass. Truck buffers retain
the model's 20 kWh plus 6% of hydrogen chemical energy assumption. Their
recorded battery power is a requirement; the inherited lead-acid specific-power
input does not validate lithium-ion traction-cell capability. This limitation
is explicit in service and production comments. The update does not resize
FCEV buffers or hydrogen tanks for future scenarios.

The ``lci-pass_cars.provenance.json`` and ``lci-trucks.provenance.json`` companion
files identify the installed workbook hashes and retain earlier provenance.
The corresponding ``*.parameters.csv`` and ``*.noise.csv`` files preserve the
full generated parameter tables and excluded direct-noise flows. Their hashes
are recorded in the provenance. Noise is not included in the imported inventory.
The raw model export is identified by hash; the private audit retains that
export without distributing the full background inventory.

``lci-truck-legacy-components.xlsx`` retains 26 original compatibility recipes
on their ecoinvent 3.7 import basis. Their original sources, descriptions and
remarks are preserved under ``inventory provenance``. They are not labelled as
newly generated carculator_truck inventories. The three workbook database
headers match their respective filenames, and service/production identities
and supplier links are preserved.
