Explicit LPG supply and vehicle proxies
---------------------------------------

The refreshed medium LPG car and four LPG trucks retain CNG vehicle manufacture,
including the tank, efficiency, maintenance and non-CO2 combustion factors as
documented proxies. Fuel purchases preserve modelled energy using LPG's approved
45.5 MJ/kg lower heating value and 0.0631 kg CO2/MJ full-oxidation convention.
CNG pump-to-tank methane leakage is excluded; combustion methane is retained.
These remain proxies rather than independently modelled LPG vehicle designs.

The ``vehicle lpg policy`` metadata records the baseline service unit, fuel
energy, original model and proxy boundary. Validation reads the final fuel
exchanges directly to check the actual supplier name, product, geography and
carbon/energy basis. Valid supplier properties take priority. Otherwise the
explicitly approved fossil LPG fallback retains zero non-fossil tailpipe CO2.
A vehicle located in RER does not imply an RER or EUR fuel supplier.

In REMIND SSP2-NPi the five services retain the ecoinvent LPG market for Europe
without Switzerland in both 2020 and 2050. There is no mapped REMIND LPG vehicle
efficiency trend or linked scenario LPG blend; 2020 operating energy is retained.
The older generic truck comment claiming scenario fuel supply does not apply
to these LPG services. This gap remains explicit in the documented policy.

The policy does not disable supported LPG pathways. For example, TIAM-UCL has
explicit LPG vehicle mappings. Their recorded energy adjustments are allowed,
and any linked fuel-market properties determine fossil/non-fossil CO2. A declared
vehicle-efficiency mapping and scenario fuel-blend coverage are separate facts.

Validation recomputes the final purchases instead of trusting stored totals.
It rejects accidental CNG or other fuel substitution, missing suppliers, wrong
units, unsupported energy adjustments, inconsistent CO2 and reintroduced CNG
leakage. Forwarding activities are excluded from direct-fuel accounting.
The fallback policy and its validation do not resize tanks or alter the approved
baseline inventory quantities.
