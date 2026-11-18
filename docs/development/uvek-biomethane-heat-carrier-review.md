# Classify delivered biomethane without adding a production technology

The combined run captured 191 inventory-based heat conversion cases. Twelve
regional copies of one CLIC biomethane boiler had the same error as LPG: its
already-purchased kg-based delivered gas was not classified, so the unchanged
equation fell back to pumping electricity alone. The other 179 cases passed.

Add a classification-only mapping entry for that published delivered biomethane
supply. It copies the existing methane-from-biomass nominal LHV of 47.5 MJ/kg,
CO2 property and biogenic share. Explicit role binding requires a positive
one-kg reference. The entry has no IAM aliases and its market-supplier list is
empty. It therefore classifies the existing input without adding an IAM
technology, importing an extra delivery root for REMIND fuels or treating a
fuelling station as another gas producer. The original amine-scrubbing
production candidate remains unchanged.

The same 191 captured post-fuels cases now pass: the 179 earlier values are
unchanged and the 12 biomethane cases give about 1.1065 delivered MJ per nominal
fuel MJ. All original inventory exchanges remain unchanged. No new efficiency,
density, loss, supplier, fuel demand, physical helper or credit is introduced.
Purity, pressure, delivery, geography, vintage and nominal-property differences
remain screening limitations. The complete combined run is still a separate
requirement; this local proof does not qualify a sector or export.
