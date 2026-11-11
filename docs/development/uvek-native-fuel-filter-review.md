# Native fuel suppliers in the shared update

The real UVEK REMIND SSP2-NPi 2050 fuel callback reproduced three successive failures: no gasoline supplier in CAZ; no wood-bioethanol supplier in CAZ; and native diesel refineries available only in CH/RER. After those filters were extended, the callback reached a missing soybean-biodiesel supplier in CAZ. This is a sequence of missing bindings/regionalization, not evidence that the IAM lacks those fuels.

Changes are limited to the existing YAML mappings:

- `fuels.yaml` binds native refinery petrol/kerosene, native wood ethanol and native soybean methyl ester to their existing IAM roles. Existing CLIC bindings supply the other reviewed technologies.
- `liquid_fuel_activities.yml` includes the exact native petrol/diesel/kerosene refinery identities in the ordinary shared regional-copy stage. Storage, blending and combustion are not regionalized as refinery production.
- `biofuels_activities.yml` places native wood ethanol and soybean methyl ester in the existing wood and soybean stages. Premise continues to choose regional templates, relink their inputs and select feedstock-specific suppliers through its existing code.

The wood-ethanol quantity basis was checked against primary installed metadata. Both the native Swedish distillery process and the original ecoinvent 3.12 wood-ethanol processes represent kg dry ethanol in 95% aqueous solution. Both exclude dehydration. The mapping therefore preserves the existing 26.5 MJ/kg dry-ethanol specification without concentration scaling. The initially considered 99.7% distillation product was rejected because it adds dehydration outside the original boundary; it is explicitly excluded by tests.

Soybean methyl ester is a final ester product, not soybean oil or its separately allocated glycerine co-product. Its native economic and carbon-balance allocation remain unchanged. Geography, feedstock/technology and vintage proxies outside original coverage have unquantified score effects; no equivalence with another allocation model is claimed. Native petrol and diesel likewise retain the allocated refinery inventories under normal regional relinking.

Metadata and quantity witnesses are in `uvek-native-fuel-basis-review.json`. Nine native supplier LCI checks are recorded separately in `uvek-native-fuel-lci-verification.json`. Filter tests cover both list and indexed inventories and protect default ecoinvent family selection; 34 mapping tests and 159 existing fuel energy/carbon tests pass.

These bindings do not yet qualify public fuel support. The diagnostic public-sector guard bypass remains confined to a private reproduction script. Full callback, validation, consumer relinking, standard public invocation and export still require verification. In particular, delivered-gas MJ references and burned-fuel service boundaries need checking before fuel markets can be qualified.

The final real diagnostic run passed those liquid-fuel supplier failures and then stopped during natural-gas market annotation: `Fuel 'natural gas' requires 'cubic meter', received 'megajoule'`. The gas quantity/boundary remains unresolved; validation was not disabled and no fuel export is claimed.
