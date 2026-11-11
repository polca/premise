# Combined five-sector export: technical success, fuel relinking unfinished

On 11 November 2026 the real REMIND SSP2-NPi 2050 scenario completed biomass, electricity, heat, cement and fuels together through the shared transformations and the default persistent update/export workflow. It wrote `BAFU:2026-remind-SSP2-NPi-2050-five-sector-review-20261111-141957` in the existing Brightway project. The source is BAFU:2026-reimported with ecoinvent-3.10-biosphere; the export contains 14,705 activities.

The public UVEK sector support guard was bypassed only in the private diagnostic script. It remains unchanged in production because the consumer audit below contradicts full fuel integration.

The written database contains 156 regional/World fuel markets across 12 market identities, including petrol, diesel, kerosene, gas and hydrogen. Every exported technosphere provider resolves internally. Eight EUR/World petrol, diesel, kerosene and high-pressure-gas inventory calculations are finite. The eleven recorded pre-export validation phases have no errors, with 14 electricity warnings and 39 unresolved fuel-carbon-origin warnings retained. Export also passed its validation gate. Exact counts and references are in `uvek-five-sector-fuel-export-verification.json`.

The native consumer audit follows only the purchased fuel carrier through forwarding nodes, rather than counting a path through unrelated energy inputs as fuel blending. Among the inspected original-native purchases it finds 1,174 diesel, 4,189 gas, 50 kerosene and 94 petrol purchases. None reaches the newly generated IAM fuel mix. These counts include source fuel/energy supply activities; they are not an assertion that every purchase should be redirected indiscriminately. Source and regional supply inventories need explicit protection from recursive market links.

Thus preparation, callback and export now work technically, but fuel consumer relinking is unfinished. Next work must declare the native delivered-fuel market identities and their exact reference-unit handovers, reuse shared market/forwarding behavior, preserve consumer quantities and uncertainty, and protect complete fossil supply prototypes before redirecting delivery markets. Mixing native gas MJ with canonical market m3 must retain the reviewed 36 MJ/m3 basis. No unit check, carbon warning or consumer-coverage requirement is relaxed.

An earlier diagnostic export failure was caused by the inspection script checking out the mutable compact inventory before export in a nonpersistent run. That read is a mutation-oriented internal API; it removes the active store. The normal persistent workflow exports successfully. No production exporter modification was made for that diagnostic artifact. Future reproduction should export first and inspect the written database.

The remaining registered sectors and multi-geography electricity prototypes remain part of the active goal.
