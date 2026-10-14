2020 vehicle inventory campaign: completed
===========================================

The fixing campaign was closed on 14 October 2026. The installed passenger-car
and truck workbooks, parameter/noise companions and provenance records contain
the completed corrections. Bus and two-wheeler workbooks retain their previous
inventories. Rebuild scenarios to apply changes; existing saved databases are
unchanged. No new release or version increment is implied by this closeout.

Completed corrections
-----------------------

* Current 2020 car defaults and EURO-6 coverage; Regional-delivery, 400 km trucks
  with EURO-VI coverage. Swiss climate and explicitly documented unsupported-
  powertrain proxies are retained.
* Full physical PHEV capacity and replacement purchases; manufacturer-based car
  PHEV capacity/mass calibration; high-power HEV truck buffers; consistent FCEV
  electrical allocation, motor proxies and paired battery specifications.
* Regional fuel-energy accounting, documented delivered CNG heating value,
  regional CNG compression, final-supplier fossil/non-fossil CO2, and hydrogen
  tank compression that scales with hydrogen consumption.
* Read-only species-aware pollutant diagnostics, explicit emission drivers,
  corrected hybrid deterioration and source-model evaporative speciation.
* Simple range-preserving BEV/PHEV capacity/mass calculations, with increased
  BEV truck payload at fixed utilization. Optional IAM intensities and the
  approved efficiency-only Large Car fallback retain model-specific mappings.
* Preserved original model/publication/component sources and baseline/final
  metadata through import and Brightway export.

The original model publications are Sacchi et al. (2022),
`passenger cars <https://doi.org/10.1016/j.rser.2022.112475>`_, and Sacchi,
Bauer and Cox (2021), `trucks <https://doi.org/10.1021/acs.est.0c07773>`_. Recent
corrections are identified separately by source hashes and repository commits.
The car FCEV pack source is
`Toyota's 30 November 2020 Mirai announcement
<https://newsroom.toyota.eu/introducing-the-all-new-toyota-mirai/>`_. The truck
FCEV pack uses the explicitly approved 2020 proxy from the
`AKASOL May 2021 specification
<https://www.act-news.com/wp-content/uploads/2020/11/210525_LowRes_AK_Datenblatt_AKASystem_15-OEM-37-PRC_2021_2159x2794mm_EN.pdf>`_.

Verification boundary
------------------------

The final REMIND SSP2-NPi / ecoinvent 3.12 cutoff builds contain 42,564 activities
in 2020 and 42,795 in 2050, with zero validation errors and unchanged 401/468
warning identities. Fuel-energy/carbon, battery allocations, future hardware
and saved metadata pass. Each saved database passes 968 operating-energy link
checks. Focused regression and installed-artifact/native-LCIA checks accompany
the changes; these do not establish a new cross-platform release qualification.

The final car-buffer incremental comparison covers 224 paired EF 3.1 scores
(eight RER and six EUR services, sixteen headline indicators). Climate changes
are +0.003--0.153%. The largest carcinogenic-toxicity decrease (2.55%) is driven
by the smaller nominal-kWh purchase in the retained MIX market. It is not a
verified manufacturing improvement for the physically heavier pack.

The original-legacy-versus-final full-catalogue comparison was not completed.
Closure does not claim that comparison or convert incremental results into it.
The `campaign guide and retained reports
<https://github.com/Laboratory-for-Energy-Systems-Analysis/carculator_utils/blob/master/docs/premise_vehicle_update.rst>`_
record the completed checks and reconstruction workflow.

Retained limitations
----------------------

* MIX capacity markets remain approved proxies where physical battery mass or
  chemistry differs. Initial/replacement kWh and disposal are counted once.
* Car FCEV charging and continuous-output limits remain unverified. Truck pack
  ratings assume a conditioned pack; neither calibration validates SOC,
  fuel-cell ramp, electrothermal behavior or revised ageing.
* The derivation of retained high truck hot-NOx coefficients could not be
  reconstructed. They are not silently clipped to regulatory limits.
* The broader PHEV cycling review and future FCEV tank resizing were skipped.
  Missing IAM signals and explicit fossil LPG fallback remain documented.

These are accepted limitations at campaign closure, not an active fixing queue.
