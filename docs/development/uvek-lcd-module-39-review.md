# Unmounted LCD display mapping review — 13 November 2026

The source production and market mappings incorrectly selected liquid mercury. Both now use the complete native **LCD module, at plant**, per signed kilogram, for the inspected ecoinvent 3.9 cut-off profile. The market retains its original rail and lorry delivery quantities and uncertainty.

The native module contains the glass, backlight, electronics and linked module assembly. The distinct native LCD screen assembly inventory is not a substitute for this component. Seven bounded component-mass checks support the module match; upstream geography, board mixes, allocation, vintage, internal uncertainty and native biosphere exclusions still differ. This is a documented screening proxy, with unquantified score differences. No mass-per-screen conversion, duplicate assembly, new omission or runtime Python change is introduced.

Standing autonomous modelling approval covers this decision. Two independently scoped rules were added; all previous approval operations remain unchanged. Other source versions and consequential profiles remain unapproved.

Verification: 32 distinct Brightpath mapping/storage checks passed across the initial run and corrected destination-count rerun; ten Premise checks passed, including cache, signed uncertainty and refusal without approval. Four signed LCIs and two independent native-demand comparisons passed entirely in memory, without new Brightway databases, registered activities or processed files. The maximum refined inventory difference was below 1e-15.

A fresh battery preparation attempt clears the LCD market. This inspected source-3.9 cohort falls from 171 to 170 unapproved rules; the next is Swiss waste polyvinylfluoride. Production was tested independently but was not pending in that cohort. Battery preparation and sector qualification remain unfinished. Eight of nineteen sectors remain qualified for the selected scenario.

Machine-readable evidence: `uvek-lcd-module-39-verification.json`. Packaged decision: Brightpath `export/uvek_lcd_module_39_review.json`. Changes are queued for the next coordinated batch push.
