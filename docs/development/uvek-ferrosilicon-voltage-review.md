# Ferrosilicon electricity supply review

46 electricity correspondences now use complete native low-voltage grids, without creating helper datasets or modifying runtime Python. Exact cut-off profiles were inspected independently for ecoinvent 3.6, 3.9 and 3.12 (138 profiles). Other versions and consequential routes are not approved by this review.

24 correspondences retain the country or region code. This does not establish identical regional membership, generation mix or network losses. The remaining 22 are explicit geographic proxies: YE/OM/BH/SY/LB/JO → RME; UZ/KG/TM/AM/AZ/GE/TJ → RU; LK/NP/KP/MM/KH/MN/BN → RAS; HK → CN; CY → RER. These account for 0.192248359 kWh of the RoW producer’s 8.56 kWh/kg (2.25%). The Chinese producer uses its native CN grid.

All signed caller quantities and uncertainty remain unchanged. Native grid inventories remain intact; no extra loss multiplier is applied. Vintage, generation/trade, geography, regional membership and network assumptions remain screening limitations. Source/native score or loss equivalence is not claimed.

Verification: 19 Brightpath tests, three Premise fresh/cache/withdrawn-approval tests, and 52 finite native LCIs with exact sign reversal passed. No source database was modified. Frozen review SHA256: fa5f985b42703a1e1b6982a26bd07edd7e10238552aa35eb7d09415ced078c68. Brightpath commit: c7b8aaf2841160699495f598abb29bef9a7308f0.

The real ferrosilicon dependency probe decreased from 77 to 31 unapproved correspondences: coal tar (1), hard coal (8), heavy fuel oil (7), low-pressure natural gas (14), petroleum coke (1). The complete ferrosilicon reconstruction is unfinished. This does not clear its preparation root. Steel still has two preparation roots, including industrial heat, and no successful full steel export is claimed.
