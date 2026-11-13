# WEEE shredding and Euro V freight: source-3.9 reviews

Two exact source-3.9 correspondences are independently reviewed. WEEE uses the
existing complete native shredding process. Freight now uses a helper containing
nine native Euro V services, preserving the requested emissions standard. The
unpublished all-standards fleet-average draft was withdrawn; it has no active
Premise grant. No runtime Python module changes or source-exchange omissions
are introduced.

WEEE shredding is a service per kg. Source reference is negative one kg of waste;
native reference is positive one kg treated. Factor -1 converts a negative waste
input to positive native treatment demand once. Both processes describe two
shredders, two magnetic-separation stages and two eddy-current stages, with the
same 8e-10 facility-unit coefficient. All 15 source direct air-emission amounts
match native2026 by publisher UUID, full receiving compartment and unit.

The operating model differs: source consumes 0.066 kWh of global medium-voltage
electricity; native consumes 0.02 kWh of Swiss supply and includes 0.014535 tkm
Swiss rail and 0.028215 tkm Swiss 16-32t lorry freight. Native also reports
0.2376 MJ waste heat absent from source. These operating, upstream, geography,
vintage and internal-uncertainty changes have unquantified score effects. Keep
the complete native kg service once, without electricity-ratio, heat-balance,
lifetime or capacity rescaling, duplicate electricity/facility/freight, extra
waste treatment or invented recovered-metal credits.

The Euro V lorry market contains one share of a complete RER transport service
per tkm. A broader search found native Euro V models under Roman-numeral names.
The final helper uses only diesel Euro V: 18t, 26t and 32t rated gross weights,
each with long-haul, regional and urban delivery. All nine complete suppliers
have positive one-tkm references in native2025 and native2026.

Weights come from the Euro V portion of the existing native 16-32t fleet,
normalized within that subset. Its size and journey coefficients are identical
in the inspected native releases. This normalizes native proxy evidence; the
source caller's tkm stays unchanged. The resulting size shares are approximately
20.49% for 18t, 28.55% for 26t and 50.96% for 32t. Journey shares are 80% long haul,
10% regional delivery and 10% urban delivery. No Euro III/IV/VI, electric or hybrid
supplier is included.

The source provides only the class-average Euro V service, without those size
and journey shares, so the native Swiss2020 distribution remains a screening
proxy. Native RER labels retain Swiss data/model provenance. Source European/
German HBEFA3.1 factors, loads, routes, SCR/EGR mixture, upstream fuel,
infrastructure, vintage and internal uncertainty can differ. Technology-year2010
labels identify the available native Euro V models; this is not an IAM fleet
forecast. Keep complete vehicle/road/operation/maintenance/end-of-life services
once, without payload, distance, fuel, emissions-ratio, lifetime or capacity
corrections or duplicate infrastructure. LCI, LCIA and stochastic equivalence and
quantified score effects are not established.

Both mappings retain signed caller quantities, supported uncertainty/formulas,
foreground ownership and separately listed caller emissions/recovery credits.
Only source-3.9 cut-off is approved; other versions/patches, reverse or
cross-system-model routes gain no permission. The old fleet-average draft remains
in the frozen evidence history, explicitly superseded by the Euro V refinement.

The Brightpath mapping/storage run passes 29 cases; eight follow-up profile guards
cover both services, overlapping four earlier guards. Thirteen Premise fresh/cache,
sign, refusal and prior approval-drift cases pass. Eleven further cases pass
against the verified final installed wheel. Its unused and superseded wheel
copies were removed after verification. Restart a notebook kernel that still
has older migration resources cached. Four signed transient LCIs over
two consumers and one freight helper are finite and reverse correctly. Two
independently flattened native-demand comparisons agree after unchanged-matrix
numerical refinement, max absolute difference 1.43e-14 at unchanged rtol=1e-8/
atol=1e-11. Ten native suppliers are bound. No database, activity or processed
file is created; source/biosphere metadata and registered names are unchanged
by these checks. These prove interfaces and point-estimate consistency, not
source/native inventory equality.

A final fresh battery retry using the Euro V refinement clears both rules:
173 becomes 171 unreviewed rules in this exact 3.9 cohort, with none newly added.
This is not a dataset count or all other source profiles. Battery preparation/
update/export remain unfinished. Next is an incorrect unmounted LCD to mercury
correspondence. Eight of 19 sectors remain qualified. See
[verification](uvek-weee-freight-39-verification.json).
