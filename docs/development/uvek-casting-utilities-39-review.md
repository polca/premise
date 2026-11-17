# Required casting utilities and recovered stock

Two close native matches replace incorrect suggestions for the inspected
source-3.9 casting dependency group. Above-100-kW modulating natural-gas heat
uses the native useful-heat service for that boiler class, rather than the
previous below-100-kW suggestion. Both reference positive one MJ useful heat.
The complete native boiler service already includes fuel, efficiency,
auxiliary electricity, infrastructure and emissions; no extra factor is added.
Native geography, efficiency, fuel supply and vintage differences remain.

Quebec medium-voltage electricity uses complete native Canadian medium-voltage
supply as an explicit geographic screening proxy, replacing the Swiss match.
This is a national Canadian mix, not an exact Quebec mix. Provincial generation,
hydro/fossil shares, imports, grid losses and their potentially substantial
impact differences remain unquantified. Native voltage transformation, losses,
network and direct emissions are retained; source losses are not added on top.
Premise's later regional relinking logic is unchanged.

New aluminium scrap reuses the existing exact source-3.9 positive-reference
cut-off-stock authority. The inspected source has production only. Caller
signs remain unchanged, with no virgin aluminium, recycling operation or
recovery credit added. The stock rule and all existing recipe contents stay
unchanged; no new helper, zero authority or source-exchange omission is created.

Brightpath conversion, generic-review and cut-off-stock regressions pass 102
tests. An older pending-profile assertion now includes two previously frozen
source-3.9 residue approvals, while retaining every other held profile.
Fresh/cached Premise conversion and approval-withdrawal checks pass four tests.
Six signed finite LCIs and three independent native/reference comparisons pass
without registering activities, creating databases or processed files, or
changing source/biosphere metadata. The normal generator reproduces all three
resources with canonical biosphere files unchanged. Production and generator
Python modules are unchanged.

A high-precision heat comparison identified an existing source-data precision
difference: SQLite holds 1.04 MJ combustion per MJ useful heat, while the
processed native matrix holds 1.0399999618530273, exactly float32(1.04). The
private comparison therefore uses that stored native coefficient; the actual
conversion factor remains +1 and neither data nor production solvers change.
Three residual-correction steps in the verifier retain the 1e-10 comparison
tolerance. The largest remaining relative difference is 1.302e-16. These checks
establish encoded-model consistency, not source/native impact equivalence.

The actual casting retry now has eleven unapproved operation interfaces:
seven disposal markets, wax, two sodium-nitrite markets and X-ray film. This
is an inspected dependency group, not the total remaining project. Its chemical
branches have additional inspected child interfaces. Full casting and battery
qualification remain unfinished. This group is held locally for the next
five-to-ten-commit push batch; the colleague pin remains the verified published
7c537d9 revision until the next installed-package check.
