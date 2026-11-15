# Functional priorities for BAFU integration — 15 November 2026

The mapping expansion was stopped for this audit. The goal remains all nineteen standard Premise updates. Eight remain qualified; no new sector is called qualified on these diagnostics alone. The recent four mapping commits remain local.

Inventory selection was checked for all eleven remaining sectors. Ten fresh, independent standard-workflow diagnostics were run; the battery result comes from the preceding run against the same unchanged package and mapping state. Each private diagnostic bypassed only the public unsupported-sector gate and kept source, target and graph validation enabled. No Brightway export was attempted.

| Sector | CLIC roots | Retained records | Diagnostic result | First reported mapping group |
|---|---:|---:|---|---:|
| fuels | 30 | 0 | Update and exhaustive graph checks passed | — |
| metals | 51 | 7 | inventory preparation stopped | — |
| battery | 24 | 8 | inventory preparation stopped | 28 |
| cars | 30 | 0 | inventory preparation stopped | 59 |
| two_wheelers | 6 | 0 | inventory preparation stopped | 13 |
| trucks | 20 | 0 | inventory preparation stopped | 90 |
| ships | 2 | 0 | inventory preparation stopped | 3 |
| buses | 4 | 0 | inventory preparation stopped | 114 |
| trains | 18 | 0 | inventory preparation stopped | 29 |
| final energy | 78 | 66 | inventory preparation stopped | 45 |
| emissions | 0 | 0 | Update and exhaustive graph checks passed | — |

These mapping counts cover the first reported source-profile group only. They are not estimates of full remaining effort. A successful graph check does not prove that every eligible process was transformed, that all native supplier filters work, or that export succeeds. In particular, emissions explicitly declares incomplete coverage.

Two concerns raised during the strategy review were checked: generic battery scenario markets are selected for retained-workbook import, and sodium-nickel-chloride capacity records are separate from sodium-ion CLIC records. Their absence from the initial CLIC graph was not a demonstrated bug.

The next work should qualify workflows rather than chase the next refused rule. Fuels and emissions already pass diagnostic graph checks and need coverage/public/export assessment. Ships is a bounded candidate with two roots and three first-group refusals. Shared exact-profile material gaps across transport and final energy deserve priority over battery-only trace auxiliary reconstructions. No technology should be silently discarded and no validation rule should be weakened.

Metals fails on an unresolved elementary-flow identity, rather than an unapproved technosphere correspondence. That distinct failure should be investigated directly.

The highest-overlap reported refusals are:

- aluminium scrap, new, Recycled Content cut-off (GLO): cars, trains, two_wheelers.
- market for polyphenylene sulfide (GLO): cars, trains, two_wheelers.
- market for used Li-ion battery (GLO): cars, trains, two_wheelers.
- air separation, cryogenic (RER): buses, trucks.
- casting, brass (CH): buses, trucks.
- epoxy resin production, liquid (RER): buses, trucks.
- laminating service, foil, with acrylic binder (RER): buses, trucks.
- market for 2-methylpentane (GLO): buses, trucks.

This audit changes the work order, not the completion criteria. See the adjacent JSON receipt for exact diagnostic scope and limitations.
