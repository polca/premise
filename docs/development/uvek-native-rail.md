# Native rail services and the remaining hydrogen-train dependency

The real REMIND SSP2-NPi 2050 scenario uses electric, diesel and hydrogen trains.
Electric and diesel rail can use existing complete BAFU services. This reduces
the required CLIC root datasets from 18 to six, all hydrogen trains. Those six
are distinct geographic inventories (Europe without Switzerland, India, China,
South Africa, United States and rest of world), not duplicate database versions.
Their active IAM role is retained.

The native electric filter selects the complete electricity-only services for
RER, AT, DE, FR and IT. Each has a positive one tonne-kilometre reference and
includes locomotives, wagons, tracks, maintenance and disposal. It excludes
diesel shunting. The native diesel filter selects the complete CH-labelled
service with particle filter, also per positive tonne-kilometre. Its documentation
uses Austrian traction consumption because Swiss traction data were unavailable;
its existing shunting and infrastructure remain included. These allocation,
fleet, operating, vintage, geography and shunting differences are explicit
screening limitations with unknown score effects, logged under the user's
standing autonomous approval. Do not add a second vehicle, track, load factor or
shunting inventory.

The ordinary shared `Transport` class handles both technologies. Native energy
aliases preserve the delivered diesel interface and the reviewed railway
medium-voltage grids; they also recognise ordinary Premise medium-voltage markets
after an electricity update. The existing shared consumer relinker now recognises
the complete native generic `Transport, freight, rail` interface. All six inspected
native country interfaces have positive one tonne-kilometre references. The
relinker preserves signed quantities and uncertainty and keeps its existing
exclusion for transport consumers and wrong service units. Operating-only and
infrastructure-only datasets are excluded from the complete-service filters.
No production Python changes were needed.

The initial native transport regression selection passed 28 checks. A further
interface selection passed ten checks, overlapping earlier tests and adding one
new case: 29 distinct cases passed. Complete scenario relinking and energy checks
remain pending because inventory preparation still stops on the hydrogen path.

That first failing ecoinvent 3.12 cohort contains **20 distinct unapproved
correspondences**, down from 27 before native electric/diesel aliases. This is a
cohort count, not the total remaining project work. The first candidate incorrectly
matches barium carbonate to lithium carbonate; it remains unapproved. Other cases
include native material deliveries and previously reviewed helper recipes that
need their 3.12 context checked. Required hydrogen trains are not silently dropped
or represented by electric/diesel trains. Identities and recipe references are
listed in `uvek-native-rail-verification.json`; no source inventory vectors are
published.

The public rail gate stays closed. The full rail update, real consumer redirection,
physical quantity checks, finite LCIs and combined export remain unfinished.
Overall qualification remains eleven of nineteen sectors.
