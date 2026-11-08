# UVEK pilot chemical quantity bases

These checks concern the quantity coordinate of three existing correspondences.
They do not approve a market-to-factory replacement, production-mix proxy,
geographic substitution or chemical grade. Those decisions retain their
independent migration gates.

The inspected source coordinates are ecoinvent cut-off 3.6 for all three
chemicals, plus 3.8 and 3.9 for sodium hydroxide. Each source has a positive
one-kg production reference. Both native UVEK 2025 and 2026 have positive one-kg
references and give the same numerical basis evidence below. The 2026 inventory
is the Brightcon `ecospold2-biosphere310` bundle at revision
`794d90f2c2dbb90f0c56ba8cc527fa1494aa8ef6`.

| Source / rule | Native target | Quantity conclusion |
| --- | --- | --- |
| Sodium hydroxide, without water, in 50% solution / `bp-1d075913a8c7a9b6` | Sodium hydroxide, 50% in H2O, production mix, at plant | Dry-NaOH kg; no factor of 2 |
| Hydrochloric acid, without water, in 30% solution / `bp-91b9cff76b3562bb` | Hydrochloric acid, 30% in H2O, at plant | Dry-HCl kg; no factor of 1/0.30 |
| Calcium chloride / `bp-995a85c7009a9601` | Calcium chloride, CaCl2, at plant | CaCl2 product kg; no extra Solvay co-product-yield factor |

## Sodium hydroxide

The native production mix buys the diaphragm, mercury and membrane cell
processes with shares summing to one. All three cell inventories document
mass allocation of 46.4% to chlorine, 52.3% to sodium hydroxide and 1.3% to
hydrogen. These are dry co-product proportions.

The common sodium-chloride input ratio of each allocated NaOH cell to its
corresponding chlorine cell is approximately 0.99749. Dividing that ratio by
`0.523 / 0.464` implies approximately 0.885 kg of chlorine co-product per
reference kg of NaOH. This is consistent with the dry-NaOH coordinate; a kg
of 50% solution would contain half that NaOH. The concentration in the name
does not justify doubling the incoming source quantity.

## Hydrochloric acid

The native 30% HCl mix buys equal quantities of its synthesis and Mannheim
suppliers. The synthesis supplier consumes approximately 0.975 kg of chlorine
and 0.027 kg of hydrogen per reference kg, consistent with one kg of HCl rather
than one kg of 30% solution. Its positive one-kg reference and the unchanged
mix coordinate therefore support retaining the source dry-HCl quantity.

## Calcium chloride

The CaCl2 record retains a description of the parent Solvay process that mentions
one kg of soda. Its own header explicitly identifies soda and CaCl2 as separate
co-products, allocated 33% and 67% respectively; its production reference is
one kg of CaCl2.

The common allocated sodium-chloride input ratio, CaCl2 divided by soda, is
approximately 1.93362. Combining it with `0.67 / 0.33` gives an implied parent
yield of approximately 1.05 kg CaCl2 per kg soda. The co-product normalization
is already present in the native inventories. Applying another yield factor
to a CaCl2 demand would change its reference coordinate.

## Limits

These are reference-basis checks, not evidence of equal inventories or LCIA
results. They establish no new chemical-production or delivery proxy, omit no
exchange and change no allocation. Formulae, signs and uncertainty must follow
the existing migration rules. The complete local inspection retains inventory
and metadata hashes without committing licensed source exchange vectors.
