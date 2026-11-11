# Preserve zero-valued uncertainty fields during supplier relinking

Exhaustive validation of a controlled REMIND solar-heat update found twelve native boiler clones with lognormal uncertainty but no `loc`. The original input amount was one, whose valid lognormal location is `log(1) = 0`. Supplier relinking calculated that location and then dropped it because its assignment tested truthiness.

The two shared relinking paths now distinguish `None` from numeric zero for location, scale and bounds. Eight condition changes preserve existing calculations, quantities, supplier selection and distribution types. This also retains a zero lower bound for uniform distributions and a zero scale where supplied. No uncertainty is inferred from missing fields and validation requirements remain unchanged.

Synthetic regressions exercise a one-unit lognormal exchange, zero scale and zero uniform lower bound through actual supplier relinking. The transformation suite passed 41 tests. This generic Python change is necessary because YAML aliases cannot correct a parameter discarded after the inventory has been cloned and relinked. The separate solar mapping qualification remains subject to full graph validation and export.
