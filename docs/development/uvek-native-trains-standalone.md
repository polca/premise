# Shared UVEK train update: standalone checkpoint

The actual train import now converts all 34 foregrounds into 89 linked records.
Premise's shared update runs successfully and passes exhaustive graph checks.
It creates 39 regional services and thirteen regional/World markets in a graph
of 12,088 activities. All 104 signed inventory calculations are finite. No
Brightway database or processed file was written. This is a standalone result;
combined upstream updates and the public gate still need verification.

All three IAM roles have positive production somewhere: electric and diesel in
thirteen region entries, hydrogen in six (including World). Each service retains
a positive tonne-kilometre reference. Quantity checks compare the exact selected
source prototype, relative IAM fuel scaling and fixed ancillary inputs. The
thirteen markets have normalized nonnegative shares; twelve regional mixes
match IAM technology shares and World uses regional markets. Conditional hydrogen
compression remains proportional to hydrogen demand, preserving the explicit
30-bar scenario and the warning that the underlying native supplier is not
proof of that delivery pressure.

The unchanged common train algorithm scales direct biosphere entries with the
traction-efficiency factor while ancillary shunting purchases remain fixed.
These checks verify that existing convention; they do not establish a corrected
exhaust balance, complete carbon balance or equivalent LCIA scores. Existing
native fleet, electricity-grid, allocation and vintage warnings still apply.

Two reproduced label failures required small generic Python fixes. The CLIC
adapter assumed every hydrogen product started with the ecoinvent label
“hydrogen,”; it now also recognizes exact kg products already declared under
hydrogen roles in the fuel YAML. It does not accept hydrogen chloride or infer
hydrogen from arbitrary process names. Regional relinking then replaced the
native electricity product while the compression contract retained the old
label. The shared compression helper now binds its explicitly marked purchases
to their current unique product. It preserves pressure caveats, quantities and
auxiliary-power exclusion. Mapping changes alone could not fix these failures
without falsely renaming native suppliers or changing their reference products.

The rail YAML also gains the exact native hydrogen fuel selector needed by the
ordinary IAM efficiency adjustment. No NewDatabase argument, alternative sector
engine or sector equation is added. Thirteen adapter tests and 28 hydrogen/rail
tests pass, including original ecoinvent products, compact records, uncertain
compression and separate auxiliary electricity. See the companion verification
JSON. The colleague notebook and default three sectors remain unchanged.
