# Published PEM stack binding through mapping data

The already prepared one-kilowatt CLIC PEM stack now has a classification-only
role declaration in `iam_variables_mapping/metals.yaml`. Its positive one-unit
reference is checked by the existing role-binding function. The existing metals
selector consumes that explicit binding alongside the exact native selector;
it does not infer a published role from native names.

The declaration has no IAM aliases and requests no additional inventory root.
It classifies the stack only when it is already prepared. Its capacity basis
is the existing 0.001 MW per unit; material intensities, physical inventories,
source conversion, uncertainty and unsupported-year guards remain unchanged.
The requirements ledger adds only this mapping-file association.

A replay of the actual prepared physical record and its provenance passes the
existing reference/binding checks, and both native and published stacks then
match. All 64 existing requirements, hybrid-planning and activity-map tests
pass; documentation references regenerate and check cleanly. This adds no Python
change or upstream process. An installed metals update/export remains required.
