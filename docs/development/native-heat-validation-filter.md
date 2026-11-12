# Include native heat names in physical validation

The legacy heat-efficiency check used a case-sensitive `heat` substring. Native names such as `Heat, natural gas, at boiler ...` were therefore skipped unless another part of their name happened to contain lowercase `heat`. Graph validation and valid market shares do not establish that this physical check ran.

The check now lowercases a local copy of the dataset name for eligibility and its existing exclusions. Original dataset identities, coefficients, thresholds and ecoinvent matching remain unchanged. Heat pumps, allocated heat and frozen portfolios retain their existing boundaries; this does not infer physical efficiency from allocated fuel draw.

Five synthetic cases cover an invalid native boiler, its existing lowercase counterpart, a valid native boiler, a heat pump and allocated cogeneration. The heat/filter group passed 59 tests. A separate diagnostic of the actual exported graph records the added eligible native records and confirms no newly detected physical issues. Whole real-scenario qualification is repeated from committed source, rather than inferred from this filter test alone. Mapping-only edits cannot fix a validator's case-sensitive string comparison without renaming native dataset identities.
