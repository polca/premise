# Retained workbook resource names

The metals diagnostic reached the retained rhenium workbook with the source
compartment stored as `natural resource::in ground`. The existing canonical-name
lookup treated this entire string as one compartment. It therefore missed the
existing YAML correspondence from `Rhenium, in ground` to `Rhenium`, and strict
source validation correctly refused the old name in the ecoinvent 3.9 catalog.

The importer now splits `::` before looking up the compartment. Lists and tuples
retain their existing interpretation. This does not add a flow correspondence:
the packaged catalog confirms the same publisher UUID,
`a2e6fb74-b047-5697-b5dd-e28cc68f29e6`, for the canonical resource. Units and the
complete compartment must still match. Source quantities, uncertainty, formulas,
comments and source records remain unchanged.

Validation: 31 tests passed in `test_clic_retained_biosphere_names.py` and
`test_clic_uvek_workbooks.py`. Added coverage checks all three compartment
representations, rejects different units/compartments, and exercises cold and warm
strict migration to the ecoinvent 3.10 biosphere. Black and Ruff pass for the
changed files.

The real snapshot diagnostic, using private experimental PGM replacements,
advanced past the rhenium name failure to ordinary unreviewed technosphere
correspondences. This is evidence for the parsing fix, not qualification of the
experimental replacements or the metals sector. No sector engine, output
database or source database was changed for this diagnostic.
