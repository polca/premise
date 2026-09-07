Documentation conventions and checks
======================================

Audience and placement
------------------------

Getting started contains complete first-run instructions. The user guide
answers operational questions. Methodology explains inventories and scientific
assumptions. Reference contains catalogues, schemas and public signatures.
Development contains implementation details and dated historical evidence.

Give each subject one authoritative home. FAQ answers and release notes should
link to it rather than repeat its current specification.

Methodology progresses from shared principles through sectors and assessment
to consequential modelling. This reading order differs from update execution
order. Group chapters with captioned toctrees; avoid empty intermediate pages.
Keep end-use heating under Heat and transport modes as sibling pages.

Sector chapter structure
--------------------------

Substantive sector chapters follow this order:

#. **Scope and outputs**: changed activities, reference products, units and boundaries.
#. **Inputs and applicability**: source inventories, scenario variables, external
   assumptions, update entry point and missing-data conditions.
#. **Transformation**: operations, equations, selected exchanges and limits.
#. **Markets and downstream links**: weighting, suppliers and consumer routing;
   explicitly state when no market or replacement operation exists.
#. **Assumptions and limitations**: proxies, preserved coefficients and boundaries.
#. **Worked example and checks**: a labelled illustration and what to inspect in
   the resulting inventory or report.
#. **Sources and inventory details**: provenance and detailed inventory catalogues.

The length of each section should reflect the transformation. Landing pages,
shared concepts and consequential modelling use their own explanatory structure.
The substantive-page list is maintained in ``methodology-structure.json`` and
checked automatically for missing, duplicate, empty or out-of-order sections.
This is an editorial check, not a scientific validation.

Distinguish import-time availability from scenario transformation and actual
consumer use. Identify whether each changing value comes from the IAM, an
external trajectory, or a fixed inventory assumption. Describe unchanged
exchange classes when that distinction is necessary to explain the operation.
Use one operational setup link rather than repeating complete initialization.
Keep developer maintenance instructions in Development; link from Methodology.

Every substantive sector scope includes an SVG process diagram. The reviewed
steps and data-availability conditions live in
``docs/methodology/process-diagrams.json``. Regenerate the portable figures
with ``python scripts/generate_methodology_diagrams.py``. CI checks generated
files and scope placement. Diagram arrows describe the current transformation;
keep optional steps and skip conditions consistent with code. Do not add
migration narratives or review-status tables to the public sector explanation.

Headings and links
--------------------

Use one page title (``=``), sections (``-``), subsections (``~``), and only when
needed a fourth level (``^``). Split a topic before adding a fifth level.
"Key outputs" is a sibling of transformation details, not their parent.
Use sentence case, preserving dataset names and public API identifiers.

Use explicit labels and ``:ref:`` for important sections and ``:doc:`` for
pages. Use root-relative paths for shared figures and downloads. Keep global
navigation three levels deep, with a shallow local contents list on long pages.

When moving an existing topic, update ``docs/legacy-links.json`` and retain its
old page/fragment as a useful link. Legacy landing pages are deliberately
excluded from the main navigation, but remain available to existing bookmarks.
Do not remove compatibility anchors as part of a content cleanup.

Examples and assumptions
--------------------------

Use explicit imports and the modern Brightway interface in current examples.
The first-scenario example uses ecoinvent 3.12 cut-off, REMIND SSP2-NPi and
2025. Explain prerequisites and mark partial examples or illustrative schemas.
Keep secrets in the environment, never in source or rendered examples.

Check behaviour against the current implementation and packaged configuration.
Distinguish regression results from independent scientific validation and
historical measurements from current guarantees. Report unresolved issues in
:doc:`documentation-audit`; do not silently change modelling assumptions.

Build and check
-----------------

From the repository root, in a dedicated Python environment:

.. code-block:: console

   python -m pip install -e ".[docs]"
   python -m sphinx -b html -W --keep-going docs docs/_build/html
   python scripts/check_documentation.py docs/_build/html
   python scripts/check_documentation_examples.py
   python scripts/generate_documentation_reference.py --check
   python scripts/generate_methodology_diagrams.py --check
   python scripts/check_documentation_content.py

For a release, review defaults and import conditions against code, regenerate
the reference tables, and update page dispositions in ``content-review.json``.
Review sector source years, the scientific backlog and unresolved citations.
Record which examples actually executed, including their inputs and date.
Do not infer scientific approval from a successful link or build check.

Run ``python scripts/generate_documentation_reference.py`` after intentionally
changing configuration or source metadata. CI checks the generated results
without importing an ecoinvent database. The isolated constructor-selection
evaluation generates a selection table; it is not a substitute for importer
or scenario validation.

The build imports the real package to validate API targets. No ecoinvent data
or IAM credentials are needed for the build. Third-party imports may create
local logs; use an isolated environment and user-data directory in CI.

External URLs are checked separately because access controls and transient
server failures must not make every documentation change fail:

.. code-block:: console

   python -m sphinx -b linkcheck docs docs/_build/linkcheck

Read the output to distinguish broken URLs from authentication, rate limiting
and network failures. Do not suppress a broken internal link to obtain a green
build.
