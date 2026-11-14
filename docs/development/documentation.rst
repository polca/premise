Documentation conventions and checks
======================================

Audience and placement
------------------------

Getting started contains complete first-run instructions. The user guide
answers operational questions. Methodology explains inventories and scientific
assumptions. Reference contains catalogues, schemas and public signatures.
Development contains implementation details and contributor workflows.

Give each subject one authoritative home. FAQ answers and release notes should
link to it rather than repeat its current specification.

Methodology progresses from shared principles through sectors and assessment
to consequential modelling. This reading order differs from update execution
order. Group chapters with captioned toctrees; avoid empty intermediate pages.
Keep end-use heating under Heat and transport modes as sibling pages.
Overview pages should introduce a subject and route readers to its chapters.
Put application-specific details under the relevant application; give recipes
used by several applications an explicitly named shared reference page.
Keep component recipes within their parent technology rather than beside
whole technologies in navigation. Catalogue pages should link to those
recipes rather than duplicate them.

Writing for readers
---------------------

Begin with the task or the modelling question, then explain what the reader
needs and what happens next. Define specialist terms before using them; link
to :doc:`/reference/glossary` for shared definitions. Prefer short sentences,
concrete verbs and one main idea per paragraph.

Keep the normal workflow visible. Put optional branches after it and move
storage internals and contributor workflows in Development. Explain source
recipes and scientific assumptions in Methodology. One-off maintenance
commands, test receipts and investigation diaries do not belong in the
published documentation. Avoid repeating complete
setup examples on every page. Label snippets that need an existing ``ndb`` or
user-specific paths, and give the setup link once.

Use shallow, task-based contents lists. Each page should have a clear purpose
and a useful next step, without requiring readers to follow several links just
to understand the paragraph in front of them. Keep scientific boundaries,
quantities and limitations when simplifying the prose. Describe current code
behaviour rather than the history of fixes or isolated successful test runs.

Sector chapter structure
--------------------------

Substantive sector chapters follow this order:

#. **Scope and outputs**: changed activities, reference products, units and boundaries.
#. **Inputs and applicability**: source inventories, scenario variables, external
   assumptions, update entry point and missing-data conditions.
#. **Transformation**: operations, equations, selected exchanges and limits.
#. **Markets and downstream links**: weighting, suppliers and links to consuming activities;
   explicitly state when no market or replacement operation exists.
#. **Assumptions and limitations**: proxies, preserved coefficients and boundaries.
#. **Worked example and checks**: a labelled illustration and what to inspect in
   the resulting inventory or report.
#. **Sources and inventory details**: data sources and detailed inventory lists.

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
Keep reusable contributor workflows in Development. Do not present repository
maintenance or inventory-generation scripts as steps a database user must run.

Every substantive sector scope includes an SVG process diagram. The reviewed
steps and data-availability conditions live in
``docs/methodology/process-diagrams.json``. Regenerate the portable figures
with ``python docs/_tools/generate_methodology_diagrams.py``. CI checks generated
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

External website links open in a new tab automatically through
``_static/external-links.js``. Links within the documentation keep the current
tab. Do not add individual target attributes to dataset links.

At a dataset's first substantive description, give a prominent direct link to
its published page on `CLIC <https://clic.psi.ch/>`__. Link the dataset name
in the explanatory prose or its inventory-table row. Keep the link with the
relevant description; avoid separate callouts that repeat selected dataset
links. Match the technology,
reference product, location and allocation variant. Link the published source
when explaining a generated scenario variant, and label that relationship.
Use CLIC dataset pages in place of bundled Excel inventory references and
download links. Retain scientific publications and their supplementary evidence
as supporting references.

When moving an existing topic, update ``docs/legacy-links.json`` and retain its
old page/fragment as a useful link. Legacy landing pages are deliberately
excluded from the main navigation, but remain available to existing bookmarks.
Do not remove compatibility anchors as part of a content cleanup.

Examples and assumptions
--------------------------

Use explicit imports and the modern Brightway interface in current examples.
The first-scenario example uses ecoinvent 3.12 cut-off, REMIND SSP2-NPi and
2025. Explain prerequisites and mark partial examples or illustrative schemas.
A worked example must show its inputs, calculation or operation, and resulting
amounts or records. Put that content in the section that promises it. Use an
explicit section reference when an example lives elsewhere, rather than an
unqualified “above” or “below”. Check units, rounding and before/after totals.
Keep secrets in environment variables, never in source or rendered examples.

Use **Python environment** for installed packages (for example, a conda
environment), and **Brightway project** for the collection containing databases.
Write **environment variable** when referring to shell settings such as
``PREMISE_KEY``; avoid using "environment" alone for these settings.

Check behaviour against the current implementation and packaged configuration.
Describe current model behaviour, sources and limitations. Keep internal
review logs, test receipts, cache migration histories and implementation
correction narratives out of the published documentation.

Build and check
-----------------

From the repository root, in a dedicated Python environment:

.. code-block:: console

   python -m pip install -e ".[docs]"
   python -m sphinx -b html -W --keep-going docs docs/_build/html
   python docs/_tools/check_documentation.py docs/_build/html
   python docs/_tools/check_documentation_examples.py
   python docs/_tools/generate_documentation_reference.py --check
   python docs/_tools/generate_methodology_diagrams.py --check
   python docs/_tools/check_documentation_content.py

For a release, review defaults and import conditions against code, regenerate
the reference tables, and check source dates and citations against the sector descriptions.
Run the documented examples with their stated prerequisites.
Do not infer scientific approval from a successful link or build check.

Run ``python docs/_tools/generate_documentation_reference.py`` after intentionally
changing configuration or source metadata. CI checks the generated results
without importing an ecoinvent database. The isolated constructor-selection
evaluation generates a selection table; it is not a substitute for importer
or scenario validation.

The build imports the real package to validate API targets. No ecoinvent data
or IAM credentials are needed for the build. Third-party imports may create
local logs; use an isolated Python environment and user-data directory in CI.

External URLs are checked separately because access controls and transient
server failures must not make every documentation change fail:

.. code-block:: console

   python -m sphinx -b linkcheck docs docs/_build/linkcheck

Read the output to distinguish broken URLs from authentication, rate limiting
and network failures. Do not suppress a broken internal link to obtain a green
build.

Local analysis outputs
----------------------

Generated scores and one-off analysis programs are kept outside version control.
The minimum documentation checks live in ``docs/_tools``; inventory-test helpers
and scientific contracts live under ``tests/support`` and ``tests/data``.
Licensed LCIA references must be supplied locally or through
``PREMISE_LCIA_REFERENCE_SCORES``; score comparisons skip when they are unavailable.
