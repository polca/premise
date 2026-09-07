Documentation migration and technical audit
=============================================

This audit accompanies the September 2026 restructuring. Its scope is a
code-backed review of documentation; source literature has not been
independently revalidated.

Content migration
-------------------

The former introduction, EXTRACT, TRANSFORM and LOAD chapters now have short
compatibility pages. Their content is organized under Getting started, User
guide, Methodology, Reference, and Development and releases. The versioned
``docs/legacy-links.json`` records the destinations of historical section URLs.

PV inventories and efficiency transformations share a dedicated page. Sector
inventory tables remain available with their associated methodology. Export
formats have separate pages. InventoryStore instructions and historical
performance/validation notes are published and reachable from navigation.

Confirmed and corrected
-------------------------

* Python requires 3.10 or newer in ``pyproject.toml``; the former 3.9 statement
  has been corrected. Brightway extras are documented using their declared
  dependency constraints.
* Public database orchestration is implemented in ``premise/new_database.py``.
  The obsolete ``premise.ecoinvent_modification.NewDatabase`` autodoc target
  has been replaced. The build imports the real package.
* Inventory inspection, transactions and explicit materialization were checked
  against ``premise/inventory_store.py`` and the public NewDatabase methods.
* The sector update order and accepted method names were checked against
  ``NewDatabase.update``. Single-sector calls do not imply a full scenario
  supply-chain update.
* PV inventory selection and transformations were checked against the packaged
  workbooks, solar parameters, metals preservation policies and electricity
  implementation. The duplicated historical efficiency table was replaced by
  the maintained trajectory description. The efficiency equation now labels
  its result as a fraction, consistent with the formula.
* Battery import conditions and current workbook filenames were checked
  against the constructor registry. External inventories support both Excel
  and CSV files, as checked in ``AdditionalInventory``.
* The final-energy description now reflects the implemented heating-dataset
  regionalization rather than implying an economy-wide demand recalculation.
* Existing sector inventories, numerical tables and source citations were
  retained during relocation. Relocation is not a claim that every historical
  observation has been independently reproduced.

Unresolved implementation and source questions
------------------------------------------------

* A self-contained ``LegacyInventoryStore.transaction`` example raises
  ``AttributeError`` because its dictionary-backed exchange state has no
  ``shallow_copy`` method. The same query/edit example is tested with
  ``CompactInventoryStore``. The constructor currently defaults to ``legacy``;
  documentation examples explicitly select ``compact``. This runtime issue
  is recorded rather than changed in a documentation refactor.
* The PV source workbook mentions annual degradation but uses lifetime times
  annual yield in its coefficients. The integration preserves those numbers;
  interpretation of the yield convention still requires author clarification.
* The inherited Global Solar Atlas values have no recorded retrieval date.
* IAM comparison tables describe broad model characteristics. They are not
  automatically derived from the exact bundled scenario files; model-version
  and scenario-specific coverage should be checked before use in a study.
* Historical performance and consequential validation appendices describe
  specific runs. Their dataset counts and timings are not current guarantees.
* External literature, licensing statements and changing third-party software
  compatibility have not received a new external-source review in this task.

Verification
--------------

The documentation check validates internal links, assets, historical anchors,
page hierarchy, navigation reachability, API output and Python example syntax.
The real-theme Sphinx build must pass with warnings treated as errors.
Credential-dependent example execution and external-link results are reported
separately from these deterministic checks.

Example execution status
--------------------------

The self-contained compact InventoryStore example passes its query, immutable
snapshot and atomic-edit assertions without licensed inputs. All rendered
Python examples are syntax-checked; partial snippets still require the setup
described on their page.

The complete REMIND SSP2-NPi 2025 example has **not been rerun** for this
documentation migration: neither ``PREMISE_KEY`` nor ``IAM_FILES_KEY`` was
available in the execution environment. This is separate from the earlier
PV scenario validation and is not counted as a passing end-to-end test.

External-link scan
--------------------

The external scan was time-limited separately from the HTML build. Its partial
results include 177 URLs: 82 working, 48 redirected,
36 reported broken or inaccessible, and 11 unchecked. Publisher 403
responses and network failures are not evidence that a citation is invalid.
Stale repository filenames found in this scan were corrected against local
files. Remaining external URLs need source-owner or network follow-up.

See :download:`external-link-audit.csv <external-link-audit.csv>` for the
failure details and disposition. The snapshot records the URLs as scanned,
including those corrected afterwards; it is not a clean external-link result.

Acceptance checks completed
-----------------------------

* Real-theme HTML build with warnings treated as errors: passed.
* Internal links, assets, navigation and API output: 90 HTML pages checked.
* Historical page/fragment compatibility: 265 anchors checked.
* Python syntax: 59 rendered examples checked, plus the standalone scripts.
* Self-contained compact-store example: executed successfully.
* Negative checks: a missing internal link fails validation; an unknown
  document reference produces a failing Sphinx build after importing Premise.
* Desktop (1440 px) and narrow-screen (390 px) browser inspection: homepage,
  first scenario, photovoltaics, Brightway export and API pages have no
  page-level horizontal overflow. Long API identifiers and parameter lists
  wrap within the reading column.

These checks do not replace the separately recorded credentialed scenario run
or a complete external literature review.

Second editorial pass: reading order
--------------------------------------

The methodology now progresses through foundations, energy supply and storage,
materials and industry, transport, carbon management, assessment and finally
consequential modelling. Shared principles have their own page. The short
system-model choice remains near the start of the workflow.

Electricity now progresses from generation to PV and markets. Biomass precedes
fuel pathways, end-use heating is nested under Heat, and road transport modes
are siblings. The supplier-selection decision tree has moved to Development.
User-guide ordering follows input selection, updates, review and export;
development guidance starts with releases and migration.

The pass also clarifies PV reference-product voltage versus consuming IAM
market layer, relative versus absolute efficiency adjustment, and battery
manufacturing versus storage operation. These are documentation changes;
scenario calculations and source inventory coefficients are unchanged.

Verification for this pass:

* Real-theme Sphinx build with warnings treated as errors.
* 95 HTML pages, 592 historical anchors from both documentation layouts,
  internal links, navigation reachability and public API output.
* Explicit reading-order and methodology-group checks, including Heat
  parentage and the consequential chapter's final position.
* Syntax checks for rendered Python examples and successful execution of the
  self-contained compact-store example.
* Desktop (1440 px) and mobile (390 px) inspection of methodology, electricity,
  Heat, transport and consequential pages; no page-level horizontal overflow.

The existing local preview serves the rebuilt documentation. The credentialed
scenario and external-source limitations recorded above still apply.

Third pass: content, examples and scientific sources
-----------------------------------------------------

Completed on 7 September 2026. The content review records dispositions for
85 canonical pages. This pass adds input preflight checks, reader-oriented
validation and report guidance, worked interpretation examples, generated
configuration tables and explicit source provenance for 17 sectors.

Verification for this pass:

* Real-theme Sphinx build with warnings treated as errors: passed.
* 101 HTML pages, 592 historical anchors and 70 rendered Python examples
  checked, including links, navigation and syntax.
* Generated reference tables and source includes agree with checked-out code
  and configuration; content coverage and example classification checks pass.
* Self-contained examples, preflight failure cases and actual emissions
  scaling edge cases executed successfully.
* The licensed REMIND SSP2-NPi 2025 example completed against the local
  ecoinvent 3.12 cut-off source: 42,754 activities written to Brightway,
  validation passed, and scenario and change reports generated. See
  :download:`example execution status <../examples/status.json>`.
* Desktop (1440 px) and mobile (390 px) browser checks covered preparation,
  validation, reports, scenario coverage, hydrogen, source review and result
  interpretation. None had page-level horizontal overflow. Code blocks
  retain internal horizontal scrolling.

The licensed run supersedes the earlier credential-related limitation for
this documentation pass. It used the checked-out code and local inputs; it
is not a clean-release comparison or an independent LCIA validation.
The PV score comparison in the interpretation guide remains a labelled
historical result, with its original metadata, rather than a newly run LCIA.

Scientific review remains incomplete at the full-text level. The additional
access audit attempted 91 cited URLs: 26 were retrieved, 64 could not be
fetched and one returned a login page. Retrieval does not establish that all
methods or supplements were verified. The 17 sector reviews distinguish
implemented sources, candidate updates and unresolved evidence. See
:doc:`/reference/source-review`, the
:download:`literature search log <literature-search-log.json>` and the
:download:`source access audit <source-access-audit.json>`.

No scientific runtime changes were made in this pass. Candidate inventory
updates and the discovered hydrogen energy-floor scaling issue are recorded
separately in :doc:`scientific-update-backlog`.

Fourth pass: consistent sector explanations
--------------------------------------------

The 25 substantive sector chapters now follow the same seven-part structure:
scope and outputs, inputs and applicability, transformation, markets and
downstream links, assumptions and limitations, worked example and checks,
and sources and inventory details. Landing pages and shared concepts retain
their own structure. Rail freight and shipping now have dedicated chapters;
heat mapping maintenance has moved to Development.

The review distinguishes inventory import, scenario transformation and actual
consumer use. It corrects the steel example's stated shares, battery capacity
versus throughput, the battery density example and outdated handwritten
density tables, duplicated cement instructions, misplaced synthetic-fuel
text, and inconsistent hydrogen loss denominators. Battery density tables
are now generated from the packaged YAML and checked for drift.

Selected claims were checked against the current fuels, battery, transport,
steel, mining and CDR routines and their mappings. In particular, the ammonia
supporting-activity update is distinct from a dedicated ammonia market,
passenger modes have no configured legacy consumer rewrite, and the biofuel
routine does not perform the previously described blanket efficiency scaling.
These are documentation corrections, not scientific runtime changes.

The structural checker verifies all seven sections in order for each listed
sector. The real-theme build, internal links, prior historical anchors,
rendered Python syntax and self-contained examples pass. Desktop (1440 px)
and mobile (390 px) browser checks of heat, PV, ammonia, stationary batteries,
rail freight and CDR show no page-level horizontal overflow.

Illustrative calculations are explicitly distinguished from scenario results.
This pass does not rerun the licensed database build or a full LCIA comparison;
no inventory coefficients or transformation code were changed. The previous
literature-access limitations remain.

Fifth pass: current implementation and process diagrams
--------------------------------------------------------------

Each of the 25 sector chapters now includes a generated SVG process diagram
in its scope section. Diagram definitions and generated files are checked
for consistency alongside the sector structure and reference tables.

Code comparisons corrected the exchange scope of electricity and transport
efficiency adjustments, cement energy and capture rules, metals market
construction, CDR energy floors, and heat cogeneration normalization. The
emissions chapter now describes the regional GAINS-IAM data path, World
aggregation before normalization, and the first eligible exchange per
pollutant and activity. Shared principles explain the implemented relative
IAM efficiency normalization and bounds. Historical migration narratives and
internal maintenance instructions were removed from sector explanations;
compatibility anchors were retained.

Focused executable checks confirmed exchange scaling, repeated GAINS species
handling and World aggregation. The warnings-as-errors Sphinx build and
documentation checks pass: 104 pages, 592 historical anchors and 62 Python
examples. Browser checks found no clipped text in the 25 diagrams or page
overflow on six representative chapters at desktop and mobile widths.

This pass changes documentation and its supporting tooling. It does not
change scientific runtime behavior, rerun a licensed database or establish
independent scientific validation of every inventory source.
