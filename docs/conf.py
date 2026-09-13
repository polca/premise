# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# http://www.sphinx-doc.org/en/master/config

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))

# -- Project information -----------------------------------------------------

project = "premise"
copyright = (
    "2026, Paul Scherrer Institut, Potsdam Institute for Climate Impact Research"
)
author = "Romain Sacchi, Alois Dirnaichner, Chris Mutel"

# The full version, including alpha/beta/rc tags
release = "2.5.2"

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",  # Core library for html generation from docstrings
    "sphinx.ext.autosummary",  # Create neat summary tables
    "sphinx_rtd_theme",
]
autosummary_generate = True  # Turn on sphinx.ext.autosummary
# Repeated tailings bibliography entries are also printed in a data table.
suppress_warnings = ["ref.footnote"]
autodoc_typehints = "none"

master_doc = "index"

# Import the real package so obsolete API paths fail the build.
autodoc_mock_imports = []

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.database", ".DS_Store"]

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "sphinx_rtd_theme"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]
html_favicon = "_static/favicon.ico"
html_css_files = ["custom.css", "hub.css"]


# html_sidebars = { '**': ['globaltoc.html', 'relations.html', 'sourcelink.html', 'searchbox.html'] }

html_theme_options = {"navigation_depth": 3, "collapse_navigation": True}


def setup(app):
    # Premise configures application logging during import. Restore Sphinx's
    # handlers afterwards so autodoc warnings cannot silently disappear.
    import logging
    from pathlib import Path

    # API builds need no user databases. Isolate Brightway's import-time files.
    bw_dir = Path(app.outdir).parent / ".brightway-docs"
    bw_dir.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("BRIGHTWAY2_DIR", str(bw_dir))
    import premise  # noqa: F401
    from sphinx.util import logging as sphinx_logging

    for name, logger in logging.Logger.manager.loggerDict.items():
        if name == "sphinx" or name.startswith("sphinx."):
            if isinstance(logger, logging.Logger):
                logger.disabled = False
    sphinx_logging.setup(app, app._status, app._warning)


# External URL checks are a separate, non-blocking workflow. Fragments on
# third-party sites often require JavaScript or authentication.
linkcheck_anchors = False
linkcheck_timeout = 10
linkcheck_retries = 1
linkcheck_workers = 10
linkcheck_rate_limit_timeout = 30
