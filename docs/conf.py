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

sys.path.insert(0, os.path.abspath(".."))

# -- Project information -----------------------------------------------------

project = "premise"
copyright = (
    "2023, Paul Scherrer Institut, Potsdam Institute for Climate Impact Research"
)
author = "Romain Sacchi, Alois Dirnaichner, Chris Mutel"

# The full version, including alpha/beta/rc tags
release = "2.2.0"

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

master_doc = "index"

autodoc_mock_imports = [
    "numpy",
    "pandas",
    "bw2io >=0.8.10",
    "bw2data",
    "wurst",
    "xarray",
    "prettytable",
    "pycountry",
    "cryptography",
    "premise_gwp",
    "pyYaml",
    "sparse>=0.14.0",
    "schema",
    "datapackage",
    "requests",
    "bottleneck",
    "constructive_geometries>=0.8.2",
    "pyarrow",
    "premise",
]

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
html_css_files = ["custom.css"]


# html_sidebars = { '**': ['globaltoc.html', 'relations.html', 'sourcelink.html', 'searchbox.html'] }
