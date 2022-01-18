# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#

import sys
from pathlib import Path
# import sphinx_rtd_theme
import shutil

src_path = Path(__file__).parent
base_path = src_path.parents[1]
sys.path.insert(0, base_path.resolve().as_posix())
# sys.path.insert(0, base_path.joinpath("WeatherDB").resolve().as_posix())

import weatherDB

# -- Project information -----------------------------------------------------

project = 'WeatherDB'
copyright = weatherDB.__copyright__
author = weatherDB.__author__
author_email = weatherDB.__email__

# The full version, including alpha/beta/rc tags
release = weatherDB.__version__


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.napoleon', 
    'sphinx_rtd_theme',
    'sphinx.ext.autodoc',
    'sphinx.ext.imgconverter',
    'nbsphinx',
    'recommonmark' 
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store', 'sync.ffs_db']

source_suffix = [".rst", ".md"]

autodoc_default_options = {
	'special-members': '__init__'
}

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "sphinx_rtd_theme"#'alabaster'

html_theme_options = {
    'display_version': True,
    'prev_next_buttons_location': 'bottom',
    # Toc options
    'collapse_navigation': True,
    'sticky_navigation': True,
    'navigation_depth': 9
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

# -- Options for PDF Output --------------------------------------------------
latex_engine = "pdflatex"
#latex_documents = (startdocname = "index")
latex_theme = "manual"

# copy the readme file
shutil.copyfile(
    base_path.joinpath("README.md"),
    src_path.joinpath("README.md")
)

# run in command: sphinx-build -b html .\source .\html