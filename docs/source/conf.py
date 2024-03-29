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

import sys, os
from pathlib import Path
# import sphinx_rtd_theme
import shutil

os.environ["RTD_documentation_import"] = "YES"

src_path = Path(__file__).parent
base_path = src_path.parents[1]
sys.path.insert(0, base_path.resolve().as_posix())

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
    # 'sphinx.ext.imgconverter',
    'nbsphinx',
    'myst_parser',
    'sphinx.ext.viewcode',
    'autoclasstoc',
    'sphinx.ext.autosummary',
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store', 'sync.ffs_db']

source_suffix = [".rst", ".md"]

source_parsers = {
   '.md': "markdown"
}

myst_enable_extensions = [
    "dollarmath",
]

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

html_css_files = [
    'custom.css',
]

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

# -- Options for PDF Output --------------------------------------------------
latex_engine = "pdflatex"
#latex_documents = (startdocname = "index")
latex_theme = "manual"

# copy the readme file to _static
readme_src = src_path.joinpath("README.md")
if readme_src.is_file(): readme_src.unlink()
shutil.copyfile(
    base_path.joinpath("README.md"),
    readme_src
)
# copy the CHANGES file to _static
changes_src = src_path.joinpath("CHANGES.md")
if changes_src.is_file(): changes_src.unlink()
shutil.copyfile(
    base_path.joinpath("CHANGES.md"),
    changes_src
)

# Autodoc options
autodoc_default_options = {
    'member-order': 'alphabetical',
    'undoc-members': True,
    'show-inheritance': True,
    'inherited-members':True,
}
autodoc_default_flags=["show-inheritance"]
autoclass_content= "both"
autodoc_inherit_docstrings= True

autoclasstoc_sections = [
        'public-methods',
]

# run in command: sphinx-build -b html .\source .\html