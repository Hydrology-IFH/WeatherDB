# Configuration file for the Sphinx documentation builder.

import sys
from pathlib import Path

# -- Path setup --------------------------------------------------------------

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
    'nbsphinx',
    'myst_parser',
    'sphinx.ext.viewcode',
    'autoclasstoc',
    'sphinx.ext.autosummary',
    'sphinx_click',
    "sphinx.ext.autosectionlabel",
    'sphinx_copybutton',
    'sphinx_design',
    'sphinx.ext.intersphinx',
    "sphinx_new_tab_link",
    "sphinx.ext.mathjax"
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
    "colon_fence",
    "attrs_inline"
]

# Autodoc options
autodoc_default_options = {
    'member-order': 'bysource',
    'undoc-members': True,
    'show-inheritance': True,
    'inherited-members':True,
    'collapse_navigation': False,
}
autodoc_default_flags=["show-inheritance"]
autoclass_content= "both"
autodoc_inherit_docstrings= True

autoclasstoc_sections = [
    'public-methods',
]

# Autosummary options
autosummary_generate = True
autosummary_generate_overwrite = True

# intersphinx
intersphinx_mapping = {
    'python': ('https://docs.python.org/3', None),
    'sqlalchemy': ('https://docs.sqlalchemy.org/en/20/', None),
    'pandas': ('https://pandas.pydata.org/docs/', None),}

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
html_theme = "sphinx_rtd_theme"

html_theme_options = {
    'version_selector': True,
    'prev_next_buttons_location': 'bottom',
    'style_external_links': True,
    # TOC options
    'collapse_navigation': True,
    'sticky_navigation': True,
    'navigation_depth': -1,
}

html_css_files = [
    'custom.css',
]
html_logo = "_static/logo.png"
html_favicon = "_static/favicon.ico"

html_context = {
    # display gitlab
    "display_gitlab": True, # Integrate Gitlab
    "gitlab_user": "hydrology", # Username
    "gitlab_repo": "weatherDB", # Repo name
    "gitlab_version": "master", # Version
    "conf_py_path": "/docs/source/", # Path in the checkout to the docs root
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']


# -- Options for PDF Output --------------------------------------------------
latex_engine = "pdflatex"
latex_theme = "manual"


# run with command: sphinx-build -b html .\source .\html