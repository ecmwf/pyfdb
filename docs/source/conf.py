# Configuration file for the Sphinx documentation builder.

import io
import os
import re
from os.path import dirname as up

version_path = os.path.join(up(up(up(os.path.abspath(__file__)))), "pyfdb/version.py")

__version__ = re.search(
    r'__version__\s*=\s*[\'"]([^\'"]*)[\'"]', io.open(version_path, encoding="utf_8_sig").read()
).group(1)

# -- Project information

project = "Pyfdb"
copyright = "2021, ECMWF"
author = "ECMWF"

release = __version__

# -- General configuration

extensions = [
    "sphinx.ext.duration",
    "sphinx.ext.doctest",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.intersphinx",
]

intersphinx_mapping = {
    "python": ("https://docs.python.org/3/", None),
    "sphinx": ("https://www.sphinx-doc.org/en/master/", None),
}
intersphinx_disabled_domains = ["std"]

templates_path = ["_templates"]

# -- Options for HTML output

html_theme = "sphinx_rtd_theme"

# -- Options for EPUB output
epub_show_urls = "footnote"
