from __future__ import annotations

import sys
from pathlib import Path

_DOCS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_DOCS_DIR.parent))
sys.path.insert(0, str(_DOCS_DIR.parent / "nix" / "shared"))

from sphinx_shared import (  # noqa: E402 -- sys.path must be extended first
    author as author,
    autodoc_default_options as autodoc_default_options,
    autodoc_member_order as autodoc_member_order,
    autodoc_typehints as autodoc_typehints,
    exclude_patterns as exclude_patterns,
    extensions as extensions,
    html_theme as html_theme,
    intersphinx_mapping as intersphinx_mapping,
    myst_enable_extensions as myst_enable_extensions,
    napoleon_google_docstring as napoleon_google_docstring,
    napoleon_numpy_docstring as napoleon_numpy_docstring,
    napoleon_use_rtype as napoleon_use_rtype,
    smartquotes as smartquotes,
    templates_path as templates_path,
)

project = "pynixd"
copyright = "2025, Carl Andersson"  # noqa: A001 - required Sphinx config name
