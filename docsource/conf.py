import os
import sys
import jobmon.client

# General configuration
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.intersphinx',
    'sphinx.ext.todo',
    'sphinx.ext.mathjax',
    'sphinx.ext.ifconfig',
    # Note: viewcode is disabled due to IndexError when used with autoapi.
    # See: https://github.com/sphinx-doc/sphinx/issues (viewcode + autoapi conflict)
    # 'sphinx.ext.viewcode',
    'sphinx.ext.napoleon',
    'sphinx_autodoc_typehints',
    'sphinx.ext.graphviz',
    'sphinx.ext.autosectionlabel',
    'sphinx_tabs.tabs',
    'autoapi.extension'
]
autoapi_dirs = ['../jobmon_core', '../jobmon_client', '../jobmon_server']

# AutoAPI configuration
autoapi_type = 'python'
autoapi_keep_files = True  # Keep generated RST files for debugging
autoapi_options = [
    'members',
    'undoc-members',
    'show-inheritance',
    'show-module-summary',
    'imported-members',
]
# Skip documenting dataclass __init__ which duplicates field docs
autoapi_python_class_content = 'both'
# Ignore internal modules and build artifacts
autoapi_ignore = [
    '*/tests/*',
    '*/_version.py',
    '*/conftest.py',
    '*/build/*',
    '*/migrations/*',  # Alembic migrations
]

templates_path = ['_templates']
source_suffix = '.rst'
master_doc = 'index'
project = 'jobmon'
copyright = '2016-2024, University of Washington'
author = 'jobmon'
version = jobmon.client.__version__
release = jobmon.client.__version__
language = 'en'
todo_include_todos = True
autoclass_content = 'init'
pygments_style = 'sphinx'

# HTML output options
html_theme = 'sphinx_rtd_theme'


# Additional configurations
intersphinx_mapping = {'python': ('https://docs.python.org/3', None)}
graphviz_output_format = 'svg'

# Suppress warnings that are expected or unavoidable
suppress_warnings = [
    'autosectionlabel.*',
    # Suppress duplicate object descriptions from dataclass fields
    'ref.python',
]

# Suppress specific RST/docutils warnings from autoapi-generated files
# These are from complex docstrings that don't convert perfectly to RST
nitpicky = False  # Don't be too strict about missing references

# Ignore unknown roles from SQLAlchemy docstrings
# SQLAlchemy uses custom Sphinx roles like :paramref: that we don't have defined
def setup(app):
    """Add custom roles to suppress warnings from SQLAlchemy docstrings."""
    from docutils.parsers.rst import roles
    from docutils import nodes

    def paramref_role(name, rawtext, text, lineno, inliner, options={}, content=[]):
        """Handle :paramref: role from SQLAlchemy docstrings."""
        node = nodes.literal(rawtext, text)
        return [node], []

    roles.register_local_role('paramref', paramref_role)

# Napoleon settings for better docstring parsing
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = False
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = True
napoleon_use_admonition_for_examples = False
napoleon_use_admonition_for_notes = False
napoleon_use_admonition_for_references = False
napoleon_use_ivar = False
napoleon_use_param = True
napoleon_use_rtype = True
napoleon_attr_annotations = True

# sphinx-autodoc-typehints settings
typehints_fully_qualified = False
always_document_param_types = False
typehints_document_rtype = True
