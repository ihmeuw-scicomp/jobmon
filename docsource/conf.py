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
    'sphinx.ext.viewcode',
    'sphinx.ext.napoleon',
    'sphinx_autodoc_typehints',
    'sphinx.ext.graphviz',
    'sphinx.ext.autosectionlabel',
    'sphinx_tabs.tabs',
    'autoapi.extension'
]
autoapi_dirs = ['../jobmon_core', '../jobmon_client', '../jobmon_server']
templates_path = ['_templates']
source_suffix = '.rst'
master_doc = 'index'
project = 'jobmon'
copyright = '2016-2022, University of Washington'
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
suppress_warnings = ['autosectionlabel.*']
