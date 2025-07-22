"""Custom YAML loader with template and include support for jobmon logging configuration."""

import os
from typing import Any, Dict, Optional

import yaml
from yaml.nodes import ScalarNode


class TemplateLoader(yaml.SafeLoader):
    """Custom YAML loader that supports !include and !template directives."""

    def __init__(self, stream: Any) -> None:
        """Initialize TemplateLoader with a YAML stream."""
        self._root = (
            os.path.split(stream.name)[0] if hasattr(stream, "name") else os.getcwd()
        )
        super().__init__(stream)
        self._templates: Optional[Dict[str, Any]] = None


def include_constructor(loader: TemplateLoader, node: ScalarNode) -> Any:
    """Include another YAML file."""
    filename = loader.construct_scalar(node)
    filepath = os.path.join(loader._root, filename)

    with open(filepath, "r") as f:
        return yaml.load(f, Loader=TemplateLoader)


def template_constructor(loader: TemplateLoader, node: ScalarNode) -> Any:
    """Reference a template from the templates directory."""
    template_name = loader.construct_scalar(node)

    # Load templates if not already loaded
    if loader._templates is None:
        loader._templates = load_all_templates(loader._root)

    # Navigate to the template
    template_path = template_name.split(".")
    template_data = loader._templates

    for part in template_path:
        if isinstance(template_data, dict) and part in template_data:
            template_data = template_data[part]
        else:
            raise ValueError(f"Template '{template_name}' not found")

    return template_data


def load_all_templates(config_root: str) -> Dict[str, Any]:
    """Load all template files from the templates directory."""
    templates = {}

    # Find templates directory - try multiple locations with package-aware resolution
    templates_dirs = [
        # First try: templates in same directory (for core configs)
        os.path.join(config_root, "templates"),
        # Second try: relative path from server to core (for server configs)
        os.path.join(config_root, "../../../../core/config/templates"),
        # Third try: relative path from client to core (for client configs)
        os.path.join(config_root, "../../../core/config/templates"),
        # Fourth try: package-based resolution using this module's location
        os.path.join(os.path.dirname(__file__), "templates"),
        # Fifth try: absolute package path resolution
        os.path.join(os.path.dirname(os.path.dirname(__file__)), "config", "templates"),
    ]

    templates_dir = None
    for candidate in templates_dirs:
        if os.path.exists(candidate):
            templates_dir = os.path.abspath(candidate)
            break

    if not templates_dir:
        return {}

    # Load all template files
    template_files = ["formatters.yaml", "otlp_exporters.yaml", "handlers.yaml"]

    for template_file in template_files:
        template_path = os.path.join(templates_dir, template_file)
        if os.path.exists(template_path):
            try:
                with open(template_path, "r") as f:
                    template_data = yaml.load(f, Loader=TemplateLoader)
                    if template_data:
                        templates.update(template_data)
            except Exception:
                # Skip malformed templates, but could log for debugging
                pass

    return templates


# Register the custom constructors
TemplateLoader.add_constructor("!include", include_constructor)  # type: ignore[misc]
TemplateLoader.add_constructor("!template", template_constructor)  # type: ignore[misc]

# Use the default merge constructor from SafeLoader
TemplateLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    yaml.constructor.SafeConstructor.construct_mapping,
)  # type: ignore[type-var]


def load_logconfig_with_templates(config_path: str) -> Dict[str, Any]:
    """Load a logconfig file with template support.

    Supports:
    - !include path/to/file.yaml
    - !template template_name
    - YAML merge operators (<<)

    Args:
        config_path: Path to the main logconfig file

    Returns:
        Fully resolved configuration dictionary
    """
    with open(config_path, "r") as f:
        return yaml.load(f, Loader=TemplateLoader)


def load_yaml_with_templates(file_path: str) -> Dict[str, Any]:
    """Convenience function to load any YAML file with template support."""
    return load_logconfig_with_templates(file_path)
