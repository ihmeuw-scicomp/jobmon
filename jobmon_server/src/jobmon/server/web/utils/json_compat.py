"""Utility functions for JSON compatibility between old and new client versions."""

import json
from typing import Any, List, Optional

# Version cutoff for JSON compatibility
# Clients <= this version expect quoted JSON strings (old format)
# Clients > this version expect unquoted JSON arrays (new format)
JSON_COMPAT_CUTOFF_VERSION = "3.4.23"


def normalize_node_ids(node_ids: Any) -> Optional[List[int]]:
    """Normalize node_ids to a list of integers, supporting both old and new formats.

    Args:
        node_ids: Can be:
            - None
            - A list of integers (new format)
            - A JSON string like "[1, 2, 3]" (old format)
            - A string representation of a list like "[1, 2, 3]"

    Returns:
        List of integers or None if input is None/empty

    Raises:
        ValueError: If the input cannot be parsed into a list of integers
    """
    if node_ids is None:
        return None

    # If it's already a list, return it
    if isinstance(node_ids, list):
        return node_ids

    # If it's a string, try to parse it
    if isinstance(node_ids, str):
        # Handle empty string
        if not node_ids.strip():
            return None

        try:
            # Try to parse as JSON first
            parsed = json.loads(node_ids)
            if isinstance(parsed, list):
                return parsed
            elif isinstance(parsed, str):
                # Handle double-quoted JSON strings like '"[1, 2, 3]"'
                try:
                    inner_parsed = json.loads(parsed)
                    if isinstance(inner_parsed, list):
                        return inner_parsed
                except json.JSONDecodeError:
                    pass
            raise ValueError(f"Expected list, got {type(parsed)}")
        except json.JSONDecodeError:
            # If JSON parsing fails, try ast.literal_eval as fallback
            try:
                import ast

                parsed = ast.literal_eval(node_ids)
                if isinstance(parsed, list):
                    return parsed
                else:
                    raise ValueError(f"Expected list, got {type(parsed)}")
            except (ValueError, SyntaxError) as e:
                raise ValueError(f"Cannot parse node_ids '{node_ids}': {e}")

    # If it's some other type, try to convert to list
    try:
        return list(node_ids)
    except (TypeError, ValueError) as e:
        raise ValueError(f"Cannot convert node_ids '{node_ids}' to list: {e}")


def ensure_json_compatible_format(node_ids: Any) -> Any:
    """Ensure the node_ids are in a format that can be stored in the database.

    This function is used when storing data to ensure consistency.

    Args:
        node_ids: The node_ids to format

    Returns:
        The node_ids in a format suitable for database storage
    """
    if node_ids is None:
        return None

    # Normalize to list first
    normalized = normalize_node_ids(node_ids)

    if normalized is None:
        return None

    # Return as list (new format) - the database JSON column will handle serialization
    return normalized


def _compare_versions(version1: str, version2: str) -> int:
    """Compare two version strings, handling dev versions.

    Args:
        version1: First version string (e.g., "3.4.23", "3.4.24.dev1")
        version2: Second version string (e.g., "3.4.24")

    Returns:
        -1 if version1 < version2
         0 if version1 == version2
         1 if version1 > version2
    """
    # Handle dev versions by removing the dev part for comparison
    clean_v1 = version1.split(".dev")[0] if ".dev" in version1 else version1
    clean_v2 = version2.split(".dev")[0] if ".dev" in version2 else version2

    v1_parts = [int(x) for x in clean_v1.split(".")]
    v2_parts = [int(x) for x in clean_v2.split(".")]

    # Pad with zeros to make equal length
    max_len = max(len(v1_parts), len(v2_parts))
    v1_parts.extend([0] * (max_len - len(v1_parts)))
    v2_parts.extend([0] * (max_len - len(v2_parts)))

    for v1_part, v2_part in zip(v1_parts, v2_parts):
        if v1_part < v2_part:
            return -1
        elif v1_part > v2_part:
            return 1
    return 0


def get_client_compatibility_mode(client_version: Optional[str]) -> str:
    """Determine the compatibility mode based on client version.

    Args:
        client_version: The client version string (e.g., "3.4.10", "3.4.24.dev1",
            "3.4.24.stage1")

    Returns:
        Compatibility mode: "old", "new", or "unknown"
        - "new" for versions with "dev" or "stage" in them
        - "old" for versions <= JSON_COMPAT_CUTOFF_VERSION
        - "new" for versions > JSON_COMPAT_CUTOFF_VERSION
    """
    if not client_version:
        return "unknown"

    # If version contains "dev" or "stage", treat as new version
    if "dev" in client_version.lower() or "stage" in client_version.lower():
        return "new"

    try:
        # Compare versions using the cutoff constant
        comparison = _compare_versions(client_version, JSON_COMPAT_CUTOFF_VERSION)

        if comparison <= 0:  # client_version <= JSON_COMPAT_CUTOFF_VERSION
            return "old"  # Client expects quoted JSON strings like "[1, 2]"
        else:  # client_version > JSON_COMPAT_CUTOFF_VERSION
            return "new"  # Client expects unquoted JSON arrays like [1, 2]

    except (ValueError, AttributeError):
        # If we can't parse the version, assume old format for safety
        return "old"


def normalize_node_ids_for_client(
    node_ids: Any, client_version: Optional[str] = None
) -> Any:
    """Normalize node_ids based on client version compatibility.

    Args:
        node_ids: The node_ids to normalize
        client_version: The client version string

    Returns:
        The node_ids in the format expected by the client:
        - For clients <= JSON_COMPAT_CUTOFF_VERSION: Returns quoted JSON string
          like "[1, 2]" (can be parsed with json.loads)
        - For clients > JSON_COMPAT_CUTOFF_VERSION: Returns unquoted JSON array
          like [1, 2]
    """
    # First normalize to a list
    normalized = normalize_node_ids(node_ids)

    if normalized is None:
        return None

    # Determine client compatibility mode
    mode = get_client_compatibility_mode(client_version)

    if mode == "old" or mode == "unknown":
        # Return as quoted JSON string for clients <= JSON_COMPAT_CUTOFF_VERSION
        # or unknown versions
        # This can be parsed with json.loads() in the client
        return json.dumps(normalized)
    else:
        # Return as list for clients > JSON_COMPAT_CUTOFF_VERSION
        return normalized
