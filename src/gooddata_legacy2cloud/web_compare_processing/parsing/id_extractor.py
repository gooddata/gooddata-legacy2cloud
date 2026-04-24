# (C) 2026 GoodData Corporation
"""
ID extraction utilities for migration log files.
"""

import re
from typing import Any, Dict, Optional, Tuple


def extract_ids_from_definitions(
    legacy_definition: Any, cloud_definition: Any
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Extract IDs from Legacy and Cloud definitions.
    Handles cases where the definitions might be strings or other non-dictionary types.

    Args:
        legacy_definition: Legacy object definition
        cloud_definition: Cloud object definition

    Returns:
        Tuple of (legacy_id, legacy_obj_id, cloud_id)
    """
    try:
        # Extract all IDs using helper functions
        legacy_id = extract_legacy_id(legacy_definition)
        legacy_obj_id = extract_legacy_obj_id(legacy_definition)
        cloud_id = extract_cloud_id(cloud_definition)

        return legacy_id, legacy_obj_id, cloud_id
    except Exception as e:
        print(f"Warning: Error extracting IDs from definitions: {e}")
        return None, None, None


def extract_legacy_id(legacy_definition: Any) -> Optional[str]:
    """
    Extract legacy_id from Legacy definition.

    Args:
        legacy_definition: Legacy object definition

    Returns:
        Extracted legacy_id or None if not found
    """
    if not legacy_definition:
        return None

    # Check if we have a dictionary
    if isinstance(legacy_definition, dict):
        # Find the root object
        root_object = _get_root_object(legacy_definition)

        # Try to get ID from meta/identifier
        if (
            isinstance(root_object, dict)
            and "meta" in root_object
            and isinstance(root_object["meta"], dict)
        ):
            meta = root_object["meta"]
            if "identifier" in meta:
                return str(meta["identifier"])

        # Check other common locations
        if isinstance(root_object, dict):
            for key in ["id", "identifier", "legacy_id"]:
                if key in root_object and isinstance(root_object[key], str):
                    return str(root_object[key])

    # Try string pattern matching if we have a string
    if isinstance(legacy_definition, str):
        return _extract_id_from_string(legacy_definition)

    return None


def extract_legacy_obj_id(legacy_definition: Any) -> Optional[str]:
    """
    Extract legacy_obj_id (numeric ID) from Legacy definition.

    Args:
        legacy_definition: Legacy object definition

    Returns:
        Extracted legacy_obj_id or None if not found
    """
    if not legacy_definition:
        return None

    # Check if we have a dictionary
    if isinstance(legacy_definition, dict):
        # Find the root object
        root_object = _get_root_object(legacy_definition)

        # Try to get obj_id from meta/uri
        if (
            isinstance(root_object, dict)
            and "meta" in root_object
            and isinstance(root_object["meta"], dict)
        ):
            meta = root_object["meta"]
            if "uri" in meta:
                return _extract_id_from_uri(meta["uri"])

    # Try string pattern matching if we have a string
    if isinstance(legacy_definition, str):
        uri_match = re.search(
            r'uri[=:][\s]*["\'](.*?)["\']', legacy_definition, re.IGNORECASE
        )
        if uri_match:
            uri = uri_match.group(1)
            return _extract_id_from_uri(uri)

    return None


def extract_cloud_id(cloud_definition: Any) -> Optional[str]:
    """
    Extract cloud_id from Cloud definition.

    Args:
        cloud_definition: Cloud object definition

    Returns:
        Extracted cloud_id or None if not found
    """
    if not cloud_definition:
        return None

    # Special case: Check for cloud_definition with just a success flag
    if isinstance(cloud_definition, dict):
        if "success" in cloud_definition:
            # The cloud_definition might have an ID in a non-standard format
            for key, value in cloud_definition.items():
                if key.lower() in ["id", "identifier", "cloud_id"] and isinstance(
                    value, str
                ):
                    return str(value)

        # Check standard API response structure
        if "data" in cloud_definition and isinstance(cloud_definition["data"], dict):
            if "id" in cloud_definition["data"]:
                return str(cloud_definition["data"]["id"])

        # Check for direct ID on the top level
        if "id" in cloud_definition:
            return str(cloud_definition["id"])

    # Try string pattern matching if we have a string
    if isinstance(cloud_definition, str):
        return _extract_id_from_string(cloud_definition)

    return None


def _get_root_object(definition: Dict) -> Dict:
    """
    Get the root object from a definition, handling wrapper objects.

    Args:
        definition: Dictionary definition

    Returns:
        Root object dictionary
    """
    # Check if this is a wrapper with a single key that contains the actual definition
    if len(definition) == 1 and isinstance(list(definition.values())[0], dict):
        root_key = list(definition.keys())[0]
        if root_key not in [
            "error",
            "raw",
            "error_description",
        ]:  # Skip error containers
            return definition[root_key]

    # Return the original if no wrapper is detected
    return definition


def _extract_id_from_uri(uri: str) -> Optional[str]:
    """
    Extract object ID from URI.

    Args:
        uri: URI string

    Returns:
        Extracted ID or None if not found
    """
    if not uri:
        return None

    # Extract the last segment from the URI path
    uri_segments = uri.rstrip("/").split("/")
    if uri_segments and uri_segments[-1].isdigit():
        return uri_segments[-1]

    return None


def _extract_id_from_string(text: str) -> Optional[str]:
    """
    Extract ID from a string using regex patterns.

    Args:
        text: String to search for ID

    Returns:
        Extracted ID or None if not found
    """
    if not text:
        return None

    # Define common ID patterns
    id_patterns = [
        r'id[=:][\s]*["\'](.*?)["\']',  # id="value" or id='value'
        r"id[=:][\s]*([a-zA-Z0-9_-]+)",  # id=value (without quotes)
        r'identifier[=:][\s]*["\'](.*?)["\']',  # identifier="value"
        r"identifier[=:][\s]*([a-zA-Z0-9_-]+)",  # identifier=value
        r"\b(id|identifier)[=:]?\s+([a-zA-Z0-9_-]+)",  # id/identifier value
    ]

    for pattern in id_patterns:
        id_match = re.search(pattern, text, re.IGNORECASE)
        if id_match:
            # Group 1 is the field name, group 2 is the value (or just value if only one group)
            if len(id_match.groups()) > 1:
                return id_match.group(2)
            else:
                return id_match.group(1)

    return None
