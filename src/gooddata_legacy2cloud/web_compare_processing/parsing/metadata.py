# (C) 2026 GoodData Corporation
"""
Metadata extraction utilities for migration log files.
"""

import re
from typing import Any, Dict, List, NamedTuple, Optional, Tuple


class LogMetadata(NamedTuple):
    """Metadata extracted from a log file."""

    legacy_hostname: Optional[str] = None
    legacy_ws: Optional[str] = None
    cloud_hostname: Optional[str] = None
    cloud_ws: Optional[str] = None
    client_prefix: Optional[str] = None
    timestamp: Optional[str] = None


def extract_metadata_from_headers(lines: List[str]) -> Tuple[Dict[str, str], int]:
    """
    Extract metadata from the beginning of log file lines.

    Args:
        lines: Lines from the log file

    Returns:
        Tuple of (metadata dict, line offset)
    """
    metadata_dict = {}
    line_offset = 0

    # Process up to 3 lines for metadata
    for i in range(min(3, len(lines))):
        if lines[i].startswith("#MIGRATION_INFO#"):
            metadata_line = lines[i].strip().replace("#MIGRATION_INFO#", "")
            line_offset += 1

            # Parse key-value pairs (using semicolons as separators)
            for item in metadata_line.split(";"):
                if "=" in item:
                    key, value = item.split("=", 1)
                    metadata_dict[key.strip()] = value.strip()
        else:
            # No more metadata lines
            break

    # Skip empty line after metadata if present
    if line_offset < len(lines) and not lines[line_offset].strip():
        line_offset += 1

    return metadata_dict, line_offset


def extract_title_metadata(line: str) -> Dict[str, str]:
    """
    Extract title metadata from a plain text line.

    Args:
        line: Line from the log file

    Returns:
        Dictionary of extracted title metadata
    """
    metadata = {}

    # Title patterns
    title_match = re.search(r'title[=:][\s]*["\'](.*?)["\']', line, re.IGNORECASE)
    if title_match:
        metadata["title"] = title_match.group(1)

    return metadata


def extract_status_metadata(line: str) -> Dict[str, bool]:
    """
    Extract status/success metadata from a plain text line.

    Args:
        line: Line from the log file

    Returns:
        Dictionary of extracted status metadata
    """
    metadata = {}

    # Status/success patterns
    if "success" in line.lower():
        if "false" in line.lower() or "fail" in line.lower():
            metadata["success"] = False
        else:
            metadata["success"] = True
    elif "error" in line.lower() or "fail" in line.lower():
        metadata["success"] = False

    return metadata


def extract_uri_metadata(line: str) -> Dict[str, str]:
    """
    Extract URI metadata from a plain text line.

    Args:
        line: Line from the log file

    Returns:
        Dictionary of extracted URI metadata
    """
    metadata = {}

    # Extract object URIs if present
    uri_match = re.search(r'uri[=:][\s]*["\'](.*?)["\']', line, re.IGNORECASE)
    if uri_match:
        metadata["uri"] = uri_match.group(1)

        # Extract object ID from URI if available
        uri = uri_match.group(1)
        uri_segments = uri.rstrip("/").split("/")
        if uri_segments and uri_segments[-1].isdigit():
            metadata["obj_id"] = uri_segments[-1]

    return metadata


def extract_metadata_from_plain_text(line: str) -> Dict[str, Any]:
    """
    Extract metadata from a plain text line that might contain key=value pairs.
    Used for backward compatibility with simple text dump logs.

    Args:
        line: Line from the log file

    Returns:
        Dictionary of extracted metadata
    """
    if not line:
        return {}

    metadata = {}

    try:
        # Extract different types of metadata using specialized methods
        metadata.update(extract_id_metadata(line))
        metadata.update(extract_title_metadata(line))
        metadata.update(extract_status_metadata(line))
        metadata.update(extract_uri_metadata(line))
    except Exception:
        # Silently continue if extraction fails
        pass

    return metadata


def extract_id_metadata(line: str) -> Dict[str, str]:
    """
    Extract ID-related metadata from a plain text line.

    Args:
        line: Line from the log file

    Returns:
        Dictionary of extracted ID metadata
    """
    metadata = {}

    # ID patterns - more comprehensive matching
    id_patterns = [
        r'id[=:][\s]*["\'](.*?)["\']',  # id="value" or id='value'
        r"id[=:][\s]*([a-zA-Z0-9_-]+)",  # id=value (without quotes)
        r'identifier[=:][\s]*["\'](.*?)["\']',  # identifier="value"
        r"identifier[=:][\s]*([a-zA-Z0-9_-]+)",  # identifier=value
        r"\b(id|identifier)[=:]?\s+([a-zA-Z0-9_-]+)",  # id/identifier value
    ]

    for pattern in id_patterns:
        id_match = re.search(pattern, line, re.IGNORECASE)
        if id_match:
            # Group 1 is the field name, group 2 is the value (or just value if only one group)
            if len(id_match.groups()) > 1:
                metadata["id"] = id_match.group(2)
            else:
                metadata["id"] = id_match.group(1)
            break

    return metadata


def extract_cloud_title(cloud_definition: Any) -> Optional[str]:
    """
    Extract title from Cloud definition.
    Handles both dictionary and non-dictionary inputs.

    Args:
        cloud_definition: Cloud object definition

    Returns:
        Extracted title or None if not found
    """
    try:
        # Check if we have a valid dictionary
        if not isinstance(cloud_definition, dict):
            # If it's a string, it might contain error information
            if isinstance(cloud_definition, str) and len(cloud_definition) > 0:
                # Look for error patterns
                if cloud_definition.startswith("[ERROR]"):
                    match = re.search(r"\[ERROR\].*?:\s+(.+)$", cloud_definition)
                    if match:
                        return match.group(1)
                # Return the string as is if it might be a title
                if not cloud_definition.startswith(
                    "{"
                ) and not cloud_definition.startswith("["):
                    return cloud_definition
            return None

        # Check if it's an error object
        if "error" in cloud_definition:
            # If the error text has specific formats, extract object title
            error_text = str(cloud_definition["error"])

            # Look for patterns like "[ERROR] Error migrating: Title"
            match = re.search(r"\[ERROR\].*?:\s+(.+)$", error_text)
            if match:
                return match.group(1)

            # If no specific pattern found, return error text as title
            return error_text

        # Look for title in standard API response structure
        if "data" in cloud_definition and isinstance(cloud_definition["data"], dict):
            data = cloud_definition["data"]

            # Title is in data/attributes/title
            if "attributes" in data and isinstance(data["attributes"], dict):
                attributes = data["attributes"]
                if "title" in attributes:
                    return attributes["title"]
    except Exception as e:
        print(f"Warning: Error extracting cloud title: {e}")
        return None

    return None
