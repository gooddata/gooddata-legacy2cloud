# (C) 2026 GoodData Corporation
"""
Status analysis utilities for migration log files.
"""

from typing import Any, Dict

from gooddata_platform2cloud.web_compare_processing.comparison_result import (
    ComparisonStatus,
)


def determine_status(entry: Dict[str, Any]) -> ComparisonStatus:
    """
    Determine the comparison status for a log entry.
    Handles both old-style logs and new structured logs.

    Args:
        entry: Log entry dictionary

    Returns:
        ComparisonStatus enum value
    """
    try:
        # Check if we have explicit success flag
        if "success" in entry:
            if not entry["success"]:
                return _determine_error_type(entry)

        # Check for successful migration based on response structure
        if _has_successful_migration_structure(entry):
            # Check title for warning/error markers even if the structure is successful
            cloud_def = entry.get("cloud_definition", {})
            if (
                isinstance(cloud_def, dict)
                and "data" in cloud_def
                and isinstance(cloud_def["data"], dict)
            ):
                data = cloud_def["data"]
                if "attributes" in data and isinstance(data["attributes"], dict):
                    attributes = data["attributes"]
                    if "title" in attributes:
                        title = attributes["title"]
                        if isinstance(title, str):
                            if title.startswith("[ERROR]"):
                                return ComparisonStatus.ERROR
                            elif title.startswith("[WARN]"):
                                return ComparisonStatus.WARNING

            return ComparisonStatus.SUCCESS

        # Check cloud definition for error indicators
        if "cloud_definition" in entry:
            cloud_def = entry.get("cloud_definition", {})

            if isinstance(cloud_def, dict) and "error" in cloud_def:
                return _get_status_from_error_text(str(cloud_def["error"]))

            # Check for title markers in data/attributes/title
            elif (
                isinstance(cloud_def, dict)
                and "data" in cloud_def
                and isinstance(cloud_def["data"], dict)
            ):
                data = cloud_def["data"]
                if "attributes" in data and isinstance(data["attributes"], dict):
                    attributes = data["attributes"]
                    if "title" in attributes:
                        title = attributes["title"]
                        if isinstance(title, str):
                            if title.startswith("[ERROR]"):
                                return ComparisonStatus.ERROR
                            elif title.startswith("[WARN]"):
                                return ComparisonStatus.WARNING

            elif isinstance(cloud_def, str):
                return _get_status_from_string(cloud_def)

        # Check if there's a cloud ID, which usually indicates success
        if "cloud_id" in entry and entry["cloud_id"]:
            return ComparisonStatus.SUCCESS

        # Check if cloud_definition has data structure, which usually indicates success
        cloud_def = entry.get("cloud_definition", {})
        if isinstance(cloud_def, dict) and "data" in cloud_def:
            return ComparisonStatus.SUCCESS

    except Exception as e:
        print(f"Warning: Error determining status: {e}")
        return ComparisonStatus.ERROR

    # Default to success for entries without detected issues
    return ComparisonStatus.SUCCESS


def _determine_error_type(entry: Dict[str, Any]) -> ComparisonStatus:
    """
    Determine the specific error type from an entry marked as unsuccessful.

    Args:
        entry: Log entry dictionary

    Returns:
        Specific ComparisonStatus for the error
    """
    if "cloud_definition" in entry:
        cloud_def = entry["cloud_definition"]

        if isinstance(cloud_def, dict) and "error" in cloud_def:
            return _get_status_from_error_text(str(cloud_def["error"]))

        elif isinstance(cloud_def, str):
            return _get_status_from_string(cloud_def)

    # Default to generic error
    return ComparisonStatus.ERROR


def _has_successful_migration_structure(entry: Dict[str, Any]) -> bool:
    """
    Check if the entry has the structure of a successful migration.

    Args:
        entry: Log entry dictionary

    Returns:
        True if the structure indicates success, False otherwise
    """
    return (
        "cloud_definition" in entry
        and isinstance(entry["cloud_definition"], dict)
        and "data" in entry["cloud_definition"]
        and isinstance(entry["cloud_definition"]["data"], dict)
        and "id" in entry["cloud_definition"]["data"]
    )


def _get_status_from_error_text(error_text: str) -> ComparisonStatus:
    """
    Determine status from error text.

    Args:
        error_text: Error message text

    Returns:
        Appropriate ComparisonStatus based on the error text
    """
    # Check for skipped object message
    if "already exists" in error_text.lower():
        return ComparisonStatus.SKIPPED

    # Check if it's a warning pattern
    if "[WARN]" in error_text:
        return ComparisonStatus.WARNING

    # Default to error
    return ComparisonStatus.ERROR


def _get_status_from_string(text: str) -> ComparisonStatus:
    """
    Determine status from a string representation.

    Args:
        text: String to analyze

    Returns:
        Appropriate ComparisonStatus based on the text content
    """
    if not text:
        return ComparisonStatus.SUCCESS

    # Check for error messages first - these are explicit error indicators
    if text.startswith("ERROR:"):
        return ComparisonStatus.ERROR

    # Check if it's a warning pattern
    if "[WARN]" in text:
        return ComparisonStatus.WARNING

    # Check for other error indicators
    if "[ERROR]" in text:
        return ComparisonStatus.ERROR

    # Default to success
    return ComparisonStatus.SUCCESS


def determine_success(platform_def: Any, cloud_def: Any) -> bool:
    """
    Determine if the migration was successful based on the definitions.

    Args:
        platform_def: Platform definition
        cloud_def: Cloud definition

    Returns:
        Boolean indicating success
    """
    # Check direct success flag
    if isinstance(cloud_def, dict) and "success" in cloud_def:
        return bool(cloud_def["success"])

    # Initialize to success
    success = True

    # Explicitly check for ERROR: prefix - this is a clear indicator of failure
    if isinstance(cloud_def, str) and cloud_def.startswith("ERROR:"):
        return False

    # Check for string errors (parsing failed or explicit error messages)
    if isinstance(platform_def, str) and (
        platform_def.startswith("ERROR:") or "error" in platform_def.lower()
    ):
        success = False
    elif isinstance(cloud_def, str) and "[ERROR]" in cloud_def:
        success = False

    # If both are dictionaries, check for error fields
    elif isinstance(cloud_def, dict) and isinstance(platform_def, dict):
        # Check for error fields in the dictionaries
        if "error" in cloud_def or "error" in platform_def:
            success = False
        # Check if cloud_def has a data field (success usually means it has a data structure)
        elif "data" not in cloud_def and "attributes" not in cloud_def:
            # No data structure usually means failure
            success = False

    # One is a dict and one is a string, likely means an error
    elif isinstance(cloud_def, dict) != isinstance(platform_def, dict):
        success = False

    return success
