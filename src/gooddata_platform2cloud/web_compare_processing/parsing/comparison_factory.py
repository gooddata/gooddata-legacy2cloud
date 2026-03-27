# (C) 2026 GoodData Corporation
"""
Factory for creating comparison result objects from parsed log data.

This module contains functions for transforming parsed log entries into ComparisonResult objects
that can be used to generate web comparison reports.
"""

from typing import Any, Dict, List, Optional, Set

from gooddata_platform2cloud.web_compare_processing.comparison_result import (
    ComparisonItem,
    ComparisonResult,
    ComparisonStatus,
)
from gooddata_platform2cloud.web_compare_processing.parsing.id_extractor import (
    extract_ids_from_definitions,
)
from gooddata_platform2cloud.web_compare_processing.parsing.metadata import (
    extract_cloud_title,
)
from gooddata_platform2cloud.web_compare_processing.url_utils import (
    ObjectUrls,
    generate_urls,
)


def create_comparison_result(
    log_entries: List[Dict],
    object_type: str,
    platform_domain: str,
    platform_workspace: str,
    cloud_domain: str,
    cloud_workspace: str,
    failed_publishing_data: Optional[List[Dict]] = None,
    skipped_ids: Optional[Set] = None,
    missing_migration_info: bool = False,
) -> ComparisonResult:
    """
    Create a ComparisonResult from parsed log entries.

    Args:
        log_entries: List of parsed log entries
        object_type: Type of object (insight, dashboard, etc.)
        platform_domain: Platform domain URL
        platform_workspace: Platform workspace ID
        cloud_domain: Cloud domain URL
        cloud_workspace: Cloud workspace ID
        failed_publishing_data: Optional list of objects that failed publishing
        skipped_ids: Optional set of skipped object IDs
        missing_migration_info: Whether migration info was missing

    Returns:
        ComparisonResult object with processed items
    """
    # Normalize object_type to singular form
    normalized_type = object_type.lower()
    if normalized_type.endswith("s"):
        normalized_type = normalized_type[:-1]  # Remove trailing 's'

    # Create comparison result
    result = ComparisonResult(
        object_type=normalized_type,
        platform_domain=platform_domain,
        platform_workspace=platform_workspace,
        cloud_domain=cloud_domain,
        cloud_workspace=cloud_workspace,
    )

    # Set missing_migration_info flag
    result.missing_migration_info = missing_migration_info  # type: ignore

    # Add debug info
    result.debug_info = {
        "log_entries_count": len(log_entries) if log_entries else 0,
        "object_type": normalized_type,
        "failed_publishing_count": (
            len(failed_publishing_data) if failed_publishing_data else 0
        ),
        "skipped_ids_count": len(skipped_ids) if skipped_ids else 0,
    }

    # Create a lookup of failed publishing IDs
    failed_publishing_lookup = _create_failed_publishing_lookup(failed_publishing_data)

    # Process each log entry
    if log_entries:
        for i, entry in enumerate(log_entries, start=1):
            try:
                # Process entry and add it to result
                _process_entry(
                    entry,
                    i,
                    result,
                    normalized_type,
                    platform_domain,
                    platform_workspace,
                    cloud_domain,
                    cloud_workspace,
                    failed_publishing_lookup,
                    skipped_ids,
                )
            except Exception as e:
                print(f"Error processing log entry {i}: {e}")
                import traceback

                traceback.print_exc()

    return result


def _create_failed_publishing_lookup(
    failed_publishing_data: Optional[List[Dict]],
) -> Dict[str, str]:
    """
    Create a lookup dictionary for failed publishing data.

    Args:
        failed_publishing_data: List of objects that failed publishing

    Returns:
        Dictionary mapping cloud_id to error message
    """
    failed_publishing_lookup = {}

    if not failed_publishing_data:
        return failed_publishing_lookup

    for failed_obj in failed_publishing_data:
        try:
            if (
                isinstance(failed_obj, dict)
                and "data" in failed_obj
                and "id" in failed_obj["data"]
            ):
                cloud_id = failed_obj["data"]["id"]

                # Extract error message if available
                error_msg = "Failed publishing to API"
                if "__api_error" in failed_obj and "error" in failed_obj["__api_error"]:
                    error_msg = failed_obj["__api_error"]["error"]

                failed_publishing_lookup[cloud_id] = error_msg
        except Exception as e:
            print(f"Warning: Error processing failed publishing data: {e}")
            continue

    return failed_publishing_lookup


def _process_entry(
    entry: Dict,
    index: int,
    result: ComparisonResult,
    object_type: str,
    platform_domain: str,
    platform_workspace: str,
    cloud_domain: str,
    cloud_workspace: str,
    failed_publishing_lookup: Dict[str, str],
    skipped_ids: Optional[Set] = None,
) -> None:
    """
    Process a single log entry and add it to the result.

    Args:
        entry: Log entry to process
        index: Index of the entry
        result: ComparisonResult to update
        object_type: Type of object
        platform_domain: Platform domain URL
        platform_workspace: Platform workspace ID
        cloud_domain: Cloud domain URL
        cloud_workspace: Cloud workspace ID
        failed_publishing_lookup: Dictionary of failed publishing errors
        skipped_ids: Optional set of skipped object IDs
    """
    # Get required fields, with defaults if not present
    try:
        platform_title = entry["platform_title"]
    except KeyError, TypeError:
        platform_title = f"Unknown Title {index}"

    # Get definitions with fallbacks to empty dict or string
    try:
        platform_definition = entry.get("platform_definition", {})
    except Exception:
        platform_definition = "{}"  # Empty JSON string as fallback

    try:
        cloud_definition = entry.get("cloud_definition", {})
    except Exception:
        cloud_definition = "{}"  # Empty JSON string as fallback

    # Get entry info with fallback
    try:
        entry_info = entry.get("line_info", {}) or entry.get("info", {})
    except Exception:
        entry_info = {}

    # Extract IDs from the definitions
    platform_id, platform_obj_id, cloud_id = extract_ids_from_definitions(
        platform_definition, cloud_definition
    )

    # If we don't have a Platform ID, try to get it from the entry itself
    if not platform_id and "platform_id" in entry:
        platform_id = entry["platform_id"]

    # If we don't have a Cloud ID, try to get it from the entry itself
    if not cloud_id and "cloud_id" in entry:
        cloud_id = entry["cloud_id"]

    # Use simple IDs based on index if still not found
    if not platform_id:
        platform_id = f"platform_{index}"

    # Determine status for this item - with try/except for safety
    try:
        from gooddata_platform2cloud.web_compare_processing.parsing import (
            determine_status,
        )

        status = determine_status(entry)

        # Make sure error_message is used to set the status
        if "error_message" in entry:
            status = ComparisonStatus.ERROR
    except Exception:
        # Default to success status if determination fails
        status = ComparisonStatus.SUCCESS

    # Check special statuses
    status = _check_special_statuses(
        status, cloud_id, failed_publishing_lookup, skipped_ids
    )

    # Extract Cloud title - fall back to Platform title if not available
    try:
        cloud_title = extract_cloud_title(cloud_definition)
        if not cloud_title:
            cloud_title = platform_title
    except Exception:
        cloud_title = platform_title

    # Generate URLs with error handling
    try:
        urls = generate_urls(
            platform_id,
            platform_obj_id,
            cloud_id,
            object_type,
            platform_domain,
            platform_workspace,
            cloud_domain,
            cloud_workspace,
            entry_info,
        )
    except Exception as e:
        print(f"Warning: Error generating URLs for entry {index}: {e}")
        urls = ObjectUrls()  # Empty URLs

    # If cloud_definition is just an ERROR line, we should not have a cloud_id
    if isinstance(cloud_definition, str) and cloud_definition.startswith("ERROR:"):
        # Clear any cloud_id that might have been set
        cloud_id = None
        entry["cloud_id"] = None
        # Recreate URLs without the cloud_id
        urls = generate_urls(
            platform_id,
            platform_obj_id,
            None,
            object_type,
            platform_domain,
            platform_workspace,
            cloud_domain,
            cloud_workspace,
            entry_info,
        )

    # Get error details - use raw_error if available
    details = ""
    if "raw_error" in entry:
        details = entry["raw_error"]
    elif isinstance(cloud_definition, str) and cloud_definition.startswith("ERROR:"):
        details = cloud_definition
    else:
        details = _get_error_details(
            status, cloud_definition, cloud_id, failed_publishing_lookup, entry
        )

    # Extract Cloud description with error handling
    cloud_description = None
    # First check for raw_error
    if "raw_error" in entry:
        cloud_description = entry["raw_error"]
    # If cloud_definition is a string starting with ERROR:, use it directly as the description
    elif isinstance(cloud_definition, str) and cloud_definition.startswith("ERROR:"):
        cloud_description = cloud_definition
    # Check for nested data/attributes/description in the cloud definition
    elif (
        isinstance(cloud_definition, dict)
        and "data" in cloud_definition
        and isinstance(cloud_definition["data"], dict)
    ):
        data = cloud_definition["data"]
        if "attributes" in data and isinstance(data["attributes"], dict):
            attributes = data["attributes"]
            if "description" in attributes:
                cloud_description = attributes["description"]
    # Otherwise check for a description field in dictionary format
    elif isinstance(cloud_definition, dict) and "description" in cloud_definition:
        cloud_description = cloud_definition["description"]
    # Use details as description if we don't have a description but have details
    elif details:
        cloud_description = details

    # Create the comparison item
    item = ComparisonItem(
        platform_id=platform_id,
        platform_title=platform_title,
        platform_url=urls.platform_url,
        cloud_id=cloud_id if cloud_id and cloud_id != "-" else None,
        cloud_title=cloud_title,
        cloud_url=urls.cloud_url if cloud_id and cloud_id != "-" else None,
        status=status,
        ordinal_number=str(index),
        details=details,
        cloud_description=cloud_description,
        platform_embedded_url=urls.platform_embedded_url,
        cloud_embedded_url=(
            urls.cloud_embedded_url if cloud_id and cloud_id != "-" else None
        ),
    )

    # Check if this is an ERROR item, and force the description
    if isinstance(cloud_definition, str) and cloud_definition.startswith("ERROR:"):
        item.cloud_description = cloud_definition
        item.cloud_id = None  # Ensure cloud_id is None for ERROR lines
        item.cloud_url = None  # Ensure cloud_url is None for ERROR lines
        item.cloud_embedded_url = (
            None  # Ensure cloud_embedded_url is None for ERROR lines
        )
    elif "raw_error" in entry:
        item.cloud_description = entry["raw_error"]
    # Check for nested description in proper JSON
    elif (
        isinstance(cloud_definition, dict)
        and "data" in cloud_definition
        and isinstance(cloud_definition["data"], dict)
    ):
        data = cloud_definition["data"]
        if "attributes" in data and isinstance(data["attributes"], dict):
            attributes = data["attributes"]
            if "description" in attributes:
                item.cloud_description = attributes["description"]

    # Add the item to the result
    result.add_item(item)


def _check_special_statuses(
    status: ComparisonStatus,
    cloud_id: Optional[str],
    failed_publishing_lookup: Dict[str, str],
    skipped_ids: Optional[Set] = None,
) -> ComparisonStatus:
    """
    Check for special status cases like API_ERROR or SKIPPED.

    Args:
        status: Current status
        cloud_id: Cloud ID
        failed_publishing_lookup: Dictionary of failed publishing errors
        skipped_ids: Optional set of skipped object IDs

    Returns:
        Updated status
    """
    # Check if this entry is in the failed publishing list
    if cloud_id and cloud_id in failed_publishing_lookup:
        # Override the status to API_ERROR
        return ComparisonStatus.API_ERROR

    # Check if this entry was skipped
    if cloud_id and skipped_ids and cloud_id in skipped_ids:
        # Override the status to SKIPPED
        return ComparisonStatus.SKIPPED

    return status


def _get_error_details(
    status: ComparisonStatus,
    cloud_definition: Any,
    cloud_id: Optional[str],
    failed_publishing_lookup: Dict[str, str],
    entry: Optional[Dict] = None,
) -> str:
    """
    Get error details for an item.

    Args:
        status: Item status
        cloud_definition: Cloud definition
        cloud_id: Cloud ID
        failed_publishing_lookup: Dictionary of failed publishing errors
        entry: Optional original entry data

    Returns:
        Error details string
    """
    details = ""

    # Always check for errors, even if status is not error
    # This allows us to capture error details even if status determination didn't catch it
    try:
        # First check for our explicitly set error_message
        if entry and "error_message" in entry:
            return entry["error_message"]

        # For string errors that start with ERROR:, prioritize capturing the entire message
        if isinstance(cloud_definition, str) and cloud_definition.startswith("ERROR:"):
            return cloud_definition  # Return the entire error line
        # For dictionary errors with error field
        elif isinstance(cloud_definition, dict) and "error" in cloud_definition:
            details = str(cloud_definition["error"])
        # Check for failed publishing error
        elif cloud_id and cloud_id in failed_publishing_lookup:
            details = failed_publishing_lookup[cloud_id]
        # Check if it's a string with other error indicators
        elif isinstance(cloud_definition, str) and "[ERROR]" in cloud_definition:
            details = cloud_definition
    except Exception as e:
        details = f"Error details not available: {e}"

    return details
