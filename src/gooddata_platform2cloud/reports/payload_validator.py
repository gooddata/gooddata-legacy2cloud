# (C) 2026 GoodData Corporation
"""
This module provides validation and size reduction functionality for report visualization payloads.
It checks if the payload exceeds the maximum API size limit and attempts to reduce the payload
size by truncating columnWidths and colorMapping properties (the most often the cause of the size limit being exceeded).
"""

import json
import logging

logger = logging.getLogger("migration")

# Maximum allowed size for API payload in characters
API_PAYLOAD_SIZE_LIMIT = 250000


def validate_and_reduce_payload_size(
    cloud_report: dict, report_title: str = "Unknown"
) -> dict:
    """
    Validates and reduces the size of a Cloud report payload if it exceeds the API size limit.

    Args:
        cloud_report (dict): The Cloud visualization object
        report_title (str): Title of the report for logging purposes

    Returns:
        dict: The potentially modified Cloud visualization object
    """
    # Convert to JSON string to check size
    payload_json = json.dumps(cloud_report, separators=(",", ":"))
    current_size = len(payload_json)

    if current_size <= API_PAYLOAD_SIZE_LIMIT:
        return cloud_report

    logger.warning(
        "Report '%s': Payload size (%d characters) exceeds API limit (%d characters)",
        report_title,
        current_size,
        API_PAYLOAD_SIZE_LIMIT,
    )

    # Try to reduce size by truncating columnWidths first
    modified_report = _reduce_column_widths(cloud_report, report_title)
    payload_json = json.dumps(modified_report, separators=(",", ":"))
    current_size = len(payload_json)

    if current_size > API_PAYLOAD_SIZE_LIMIT:
        # If still over limit, try to reduce colorMapping
        modified_report = _reduce_color_mapping(modified_report, report_title)

    return modified_report


def _reduce_column_widths(cloud_report: dict, report_title: str) -> dict:
    """
    Reduces the size of columnWidths by keeping only the first 50 items if it has more than 50.

    Args:
        cloud_report (dict): The Cloud visualization object
        report_title (str): Title of the report for logging purposes

    Returns:
        dict: The potentially modified Cloud visualization object
    """
    try:
        properties = (
            cloud_report.get("data", {})
            .get("attributes", {})
            .get("content", {})
            .get("properties", {})
        )
        controls = properties.get("controls", {})
        column_widths = controls.get("columnWidths", [])

        if len(column_widths) > 50:
            # Keep only first 50 items
            controls["columnWidths"] = column_widths[:50]
            logger.warning(
                "Report '%s': columnWidths truncated from %d to 50 items to reduce payload size",
                report_title,
                len(column_widths),
            )

    except (KeyError, TypeError, AttributeError) as e:
        # If the structure is unexpected, just log and continue
        logger.info(
            "Could not access columnWidths for report '%s': %s", report_title, e
        )

    return cloud_report


def _reduce_color_mapping(cloud_report: dict, report_title: str) -> dict:
    """
    Reduces the size of colorMapping by keeping only the first 50 items if it has more than 50.

    Args:
        cloud_report (dict): The Cloud visualization object
        report_title (str): Title of the report for logging purposes

    Returns:
        dict: The potentially modified Cloud visualization object
    """
    try:
        properties = (
            cloud_report.get("data", {})
            .get("attributes", {})
            .get("content", {})
            .get("properties", {})
        )
        controls = properties.get("controls", {})
        color_mapping = controls.get("colorMapping", [])

        if len(color_mapping) > 50:
            # Keep only first 50 items
            controls["colorMapping"] = color_mapping[:50]
            logger.warning(
                "Report '%s': colorMapping truncated from %d to 50 items to reduce payload size",
                report_title,
                len(color_mapping),
            )

    except (KeyError, TypeError, AttributeError) as e:
        # If the structure is unexpected, just log and continue
        logger.info(
            "Could not access colorMapping for report '%s': %s", report_title, e
        )

    return cloud_report


def get_payload_size(cloud_report: dict) -> int:
    """
    Returns the size of the payload in characters.

    Args:
        cloud_report (dict): The Cloud visualization object

    Returns:
        int: Size of the payload in characters
    """
    payload_json = json.dumps(cloud_report, separators=(",", ":"))
    return len(payload_json)
