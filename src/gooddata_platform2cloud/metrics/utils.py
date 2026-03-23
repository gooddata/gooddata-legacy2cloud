# (C) 2026 GoodData Corporation
"""
This module provides utility functions for working with metrics.
"""

import json
import re

from gooddata_platform2cloud.backends.platform.client import PlatformClient
from gooddata_platform2cloud.constants import UNKNOWN_DATE_MIGRATION_GRANULARITY
from gooddata_platform2cloud.metrics.contants import DAY_SHORTCUTS


def _comment_out_lines(text: str) -> str:
    """Helper function to comment out all lines in a text string."""
    return "\n".join(f"#{line}" for line in text.splitlines())


def get_identifiers_with_unknown_granularity(maql_string):
    pattern = re.compile(rf"{UNKNOWN_DATE_MIGRATION_GRANULARITY}\.([^\}}]+)\}}")

    # Search for the pattern in the string
    match = re.search(pattern, maql_string)

    # If a match is found, return the captured group
    if match:
        return match.groups()
    else:
        return None


def adjust_comment_for_broken_metric(output):
    """
    Adjusts the comment for broken metric.
    """
    if UNKNOWN_DATE_MIGRATION_GRANULARITY in output:
        unknown_identifiers = get_identifiers_with_unknown_granularity(output)
        for item in unknown_identifiers:
            output = f"{output}\n#GoodData Cloud does not support {item}, please modify your metric"

    return output


def disable_broken_metric(metric, error_response=None):
    """
    Fixes a broken metric by adding a tag and commenting out the original MAQL.
    Optionally includes API error response in the comments.
    Processes one metric at a time.

    Args:
        metric: The metric object to fix
        error_response: Optional error response from API (should have status_code and text attributes)
    """
    attribute = metric["data"]["attributes"]
    if attribute["tags"] is None:
        attribute["tags"] = ["ERROR"]
    else:
        attribute["tags"].append("ERROR")

    attribute["title"] = f"[ERROR] {attribute['title']}"
    maql = metric["data"]["attributes"]["content"]["maql"]
    commented_maql = _comment_out_lines(maql)
    commented_maql = adjust_comment_for_broken_metric(commented_maql)

    # Add API error response if provided
    if error_response is not None:
        # Extract the error text (either detail field or full response)
        error_message = ""
        if hasattr(error_response, "text") and error_response.text:
            try:
                error_data = json.loads(error_response.text)
                error_message = error_data.get("detail", error_response.text)
            except json.JSONDecodeError:
                error_message = error_response.text

        # Add the commented error to the MAQL
        if error_message:
            commented_error = _comment_out_lines(error_message)
            commented_maql += (
                f"\n\n#API Error {error_response.status_code}:\n{commented_error}"
            )

    metric["data"]["attributes"]["content"]["maql"] = (
        f"#Failed MAQL:\n{commented_maql}\n\nSELECT SQRT(-1)"
    )
    return metric


def get_folders_names(platform_client: PlatformClient, folders_URIs: list[str]):
    """
    Returns the folders from the Platform object.
    """
    folders = []
    for folder_URI in folders_URIs:
        folder_obj = platform_client.get_object(folder_URI)
        title = folder_obj["folder"]["meta"]["title"]
        folders.append(title)
    return folders


def parse_day_shortcut_to_number(day_shortcut):
    """
    Parses a day representation in shortcut format to a number.
    """
    return DAY_SHORTCUTS.get(
        day_shortcut, -1
    )  # Return -1 if the day shortcut is not found
