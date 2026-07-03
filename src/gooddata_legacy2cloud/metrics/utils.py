# (C) 2026 GoodData Corporation
"""
This module provides utility functions for working with metrics.
"""

import json
import re

from gooddata_legacy2cloud.backends.legacy.client import LegacyClient
from gooddata_legacy2cloud.constants import UNKNOWN_DATE_MIGRATION_GRANULARITY
from gooddata_legacy2cloud.metrics.contants import DAY_SHORTCUTS


def comment_out_lines(text: str) -> str:
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


def build_placeholder_maql(
    original_maql: str,
    extra_message: str | None = None,
    error_label: str | None = "Error",
) -> str:
    """
    Comments out the original MAQL, optionally appends a labeled note, and adds a
    SQRT(-1) placeholder expression so the metric can still be created in Cloud.
    """
    commented_maql = adjust_comment_for_broken_metric(comment_out_lines(original_maql))
    if extra_message:
        label = f"#{error_label}:\n" if error_label else ""
        commented_maql += f"\n\n{label}{comment_out_lines(extra_message)}"
    return f"#Failed MAQL:\n{commented_maql}\n\nSELECT SQRT(-1)"


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

    error_message = ""
    extra_label = None
    if error_response is not None:
        # Extract the error text (either detail field or full response)
        if hasattr(error_response, "text") and error_response.text:
            try:
                error_data = json.loads(error_response.text)
                error_message = (
                    error_data.get("detail", error_response.text)
                    if isinstance(error_data, dict)
                    else error_response.text
                )
            except json.JSONDecodeError:
                error_message = error_response.text

        if error_message:
            extra_label = f"API Error {error_response.status_code}"

    metric["data"]["attributes"]["content"]["maql"] = build_placeholder_maql(
        maql, error_message, extra_label
    )
    return metric


def get_folders_names(legacy_client: LegacyClient, folders_URIs: list[str]):
    """
    Returns the folders from the Legacy object.
    """
    folders = []
    for folder_URI in folders_URIs:
        folder_obj = legacy_client.get_object(folder_URI)
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
