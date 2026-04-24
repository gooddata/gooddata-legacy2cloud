# (C) 2026 GoodData Corporation
"""
Utility functions for working with Legacy display forms.

This module provides shared functionality for finding and processing
display forms across different parts of the migration tool.
"""

import re
from typing import Any


def get_primary_display_form(
    display_forms: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """
    Get the primary display form from a list of display forms.

    The primary display form is determined by finding the one with the
    shortest identifier (fewest dots). This follows Legacy's convention
    where simpler identifiers represent primary display forms.

    Args:
        display_forms: List of display form objects from an attribute

    Returns:
        The primary display form object, or None if the list is empty

    Example:
        >>> display_forms = [
        ...     {"meta": {"identifier": "attr.name.default"}},
        ...     {"meta": {"identifier": "attr.name"}}
        ... ]
        >>> primary = get_primary_display_form(display_forms)
        >>> primary["meta"]["identifier"]
        'attr.name'
    """
    if not display_forms:
        return None

    primary_display_form = None
    for display_form in display_forms:
        current_length = display_form["meta"]["identifier"].count(".")
        if not primary_display_form:
            primary_display_form = display_form
            continue

        primary_length = primary_display_form["meta"]["identifier"].count(".")
        if current_length < primary_length:
            primary_display_form = display_form

    return primary_display_form


def extract_object_id_from_uri(uri: str) -> str | None:
    """
    Extract the object ID from a Legacy URI.

    Args:
        uri: Legacy object URI (e.g., "/gdc/md/workspace/obj/12345")

    Returns:
        The object ID string, or None if not found

    Example:
        >>> extract_object_id_from_uri("/gdc/md/workspace/obj/12345")
        '12345'
    """
    match = re.search(r"/obj/(\d+)(?:/|$)", uri)
    return match.group(1) if match else None
