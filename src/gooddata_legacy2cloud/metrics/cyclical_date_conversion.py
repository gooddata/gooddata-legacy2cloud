# (C) 2026 GoodData Corporation
"""
Shared cyclical date conversion logic for Legacy to Cloud migration.

This module provides utilities for converting cyclical date attribute element URIs
(day.in.week, quarter.in.year, month.in.year, etc.) to their Cloud values using
hardcoded mappings based on element IDs.

This logic is shared between insights and reports migration to ensure consistent
behavior across all migration types.
"""

import re

from gooddata_legacy2cloud.insights.data_classes import InsightContext
from gooddata_legacy2cloud.metrics.attribute_element import AttributeElement
from gooddata_legacy2cloud.metrics.contants import MISSING_VALUE
from gooddata_legacy2cloud.reports.data_classes import ContextWithWarnings


def _get_legacy_attribute_type(attribute_identifier):
    """
    Extract the Legacy attribute type from the full identifier.

    This function strips the dataset prefix and any display form suffix to get
    the core attribute type used for cyclical date detection.

    Args:
        attribute_identifier: The Legacy attribute identifier
                            (e.g., "order.day.in.week" or "date.day.in.week.short")

    Returns:
        str: The attribute type part (e.g., "day.in.week") or None if invalid
    """
    if not attribute_identifier:
        return None

    # Split by dot and take everything after the first part (dataset name)
    parts = attribute_identifier.split(".", 1)
    if len(parts) < 2:
        return None

    attr_type = parts[1]

    # Remove display form suffixes (.short, .long, .number, .default, etc.)
    # These are common display form variations that don't change the attribute type
    display_form_suffixes = [
        ".short_us",  # Geographic variation
        ".number_us",  # Geographic variation
        ".number_eu",  # Geographic variation
        ".short",
        ".long",
        ".number",
        ".default",
        ".wk_year",  # Week/year format
        ".starting",  # Week starting date
        ".from_to",  # Week date range
        ".m_q",  # Month/quarter format
    ]
    for suffix in display_form_suffixes:
        if attr_type.endswith(suffix):
            attr_type = attr_type[: -len(suffix)]
            break

    return attr_type


def _is_cyclical_legacy_attribute(attribute_identifier):
    """
    Check if a Legacy date attribute is cyclical based on predefined Legacy attribute patterns.

    Args:
        attribute_identifier: The Legacy attribute identifier (e.g., "order.day.in.week")

    Returns:
        bool: True if the attribute is cyclical, False if ordinal
    """
    attr_type = _get_legacy_attribute_type(attribute_identifier)
    if not attr_type:
        return False

    # Cyclical patterns as defined by the user
    # NOTE: Only "X.in.Y" patterns are cyclical (repeating values within a period)
    # Regular date attributes like "euweek", "week", "month" are ordinal (contain year+value)
    cyclical_patterns = {
        "quarter.in.year",  # Q1-Q4 (repeats yearly)
        "week.in.year",  # Week 1-53 (repeats yearly)
        "week.in.quarter",  # Week 1-13 (repeats per quarter)
        "euweek.in.year",  # EU Week 1-53 (repeats yearly)
        "euweek.in.quarter",  # EU Week 1-13 (repeats per quarter)
        "month.in.year",  # Month 1-12 (repeats yearly)
        "month.in.quarter",  # Month 1-3 (repeats per quarter)
        "day.in.year",  # Day 1-366 (repeats yearly)
        "day.in.week",  # Day 1-7 (repeats weekly)
        "day.in.euweek",  # Day 1-7 (repeats weekly)
        "day.in.quarter",  # Day 1-92 (repeats per quarter)
        "day.in.month",  # Day 1-31 (repeats monthly)
    }

    return attr_type in cyclical_patterns


def convert_cyclical_date_elements(
    ctx: InsightContext | ContextWithWarnings, element_uris, attribute_identifier
):
    """
    Convert cyclical date attribute element URIs to their Cloud values using hardcoded mappings.

    WARNING: ELEMENT ID DEPENDENCY
    This function relies on ELEMENT IDs from Legacy URIs (e.g., elements?id=2) rather than reading
    the actual element values from the date dimension. This is intentional because:
    1. For STANDARD Legacy date dimensions, element IDs follow predictable patterns:
       - day.in.week: ID 1=Sunday, 2=Monday, ..., 7=Saturday
       - quarter.in.year: ID 1=Q1, 2=Q2, 3=Q3, 4=Q4
       - month.in.year: ID 1=January, 2=February, ..., 12=December
    2. For CUSTOM/CLIENT-LOADED date dimensions, the actual element LABELS/VALUES might be
       different (e.g., "Domingo" instead of "Sunday", custom quarter names, etc.) so we can not rely on those.
    3. Cloud requires standardized values (e.g., "00" for Sunday, "01" for Monday), so we
       convert based on positional/numeric element IDs rather than textual labels.
    4. This approach avoids the overhead of fetching and parsing actual element values for
       every cyclical date filter during migration.

    RISK: If a client has customized their date dimension with non-standard element ID
    ordering (which would be very unusual), this conversion might produce incorrect results.
    In such cases, manual review and adjustment of the migrated filters would be required.

    Args:
        ctx: The context object with API and mappings
        element_uris: List of element URIs to convert (format: .../elements?id=N)
        attribute_identifier: The attribute identifier to determine conversion type

    Returns:
        tuple: (converted_values, missing_elements, null_elements) with converted values,
               list of URIs that couldn't be converted, and list of null element URIs
    """
    converted_values = []
    missing_elements = []
    null_elements = []

    # Get the attribute type to determine conversion rules
    attr_type = _get_legacy_attribute_type(attribute_identifier)
    if not attr_type:
        return [], element_uris, []

    # Hardcoded conversion mappings based on Legacy element IDs (NOT element values)
    # These mappings assume standard Legacy date dimension element ID ordering patterns
    # (that should work for the urn:gooddata:date, urn:custom:date, urn:custom_v2:date, and most of custom ones)
    conversion_rules: dict = {
        "quarter.in.year": {
            "null_id": 0,
            "mapping": lambda x: f"{x:02d}" if 1 <= x <= 4 else None,
            "range": (1, 4),
        },
        "week.in.year": {
            "null_id": 0,
            "mapping": lambda x: f"{x:02d}" if 1 <= x <= 53 else None,
            "range": (1, 53),
        },
        "euweek.in.year": {
            "null_id": 0,
            "mapping": lambda x: f"{x:02d}" if 1 <= x <= 53 else None,
            "range": (1, 53),
        },
        "month.in.year": {
            "null_id": 0,
            "mapping": lambda x: f"{x:02d}" if 1 <= x <= 12 else None,
            "range": (1, 12),
        },
        "day.in.year": {
            "null_id": 0,
            "mapping": lambda x: f"{x:03d}" if 1 <= x <= 366 else None,
            "range": (1, 366),
        },
        "day.in.week": {
            "null_id": 0,
            # Legacy element IDs: 1=Sunday, 2=Monday, 3=Tuesday, 4=Wednesday, 5=Thursday, 6=Friday, 7=Saturday
            # Cloud values: 00=Sunday, 01=Monday, 02=Tuesday, 03=Wednesday, 04=Thursday, 05=Friday, 06=Saturday
            "mapping": lambda x: f"{(x - 1):02d}" if 1 <= x <= 7 else None,
            "range": (1, 7),
        },
        "day.in.euweek": {
            "null_id": 0,
            # Legacy element IDs: 1=Monday, 2=Tuesday, 3=Wednesday, 4=Thursday, 5=Friday, 6=Saturday, 7=Sunday
            # Cloud values: 01=Monday, 02=Tuesday, 03=Wednesday, 04=Thursday, 05=Friday, 06=Saturday, 00=Sunday
            "mapping": lambda x: (
                f"{x:02d}" if 1 <= x <= 6 else ("00" if x == 7 else None)
            ),
            "range": (1, 7),
        },
        "day.in.month": {
            "null_id": 0,
            "mapping": lambda x: f"{x:02d}" if 1 <= x <= 31 else None,
            "range": (1, 31),
        },
    }

    # Get the conversion rule for this attribute type
    rule = conversion_rules.get(attr_type)
    if not rule:
        # Fallback to regular conversion for unknown types
        converted_values = []
        missing_elements = []
        for uri in element_uris:
            val = AttributeElement(ctx, uri).get()
            if val == MISSING_VALUE:
                missing_elements.append(uri)
            else:
                converted_values.append(val)
        return converted_values, missing_elements, []

    for uri in element_uris:
        try:
            # Extract element ID from URI - we rely on the ID number, NOT the actual element value/label
            # This assumes standard Legacy date dimension element ID ordering (e.g., 1=Sunday, 2=Monday, etc.)
            match = re.search(r"elements\?id=(\d+)", uri)
            if not match:
                missing_elements.append(uri)
                continue

            element_id = int(match.group(1))

            # Check for null/empty values
            if element_id == rule["null_id"]:
                null_elements.append(uri)
                # Note: NULL warnings are now handled upstream in DateFilter._detect_null_elements()
                continue

            # Convert using the mapping function
            converted_value = rule["mapping"](element_id)
            if converted_value is not None:
                converted_values.append(converted_value)
            else:
                missing_elements.append(uri)

        except ValueError, AttributeError:
            missing_elements.append(uri)

    return converted_values, missing_elements, null_elements
