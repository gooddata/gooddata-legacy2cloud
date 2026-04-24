# (C) 2026 GoodData Corporation
"""
Date filter classification logic for Legacy to Cloud migration.

This module handles the classification of date filters as cyclical or ordinal,
and manages granularity mappings and attribute type detection.
"""

# Import shared cyclical date utilities
from gooddata_legacy2cloud.metrics.cyclical_date_conversion import (
    _get_legacy_attribute_type,
    _is_cyclical_legacy_attribute,
)
from gooddata_legacy2cloud.reports.data_classes import ContextWithWarnings


def classify_date_filter_type(ctx: ContextWithWarnings, attr_uri, element_uris):
    """
    Classify a date filter as either cyclical or ordinal based on the Legacy attribute type.

    Cyclical date filters (Quarter of Year, Month of Year, Week of Year, Day of Week,
    Day of Month, Day of Year) are treated as attribute filters in Cloud but need
    a companion relativeDateFilter.

    Ordinal date filters (specific dates, months, years, quarters, weeks, etc.)
    need to be converted to absoluteDateFilter with from/to ranges.

    Args:
        ctx: The context object with API and mappings
        attr_uri: The URI of the date attribute
        element_uris: List of element URIs from the filter (unused now, kept for compatibility)

    Returns:
        tuple: (filter_type, attribute_identifier, granularity) where filter_type is
               "cyclical" or "ordinal", attribute_identifier is the attribute identifier,
               and granularity is the date granularity
    """
    try:
        # Get the attribute object to determine the attribute identifier
        obj = ctx.legacy_client.get_object(attr_uri)
        attribute_identifier = None

        if "attributeDisplayForm" in obj:
            attribute_identifier = obj["attributeDisplayForm"]["meta"]["identifier"]
        elif "attribute" in obj:
            attribute_identifier = obj["attribute"]["meta"]["identifier"]

        if not attribute_identifier:
            return "ordinal", attr_uri, "GDC.time.year"

        # Determine if this is a cyclical attribute based on the Legacy attribute type
        is_cyclical = _is_cyclical_legacy_attribute(attribute_identifier)

        # Get the granularity using the DATE_MAPPINGS
        granularity, is_supported = _get_date_granularity_from_mappings(
            attribute_identifier, ctx
        )

        if not is_supported:
            # Unsupported granularity, return None to indicate filter should be removed
            return None, None, None

        if is_cyclical:
            return "cyclical", attribute_identifier, granularity
        else:
            return "ordinal", attribute_identifier, granularity

    except Exception:
        # Log error and remove filter if we can't determine the granularity
        # Try to get the Legacy identifier, fallback to attr_uri
        try:
            obj = ctx.legacy_client.get_object(attr_uri)
            if "attributeDisplayForm" in obj:
                legacy_id = obj["attributeDisplayForm"]["meta"]["identifier"]
            elif "attribute" in obj:
                legacy_id = obj["attribute"]["meta"]["identifier"]
            else:
                legacy_id = attr_uri
        except Exception:
            legacy_id = attr_uri

        ctx.log_warning(
            f"FILTER REMOVED: can not get granularity for {legacy_id}", to_stderr=True
        )
        return None, None, None


def _get_date_granularity_from_mappings(attribute_identifier, ctx: ContextWithWarnings):
    """
    Get the Cloud granularity for a Legacy date attribute using the DATE_MAPPINGS.

    Args:
        attribute_identifier: The Legacy attribute identifier
        ctx: Context for logging warnings

    Returns:
        tuple: (cloud_granularity, is_supported) where is_supported indicates
               if the granularity is supported (not UNKNOWN_DATE_MIGRATION_GRANULARITY)
    """
    from gooddata_legacy2cloud.constants import UNKNOWN_DATE_MIGRATION_GRANULARITY
    from gooddata_legacy2cloud.ldm.cloud_model_builder import DATE_MAPPINGS

    # Convert the DATE_MAPPINGS list to a dictionary for easy lookup
    date_mappings = {
        legacy_pattern: cloud_pattern for legacy_pattern, cloud_pattern in DATE_MAPPINGS
    }

    attr_type = _get_legacy_attribute_type(attribute_identifier)
    if not attr_type:
        return "GDC.time.year", False

    cloud_granularity = date_mappings.get(attr_type)
    if not cloud_granularity:
        return "GDC.time.year", False

    # Check if it's an unsupported granularity
    if UNKNOWN_DATE_MIGRATION_GRANULARITY in cloud_granularity:
        if hasattr(ctx, "log_warning"):
            ctx.log_warning(
                f"FILTER REMOVED: date granularity {attribute_identifier} not supported",
                to_stderr=True,
            )
        return None, False

    # Convert simple granularity names to GDC format
    granularity_mapping = {
        "year": "GDC.time.year",
        "quarter": "GDC.time.quarter",
        "month": "GDC.time.month",
        "week": "GDC.time.week_us",
        "day": "GDC.time.date",
        "quarterOfYear": "GDC.time.quarter",
        "monthOfYear": "GDC.time.month",
        "weekOfYear": "GDC.time.week_us",
        "dayOfYear": "GDC.time.date",
        "dayOfWeek": "GDC.time.date",
        "dayOfMonth": "GDC.time.date",
    }

    gdc_granularity = granularity_mapping.get(cloud_granularity, "GDC.time.year")
    return gdc_granularity, True
