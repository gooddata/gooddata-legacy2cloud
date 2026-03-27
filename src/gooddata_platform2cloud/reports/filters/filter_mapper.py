# (C) 2026 GoodData Corporation
"""
Filter mapping functionality for Platform to Cloud migration.

This module provides the main entry point for filter processing, which dispatches
filters to the appropriate specialized processing function based on their type.
"""

from gooddata_platform2cloud.reports.data_classes import ContextWithWarnings
from gooddata_platform2cloud.reports.filters.attribute_filter import (
    NegativeAttributeFilter,
    PositiveAttributeFilter,
)
from gooddata_platform2cloud.reports.filters.date_filter import DateFilter
from gooddata_platform2cloud.reports.filters.date_helpers import is_date_attribute
from gooddata_platform2cloud.reports.filters.measure_filter import MeasureFilter
from gooddata_platform2cloud.reports.filters.ranking_filter import RankingFilter


def _get_attribute_uri_from_filter(filter_obj):
    """
    Extract the attribute URI from an attribute filter object.

    Args:
        filter_obj (dict): The Platform filter object

    Returns:
        str or None: The attribute URI if found, None otherwise
    """
    tree = filter_obj.get("tree", {})
    filter_type = tree.get("type")

    # For positive attribute filters ("in" type)
    if filter_type == "in":
        for node in tree.get("content", []):
            if node.get("type") == "attribute object":
                return node.get("value")

    # For negative attribute filters ("not in" type)
    elif filter_type == "not in":
        for node in tree.get("content", []):
            if node.get("type") == "attribute object":
                return node.get("value")

    # For unary NOT filters (NOT (A IN B) format)
    elif filter_type == "not":
        for child in tree.get("content", []):
            if child.get("type") == "()":
                for inner_child in child.get("content", []):
                    if inner_child.get("type") == "in":
                        for node in inner_child.get("content", []):
                            if node.get("type") == "attribute object":
                                return node.get("value")

    # For comparison operators (=, !=, <, <=, >, >=)
    elif filter_type in ["=", "<>", "<", "<=", ">", ">="]:
        for node in tree.get("content", []):
            if node.get("type") == "attribute object":
                return node.get("value")

    return None


def map_filter(
    filter_obj: dict,
    metric_local_ids: dict,
    column_attributes: list,
    displayed_attributes: set,
    ctx: ContextWithWarnings,
    buckets: list | None = None,
) -> tuple[dict | None, dict]:
    """
    Maps a Platform filter to a Cloud filter.

    This is the main entry point for filter processing. It determines the type of filter
    and dispatches it to the appropriate specialized processing function.

    Args:
        filter_obj (dict): The Platform filter object
        metric_local_ids (dict): Dictionary mapping metric URIs to local IDs
        column_attributes (list): List of column attributes
        displayed_attributes (set): Set of displayed attribute URIs
        ctx: The context object with API and mappings
        buckets (list): Optional list of visualization buckets for validation

    Returns:
        tuple: (cloud_filter, filter_config) with the converted filter and its configuration
    """
    # New branch: if filter is a variable filter then log warning and ignore
    if (
        filter_obj.get("type") == "prompt object"
        or filter_obj.get("tree", {}).get("type") == "prompt object"
    ):
        ctx.log_warning("FILTER REMOVED: variable filter not supported", to_stderr=True)
        return None, {}

    # Ensure displayed_attributes is a set to prevent type errors
    if displayed_attributes is None or not isinstance(displayed_attributes, set):
        displayed_attributes = set()

    # Get the filter type
    filter_type = filter_obj.get("tree", {}).get("type")

    # Dispatch to the appropriate filter class based on filter type

    # Ranking filters (top/bottom)
    if filter_type in ["top", "bottom"]:
        ranking_filter = RankingFilter(ctx)
        return ranking_filter.process(
            filter_obj, metric_local_ids, displayed_attributes, buckets=buckets
        )

    # Negative attribute filter (both unary NOT and binary NOT IN)
    elif filter_type in ["not", "not in"]:
        # Check if this is actually a date filter
        attr_uri = _get_attribute_uri_from_filter(filter_obj)

        is_date = is_date_attribute(ctx, attr_uri) if attr_uri else False

        if attr_uri and is_date:
            date_filter = DateFilter(ctx)
            return date_filter.process(filter_obj, filter_type="negative_absolute")
        else:
            negative_attribute_filter = NegativeAttributeFilter(ctx)
            return negative_attribute_filter.process(filter_obj)

    # Date between filter
    elif filter_type == "between":
        date_filter = DateFilter(ctx)
        return date_filter.process(filter_obj)

    # Comparison operators (measure value filters or date equality filters)
    elif filter_type in [">", ">=", "<", "<=", "=", "<>"]:
        # Check if this is actually a date filter
        attr_uri = _get_attribute_uri_from_filter(filter_obj)

        is_date = is_date_attribute(ctx, attr_uri) if attr_uri else False

        if attr_uri and is_date:
            date_filter = DateFilter(ctx)
            return date_filter.process(filter_obj, filter_type="equality")
        else:
            measure_filter = MeasureFilter(ctx)
            return measure_filter.process(
                filter_obj,
                metric_local_ids,
                buckets=buckets,
                displayed_attributes=displayed_attributes,
            )

    # Positive attribute filter
    elif filter_type == "in":
        # Check if this is actually a date filter
        attr_uri = _get_attribute_uri_from_filter(filter_obj)

        is_date = is_date_attribute(ctx, attr_uri) if attr_uri else False

        if attr_uri and is_date:
            date_filter = DateFilter(ctx)
            return date_filter.process(filter_obj, filter_type="positive_absolute")
        else:
            positive_attribute_filter = PositiveAttributeFilter(ctx)
            return positive_attribute_filter.process(filter_obj)

    # Date between filter
    elif filter_type == "not between":
        ctx.log_warning(
            f"FILTER REMOVED: Negative date filter not supported: {filter_type}",
            to_stderr=True,
        )
        return None, {}

    # Unknown filter type
    else:
        ctx.log_warning(
            f"FILTER REMOVED: Unknown filter type: {filter_type}", to_stderr=True
        )
        return None, {}
