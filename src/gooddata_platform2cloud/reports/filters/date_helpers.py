# (C) 2026 GoodData Corporation
"""
Basic date helper functions for filter processing.

This module contains fundamental date operations and dataset detection
used across different filter types.
"""

from gooddata_platform2cloud.pp_dashboards.data_classes import PPDashboardContext
from gooddata_platform2cloud.reports.data_classes import ContextWithWarnings


def is_date_attribute(ctx: ContextWithWarnings, attr_uri):
    """
    Check if an attribute is a date attribute.

    Args:
        ctx: The context object with API and mappings
        attr_uri: The URI of the attribute

    Returns:
        bool: True if it's a date attribute, False otherwise
    """
    try:
        obj = ctx.platform_client.get_object(attr_uri)
        if "attribute" in obj and "content" in obj["attribute"]:
            if (
                "type" in obj["attribute"]["content"]
                and "GDC.time" in obj["attribute"]["content"]["type"]
            ):
                return True
        return False
    except Exception:
        return False


def get_date_dataset_and_granularity(
    ctx: ContextWithWarnings | PPDashboardContext, attribute_identifier
):
    """
    Extract dataset name and granularity from a date attribute identifier.

    Args:
        ctx: The context object with API and mappings
        attribute_identifier: The identifier of the date attribute

    Returns:
        tuple: (dataset_id, granularity) with the dataset ID and granularity
    """
    dataset_id = None
    granularity = "GDC.time.year"  # Default granularity
    platform_dataset_name = None

    # Extract dataset name from attribute identifier
    if isinstance(attribute_identifier, str):
        parts = attribute_identifier.split(".")
        if len(parts) >= 2:
            platform_dataset_name = parts[0]
            # The part after the first dot can indicate granularity
            attr_part = parts[1]
            if attr_part == "date":
                granularity = "GDC.time.date"
            elif attr_part == "month":
                granularity = "GDC.time.month"
            elif attr_part == "quarter":
                granularity = "GDC.time.quarter"
            elif attr_part == "year":
                granularity = "GDC.time.year"
            elif attr_part == "week":
                granularity = "GDC.time.week_us"

    # Try to find the dataset ID directly
    if platform_dataset_name:
        # DIRECT MAPPING: First try to find the dataset ID directly by looking for dt_datasetname
        dataset_id = f"dt_{platform_dataset_name}"

        # MAPPING LOOKUP: If direct doesn't work, try finding it in mappings
        if not dataset_id or dataset_id not in ctx.ldm_mappings.get().values():
            try:
                # Try to find using dataset.name format
                mapped_id = ctx.ldm_mappings.search_mapping_identifier(
                    f"dataset.{platform_dataset_name}"
                )
                if mapped_id and mapped_id.startswith("dt_"):
                    dataset_id = mapped_id
            except Exception:
                # Fallback: try to find any mapping containing the dataset name
                for key, value in ctx.ldm_mappings.get().items():
                    if (
                        platform_dataset_name.lower() in key.lower()
                        and value.startswith("dt_")
                    ):
                        dataset_id = value
                        break

    return dataset_id, granularity


def contains_date_range_recursively(node):
    """
    Recursively check if a node contains date range indicators (time macros, dates).

    Args:
        node (dict): The tree node to check

    Returns:
        bool: True if date range indicators are found, False otherwise
    """
    if not isinstance(node, dict):
        return False

    # Check if this node itself is a time macro or date
    node_type = node.get("type")
    if node_type in ["time macro", "date"]:
        return True

    # Recursively check all children
    for child in node.get("content", []):
        if contains_date_range_recursively(child):
            return True

    return False
