# (C) 2026 GoodData Corporation
"""
Waterfall chart processing for Platform to Cloud migration.

This module handles processing of waterfall charts, including:
- Checking for unsupported features such as attributes and fixedMapping
- Processing waterfall-specific configurations
"""

from gooddata_platform2cloud.reports.data_classes import ContextWithWarnings
from gooddata_platform2cloud.reports.mappings import map_chart_metric


def check_waterfall_config(ctx: ContextWithWarnings, chart, content):
    """
    Check for unsupported waterfall chart configurations and issue warnings.

    Args:
        ctx (ReportContext): The context object containing mappings
        chart (dict): The Platform chart configuration
        content (dict): The report content

    Returns:
        list: List of metrics that were in fixedMapping (if any)
    """
    fixed_mapping_metrics = []

    # Check for waterfall config with fixedMapping (not supported in Cloud)
    global_styles = chart.get("styles", {}).get("global", {})
    waterfall_config = global_styles.get("waterfallConfig", {})
    fixed_mapping = waterfall_config.get("fixedMapping", [])

    if fixed_mapping:
        for metric_uri in fixed_mapping:
            # Process the string URI directly
            try:
                # Retrieve Platform object and extract metric meta identifier
                platform_obj = ctx.platform_client.get_object(metric_uri)
                if "metric" in platform_obj:
                    platform_metric_identifier = platform_obj["metric"]["meta"][
                        "identifier"
                    ]
                else:
                    platform_metric_identifier = metric_uri
                converted_id = ctx.metric_mappings.search_mapping_identifier(
                    platform_metric_identifier
                )
                fixed_mapping_metrics.append(converted_id)
            except Exception:
                fixed_mapping_metrics.append(metric_uri)

        if fixed_mapping_metrics:
            ctx.log_warning(
                f"Total defined by metric not supported in waterfall chart. Metrics displayed as normal: {', '.join(fixed_mapping_metrics)}"
            )

    # Check for attributes in waterfall chart (not supported in Cloud)
    buckets = chart.get("buckets", {})
    attribute_found = False
    skipped_attributes = []

    for bucket_name in ["x", "y", "color"]:
        bucket_items = buckets.get(bucket_name, [])
        for item in bucket_items:
            uri = item.get("uri")
            if uri and uri not in ["metric", "metricGroup"]:
                attribute_found = True
                try:
                    # Try to get the attribute identifier
                    attr_obj = ctx.platform_client.get_object(uri)
                    if "attribute" in attr_obj:
                        platform_attr_identifier = attr_obj["attribute"]["meta"][
                            "identifier"
                        ]
                    elif "attributeDisplayForm" in attr_obj:
                        platform_attr_identifier = attr_obj["attributeDisplayForm"][
                            "meta"
                        ]["identifier"]
                    else:
                        platform_attr_identifier = uri

                    # Convert Platform identifier to Cloud identifier
                    cloud_id = ctx.ldm_mappings.search_mapping_identifier(
                        platform_attr_identifier
                    )
                    skipped_attributes.append(cloud_id)
                except Exception:
                    skipped_attributes.append(uri)

    if attribute_found:
        skip_attr_str = ", ".join(skipped_attributes)
        ctx.log_warning(
            f"Split of Waterfall chart by attribute not supported. Attribute not used: {skip_attr_str}"
        )

    return fixed_mapping_metrics


def process_waterfall_chart(ctx: ContextWithWarnings, chart, content):
    """
    Process waterfall chart metrics into the measures bucket.

    Args:
        ctx (ReportContext): The context object containing mappings
        chart (dict): The Platform chart configuration
        content (dict): The report content

    Returns:
        list: The buckets for the waterfall chart
    """
    # Check for unsupported features and issue warnings
    check_waterfall_config(ctx, chart, content)

    # Get metrics from grid
    grid_metrics = content.get("grid", {}).get("metrics", [])

    # Create measures bucket with grid metrics
    measures_items = [
        map_chart_metric(ctx, metric, i + 1) for i, metric in enumerate(grid_metrics)
    ]

    # Return a simple measures bucket
    return [{"localIdentifier": "measures", "items": measures_items}]
