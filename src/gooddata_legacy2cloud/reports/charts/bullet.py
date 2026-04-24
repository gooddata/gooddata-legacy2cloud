# (C) 2026 GoodData Corporation
"""
Bullet chart processing for Legacy to Cloud migration.

This module handles processing of bullet charts (thermometer in Legacy),
mapping the various elements and metrics appropriately.
"""

from typing import Any

from gooddata_legacy2cloud.reports.data_classes import ContextWithWarnings


def process_bullet_chart_mapping(ctx: ContextWithWarnings, chart, content):
    """
    Process bullet chart element mappings and create appropriate buckets.

    Args:
        ctx (ReportContext): The context object containing mappings
        chart (dict): The Legacy chart configuration
        content (dict): The report content

    Returns:
        list: The buckets configuration for the bullet chart
    """
    # Get styles and element mappings
    styles = chart.get("styles", {})
    global_styles = styles.get("global", {})
    element_mapping = global_styles.get("elementMapping", [])
    grid_metrics = content.get("grid", {}).get("metrics", [])

    # Create mappings for different chart types
    metric_mappings: dict[str, dict[str, Any] | None] = {
        "overbar": None,  # Primary measure (main metric)
        "targetbar": None,  # Secondary measure (target)
        "lowbar": None,  # Comparison ranges
        "mediumbar": None,  # Comparison ranges
        "bar": None,  # Comparison ranges
    }

    # Process each mapping entry
    for mapping in element_mapping:
        chart_type = mapping.get("charttype")
        metric_id = mapping.get("id")

        if chart_type and metric_id and chart_type in metric_mappings:
            try:
                # Retrieve Legacy object and extract metric meta identifier
                legacy_obj = ctx.legacy_client.get_object(metric_id)
                if "metric" in legacy_obj:
                    legacy_metric_identifier = legacy_obj["metric"]["meta"][
                        "identifier"
                    ]
                else:
                    legacy_metric_identifier = metric_id
                converted_id = ctx.metric_mappings.search_mapping_identifier(
                    legacy_metric_identifier
                )

                # Store the mapping with the converted ID
                metric_mappings[chart_type] = {
                    "id": converted_id,
                    "original_uri": metric_id,
                    "legacy_identifier": legacy_metric_identifier,
                }
            except Exception as e:
                ctx.log_warning(
                    f"Failed to process bullet chart metric mapping for {chart_type}: {str(e)}"
                )

    # Initialize buckets
    buckets = []

    # Helper function to find a metric in grid_metrics by URI
    def find_metric_by_uri(uri):
        for i, metric in enumerate(grid_metrics):
            if metric.get("uri") == uri:
                return metric, i
        return None, -1

    # Helper function to create a measure bucket for bullet chart
    def create_measure_bucket(bucket_name, metric_mapping):
        if not metric_mapping:
            return None

        bucket = {"localIdentifier": bucket_name, "items": []}
        metric_uri = metric_mapping["original_uri"]
        metric, idx = find_metric_by_uri(metric_uri)

        if metric:
            # Use the existing mapping function
            from gooddata_legacy2cloud.reports.mappings import map_chart_metric

            item = map_chart_metric(ctx, metric, idx + 1)
        else:
            # Create a dummy metric with the URI
            dummy_metric = {"uri": metric_uri}
            from gooddata_legacy2cloud.reports.mappings import map_chart_metric

            item = map_chart_metric(ctx, dummy_metric, 1)

        # Ensure the item uses the correct converted ID
        if "measure" in item:
            item["measure"]["definition"]["measureDefinition"]["item"]["identifier"][
                "id"
            ] = metric_mapping["id"]

        bucket["items"].append(item)
        return bucket

    # Create the three measure buckets
    measures_bucket = create_measure_bucket("measures", metric_mappings["overbar"])
    if measures_bucket:
        buckets.append(measures_bucket)

    secondary_bucket = create_measure_bucket(
        "secondary_measures", metric_mappings["targetbar"]
    )
    if secondary_bucket:
        buckets.append(secondary_bucket)

    # Process comparison metrics (lowbar, mediumbar, bar)
    comparison_metrics = []
    if metric_mappings["bar"]:
        comparison_metrics.append({"type": "bar", "info": metric_mappings["bar"]})
    if metric_mappings["mediumbar"]:
        comparison_metrics.append(
            {"type": "mediumbar", "info": metric_mappings["mediumbar"]}
        )
    if metric_mappings["lowbar"]:
        comparison_metrics.append({"type": "lowbar", "info": metric_mappings["lowbar"]})

    # Apply the priority rule: prefer "bar" if available
    chosen_comparison = None

    if comparison_metrics:
        # First try to find "bar"
        for cm in comparison_metrics:
            if cm["type"] == "bar":
                chosen_comparison = cm
                break

        # If not found, use the first one available
        if not chosen_comparison:
            chosen_comparison = comparison_metrics[0]

        # Add warning for metrics that couldn't be shown
        if len(comparison_metrics) > 1:
            not_shown_list = []
            for cm in comparison_metrics:
                if cm != chosen_comparison:
                    # Use the Cloud identifier (converted_id) instead of Legacy identifier
                    not_shown_list.append(cm["info"]["id"])

            if not_shown_list:
                not_shown_str = ", ".join(not_shown_list)
                ctx.log_warning(
                    f"Only one comparison metric supported in bullet chart. Not shown metrics: {not_shown_str}"
                )

    # Add the chosen comparison to the tertiary_measures bucket
    if chosen_comparison:
        tertiary_bucket = create_measure_bucket(
            "tertiary_measures", chosen_comparison["info"]
        )
        if tertiary_bucket:
            buckets.append(tertiary_bucket)

    # Process attributes - take only the first one
    attributes = []
    attribute_uris = set()  # Track attribute URIs to avoid duplicates

    # Gather all attribute URIs from buckets and rows
    for bucket_name in ["x", "y"]:
        for item in chart.get("buckets", {}).get(bucket_name, []):
            if item.get("uri") and item.get("uri") not in ["metric", "metricGroup"]:
                attr_uri = item.get("uri")
                if attr_uri not in attribute_uris:
                    attribute_uris.add(attr_uri)
                    attributes.append(item)

    # Also check for attributes in rows
    for item in content.get("grid", {}).get("rows", []):
        if isinstance(item, dict) and item.get("attribute", {}).get("uri"):
            attr_uri = item["attribute"]["uri"]
            if attr_uri not in attribute_uris:
                attribute_uris.add(attr_uri)
                attributes.append({"uri": attr_uri})

    # Use only the first attribute, warn if more are present
    if len(attributes) > 1:
        not_shown_attrs = []
        for i in range(1, len(attributes)):
            attr_uri = attributes[i].get("uri")
            legacy_identifier = attr_uri  # Default to URI
            try:
                attr_obj = ctx.legacy_client.get_object(attr_uri)
                if "attribute" in attr_obj:
                    legacy_identifier = attr_obj["attribute"]["meta"]["identifier"]
                elif "attributeDisplayForm" in attr_obj:
                    legacy_identifier = attr_obj["attributeDisplayForm"]["meta"][
                        "identifier"
                    ]
            except Exception:
                pass
            not_shown_attrs.append(legacy_identifier)

        if not_shown_attrs:
            not_shown_str = ", ".join(not_shown_attrs)
            ctx.log_warning(
                f"Only one attribute supported in bullet chart. Not shown attributes: {not_shown_str}"
            )

    # Add the first attribute to the view bucket
    if attributes:
        view_bucket = {"localIdentifier": "view", "items": []}
        from gooddata_legacy2cloud.reports.mappings import map_chart_attribute

        attr_item = map_chart_attribute(ctx, attributes[0], 1)
        view_bucket["items"].append(attr_item)
        buckets.append(view_bucket)

    return buckets
