# (C) 2026 GoodData Corporation
"""
Axis processing for Legacy to Cloud chart migration.

This module handles processing of axis-related configurations, including:
- Secondary y-axis measures
- Axis styling (labels, visibility)
"""

from gooddata_legacy2cloud.reports.data_classes import ContextWithWarnings


def process_secondary_yaxis(ctx: ContextWithWarnings, chart, properties):
    """
    Process secondary y-axis measures from chart axes and add them to properties.

    Args:
        ctx (ReportContext): The context object containing mappings
        chart (dict): The Legacy chart configuration
        properties (dict): The properties dictionary to update

    Returns:
        tuple: (properties, secondary_yaxis_measures) with updated properties and the list of measures
    """
    axes = chart.get("axes", [])
    secondary_yaxis_measures = []

    for axis in axes:
        locators = axis.get("locators", [])
        for locator in locators:
            if "metricLocator" in locator:
                uri = locator["metricLocator"].get("uri")
                if uri:
                    try:
                        # Retrieve Legacy object and extract metric meta identifier
                        legacy_obj = ctx.legacy_client.get_object(uri)
                        if "metric" in legacy_obj:
                            legacy_metric_identifier = legacy_obj["metric"]["meta"][
                                "identifier"
                            ]
                        else:
                            legacy_metric_identifier = uri
                        converted_axis_id = (
                            ctx.metric_mappings.search_mapping_identifier(
                                legacy_metric_identifier
                            )
                        )
                    except Exception:
                        converted_axis_id = uri
                    secondary_yaxis_measures.append(converted_axis_id)

    if secondary_yaxis_measures:
        properties["controls"]["secondary_yaxis"] = {
            "measures": secondary_yaxis_measures
        }

    return properties, secondary_yaxis_measures


def process_axis_styles(chart, properties):
    """
    Process axis styles configuration for chart reports.

    Args:
        chart (dict): The Legacy chart configuration
        properties (dict): The properties dictionary to update

    Returns:
        dict: Updated properties dictionary
    """
    styles = chart.get("styles", {})

    # First map style IDs to axis types by matching with bucket IDs
    axis_mapping = {}
    chart_buckets = chart.get("buckets", {})

    # Check x bucket items
    for item in chart_buckets.get("x", []):
        item_id = item.get("id")
        if item_id and item_id.startswith("yui_"):
            axis_mapping[item_id] = "xaxis"

    # Check y bucket items
    for item in chart_buckets.get("y", []):
        item_id = item.get("id")
        if item_id and item_id.startswith("yui_"):
            axis_mapping[item_id] = "yaxis"

    # Now process each style entry with a yui_ ID
    for style_id, style_data in styles.items():
        if style_id != "global" and style_id in axis_mapping:
            axis_type = axis_mapping[style_id]

            # Initialize axis config if needed
            if axis_type not in properties["controls"]:
                properties["controls"][axis_type] = {}

            # Handle different display options
            if "axis" in style_data:
                axis_config = style_data["axis"]

                # Case 1: Entire axis display:none
                if "display" in axis_config and axis_config["display"] == "none":
                    properties["controls"][axis_type]["visible"] = False

                # Case 2: Axis label (name) display:none
                if (
                    "label" in axis_config
                    and "display" in axis_config["label"]
                    and axis_config["label"]["display"] == "none"
                ):
                    # Initialize name object if needed
                    if "name" not in properties["controls"][axis_type]:
                        properties["controls"][axis_type]["name"] = {"position": "auto"}
                    properties["controls"][axis_type]["name"]["visible"] = False

                # Case 3: Axis values display:none (majorlabel)
                if (
                    "majorlabel" in axis_config
                    and "display" in axis_config["majorlabel"]
                    and axis_config["majorlabel"]["display"] == "none"
                ):
                    properties["controls"][axis_type]["labelsEnabled"] = False

    return properties


def map_secondary_yaxis_to_local_ids(
    properties, secondary_yaxis_measures, buckets_final
):
    """
    Map secondary y-axis measures to actual measure localIdentifiers.

    Args:
        properties (dict): The properties dictionary to update
        secondary_yaxis_measures (list): List of secondary y-axis measure IDs
        buckets_final (list): The final buckets configuration

    Returns:
        dict: Updated properties with mapped secondary y-axis measures
    """
    if not secondary_yaxis_measures:
        return properties

    # Find all measure localIdentifiers and their corresponding converted_ids
    measure_id_map = {}
    for bucket in buckets_final:
        if bucket["localIdentifier"] == "measures":
            for item in bucket.get("items", []):
                if "measure" in item:
                    measure_def = item["measure"]["definition"].get(
                        "measureDefinition", {}
                    )
                    measure_id = (
                        measure_def.get("item", {}).get("identifier", {}).get("id")
                    )
                    if measure_id:
                        measure_id_map[measure_id] = item["measure"]["localIdentifier"]

    # Update secondary_yaxis measures with actual localIdentifiers
    mapped_secondary_measures = []
    for metric_id in secondary_yaxis_measures:
        if metric_id in measure_id_map:
            mapped_secondary_measures.append(measure_id_map[metric_id])

    if mapped_secondary_measures:
        properties["controls"]["secondary_yaxis"] = {
            "measures": mapped_secondary_measures
        }
    else:
        # If no mappings were found, remove the secondary_yaxis property
        properties["controls"].pop("secondary_yaxis", None)

    return properties
