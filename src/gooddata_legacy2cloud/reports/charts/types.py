# (C) 2026 GoodData Corporation
"""
Chart type detection and visualization URL mapping for Legacy to Cloud migration.

This module handles determining the type of chart (line, bar, etc.) and its
properties (orientation, stacking) based on the Legacy chart configuration.
"""

import logging

# Configure logger
logger = logging.getLogger("migration")
logger.setLevel(logging.DEBUG)

# Chart visualization type mapping
CHART_VISUALIZATION_URL_MAP = {
    "line": "local:line",
    "area": "local:line",
    "stackedArea": "local:area",
    "bar": "local:column",  # Default to column, will be overridden if horizontal
    "stackedBar": "local:column",  # Default to column with stacking, will be overridden if horizontal
    "pie": "local:pie",
    "donut": "local:donut",
    "thermometer": "local:bullet",  # Bullet chart
    "waterfall": "local:waterfall",  # Waterfall chart
    "funnel": "local:funnel",  # Funnel chart
    "scattered": "local:scatter",  # Scatter plot
    "bubble": "local:bubble",  # Bubble chart
}


def is_horizontal_bar_chart(chart):
    """
    Determine if a Legacy bar chart is horizontal (maps to Cloud 'bar') or
    vertical (maps to Cloud 'column').

    Args:
        chart (dict): The Legacy chart configuration

    Returns:
        bool: True if horizontal, False if vertical
    """
    # In Legacy, if x contains attributes and y contains metrics, it's a vertical bar chart (Cloud column)
    # If x contains metrics and y contains attributes, it's a horizontal bar chart (Cloud bar)
    buckets = chart.get("buckets", {})
    y_items = buckets.get("y", [])

    # Check if y bucket contains attributes (indicating horizontal orientation)
    y_has_attributes = any(
        item.get("uri") and item.get("uri") not in ["metric", "metricGroup"]
        for item in y_items
    )

    # If y has attributes, it's a horizontal bar chart
    return y_has_attributes


def is_stacked_chart(chart_type):
    """
    Determine if a Legacy chart type is stacked.

    Args:
        chart_type (str): The Legacy chart type

    Returns:
        bool: True if stacked, False otherwise
    """
    return chart_type.startswith("stacked")


def determine_waterfall_orientation(chart):
    """
    Determine the orientation of a waterfall chart based on which bucket contains metricGroup.

    Args:
        chart (dict): The Legacy chart configuration

    Returns:
        str: "vertical" if metricGroup is in y bucket, "horizontal" if in x bucket
    """
    buckets = chart.get("buckets", {})
    x_items = buckets.get("x", [])

    # Check if x bucket contains metricGroup
    x_has_metric_group = any(item.get("uri") == "metricGroup" for item in x_items)

    # If metricGroup is in y bucket, orientation is vertical; otherwise horizontal
    return "horizontal" if x_has_metric_group else "vertical"


def process_chart_type_and_visualization(chart, content=None):
    """
    Determines the chart type and visualization URL based on the Legacy chart configuration.

    Args:
        chart (dict): The Legacy chart configuration
        content (dict, optional): The parent content object containing the grid metrics

    Returns:
        tuple: (visualization_url, properties) where properties contains chart-specific settings
    """
    properties = {"controls": {}}
    chart_type = chart.get("type", "line")  # default to "line" if not provided
    visualization_url = CHART_VISUALIZATION_URL_MAP.get(chart_type, "local:line")

    # Determine correct visualization URL for bar charts based on orientation
    if chart_type in ["bar", "stackedBar"]:
        is_horizontal = is_horizontal_bar_chart(chart)
        is_stacked = is_stacked_chart(chart_type)

        if is_horizontal:
            visualization_url = "local:bar"
        else:
            visualization_url = "local:column"

        # Set stacking property if needed
        if is_stacked:
            # Check if we need to stack measures instead of stacking by attribute
            chart_buckets = chart.get("buckets", {})
            color_items = chart_buckets.get("color", [])

            # Check for metricGroup in color bucket
            has_metric_group = any(
                item.get("uri") == "metricGroup" for item in color_items
            )

            # Get metrics count from the right location - either from content.grid.metrics or an empty list if not available
            metrics_count = 0
            if content:
                metrics_count = len(content.get("grid", {}).get("metrics", []))

            # Log the values for debugging
            logger.debug(
                f"Stacking determination: has_metric_group={has_metric_group}, metrics_count={metrics_count}"
            )

            # Use stackMeasures only when we have metric-based stacking (metricGroup + multiple metrics)
            if has_metric_group and metrics_count > 1:
                # For stacked bar charts with multiple metrics in color bucket
                logger.debug("Using stackMeasures: true for metric-based stacking")
                properties["controls"]["stackMeasures"] = True
            else:
                # For stacked bar charts with attribute in color bucket
                logger.debug("Using stacking: true for attribute-based stacking")
                properties["controls"]["stacking"] = True

    # Set orientation for waterfall charts
    elif chart_type == "waterfall":
        orientation = determine_waterfall_orientation(chart)
        properties["controls"]["orientation"] = {"position": orientation}

        # Add total property (by default disabled)
        properties["controls"]["total"] = {"enabled": False, "name": "Total"}

    # Set properties for scatter plots
    elif chart_type == "scattered":
        # Add clustering property (disabled by default)
        properties["controls"]["clustering"] = {"enabled": False}

    # Set properties for bubble charts (similar to scatter plots)
    elif chart_type == "bubble":
        # Bubble charts don't need special properties, but we could add
        # customization options here if needed in the future
        pass

    logger.debug(
        f"Processing chart of type: {chart_type}, visualization URL: {visualization_url}"
    )
    return visualization_url, properties
