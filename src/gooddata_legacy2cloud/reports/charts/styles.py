# (C) 2026 GoodData Corporation
"""
Chart styles processing for Legacy to Cloud migration.

This module handles processing of chart style configurations, including:
- Data points visibility
- Gridlines
- Legend
- Data labels
"""


def process_chart_styles(chart, properties):
    """
    Process global styles configuration for chart reports.

    Args:
        chart (dict): The Legacy chart configuration
        properties (dict): The properties dictionary to update

    Returns:
        dict: Updated properties dictionary
    """
    styles = chart.get("styles", {})
    global_styles = styles.get("global", {})

    if not global_styles:
        return properties

    # Process data points
    if "dataPoints" in global_styles:
        display = global_styles["dataPoints"].get("display")
        if display == "none":
            properties["controls"]["dataPoints"] = {"visible": False}

    # Process gridlines
    if "gridlines" in global_styles:
        display = global_styles["gridlines"].get("display")
        if display == "none":
            properties["controls"]["grid"] = {"enabled": False}

    # Process legend
    if "legend" in global_styles:
        display = global_styles["legend"].get("display")
        if display == "none":
            properties["controls"]["legend"] = {"enabled": False}

    # Process data labels
    if "datalabels" in global_styles:
        display = global_styles["datalabels"].get("display")
        if display == "inline":
            properties["controls"]["dataLabels"] = {"visible": True}

        # Map displayTotals to totalsVisible
        if "displayTotals" in global_styles["datalabels"]:
            display_totals = global_styles["datalabels"]["displayTotals"]
            # Create dataLabels dict if it doesn't exist
            if "dataLabels" not in properties["controls"]:
                properties["controls"]["dataLabels"] = {}
            properties["controls"]["dataLabels"]["totalsVisible"] = display_totals == 1

        # Map displayValues to visible
        if "displayValues" in global_styles["datalabels"]:
            display_values = global_styles["datalabels"]["displayValues"]
            # Create dataLabels dict if it doesn't exist
            if "dataLabels" not in properties["controls"]:
                properties["controls"]["dataLabels"] = {}
            properties["controls"]["dataLabels"]["visible"] = display_values == 1

    return properties
