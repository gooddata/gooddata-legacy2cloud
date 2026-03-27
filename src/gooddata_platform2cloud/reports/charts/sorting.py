# (C) 2026 GoodData Corporation
"""
Chart sorting for Platform to Cloud migration.

This module handles processing of chart sorting configurations.
"""

from gooddata_platform2cloud.reports.data_classes import ContextWithWarnings


def process_chart_sort(ctx: ContextWithWarnings, chart, content):
    """
    Process chart-level sort configuration.

    Args:
        ctx (ContextWithWarnings): The context object containing mappings
        chart (dict): The Platform chart configuration
        content (dict): The report content

    Returns:
        list: The sort configuration for Cloud
    """
    chart_sort = chart.get("sort")
    sorts = []

    if chart_sort:
        for sort_item in chart_sort.get("x", []):
            if "metricChartSort" in sort_item:
                sort_def = sort_item["metricChartSort"]
                direction = sort_def.get("direction", "asc")
                locators_obj = sort_def.get("locators", {})
                locators_list = []
                for key, locator_items in locators_obj.items():
                    for locator in locator_items:
                        if "metricLocator" in locator:
                            uri = locator["metricLocator"].get("uri")
                            if uri:
                                try:
                                    platform_obj = ctx.platform_client.get_object(uri)
                                    if "metric" in platform_obj:
                                        platform_metric_identifier = platform_obj[
                                            "metric"
                                        ]["meta"]["identifier"]
                                    else:
                                        platform_metric_identifier = uri
                                    converted_id = (
                                        ctx.metric_mappings.search_mapping_identifier(
                                            platform_metric_identifier
                                        )
                                    )
                                except Exception:
                                    converted_id = uri
                                locators_list.append(
                                    {
                                        "measureLocatorItem": {
                                            "measureIdentifier": converted_id
                                        }
                                    }
                                )
                sorts.append(
                    {
                        "measureSortItem": {
                            "direction": direction,
                            "locators": locators_list,
                        }
                    }
                )
            elif "attributeChartSort" in sort_item:
                sort_def = sort_item["attributeChartSort"]
                direction = sort_def.get("direction", "asc")
                attribute_uri = sort_def.get("uri")
                try:
                    # Retrieve the label (attributeDisplayForm) object, then obtain its associated attribute via "formOf"
                    label_obj = ctx.platform_client.get_object(attribute_uri)
                    if "attributeDisplayForm" in label_obj:
                        form_uri = label_obj["attributeDisplayForm"]["content"].get(
                            "formOf", ""
                        )
                        if form_uri:
                            attr_obj = ctx.platform_client.get_object(form_uri)
                            platform_attr_identifier = attr_obj["attribute"]["meta"][
                                "identifier"
                            ]
                            converted_id = ctx.ldm_mappings.search_mapping_identifier(
                                platform_attr_identifier
                            )
                        else:
                            converted_id = attribute_uri
                    else:
                        converted_id = attribute_uri
                except Exception:
                    converted_id = attribute_uri
                sorts.append(
                    {
                        "attributeSortItem": {
                            "attributeIdentifier": converted_id,
                            "direction": direction,
                        }
                    }
                )
    else:
        # Use computed chart sorts if no chart-specific sorts
        sorts = content.get("sorts", [])

    return sorts
