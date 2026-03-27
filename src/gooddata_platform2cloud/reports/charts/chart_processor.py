# (C) 2026 GoodData Corporation
"""
Chart processing implementation for Platform to Cloud migration.
"""

from typing import Any

from gooddata_platform2cloud.reports.charts.axes import (
    map_secondary_yaxis_to_local_ids,
    process_axis_styles,
    process_secondary_yaxis,
)
from gooddata_platform2cloud.reports.charts.bullet import process_bullet_chart_mapping
from gooddata_platform2cloud.reports.charts.color_mapping import (
    apply_color_mapping,
    process_color_mapping,
)
from gooddata_platform2cloud.reports.charts.sorting import process_chart_sort
from gooddata_platform2cloud.reports.charts.styles import process_chart_styles
from gooddata_platform2cloud.reports.charts.types import (
    process_chart_type_and_visualization,
)
from gooddata_platform2cloud.reports.charts.waterfall import process_waterfall_chart
from gooddata_platform2cloud.reports.data_classes import ContextWithWarnings
from gooddata_platform2cloud.reports.filters import map_filter
from gooddata_platform2cloud.reports.filters.date_classification import (
    _is_cyclical_platform_attribute,
)
from gooddata_platform2cloud.reports.filters.date_filter_post_processor import (
    DateFilterPostProcessor,
)
from gooddata_platform2cloud.reports.filters.filter_mapper import (
    _get_attribute_uri_from_filter,
)


def process_chart_report(ctx: ContextWithWarnings, report, content):
    """
    Process a Platform chart report into a Cloud visualizationObject.

    Args:
        ctx (ReportContext): The context object containing mappings
        report (dict): The Platform report definition
        content (dict): The report content

    Returns:
        dict: The Cloud visualization object content
    """
    chart = content.get("chart", {})

    # Process chart type and get initial properties - passing content to access metrics
    visualization_url, properties = process_chart_type_and_visualization(chart, content)

    # Process secondary y-axis measures
    properties, secondary_yaxis_measures = process_secondary_yaxis(
        ctx, chart, properties
    )

    # Process chart styles
    properties = process_chart_styles(chart, properties)

    # Process axis styles
    properties = process_axis_styles(chart, properties)

    # Process color mapping
    properties = process_color_mapping(ctx, chart, properties)

    # Process chart sort
    sorts = process_chart_sort(ctx, chart, content)

    # Create buckets differently depending on chart type
    if chart.get("type") == "thermometer":
        # For bullet chart/thermometer, use the special processing
        buckets_final = process_bullet_chart_mapping(ctx, chart, content)
    elif chart.get("type") == "waterfall":
        # For waterfall charts, use the waterfall-specific processing
        buckets_final = process_waterfall_chart(ctx, chart, content)
    else:
        # For all other chart types
        from gooddata_platform2cloud.reports.visualization_adjustments import (
            adjust_buckets,
        )

        buckets_final = adjust_buckets(ctx, "chart", content, [])

    # Process filters
    cloud_filters = []
    attribute_filter_configs = {}

    # Get all display attributes from buckets
    displayed_attributes = set()
    for bucket in buckets_final:
        for item in bucket.get("items", []):
            if "attribute" in item and "displayForm" in item["attribute"]:
                displayed_attributes.add(
                    item["attribute"]["displayForm"].get("uri", "")
                )

    # Build a map of Platform URIs to local IDs for filter conversion
    # This needs to map Platform URIs (used in filters) to local IDs (used in buckets)
    metric_local_ids = {}

    # Get the original Platform metrics from the grid section
    platform_metrics = content.get("grid", {}).get("metrics", [])

    # Build mapping from Platform URI to Cloud local identifier
    for platform_metric in platform_metrics:
        platform_uri = platform_metric.get("uri", "")
        if platform_uri:
            try:
                # Get the Platform identifier to map to Cloud identifier
                platform_obj = ctx.platform_client.get_object(platform_uri)
                platform_identifier = platform_obj["metric"]["meta"]["identifier"]
                cloud_local_id = ctx.metric_mappings.search_mapping_identifier(
                    platform_identifier
                )
                if cloud_local_id:
                    metric_local_ids[platform_uri] = cloud_local_id
            except Exception:
                # Fallback: use the URI directly as local ID
                metric_local_ids[platform_uri] = platform_uri

    # Process filters and track cyclical filter metadata
    cyclical_filter_info: dict[int, dict[str, Any]] = {}

    for platform_index, filt in enumerate(content.get("filters", [])):
        pan_filter, filt_config = map_filter(
            filt,
            metric_local_ids,
            [],  # No column attributes in chart reports
            displayed_attributes,
            ctx,
            buckets=buckets_final,
        )
        if pan_filter:
            # Handle both single filters and lists of filters
            if isinstance(pan_filter, list):
                # Track only cyclical date filter groups (NULL date filters also return lists).
                is_cyclical_group = False
                try:
                    attr_uri = _get_attribute_uri_from_filter(filt)
                    if attr_uri:
                        obj = ctx.platform_client.get_object(attr_uri)
                        platform_identifier = None
                        if "attributeDisplayForm" in obj:
                            platform_identifier = obj["attributeDisplayForm"]["meta"][
                                "identifier"
                            ]
                        elif "attribute" in obj:
                            platform_identifier = obj["attribute"]["meta"]["identifier"]

                        is_cyclical_group = bool(
                            platform_identifier
                            and _is_cyclical_platform_attribute(platform_identifier)
                        )
                except Exception:
                    is_cyclical_group = False

                if is_cyclical_group:
                    cyclical_filter_info[platform_index] = {
                        "platform_filter": filt,
                        "cloud_start_index": len(cloud_filters),
                        "filter_count": len(pan_filter),
                    }
                cloud_filters.extend(pan_filter)
            else:
                cloud_filters.append(pan_filter)
        if filt_config:
            attribute_filter_configs.update(filt_config)

    # Post-process date filters to handle Cloud cyclical filter constraints
    if cyclical_filter_info:
        post_processor = DateFilterPostProcessor(ctx)
        cloud_filters, attribute_filter_configs = post_processor.process(
            cloud_filters,
            content.get("filters", []),
            attribute_filter_configs,
            cyclical_filter_info,
        )

    # Check for conflicting filter types
    has_measure_value = any("measureValueFilter" in f for f in cloud_filters)
    has_ranking = any("rankingFilter" in f for f in cloud_filters)
    if has_measure_value and has_ranking:
        ctx.log_warning(
            "Metric value and top/bottom filters not supported together. Please remove one category",
            to_stderr=True,
        )

    # Apply color mapping using the actual buckets
    properties = apply_color_mapping(properties, buckets_final)

    # Map secondary_yaxis_measures to actual measure localIdentifiers
    properties = map_secondary_yaxis_to_local_ids(
        properties, secondary_yaxis_measures, buckets_final
    )

    return {
        "buckets": buckets_final,
        "filters": cloud_filters,
        "attributeFilterConfigs": attribute_filter_configs,
        "sorts": sorts,
        "properties": properties,
        "visualizationUrl": visualization_url,
        "version": "2",
    }
