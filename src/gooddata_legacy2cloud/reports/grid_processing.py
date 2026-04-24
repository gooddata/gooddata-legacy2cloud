# (C) 2026 GoodData Corporation
"""
Functions for processing grid (table) reports in the Legacy to Cloud migration.
These functions handle the conversion of Legacy grid/table configurations, including
metrics, attributes, column widths, totals, and sorting.
"""

import logging

from gooddata_legacy2cloud.reports.data_classes import ContextWithWarnings
from gooddata_legacy2cloud.reports.filters.date_classification import (
    _is_cyclical_legacy_attribute,
)
from gooddata_legacy2cloud.reports.filters.filter_mapper import (
    _get_attribute_uri_from_filter,
)
from gooddata_legacy2cloud.reports.mappings import (
    map_attribute,
    map_metric,
    map_total_entry,
)

# Configure logger
logger = logging.getLogger("migration")
logger.setLevel(logging.DEBUG)

# Global parameters for column width correction.
COLUMN_WIDTH_SCALE = 1.15

# Visualization URL mapping for grid reports
VISUALIZATION_URL_MAP = {"grid": "local:table", "oneNumber": "local:headline"}


def process_metrics(ctx: ContextWithWarnings, legacy_metrics):
    """
    Process metrics for grid reports.

    Args:
        ctx (ReportContext): The context object containing mappings
        legacy_metrics (list): The list of Legacy metrics

    Returns:
        tuple: (measures, metric_local_ids) where measures is a list of mapped metrics
               and metric_local_ids is a dict mapping metric URIs to local IDs
    """
    measures = []
    metric_local_ids = {}

    # Map each Legacy metric with lookup using ctx.metric_mappings
    for idx, metric in enumerate(legacy_metrics, start=1):
        mapped = map_metric(ctx, metric, idx)
        legacy_metric_uri = metric.get("uri", "")
        local_id = mapped["measure"]["localIdentifier"]
        metric_local_ids[legacy_metric_uri] = local_id
        measures.append(mapped)

    return measures, metric_local_ids


def process_row_attributes(
    ctx: ContextWithWarnings, legacy_row_attrs, metric_local_ids, legacy_metrics
):
    """
    Process row attributes for grid reports.

    Args:
        ctx (ReportContext): The context object containing mappings
        legacy_row_attrs (list): The list of Legacy row attributes
        metric_local_ids (dict): Mapping of metric URIs to local IDs
        legacy_metrics (list): The list of Legacy metrics

    Returns:
        tuple: (attribute_bucket, row_attr_local_ids) where attribute_bucket contains
               the mapped attributes and row_attr_local_ids maps attribute URIs to local IDs
    """
    attribute_bucket = {"localIdentifier": "attribute", "items": [], "totals": []}
    row_attr_local_ids = {}

    for idx, row in enumerate(legacy_row_attrs, start=1):
        attr_obj = row.get("attribute", row)
        mapped = map_attribute(ctx, attr_obj, idx)
        legacy_attr_uri = attr_obj.get("uri", "")
        local_id = mapped["attribute"]["localIdentifier"]
        row_attr_local_ids[legacy_attr_uri] = local_id

        # Add the mapped attribute to the bucket
        attribute_bucket["items"].append(mapped)

        # Process totals for this attribute
        totals = attr_obj.get("totals", [])
        if totals:
            # In Legacy, the totals structure for row attributes is:
            # [
            #   [total_types_for_metric1],  # index 0
            #   [total_types_for_metric2]   # index 1
            # ]
            logger.debug(f"Processing row attribute totals: {totals}")

            for metric_idx, metric in enumerate(legacy_metrics):
                if metric_idx < len(totals):
                    total_types_for_metric = totals[metric_idx]
                    # Handle both single total type string and array of total types
                    if not isinstance(total_types_for_metric, list):
                        total_types_for_metric = [total_types_for_metric]

                    for total_type in total_types_for_metric:
                        if total_type:  # Skip empty total types
                            m_uri = metric.get("uri", "")
                            m_local_id = metric_local_ids.get(m_uri)
                            if m_local_id:
                                logger.debug(
                                    f"Adding total {total_type} for metric {m_local_id} on attribute {local_id}"
                                )
                                total_entry = map_total_entry(
                                    m_local_id, local_id, total_type=total_type
                                )
                                attribute_bucket["totals"].append(total_entry)
                            else:
                                logger.warning(
                                    f"Could not find local ID for metric URI: {m_uri}"
                                )

    return attribute_bucket, row_attr_local_ids


def process_column_attributes(
    ctx: ContextWithWarnings, legacy_columns, legacy_metrics, metric_local_ids
):
    """
    Process column attributes for grid reports.

    Args:
        ctx (ReportContext): The context object containing mappings
        legacy_columns (list): The list of Legacy column attributes
        legacy_metrics (list): The list of Legacy metrics
        metric_local_ids (dict): Mapping of metric URIs to local IDs

    Returns:
        tuple: (column_bucket, column_attributes) where column_bucket contains the mapped
               column attributes and column_attributes is a list for filter processing
    """
    column_bucket = {"localIdentifier": "columns", "items": []}
    column_attributes = []
    column_totals = []
    col_attr_index = 1

    for col in legacy_columns:
        if isinstance(col, dict) and col.get("attribute"):
            attr_obj = col.get("attribute")
            mapped = map_attribute(ctx, attr_obj, col_attr_index)
            column_bucket["items"].append(mapped)
            column_attributes.append({"attribute": attr_obj})

            # Process totals for this column attribute
            totals = attr_obj.get("totals", [])
            if totals:
                attr_local_id = mapped["attribute"]["localIdentifier"]
                logger.debug(f"Processing column attribute totals: {totals}")

                for metric_idx, metric in enumerate(legacy_metrics):
                    if metric_idx < len(totals):
                        total_types_for_metric = totals[metric_idx]
                        # Handle both single total type string and array of total types
                        if not isinstance(total_types_for_metric, list):
                            total_types_for_metric = [total_types_for_metric]

                        for total_type in total_types_for_metric:
                            if total_type:  # Skip empty total types
                                m_uri = metric.get("uri", "")
                                m_local_id = metric_local_ids.get(m_uri)
                                if m_local_id:
                                    logger.debug(
                                        f"Adding total {total_type} for metric {m_local_id} on column attribute {attr_local_id}"
                                    )
                                    total_entry = map_total_entry(
                                        m_local_id, attr_local_id, total_type=total_type
                                    )
                                    column_totals.append(total_entry)
                                else:
                                    logger.warning(
                                        f"Could not find local ID for metric URI: {m_uri}"
                                    )
            col_attr_index += 1

    if column_totals:
        column_bucket["totals"] = column_totals

    return column_bucket, column_attributes


def process_column_widths(
    ctx: ContextWithWarnings,
    grid,
    report_format,
    row_attr_local_ids,
    attribute_bucket,
    metric_local_ids,
):
    """
    Process column widths for grid reports.

    Args:
        ctx (ReportContext): The context object containing mappings
        grid (dict): The Legacy grid configuration
        report_format (str): The report format (grid, oneNumber, etc.)
        row_attr_local_ids (dict): Mapping of row attribute URIs to local IDs
        attribute_bucket (dict): The attribute bucket with items
        metric_local_ids (dict): Mapping of metric URIs to local IDs

    Returns:
        list: The column width items for Cloud
    """
    grid_column_widths = grid.get("columnWidths")
    column_width_items = []

    if report_format not in ["line", "chart"] and grid_column_widths:
        # Build a mapping for attribute URIs using row attributes and attribute bucket items
        displayed_attr_map = {}
        for uri, lid in row_attr_local_ids.items():
            displayed_attr_map[uri] = lid

        for item in attribute_bucket.get("items", []):
            attr = item.get("attribute", {})
            if "uri" in attr and "localIdentifier" in attr:
                displayed_attr_map[attr["uri"]] = attr["localIdentifier"]

        # Helper to resolve an attribute local identifier from a locator URI
        def resolve_attribute_local_id(locator_uri):
            if locator_uri in displayed_attr_map:
                return displayed_attr_map[locator_uri]
            try:
                obj = ctx.legacy_client.get_object(locator_uri)
                if "attributeDisplayForm" in obj:
                    form_of_uri = obj["attributeDisplayForm"]["content"].get(
                        "formOf", ""
                    )
                    if form_of_uri:
                        for key, lid in displayed_attr_map.items():
                            if form_of_uri in key or key.endswith(form_of_uri):
                                return lid
                return None
            except Exception:
                return None

        # Process each column width item
        for cw in grid_column_widths:
            width_value = cw.get("width")
            try:
                adjusted_width = int(round(float(width_value) * COLUMN_WIDTH_SCALE))
            except Exception:
                adjusted_width = width_value

            locators = cw.get("locator", [])
            for cw_item in locators:
                if "attributeHeaderLocator" in cw_item:
                    locator_uri = cw_item["attributeHeaderLocator"].get("uri")
                    if locator_uri:
                        local_id = resolve_attribute_local_id(locator_uri)
                        if local_id:
                            column_width_items.append(
                                {
                                    "attributeColumnWidthItem": {
                                        "attributeIdentifier": local_id,
                                        "width": {"value": adjusted_width},
                                    }
                                }
                            )
                elif "metricLocator" in cw_item:
                    locator_uri = cw_item["metricLocator"].get("uri")
                    if locator_uri:
                        local_id = metric_local_ids.get(locator_uri)
                        if local_id:
                            column_width_items.append(
                                {
                                    "measureColumnWidthItem": {
                                        "locators": [
                                            {
                                                "measureLocatorItem": {
                                                    "measureIdentifier": local_id
                                                }
                                            }
                                        ],
                                        "width": {"value": adjusted_width},
                                    }
                                }
                            )

    return column_width_items


def process_grid_sort(
    ctx: ContextWithWarnings, grid, row_attr_local_ids, metric_local_ids, report_format
):
    """
    Process grid sort configuration.

    Args:
        ctx (ReportContext): The context object containing mappings
        grid (dict): The Legacy grid configuration
        row_attr_local_ids (dict): Mapping of row attribute URIs to local IDs
        metric_local_ids (dict): Mapping of metric URIs to local IDs
        report_format (str): The report format

    Returns:
        list: The sort configuration for Cloud
    """
    sorts = []
    grid_sort = grid.get("sort", {})

    if grid_sort.get("rows") and len(grid_sort["rows"]) > 0:
        sort_item = grid_sort["rows"][0]
        if "attributeSort" in sort_item:
            sort_def = sort_item["attributeSort"]
            sorted_attr_uri = sort_def.get("uri", "")
            direction = sort_def.get("direction", "asc")
            attr_local_id = row_attr_local_ids.get(sorted_attr_uri)
            sorts.append(
                {
                    "attributeSortItem": {
                        "attributeIdentifier": attr_local_id,
                        "direction": direction,
                    }
                }
            )
        elif "metricSort" in sort_item:
            sort_def = sort_item["metricSort"]
            direction = sort_def.get("direction", "asc")
            locators = sort_def.get("locators", [])
            if locators:
                first_locator = locators[0]
                metric_locator = first_locator.get("metricLocator", {})
                metric_uri = metric_locator.get("uri", "")
            measure_local_id = (
                {} if report_format == "chart" else metric_local_ids
            ).get(metric_uri)
            sorts.append(
                {
                    "measureSortItem": {
                        "direction": direction,
                        "locators": [
                            {
                                "measureLocatorItem": {
                                    "measureIdentifier": measure_local_id
                                }
                            }
                        ],
                    }
                }
            )
    elif grid_sort.get("columns"):
        sort_item = grid_sort["columns"][0]
        if "attributeSort" in sort_item:
            sort_def = sort_item["attributeSort"]
            sorted_attr_uri = sort_def.get("uri", "")
            direction = sort_def.get("direction", "asc")
            try:
                from gooddata_legacy2cloud.reports.common import generate_local_id
            except ImportError:

                def generate_local_id(x):
                    return x

            attr_local_id = generate_local_id(sorted_attr_uri)
            sorts.append(
                {
                    "attributeSortItem": {
                        "attributeIdentifier": attr_local_id,
                        "direction": direction,
                    }
                }
            )
        elif "metricSort" in sort_item:
            sort_def = sort_item["metricSort"]
            direction = sort_def.get("direction", "asc")
            locators = sort_def.get("locators", [])
            if locators:
                first_locator = locators[0]
                metric_locator = first_locator.get("metricLocator", {})
                metric_uri = metric_locator.get("uri", "")
            measure_local_id = (
                {} if report_format == "chart" else metric_local_ids
            ).get(metric_uri)
            sorts.append(
                {
                    "measureSortItem": {
                        "direction": direction,
                        "locators": [
                            {
                                "measureLocatorItem": {
                                    "measureIdentifier": measure_local_id
                                }
                            }
                        ],
                    }
                }
            )

    # If no sorts were defined in grid sort, fallback
    if not sorts:
        return grid.get("content", {}).get("sorts", [])

    return sorts


def process_grid_report(ctx: ContextWithWarnings, report, content, report_format):
    """
    Process a Legacy grid report into a Cloud visualizationObject.

    Args:
        ctx (ReportContext): The context object containing mappings
        report (dict): The Legacy report definition
        content (dict): The report content
        report_format (str): The report format (grid, oneNumber, etc.)

    Returns:
        dict: The Cloud visualization object content
    """
    visualization_url = VISUALIZATION_URL_MAP.get(report_format, "local:table")
    grid = content.get("grid", {})

    # Initialize the controls properties
    properties = {"controls": {}}
    properties["controls"]["measureGroupDimension"] = "columns"

    raw_legacy_row_attrs = grid.get("rows", [])

    # Separate out rows that equal the string "metricGroup"
    legacy_row_attrs = []
    for row in raw_legacy_row_attrs:
        if isinstance(row, str) and row == "metricGroup":
            properties["controls"]["measureGroupDimension"] = "rows"
        else:
            legacy_row_attrs.append(row)

    legacy_metrics = grid.get("metrics", [])

    # Process metrics
    measures, metric_local_ids = process_metrics(ctx, legacy_metrics)

    # Process row attributes
    attribute_bucket, row_attr_local_ids = process_row_attributes(
        ctx, legacy_row_attrs, metric_local_ids, legacy_metrics
    )

    # Process column attributes
    legacy_columns = grid.get("columns", [])
    column_bucket, column_attributes = process_column_attributes(
        ctx, legacy_columns, legacy_metrics, metric_local_ids
    )

    # Process column widths
    column_width_items = process_column_widths(
        ctx, grid, report_format, row_attr_local_ids, attribute_bucket, metric_local_ids
    )

    if column_width_items:
        properties.setdefault("controls", {})["columnWidths"] = column_width_items

    # Initialize displayed_attributes set for non-chart reports
    displayed_attributes = set()

    # Add row attribute URIs to the displayed_attributes set
    for uri in row_attr_local_ids.keys():
        if uri:
            displayed_attributes.add(uri)

    # Also collect URIs from the column attributes
    for col in column_bucket.get("items", []):
        attr = col.get("attribute", {})
        if "uri" in attr:
            displayed_attributes.add(attr["uri"])

    # Process filters
    cloud_filters = []
    attribute_filter_configs = {}

    # Prepare buckets for filter validation
    buckets_for_filters = [
        {"localIdentifier": "measures", "items": measures},
        attribute_bucket,
        column_bucket,
    ]

    from gooddata_legacy2cloud.reports.filters import map_filter

    # Track cyclical filter metadata
    cyclical_filter_info: dict[int, dict[str, object]] = {}

    for legacy_index, filt in enumerate(content.get("filters", [])):
        pan_filter, filt_config = map_filter(
            filt,
            metric_local_ids,
            column_attributes if report_format not in ["line", "chart"] else [],
            displayed_attributes,
            ctx,
            buckets_for_filters,
        )
        if pan_filter:
            # Handle both single filters and lists of filters
            if isinstance(pan_filter, list):
                # Track only cyclical date filter groups (NULL date filters also return lists).
                is_cyclical_group = False
                try:
                    attr_uri = _get_attribute_uri_from_filter(filt)
                    if attr_uri:
                        obj = ctx.legacy_client.get_object(attr_uri)
                        legacy_identifier = None
                        if "attributeDisplayForm" in obj:
                            legacy_identifier = obj["attributeDisplayForm"]["meta"][
                                "identifier"
                            ]
                        elif "attribute" in obj:
                            legacy_identifier = obj["attribute"]["meta"]["identifier"]

                        is_cyclical_group = bool(
                            legacy_identifier
                            and _is_cyclical_legacy_attribute(legacy_identifier)
                        )
                except Exception:
                    is_cyclical_group = False

                if is_cyclical_group:
                    cyclical_filter_info[legacy_index] = {
                        "legacy_filter": filt,
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
        from gooddata_legacy2cloud.reports.filters.date_filter_post_processor import (
            DateFilterPostProcessor,
        )

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

    # Process sorts for grid reports
    sorts = process_grid_sort(
        ctx, grid, row_attr_local_ids, metric_local_ids, report_format
    )

    # Use fallback buckets if needed
    buckets_final = [
        {"localIdentifier": "measures", "items": measures},
        attribute_bucket,
        column_bucket,
    ]

    return {
        "buckets": buckets_final,
        "filters": cloud_filters,
        "attributeFilterConfigs": attribute_filter_configs,
        "sorts": sorts,
        "properties": properties,
        "visualizationUrl": visualization_url,
        "version": "2",
    }
