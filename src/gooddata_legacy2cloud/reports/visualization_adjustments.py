# (C) 2026 GoodData Corporation
from gooddata_legacy2cloud.reports.charts.types import is_horizontal_bar_chart
from gooddata_legacy2cloud.reports.data_classes import ContextWithWarnings
from gooddata_legacy2cloud.reports.mappings import (
    map_chart_attribute,
    map_chart_metric,
)


class ChartBucketAdjuster:
    """
    Class for handling chart bucket adjustments for different chart types.

    This class provides methods to create and adjust Cloud buckets based on the Legacy report format,
    with specific handlers for each chart type.
    """

    def __init__(self, ctx: ContextWithWarnings):
        """Initialize with context."""
        self.ctx = ctx

    def adjust_buckets(
        self, report_format: str, content: dict, common_buckets: list
    ) -> list:
        """
        Create and adjust the Cloud buckets based on the Legacy report format.

        This function creates all buckets for chart reports from scratch, based on the chart type.
        For grid reports, it uses the common_buckets passed from the main transformation function.

        Args:
            report_format: Format of the report ('chart', 'line', 'grid', etc.)
            content: The report content containing chart and grid information
            common_buckets: Common buckets for grid reports

        Returns:
            List of adjusted Cloud buckets
        """
        if report_format not in ["line", "chart"]:
            # For grid reports, use the common_buckets passed from transform_legacy_report
            return common_buckets

        chart = content.get("chart", {})
        chart_type = chart.get("type", "line")
        chart_buckets = chart.get("buckets", {})
        grid_metrics = content.get("grid", {}).get("metrics", [])

        # Route to the appropriate chart handler based on chart type
        if chart_type in ["pie", "donut"]:
            return self._handle_pie_donut_chart(chart, chart_buckets, grid_metrics)
        elif chart_type in ["bar", "stackedBar"]:
            return self._handle_bar_chart(chart, chart_buckets, grid_metrics)
        elif chart_type == "stackedArea":
            return self._handle_stacked_area_chart(chart_buckets, grid_metrics)
        elif chart_type == "waterfall":
            return self._handle_waterfall_chart(chart_buckets, grid_metrics)
        elif chart_type == "funnel":
            return self._handle_funnel_chart(chart_buckets, grid_metrics)
        elif chart_type == "scattered":
            return self._handle_scatter_chart(chart_buckets, grid_metrics)
        elif chart_type == "bubble":
            return self._handle_bubble_chart(chart_buckets, grid_metrics)
        else:
            # Default handler for line/area charts and other chart types
            return self._handle_line_area_chart(chart_buckets, grid_metrics)

    def _has_placeholder(self, items):
        """Check if bucket items contain a placeholder value ('metric' or 'metricGroup')."""
        return any(item.get("uri") in ["metric", "metricGroup"] for item in items)

    def _filter_placeholders(self, items):
        """Filter out placeholder values from bucket items."""
        return [
            item for item in items if item.get("uri") not in ["metric", "metricGroup"]
        ]

    def _handle_pie_donut_chart(self, chart, chart_buckets, grid_metrics):
        """Handle bucket adjustments for pie/donut charts."""
        cloud_buckets = []

        # For pie/donut charts, use angle bucket for measures
        angle_items = chart_buckets.get("angle", [])
        # Check if any angle item contains "metric" or "metricGroup"
        use_grid_metrics = self._has_placeholder(angle_items)

        # If angle bucket contains "metric" or "metricGroup" placeholder,
        # or if it's empty, use grid metrics
        if use_grid_metrics or not angle_items:
            measures_items = [
                map_chart_metric(self.ctx, metric, i + 1)
                for i, metric in enumerate(grid_metrics)
            ]
            num_measures = len(grid_metrics)
        else:
            # Only use angle bucket items if they're not placeholders
            measures_items = [
                map_chart_metric(self.ctx, item, i + 1)
                for i, item in enumerate(angle_items)
            ]
            num_measures = len(angle_items)

        # Add measures bucket
        cloud_buckets.append({"localIdentifier": "measures", "items": measures_items})

        # Warn if x or y buckets are used - not supported for pie/donut
        if chart_buckets.get("x") and len(chart_buckets.get("x")) > 0:
            self.ctx.log_warning("pie/donut chart does not support x/y buckets")
        if chart_buckets.get("y") and len(chart_buckets.get("y")) > 0:
            self.ctx.log_warning("pie/donut chart does not support x/y buckets")

        # Handle percent settings in datalabels
        datalabels = chart.get("styles", {}).get("global", {}).get("datalabels", {})
        if datalabels.get("percent") == 1:
            if num_measures > 1:
                self.ctx.log_warning(
                    "show as percent in pie/donut chart not supported for multiple metrics"
                )
            elif num_measures == 1 and measures_items:
                measure = measures_items[0]["measure"]
                measure_def = measure.get("definition", {}).get("measureDefinition", {})
                measure_def["computeRatio"] = True
                measure["format"] = "#,##0%"

        # Process color bucket for view items
        color_items = chart_buckets.get("color", [])

        # Skip creating view bucket if color items contain "metricGroup"
        if self._has_placeholder(color_items):
            # When color bucket has "metricGroup", we don't create a view bucket
            # This is a special case for pie/donut charts with multiple metrics
            pass
        else:
            # Only create view items for normal attributes
            view_items = [
                map_chart_attribute(self.ctx, item, i + 1)
                for i, item in enumerate(self._filter_placeholders(color_items))
            ]
            if view_items:
                cloud_buckets.append({"localIdentifier": "view", "items": view_items})

        return cloud_buckets

    def _handle_bar_chart(self, chart, chart_buckets, grid_metrics):
        """Handle bucket adjustments for bar/column charts."""
        cloud_buckets = []

        # For bar/column charts
        # If horizontal: x -> measures, y -> view
        # If vertical: y -> measures, x -> view
        is_horizontal = is_horizontal_bar_chart(chart)

        if is_horizontal:
            # For horizontal bar charts (Cloud "bar")
            # Build measures bucket from x bucket
            x_items = chart_buckets.get("x", [])
            use_grid_metrics = self._has_placeholder(x_items)

            if use_grid_metrics or not x_items:
                measures_items = [
                    map_chart_metric(self.ctx, metric, i + 1)
                    for i, metric in enumerate(grid_metrics)
                ]
            else:
                measures_items = [
                    map_chart_metric(self.ctx, item, i + 1)
                    for i, item in enumerate(x_items)
                ]
        else:
            # For vertical bar charts (Cloud "column")
            # Build measures bucket from y bucket
            y_items = chart_buckets.get("y", [])
            use_grid_metrics = self._has_placeholder(y_items)

            if use_grid_metrics or not y_items:
                measures_items = [
                    map_chart_metric(self.ctx, metric, i + 1)
                    for i, metric in enumerate(grid_metrics)
                ]
            else:
                measures_items = [
                    map_chart_metric(self.ctx, item, i + 1)
                    for i, item in enumerate(y_items)
                ]

        cloud_buckets.append({"localIdentifier": "measures", "items": measures_items})

        # Build view bucket from appropriate axis bucket
        if is_horizontal:
            view_source_items = chart_buckets.get("y", [])
        else:
            view_source_items = chart_buckets.get("x", [])

        if not self._has_placeholder(view_source_items) and view_source_items:
            view_items = [
                map_chart_attribute(self.ctx, item, i + 1)
                for i, item in enumerate(view_source_items)
            ]
            if len(view_items) > 2:
                # Get the Cloud IDs of attributes that will be removed
                removed_attrs = []
                for item in view_items[2:]:
                    if "attribute" in item and "localIdentifier" in item["attribute"]:
                        removed_attrs.append(item["attribute"]["localIdentifier"])

                removed_attrs_str = (
                    ", ".join(removed_attrs) if removed_attrs else "unknown"
                )
                self.ctx.log_warning(
                    f"only up to two attributes allowed in view bucket of bar/column chart. Removed attributes: {removed_attrs_str}"
                )
                view_items = view_items[:2]

            if view_items:
                cloud_buckets.append({"localIdentifier": "view", "items": view_items})

        # Build stack bucket from color bucket
        color_items = chart_buckets.get("color", [])
        # Skip creating stack bucket if color items contain "metricGroup"
        if self._has_placeholder(color_items):
            # When color bucket has "metricGroup", we don't create a stack bucket
            pass
        else:
            stack_items = [
                map_chart_attribute(self.ctx, item, i + 1)
                for i, item in enumerate(self._filter_placeholders(color_items))
            ]
            if len(stack_items) > 1:
                self.ctx.log_warning(
                    "only one attribute allowed in stack bucket of bar/column chart"
                )
                stack_items = stack_items[:1]

            if stack_items:
                cloud_buckets.append({"localIdentifier": "stack", "items": stack_items})

        return cloud_buckets

    def _handle_stacked_area_chart(self, chart_buckets, grid_metrics):
        """Handle bucket adjustments for stacked area charts."""
        cloud_buckets = []

        # For stacked area charts
        # Build measures bucket from y bucket
        y_items = chart_buckets.get("y", [])
        use_grid_metrics = self._has_placeholder(y_items)

        if use_grid_metrics or not y_items:
            measures_items = [
                map_chart_metric(self.ctx, metric, i + 1)
                for i, metric in enumerate(grid_metrics)
            ]
        else:
            measures_items = [
                map_chart_metric(self.ctx, item, i + 1)
                for i, item in enumerate(y_items)
            ]

        cloud_buckets.append({"localIdentifier": "measures", "items": measures_items})

        # Warn if angle buckets are used - not supported for stacked area
        if chart_buckets.get("angle") and len(chart_buckets.get("angle")) > 0:
            self.ctx.log_warning("stacked area chart does not support angle buckets")

        # Build view bucket from x bucket (for categories/time)
        x_items = chart_buckets.get("x", [])
        # Skip creating view bucket if x items contain "metric" or "metricGroup"
        if not self._has_placeholder(x_items) and x_items:
            # Only map items that aren't placeholder values
            view_items = [
                map_chart_attribute(self.ctx, item, i + 1)
                for i, item in enumerate(self._filter_placeholders(x_items))
            ]
            if len(view_items) > 1:
                self.ctx.log_warning(
                    "only one attribute allowed in view bucket of stacked area chart"
                )
                view_items = view_items[:1]

            if view_items:
                cloud_buckets.append({"localIdentifier": "view", "items": view_items})

        # Build stack bucket from color bucket (for segmentation)
        color_items = chart_buckets.get("color", [])
        # Skip creating stack bucket if color items contain "metricGroup"
        if self._has_placeholder(color_items):
            # When color bucket has "metricGroup", we don't create a stack bucket
            # This is a special case for stacked area charts with multiple metrics
            pass
        else:
            # Only map items that aren't placeholder values
            stack_items = [
                map_chart_attribute(self.ctx, item, i + 1)
                for i, item in enumerate(self._filter_placeholders(color_items))
            ]
            if len(stack_items) > 1:
                self.ctx.log_warning(
                    "only one attribute allowed in stack bucket of stacked area chart"
                )
                stack_items = stack_items[:1]

            if stack_items:
                cloud_buckets.append({"localIdentifier": "stack", "items": stack_items})

        return cloud_buckets

    def _handle_waterfall_chart(self, chart_buckets, grid_metrics):
        """Handle bucket adjustments for waterfall charts."""
        cloud_buckets = []

        # For waterfall charts, use grid metrics for measures bucket
        measures_items = [
            map_chart_metric(self.ctx, metric, i + 1)
            for i, metric in enumerate(grid_metrics)
        ]

        cloud_buckets.append({"localIdentifier": "measures", "items": measures_items})

        # Check for attributes in any bucket (not supported in Cloud)
        attribute_found = False
        for bucket_name in ["x", "y", "color"]:
            bucket_items = chart_buckets.get(bucket_name, [])
            for item in bucket_items:
                uri = item.get("uri")
                if uri and uri not in ["metric", "metricGroup"]:
                    attribute_found = True

        if attribute_found:
            self.ctx.log_warning("Split of Waterfall chart by attribute not supported")

        return cloud_buckets

    def _handle_funnel_chart(self, chart_buckets, grid_metrics):
        """Handle bucket adjustments for funnel charts."""
        cloud_buckets = []

        # For funnel charts, use y bucket for measures and color bucket for view
        y_items = chart_buckets.get("y", [])
        use_grid_metrics = self._has_placeholder(y_items)

        if use_grid_metrics or not y_items:
            measures_items = [
                map_chart_metric(self.ctx, metric, i + 1)
                for i, metric in enumerate(grid_metrics)
            ]
        else:
            measures_items = [
                map_chart_metric(self.ctx, item, i + 1)
                for i, item in enumerate(y_items)
            ]

        cloud_buckets.append({"localIdentifier": "measures", "items": measures_items})

        # Build view bucket from color bucket
        color_items = chart_buckets.get("color", [])
        # Skip creating view bucket if color items contain "metricGroup"
        if not self._has_placeholder(color_items) and color_items:
            view_items = [
                map_chart_attribute(self.ctx, item, i + 1)
                for i, item in enumerate(color_items)
            ]
            if len(view_items) > 1:
                self.ctx.log_warning(
                    "only one attribute allowed in view bucket of funnel chart"
                )
                view_items = view_items[:1]

            if view_items:
                cloud_buckets.append({"localIdentifier": "view", "items": view_items})

        return cloud_buckets

    def _handle_scatter_chart(self, chart_buckets, grid_metrics):
        """Handle bucket adjustments for scatter plots."""
        cloud_buckets = []

        # Build measures bucket from x bucket (x-axis metric)
        x_items = chart_buckets.get("x", [])
        use_grid_metrics_x = self._has_placeholder(x_items)

        if use_grid_metrics_x or not x_items:
            # If no specific x bucket metrics or using placeholder, use the first grid metric
            if grid_metrics:
                measures_items = [map_chart_metric(self.ctx, grid_metrics[0], 1)]
            else:
                measures_items = []
        else:
            # Use the first metric in the x bucket
            if x_items:
                measures_items = [map_chart_metric(self.ctx, x_items[0], 1)]
            else:
                measures_items = []

        # Add measures bucket for x-axis
        cloud_buckets.append({"localIdentifier": "measures", "items": measures_items})

        # Build secondary_measures bucket from y bucket (y-axis metric)
        y_items = chart_buckets.get("y", [])
        use_grid_metrics_y = self._has_placeholder(y_items)

        if use_grid_metrics_y or not y_items:
            # If no specific y bucket metrics or using placeholder, use the second grid metric
            if len(grid_metrics) > 1:
                secondary_items = [map_chart_metric(self.ctx, grid_metrics[1], 1)]
            else:
                secondary_items = []
        else:
            # Use the first metric in the y bucket
            if y_items:
                secondary_items = [map_chart_metric(self.ctx, y_items[0], 1)]
            else:
                secondary_items = []

        # Add secondary_measures bucket for y-axis
        if secondary_items:
            cloud_buckets.append(
                {"localIdentifier": "secondary_measures", "items": secondary_items}
            )

        # Build attribute bucket from detail bucket
        detail_items = chart_buckets.get("detail", [])
        if not self._has_placeholder(detail_items) and detail_items:
            attribute_items = [
                map_chart_attribute(self.ctx, item, i + 1)
                for i, item in enumerate(detail_items)
            ]
            if len(attribute_items) > 1:
                self.ctx.log_warning(
                    "only one attribute allowed in attribute bucket of scatter plot"
                )
                attribute_items = attribute_items[:1]

            if attribute_items:
                cloud_buckets.append(
                    {"localIdentifier": "attribute", "items": attribute_items}
                )

        # Build segment bucket from color bucket
        color_items = chart_buckets.get("color", [])
        if not self._has_placeholder(color_items) and color_items:
            segment_items = [
                map_chart_attribute(self.ctx, item, i + 1)
                for i, item in enumerate(color_items)
            ]
            if len(segment_items) > 1:
                self.ctx.log_warning(
                    "only one attribute allowed in segment bucket of scatter plot"
                )
                segment_items = segment_items[:1]

            if segment_items:
                cloud_buckets.append(
                    {"localIdentifier": "segment", "items": segment_items}
                )

        return cloud_buckets

    def _handle_bubble_chart(self, chart_buckets, grid_metrics):
        """Handle bucket adjustments for bubble charts."""
        cloud_buckets = []

        # Build measures bucket from x bucket (x-axis metric)
        x_items = chart_buckets.get("x", [])
        use_grid_metrics_x = self._has_placeholder(x_items)

        if use_grid_metrics_x or not x_items:
            # If no specific x bucket metrics or using placeholder, use the first grid metric
            if grid_metrics:
                measures_items = [map_chart_metric(self.ctx, grid_metrics[0], 1)]
            else:
                measures_items = []
        else:
            # Use the first metric in the x bucket
            if x_items:
                measures_items = [map_chart_metric(self.ctx, x_items[0], 1)]
            else:
                measures_items = []

        # Add measures bucket for x-axis
        cloud_buckets.append({"localIdentifier": "measures", "items": measures_items})

        # Build secondary_measures bucket from y bucket (y-axis metric)
        y_items = chart_buckets.get("y", [])
        use_grid_metrics_y = self._has_placeholder(y_items)

        if use_grid_metrics_y or not y_items:
            # If no specific y bucket metrics or using placeholder, use the second grid metric
            if len(grid_metrics) > 1:
                secondary_items = [map_chart_metric(self.ctx, grid_metrics[1], 1)]
            else:
                secondary_items = []
        else:
            # Use the first metric in the y bucket
            if y_items:
                secondary_items = [map_chart_metric(self.ctx, y_items[0], 1)]
            else:
                secondary_items = []

        # Add secondary_measures bucket for y-axis
        if secondary_items:
            cloud_buckets.append(
                {"localIdentifier": "secondary_measures", "items": secondary_items}
            )

        # Build tertiary_measures bucket from size bucket (bubble size metric)
        size_items = chart_buckets.get("size", [])
        use_grid_metrics_size = self._has_placeholder(size_items)

        if use_grid_metrics_size or not size_items:
            # If no specific size bucket metrics or using placeholder, use the third grid metric
            if len(grid_metrics) > 2:
                tertiary_items = [map_chart_metric(self.ctx, grid_metrics[2], 1)]
            else:
                tertiary_items = []
        else:
            # Use the first metric in the size bucket
            if size_items:
                tertiary_items = [map_chart_metric(self.ctx, size_items[0], 1)]
            else:
                tertiary_items = []

        # Add tertiary_measures bucket for bubble size
        if tertiary_items:
            cloud_buckets.append(
                {"localIdentifier": "tertiary_measures", "items": tertiary_items}
            )

        # Build view bucket from detail bucket
        detail_items = chart_buckets.get("detail", [])
        if not self._has_placeholder(detail_items) and detail_items:
            view_items = [
                map_chart_attribute(self.ctx, item, i + 1)
                for i, item in enumerate(detail_items)
            ]
            if len(view_items) > 1:
                self.ctx.log_warning(
                    "only one attribute allowed in view bucket of bubble chart"
                )
                view_items = view_items[:1]

            if view_items:
                cloud_buckets.append({"localIdentifier": "view", "items": view_items})

        # Check if color bucket is used and issue info-level warning (not supported in Cloud for bubble chart)
        color_items = chart_buckets.get("color", [])
        if not self._has_placeholder(color_items) and color_items:
            # Get attribute identifiers for warning message
            color_attr_ids = []
            for item in color_items:
                try:
                    uri = item.get("uri")
                    if uri and uri not in ["metric", "metricGroup"]:
                        # Try to get the attribute identifier
                        attr_obj = self.ctx.legacy_client.get_object(uri)
                        if "attribute" in attr_obj:
                            legacy_attr_identifier = attr_obj["attribute"]["meta"][
                                "identifier"
                            ]
                        elif "attributeDisplayForm" in attr_obj:
                            legacy_attr_identifier = attr_obj["attributeDisplayForm"][
                                "meta"
                            ]["identifier"]
                        else:
                            legacy_attr_identifier = uri

                        # Convert Legacy identifier to Cloud identifier
                        cloud_id = self.ctx.ldm_mappings.search_mapping_identifier(
                            legacy_attr_identifier
                        )
                        color_attr_ids.append(cloud_id)
                except Exception:
                    color_attr_ids.append(uri)

            # Issue info-level warning for unsupported color bucket
            if color_attr_ids:
                self.ctx.log_warning(
                    f"Segment bucket not available in Bubble chart. Attribute not displayed: {', '.join(color_attr_ids)}"
                )

        return cloud_buckets

    def _handle_line_area_chart(self, chart_buckets, grid_metrics):
        """Handle bucket adjustments for line/area charts and other chart types."""
        cloud_buckets = []

        # Build measures bucket from y bucket or grid metrics
        y_items = chart_buckets.get("y", [])
        use_grid_metrics = self._has_placeholder(y_items)

        if use_grid_metrics or not y_items:
            measures_items = [
                map_chart_metric(self.ctx, metric, i + 1)
                for i, metric in enumerate(grid_metrics)
            ]
            num_metrics = len(grid_metrics)
        else:
            measures_items = [
                map_chart_metric(self.ctx, item, i + 1)
                for i, item in enumerate(y_items)
            ]
            num_metrics = len(y_items)

        cloud_buckets.append({"localIdentifier": "measures", "items": measures_items})

        # Build trend bucket from x bucket
        x_items = chart_buckets.get("x", [])
        # Skip creating trend bucket if x items contain "metric" or "metricGroup"
        if not self._has_placeholder(x_items) and x_items:
            # Only map items that aren't placeholder values
            trend_items = [
                map_chart_attribute(self.ctx, item, i + 1)
                for i, item in enumerate(self._filter_placeholders(x_items))
            ]
            if len(trend_items) > 1:
                self.ctx.log_warning(
                    "only one attribute allowed in trend bucket of line/area chart"
                )
                trend_items = trend_items[:1]

            if trend_items:
                cloud_buckets.append({"localIdentifier": "trend", "items": trend_items})

        # Build segment bucket from color bucket
        color_items = chart_buckets.get("color", [])
        # Skip creating segment bucket if color items contain "metricGroup"
        if self._has_placeholder(color_items):
            # When color bucket has "metricGroup", we don't create a segment bucket
            # This is a special case for line/area charts with multiple metrics
            segment_items = []
        elif num_metrics > 1:
            if color_items and any(
                item.get("uri") not in ["metric", "metricGroup"] for item in color_items
            ):
                self.ctx.log_warning(
                    "no attributes in segment bucket allowed if using more metrics"
                )
                segment_items = []
            else:
                segment_items = []
        else:
            # Only map items that aren't placeholder values
            segment_items = [
                map_chart_attribute(self.ctx, item, i + 1)
                for i, item in enumerate(self._filter_placeholders(color_items))
            ]
            if len(segment_items) > 1:
                self.ctx.log_warning(
                    "only one attribute allowed in segment bucket of line/area chart"
                )
                segment_items = segment_items[:1]

        if segment_items:
            cloud_buckets.append({"localIdentifier": "segment", "items": segment_items})

        return cloud_buckets


def adjust_buckets(
    ctx: ContextWithWarnings, report_format: str, content: dict, common_buckets: list
) -> list:
    """
    Create and adjust the Cloud buckets based on the Legacy report format.

    This function is a wrapper around the ChartBucketAdjuster class for backward compatibility.

    Args:
        ctx: The context object with API and mappings
        report_format: Format of the report ('chart', 'line', 'grid', etc.)
        content: The report content containing chart and grid information
        common_buckets: Common buckets for grid reports

    Returns:
        List of adjusted Cloud buckets
    """
    adjuster = ChartBucketAdjuster(ctx)
    return adjuster.adjust_buckets(report_format, content, common_buckets)
