# (C) 2026 GoodData Corporation
"""
Chart processing for Legacy to Cloud migration.

This package handles the conversion of Legacy chart formatting, styles,
and data structure into the Cloud equivalent.
"""

from gooddata_legacy2cloud.reports.charts.axes import (
    map_secondary_yaxis_to_local_ids,
    process_axis_styles,
    process_secondary_yaxis,
)
from gooddata_legacy2cloud.reports.charts.bullet import process_bullet_chart_mapping
from gooddata_legacy2cloud.reports.charts.chart_processor import process_chart_report
from gooddata_legacy2cloud.reports.charts.color_mapping import (
    apply_color_mapping,
    process_color_mapping,
)
from gooddata_legacy2cloud.reports.charts.sorting import process_chart_sort
from gooddata_legacy2cloud.reports.charts.styles import process_chart_styles
from gooddata_legacy2cloud.reports.charts.types import (
    process_chart_type_and_visualization,
)
from gooddata_legacy2cloud.reports.charts.waterfall import process_waterfall_chart

# Export the main processing function
__all__ = [
    "process_chart_report",
    "process_chart_styles",
    "process_chart_type_and_visualization",
    "process_chart_sort",
    "process_secondary_yaxis",
    "process_axis_styles",
    "process_color_mapping",
    "process_bullet_chart_mapping",
    "process_waterfall_chart",
    "apply_color_mapping",
    "map_secondary_yaxis_to_local_ids",
]
