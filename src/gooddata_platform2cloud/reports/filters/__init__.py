# (C) 2026 GoodData Corporation
"""
Filter processing functionality for Platform to Cloud migration.

This package handles the conversion of Platform filter definitions to Cloud filter objects.
It is organized by filter type, with each class responsible for one category of filters:
- RankingFilter: Top/Bottom filters
- AttributeFilter: Positive and negative attribute filters (PositiveAttributeFilter, NegativeAttributeFilter)
- DateFilter: Date-based filters (relative dates, between)
- MeasureFilter: Measure value filters

The main entry point is the map_filter function, which dispatches the filter to the appropriate
processing class based on its type.
"""

from gooddata_platform2cloud.reports.filters.attribute_filter import (
    AttributeFilter,
    NegativeAttributeFilter,
    PositiveAttributeFilter,
)
from gooddata_platform2cloud.reports.filters.base_filter import Filter
from gooddata_platform2cloud.reports.filters.date_filter import DateFilter
from gooddata_platform2cloud.reports.filters.filter_mapper import map_filter
from gooddata_platform2cloud.reports.filters.measure_filter import MeasureFilter
from gooddata_platform2cloud.reports.filters.ranking_filter import RankingFilter

__all__ = [
    # Main entry point function
    "map_filter",
    # Base filter class
    "Filter",
    # Filter classes
    "AttributeFilter",
    "NegativeAttributeFilter",
    "PositiveAttributeFilter",
    "DateFilter",
    "MeasureFilter",
    "RankingFilter",
]
