# (C) 2026 GoodData Corporation
"""
Ordinal date filter processor for Legacy to Cloud migration.

This module handles the complex processing of ordinal date filters,
which need to be converted to absoluteDateFilter with from/to ranges.
"""

import calendar
import re
from datetime import datetime, timedelta

from gooddata_legacy2cloud.reports.data_classes import ContextWithWarnings
from gooddata_legacy2cloud.reports.filters.date_helpers import (
    get_date_dataset_and_granularity,
)
from gooddata_legacy2cloud.reports.filters.helpers import convert_attribute_elements


class OrdinalDateProcessor:
    """
    Processor for ordinal date filters that creates absoluteDateFilter objects.
    """

    def __init__(self, ctx: ContextWithWarnings):
        """
        Initialize the processor with a context.

        Args:
            ctx: The context object with API and mappings
        """
        self.ctx = ctx

    def log_warning(self, message, to_stderr=True):
        """
        Log a warning message using the context's warning collector.

        Args:
            message (str): The warning message
            to_stderr (bool, optional): Whether to print to stderr. Defaults to True.
        """
        if hasattr(self.ctx, "log_warning"):
            self.ctx.log_warning(message, to_stderr=to_stderr)
        elif hasattr(self.ctx, "warning_collector") and hasattr(
            self.ctx.warning_collector, "log_warning"
        ):
            self.ctx.warning_collector.log_warning(message, to_stderr=to_stderr)

    def process_ordinal_date_filter(
        self,
        filter_obj,
        attr_uri,
        values,
        is_negative,
        attribute_identifier,
        granularity,
    ):
        """
        Process an ordinal date filter - creates an absoluteDateFilter.

        Args:
            filter_obj (dict): The Legacy filter object
            attr_uri (str): The attribute URI
            values (list): The element values
            is_negative (bool): Whether this is a negative filter
            attribute_identifier (str): The attribute identifier
            granularity (str): The date granularity

        Returns:
            tuple: (cloud_filter, filter_config) with the converted filter
        """
        try:
            # Get dataset ID from the attribute identifier
            dataset_id, _ = get_date_dataset_and_granularity(
                self.ctx, attribute_identifier
            )

            if not dataset_id:
                self.log_warning(
                    f"FILTER REMOVED: Could not determine dataset ID for ordinal date attribute: {attr_uri}",
                    to_stderr=True,
                )
                return None, {}

            # Convert attribute elements to their actual values
            # Note: NULL values are already filtered out upstream in DateFilter.process()
            converted_values, missing_elements = convert_attribute_elements(
                self.ctx, values
            )

            if missing_elements:
                self.log_warning(
                    f"FILTER MODIFIED: ordinal date filter {attribute_identifier} is missing values: {', '.join(missing_elements)}",
                    to_stderr=True,
                )

            # Filter out any remaining empty values from conversion
            non_empty_values = [v for v in converted_values if v and str(v).strip()]

            if not non_empty_values:
                self.log_warning(
                    f"FILTER REMOVED: ordinal date filter {attribute_identifier} has no valid values after conversion",
                    to_stderr=True,
                )
                return None, {}

            if is_negative:
                self.log_warning(
                    f"FILTER REMOVED: Negative date range filters not supported: ({attribute_identifier})",
                    to_stderr=True,
                )
                return None, {}

            # Convert the ordinal date values to absolute date ranges
            from_date, to_date = self._convert_ordinal_dates_to_range(
                non_empty_values, granularity, attribute_identifier, values
            )

            if not from_date or not to_date:
                self.log_warning(
                    f"FILTER REMOVED: Could not convert ordinal date values to date range for {attribute_identifier}",
                    to_stderr=True,
                )
                return None, {}

            # Create the absoluteDateFilter
            absolute_date_filter = {
                "absoluteDateFilter": {
                    "dataSet": {"identifier": {"id": dataset_id, "type": "dataset"}},
                    "from": from_date,
                    "to": to_date,
                }
            }

            return absolute_date_filter, {}

        except Exception as e:
            self.log_warning(
                f"Failed to create ordinal date filter: {str(e)}", to_stderr=True
            )
            return None, {}

    def _convert_ordinal_dates_to_range(
        self, date_values, granularity, attribute_identifier, original_uris=None
    ):
        """
        Convert ordinal date values to absolute date range (from/to format).

        Args:
            date_values (list): List of date values from the filter
            granularity (str): The date granularity
            attribute_identifier (str): The attribute identifier for warnings

        Returns:
            tuple: (from_date, to_date) in "YYYY-MM-DD HH:MI" format, or (None, None) if conversion fails
        """

        if not date_values:
            return None, None

        try:
            # Check for non-continuous date ranges using element IDs (much more reliable than date parsing)
            if original_uris and not self._is_continuous_element_sequence(
                original_uris
            ):
                original_values_str = ", ".join(str(v) for v in date_values)
                self.log_warning(
                    f"FILTER MODIFIED: Non-continuous date filter not supported, was: [{original_values_str}] ({attribute_identifier}). Using full range from first to last value.",
                    to_stderr=True,
                )

            # Parse and sort the date values to get the date range
            parsed_dates = []

            for value in date_values:
                parsed_date = self._parse_ordinal_date_value(value, granularity)
                if parsed_date:
                    parsed_dates.append(parsed_date)

            if not parsed_dates:
                return None, None

            # Sort the dates to find the range
            parsed_dates.sort(key=lambda x: x["start_date"])

            # Get the overall range from first start to last end
            from_date = parsed_dates[0]["start_date"]
            to_date = parsed_dates[-1]["end_date"]

            # Format as "YYYY-MM-DD HH:MI"
            from_str = from_date.strftime("%Y-%m-%d 00:00")
            to_str = to_date.strftime("%Y-%m-%d 23:59")

            return from_str, to_str

        except Exception as e:
            self.log_warning(
                f"Error converting ordinal dates to range: {str(e)}", to_stderr=True
            )
            return None, None

    def _parse_ordinal_date_value(self, value, granularity):
        """
        Parse a single ordinal date value and return its start and end dates.

        Args:
            value (str): The date value to parse
            granularity (str): The date granularity

        Returns:
            dict: {'start_date': datetime, 'end_date': datetime} or None if parsing fails
        """

        try:
            value_str = str(value).strip()

            # Year format (2024, 2025)
            if re.match(r"^\d{4}$", value_str):
                year = int(value_str)
                start_date = datetime(year, 1, 1)
                end_date = datetime(year, 12, 31)
                return {"start_date": start_date, "end_date": end_date}

            # Quarter format without Q prefix (2049-1, 2049-2, 2049-3, 2049-4)
            # This must come BEFORE the month pattern to avoid ambiguity
            quarter_numeric_match = re.match(r"^(\d{4})[-/]([1-4])$", value_str)
            if quarter_numeric_match:
                year, quarter = (
                    int(quarter_numeric_match.group(1)),
                    int(quarter_numeric_match.group(2)),
                )
                quarter_months = {
                    1: (1, 3),  # Q1: Jan-Mar
                    2: (4, 6),  # Q2: Apr-Jun
                    3: (7, 9),  # Q3: Jul-Sep
                    4: (10, 12),  # Q4: Oct-Dec
                }
                start_month, end_month = quarter_months[quarter]
                start_date = datetime(year, start_month, 1)
                last_day = calendar.monthrange(year, end_month)[1]
                end_date = datetime(year, end_month, last_day)
                return {"start_date": start_date, "end_date": end_date}

            # Year-month format (2025-01, 2025/01, 2025-11)
            # Note: This comes after quarter pattern to avoid matching quarters as months
            month_match = re.match(r"^(\d{4})[-/](\d{1,2})$", value_str)
            if month_match:
                year, month = int(month_match.group(1)), int(month_match.group(2))
                start_date = datetime(year, month, 1)
                last_day = calendar.monthrange(year, month)[1]
                end_date = datetime(year, month, last_day)
                return {"start_date": start_date, "end_date": end_date}

            # Quarter format with Q prefix (2025/Q1, 2025-Q1, 2025Q1)
            quarter_match = re.match(r"^(\d{4})[-/]?[Qq]([1-4])$", value_str)
            if quarter_match:
                year, quarter = int(quarter_match.group(1)), int(quarter_match.group(2))
                quarter_months = {
                    1: (1, 3),  # Q1: Jan-Mar
                    2: (4, 6),  # Q2: Apr-Jun
                    3: (7, 9),  # Q3: Jul-Sep
                    4: (10, 12),  # Q4: Oct-Dec
                }
                start_month, end_month = quarter_months[quarter]
                start_date = datetime(year, start_month, 1)
                last_day = calendar.monthrange(year, end_month)[1]
                end_date = datetime(year, end_month, last_day)
                return {"start_date": start_date, "end_date": end_date}

            # Week format (2025/W12, 2025-W12, 2025W12)
            week_match = re.match(r"^(\d{4})[-/]?[Ww](\d{1,2})$", value_str)
            if week_match:
                year, week = int(week_match.group(1)), int(week_match.group(2))
                # Calculate the date for the given ISO week

                jan4 = datetime(year, 1, 4)  # Jan 4th is always in week 1
                week1_monday = jan4 - timedelta(days=jan4.weekday())  # Monday of week 1
                target_monday = week1_monday + timedelta(weeks=week - 1)
                start_date = target_monday
                end_date = target_monday + timedelta(days=6)  # Sunday of the same week
                return {"start_date": start_date, "end_date": end_date}

            # Full date format (2025-01-01, 2025/01/01)
            date_match = re.match(r"^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$", value_str)
            if date_match:
                year, month, day = (
                    int(date_match.group(1)),
                    int(date_match.group(2)),
                    int(date_match.group(3)),
                )
                date_obj = datetime(year, month, day)
                return {"start_date": date_obj, "end_date": date_obj}

            # If no pattern matches, return None
            return None

        except Exception:
            # If parsing fails for any reason, return None
            return None

    def _is_continuous_element_sequence(self, element_uris):
        """
        Check if element URIs form a continuous sequence based on their element IDs.

        This is much more reliable than parsing date strings, since ordinal date attributes
        have sequential element IDs that represent the natural ordering.

        Args:
            element_uris (list): List of element URIs (e.g. .../elements?id=8199)

        Returns:
            bool: True if the element IDs are continuous, False otherwise
        """
        if len(element_uris) <= 1:
            return True

        try:
            # Extract element IDs from URIs
            element_ids = []
            for uri in element_uris:
                match = re.search(r"elements\?id=(\d+)", uri)
                if match:
                    element_ids.append(int(match.group(1)))

            if len(element_ids) != len(element_uris):
                # Couldn't extract all IDs, assume continuous to avoid false warnings
                return True

            # Sort the IDs
            element_ids.sort()

            # Check if they form a continuous sequence
            for i in range(1, len(element_ids)):
                if element_ids[i] != element_ids[i - 1] + 1:
                    return False

            return True

        except Exception:
            # If we can't determine, assume it's continuous to avoid false warnings
            return True
