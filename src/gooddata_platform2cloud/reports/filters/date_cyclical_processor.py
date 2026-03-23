# (C) 2026 GoodData Corporation
"""
Cyclical date filter processor for Platform to Cloud migration.

This module handles the processing of cyclical date filters,
which are treated as attribute filters in Cloud but need
a companion relativeDateFilter.
"""

from gooddata_platform2cloud.metrics.cyclical_date_conversion import (
    convert_cyclical_date_elements,
)
from gooddata_platform2cloud.reports.common import generate_local_id
from gooddata_platform2cloud.reports.data_classes import ContextWithWarnings
from gooddata_platform2cloud.reports.filters.date_helpers import (
    get_date_dataset_and_granularity,
)
from gooddata_platform2cloud.reports.filters.helpers import get_display_form_identifiers


class CyclicalDateProcessor:
    """
    Processor for cyclical date filters that creates both an attribute filter
    and a companion relativeDateFilter.
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

    def process_cyclical_date_filter(
        self,
        filter_obj,
        attr_uri,
        values,
        is_negative,
        attribute_identifier,
        granularity,
    ):
        """
        Process a cyclical date filter - creates both an attribute filter and a relativeDateFilter.

        Args:
            filter_obj (dict): The Platform filter object
            attr_uri (str): The attribute URI
            values (list): The element values
            is_negative (bool): Whether this is a negative filter
            attribute_identifier (str): The attribute identifier
            granularity (str): The date granularity

        Returns:
            tuple: (cloud_filter_list, filter_config) with multiple filters and configuration
        """
        try:
            # Get dataset ID from the attribute identifier
            dataset_id, _ = get_date_dataset_and_granularity(
                self.ctx, attribute_identifier
            )

            if not dataset_id:
                self.log_warning(
                    f"FILTER REMOVED: Could not determine dataset ID for cyclical date attribute: {attr_uri}",
                    to_stderr=True,
                )
                return None, {}

            # Generate a unique local ID for the attribute filter
            filter_seed = attribute_identifier + "_cyclical_filter"
            filter_local_id = generate_local_id(filter_seed)

            # Get display form identifiers
            primary_df_id, original_df_id, original_df_type = (
                get_display_form_identifiers(self.ctx, attr_uri)
            )

            # Convert cyclical date elements using hardcoded mappings
            # Note: NULL values are already filtered out upstream in DateFilter.process()
            converted_values, missing_elements, null_elements = (
                convert_cyclical_date_elements(self.ctx, values, attribute_identifier)
            )

            if missing_elements:
                self.log_warning(
                    f"FILTER MODIFIED: cyclical date filter {original_df_id} is missing values: {', '.join(missing_elements)}",
                    to_stderr=True,
                )

            # Sanity check - should not happen since nulls are filtered upstream
            if not converted_values:
                self.log_warning(
                    f"FILTER REMOVED: cyclical date filter {original_df_id} has no valid values after conversion",
                    to_stderr=True,
                )
                return None, {}

            # Create the attribute filter (positive or negative based on original filter)
            if is_negative:
                attribute_filter = {
                    "negativeAttributeFilter": {
                        "localIdentifier": filter_local_id,
                        "displayForm": {
                            "identifier": {"id": primary_df_id, "type": "label"}
                        },
                        "notIn": {"values": converted_values},
                    }
                }
            else:
                attribute_filter = {
                    "positiveAttributeFilter": {
                        "localIdentifier": filter_local_id,
                        "displayForm": {
                            "identifier": {"id": primary_df_id, "type": "label"}
                        },
                        "in": {"values": converted_values},
                    }
                }

            # Create the companion relativeDateFilter (always present for cyclical dates)
            relative_date_filter = {
                "relativeDateFilter": {
                    "dataSet": {"identifier": {"id": dataset_id, "type": "dataset"}},
                    "granularity": granularity,
                }
            }

            # Build the filter configuration
            filter_config = {
                filter_local_id: {
                    "displayAsLabel": {
                        "identifier": {"id": original_df_id, "type": original_df_type}
                    }
                }
            }

            # Return both filters as a list and the configuration
            return [attribute_filter, relative_date_filter], filter_config

        except Exception as e:
            self.log_warning(
                f"Failed to create cyclical date filter: {str(e)}", to_stderr=True
            )
            return None, {}
