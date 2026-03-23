# (C) 2026 GoodData Corporation
"""
Base filter class for Platform to Cloud filter migration.

This module provides the base class for all filter types.
"""

from gooddata_platform2cloud.reports.data_classes import ContextWithWarnings


class Filter:
    """
    Base class for all filter types.

    This class defines the common interface and functionality for all filter types.
    """

    def __init__(self, ctx: ContextWithWarnings):
        """
        Initialize the filter with a context.

        Args:
            ctx: The context object with API and mappings
        """
        self.ctx = ctx

    def process(self, filter_obj, **kwargs):
        """
        Process a filter and convert it to Cloud format.

        This is the main method to be implemented by subclasses.

        Args:
            filter_obj (dict): The Platform filter object
            **kwargs: Additional arguments specific to the filter type

        Returns:
            tuple: (cloud_filter, filter_config) with the converted filter and its configuration
        """
        raise NotImplementedError("Subclasses must implement the process method")

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
