# (C) 2026 GoodData Corporation
"""
This module contains the CloudReport class,
which transforms a Platform report (with "reportDefinition")
into a Cloud visualizationObject.
It uses the transformation logic in transformation.py where each Platform URI is first resolved to its Platform identifier
and then converted to a Cloud identifier using the same logic as insights migration.
Each CloudReport instance manages its own warning collection for thread-safe processing.
"""

from gooddata_platform2cloud.reports.data_classes import ReportContext
from gooddata_platform2cloud.reports.transformation import transform_platform_report
from gooddata_platform2cloud.reports.warning_collector import WarningCollector


class CloudReport:
    """
    The CloudReport class transforms a Platform report into a Cloud visualizationObject.
    Each instance manages its own warning collection for thread-safe processing.
    """

    def __init__(self, ctx: ReportContext, metadata: dict):
        self.ctx = ctx
        self.warning_collector = WarningCollector()

        # Handle different report formats
        # Objects from fetch_objects_with_filters will have the report key that contains the metadata
        if "report" in metadata:
            self.platform_report = {"reportDefinition": metadata["report"]}
        # Original merged reports format directly uses reportDefinition
        elif "reportDefinition" in metadata:
            self.platform_report = metadata
        else:
            # In case we have a direct wrapper structure from fetch_objects_with_filters
            for key in metadata:
                if (
                    isinstance(metadata[key], dict)
                    and "content" in metadata[key]
                    and "meta" in metadata[key]
                ):
                    self.platform_report = {"reportDefinition": metadata[key]}
                    break
            else:
                # If we couldn't find a valid structure, use the original
                self.platform_report = metadata

    def get(self):
        """
        Returns the Cloud report object with warnings included.
        """
        return transform_platform_report(
            self.ctx, self.platform_report, self.warning_collector
        )

    def log_warning(self, message, to_stderr=True, severity="warning"):
        """
        Log a warning message.

        Args:
            message (str): The warning message to log
            to_stderr (bool, optional): Whether to print the message to stderr
            severity (str, optional): The severity of the warning
        """
        self.warning_collector.log_warning(message, to_stderr, severity)

    def log_info(self, message, to_stderr=True):
        """
        Log an informational message.

        Args:
            message (str): The info message to log
            to_stderr (bool, optional): Whether to print the message to stderr
        """
        self.warning_collector.log_info(message, to_stderr)

    def log_error(self, message, to_stderr=True):
        """
        Log an error message.

        Args:
            message (str): The error message to log
            to_stderr (bool, optional): Whether to print the message to stderr
        """
        self.warning_collector.log_error(message, to_stderr)

    def get_warnings(self):
        """
        Get all warning messages.

        Returns:
            list: List of warning messages
        """
        return self.warning_collector.get_warnings()

    def get_errors(self):
        """
        Get all error messages.

        Returns:
            list: List of error messages
        """
        return self.warning_collector.get_errors()

    def has_warnings(self):
        """
        Check if there are any warnings.

        Returns:
            bool: True if there are warnings, False otherwise
        """
        return self.warning_collector.has_warnings()
