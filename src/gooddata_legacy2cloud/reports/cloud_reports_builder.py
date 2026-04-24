# (C) 2026 GoodData Corporation
"""
The cloud_reports_builder module contains the CloudReportsBuilder class,
which is responsible for building Cloud reports.
It processes Legacy reports in parallel and produces a merged report object for migration.
"""

import concurrent.futures
import logging
import threading

from gooddata_legacy2cloud.logging.context import ObjectContext
from gooddata_legacy2cloud.output_writer import OutputWriter
from gooddata_legacy2cloud.reports.cloud_report import CloudReport
from gooddata_legacy2cloud.reports.data_classes import ReportContext
from gooddata_legacy2cloud.reports.payload_validator import (
    validate_and_reduce_payload_size,
)

logger = logging.getLogger("migration")

REPORTS_LOGGER_FILE = "report_logs.log"
MAX_WORKERS = 5


class CloudReportsBuilder:
    """
    The CloudReportsBuilder class contains the methods
    required to build Cloud reports.
    Processes Legacy reports in parallel where each report manages its own warning collection
    and thread-safe logging ensures clean log files during migration.
    """

    def __init__(self, ctx: ReportContext):
        self.legacy_reports_raw = None
        self.ctx = ctx
        self.cloud_reports = []
        # Add locks for thread safety
        self.warning_lock = threading.Lock()  # For coordinating warning output
        self.logging_lock = threading.Lock()  # For atomic log file writes

    def process_legacy_reports(self, legacy_reports_raw: list):
        """
        Load Legacy reports. For each report, identify the last report definition,
        discard the definition's metadata and replace it with the metadata from the parent report,
        and produce a single combined object.
        The operation is executed in parallel where each report manages its own warning collection
        and thread-safe logging ensures clean log files and consistent results.
        """
        self.legacy_reports_raw = legacy_reports_raw
        reports_logger = OutputWriter(REPORTS_LOGGER_FILE)

        # Write metadata as the first line in the log file
        reports_logger.write_migration_metadata(
            self.ctx.legacy_client.domain,
            self.ctx.legacy_client.pid,
            self.ctx.cloud_client.domain,
            self.ctx.cloud_client.ws,
            self.ctx.client_prefix if hasattr(self.ctx, "client_prefix") else None,
        )

        def worker(index: int, report):
            if "report" in report:
                report_title = report["report"]["meta"]["title"]
                report_id = report["report"]["meta"]["identifier"]
            elif "reportDefinition" in report:
                report_title = report["reportDefinition"]["meta"]["title"]
                report_id = report["reportDefinition"]["meta"]["identifier"]
            else:
                report_title = "Unknown Report"
                report_id = "unknown"
            with ObjectContext(report_id, report_title):
                return process_report(index, report, report_title)

        def process_report(index: int, report, report_title: str):
            """Process a single report with instance-managed warning collection."""
            # TODO: Move the execution logic to a separate private method

            # Acquire the lock to safely log the processing message
            with self.warning_lock:
                logger.info("Processing %d", index + 1)

            try:
                # Each CloudReport instance manages its own warning collection
                # The shared context can be safely used across all threads
                cloud_report = CloudReport(self.ctx, report)
                report_obj = cloud_report.get()

                # Validate and reduce payload size if necessary
                if report_obj and isinstance(report_obj, dict):
                    report_obj = validate_and_reduce_payload_size(
                        report_obj, report_title
                    )

            except Exception as exc:
                report_obj = f"ERROR: {exc}"
                with self.warning_lock:
                    logger.error("Processing %d failed: %s", index + 1, exc)
            finally:
                # Ensure atomic write of all log lines for this report
                with self.logging_lock:
                    reports_logger.write_transformation(
                        report_title, report, report_obj
                    )

            return report_obj

        # Use a smaller max_workers to avoid too many concurrent stderr writes
        # This helps keep warnings more organized
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            results = list(
                executor.map(
                    lambda pair: worker(*pair), enumerate(self.legacy_reports_raw)
                )
            )

        self.cloud_reports = results

    def get_cloud_reports(self):
        """
        Returns the reports.
        """
        return self.cloud_reports
