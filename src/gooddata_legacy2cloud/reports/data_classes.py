# (C) 2026 GoodData Corporation

from dataclasses import dataclass, field

from gooddata_legacy2cloud.backends.cloud.client import CloudClient
from gooddata_legacy2cloud.backends.legacy.client import LegacyClient
from gooddata_legacy2cloud.id_mappings import IdMappings
from gooddata_legacy2cloud.output_writer import OutputWriter
from gooddata_legacy2cloud.reports.warning_collector import WarningCollector


@dataclass
class ReportContext:
    """
    Shared context for report migration containing APIs, mappings, and shared resources.
    This context is thread-safe for read operations and can be safely shared across
    multiple report processing threads. Warning collection is now handled at the
    individual report level rather than in this shared context.
    """

    legacy_client: LegacyClient
    cloud_client: CloudClient
    ldm_mappings: IdMappings
    metric_mappings: IdMappings
    mapping_logger: OutputWriter
    suppress_warnings: bool = field(default=False)
    client_prefix: str | None = field(default=None)


# TODO: The ContextWithWarnings class should be untangled and removed.
# It was moved here from utils/reports/transformation.py to avoid circular imports.
class ContextWithWarnings:
    """
    A context proxy that provides warning methods while delegating to the original context
    for all other operations. This allows filters to log warnings to a specific warning
    collector while still accessing shared resources from the main context.
    """

    def __init__(
        self, original_ctx: ReportContext, warning_collector: WarningCollector
    ):
        """
        Initialize the context proxy.

        Args:
            original_ctx: The original shared context
            warning_collector: The warning collector to use for this transformation
        """
        self._original_ctx: ReportContext = original_ctx
        self._warning_collector: WarningCollector = warning_collector

    def __getattr__(self, name):
        """Delegate attribute access to the original context."""
        return getattr(self._original_ctx, name)

    def log_warning(self, message, to_stderr=True, severity="warning"):
        """
        Log a warning message using the instance warning collector.

        Args:
            message (str): The warning message to log
            to_stderr (bool, optional): Whether to print the message to stderr
            severity (str, optional): The severity of the warning
        """
        self._warning_collector.log_warning(message, to_stderr, severity)

    def log_info(self, message, to_stderr=True):
        """
        Log an informational message using the instance warning collector.

        Args:
            message (str): The info message to log
            to_stderr (bool, optional): Whether to print the message to stderr
        """
        self._warning_collector.log_info(message, to_stderr)

    def log_error(self, message, to_stderr=True):
        """
        Log an error message using the instance warning collector.

        Args:
            message (str): The error message to log
            to_stderr (bool, optional): Whether to print the message to stderr
        """
        self._warning_collector.log_error(message, to_stderr)
