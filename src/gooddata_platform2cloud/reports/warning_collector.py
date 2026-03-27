# (C) 2026 GoodData Corporation
import logging

logger = logging.getLogger("migration")

# TODO: This functionality is wrapped by a bunch of classes. These calls should
# be consolidated
# TODO: Eventually replace this with something more simple


class WarningCollector:
    """Collects warnings and errors during processing."""

    def __init__(self):
        self.warnings = []
        self.info_warnings = []
        self.errors = []
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

    def log_warning(self, message, to_stderr=True, severity="warning"):
        """
        Log a warning message.

        Args:
            message (str): The warning message to log
            to_stderr (bool, optional): Whether to print the message to stderr. Defaults to True.
            severity (str, optional): The severity of the warning, "warning" or "info". Defaults to "warning".
        """
        if severity == "info":
            self.info_warnings.append(message)
            if to_stderr:
                logger.info("INFO: %s", message)
        else:  # Default is "warning"
            self.warnings.append(message)
            if to_stderr:
                logger.warning("%s", message)

    def log_info(self, message, to_stderr=True):
        """
        Log an informational message (lower severity warning).

        Args:
            message (str): The info message to log
            to_stderr (bool, optional): Whether to print the message to stderr. Defaults to True.
        """
        self.log_warning(message, to_stderr, severity="info")

    def log_error(self, message, to_stderr=True):
        """
        Log an error message.

        Args:
            message (str): The error message to log
            to_stderr (bool, optional): Whether to print the message to stderr. Defaults to True.
        """
        self.errors.append(message)
        self.logger.error(message)
        if to_stderr:
            logger.error("%s", message)

    def get_warnings(self):
        """
        Get all warning messages (both regular warnings and info warnings).
        Info warnings are only included if there are regular warnings.

        Returns:
            list: List of warning messages, sorted with regular warnings first
        """
        result = list(self.warnings)

        # Only include info warnings if there are regular warnings
        if result:
            result.extend(self.info_warnings)

        return result

    def get_errors(self):
        """
        Get all error messages.

        Returns:
            list: List of error messages
        """
        return list(self.errors)

    def has_warnings(self):
        """
        Check if there are any regular warnings (excluding info warnings).

        Returns:
            bool: True if there are regular warnings, False otherwise
        """
        return len(self.warnings) > 0
