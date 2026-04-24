# (C) 2026 GoodData Corporation
"""
Utilities for detecting log file properties.
"""

import os
import re
from typing import Optional

# Constants
OBJECT_TYPES = ["insight", "dashboard", "report"]
LOG_FILE_PATTERNS = {
    "insight": r".*insight_logs\.log$",
    "dashboard": r".*dashboard_logs\.log$",
    "report": r".*report_logs\.log$",
}


class LogFileDetector:
    """Utility for detecting log file properties."""

    @staticmethod
    def detect_object_type(filename: str) -> Optional[str]:
        """
        Detect the object type from the log filename.

        Patterns:
        - [prefix-]insight_logs.log -> insight
        - [prefix-]dashboard_logs.log -> dashboard
        - [prefix-]report_logs.log -> report

        Args:
            filename: Log filename to analyze

        Returns:
            Object type as string ('insight', 'dashboard', 'report') or None if not detected
        """
        base_filename = os.path.basename(filename)

        for obj_type, pattern in LOG_FILE_PATTERNS.items():
            if re.match(pattern, base_filename):
                return obj_type

        return None

    @staticmethod
    def detect_prefix(filename: str) -> str:
        """
        Detect the client prefix from the log filename.

        Patterns:
        - dashboard_logs.log -> no prefix
        - bflmpsvz_dashboard_logs.log -> "bflmpsvz_" prefix
        - bflmpsvz_abcddashboard_logs.log -> "bflmpsvz_abcd" prefix

        Args:
            filename: Log filename to analyze

        Returns:
            Client prefix as string (with trailing underscore) or empty string if no prefix
        """
        base_filename = os.path.basename(filename)

        for obj_type in OBJECT_TYPES:
            suffix = f"{obj_type}_logs.log"
            if base_filename.endswith(suffix):
                if base_filename == suffix:
                    return ""
                return base_filename[: -len(suffix)]

        return ""

    @staticmethod
    def has_migration_info(log_file_path: str) -> bool:
        """
        Check if a log file contains the #MIGRATION_INFO# header on three first lines.

        Args:
            log_file_path: Path to the log file

        Returns:
            Boolean indicating if the file has migration info headers
        """
        try:
            with open(log_file_path, "r", encoding="utf-8") as f:
                # Read up to 3 lines
                lines = [f.readline() for _ in range(3)]

            # Check if all lines start with #MIGRATION_INFO#
            return all(line.startswith("#MIGRATION_INFO#") for line in lines)
        except Exception:
            return False
