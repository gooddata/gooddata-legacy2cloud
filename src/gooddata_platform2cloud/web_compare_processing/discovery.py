# (C) 2026 GoodData Corporation
"""
Utilities for discovering and analyzing log files.
"""

import glob
import os
from typing import Dict, List, Set, Tuple

from gooddata_platform2cloud.web_compare_processing.detector import (
    OBJECT_TYPES,
    LogFileDetector,
)


class LogFileInfo:
    """Information about a log file."""

    def __init__(
        self,
        path: str,
        object_type: str,
        prefix: str = "",
        has_migration_info: bool = False,
    ):
        self.path = path
        self.object_type = object_type
        self.prefix = prefix
        self.has_prefix = bool(prefix)
        self.basename = os.path.basename(path)
        self.has_migration_info = has_migration_info

    def __str__(self) -> str:
        """Human-readable representation of log file info."""
        prefix_info = f"{self.prefix}" if self.prefix else "no prefix"
        info_status = "INFO" if self.has_migration_info else "no info"
        return (
            f"Found: {self.basename} ({self.object_type}; {info_status}; {prefix_info})"
        )


class LogFileDiscovery:
    """Discover and analyze log files."""

    @staticmethod
    def find_log_files(log_dir: str) -> List[str]:
        """
        Find all log files in a directory.

        Args:
            log_dir: Directory to search

        Returns:
            List of log file paths sorted with unprefixed logs first
        """
        log_pattern = os.path.join(log_dir, "*_logs.log")
        log_files = glob.glob(log_pattern)

        # Sort log files to ensure unprefixed logs are processed first
        def is_prefixed(file_path):
            # Use the detector to check if the file has a prefix
            prefix = LogFileDetector.detect_prefix(file_path)
            return bool(prefix)

        # Sort: unprefixed files first, then alphabetically within each group
        log_files.sort(key=lambda file_path: (is_prefixed(file_path), file_path))

        return log_files

    @staticmethod
    def analyze_log_files(
        log_files: List[str],
    ) -> Tuple[List[LogFileInfo], Dict[str, Set[str]]]:
        """
        Analyze log files to detect object types and prefixes.

        Args:
            log_files: List of log file paths

        Returns:
            Tuple of (list of LogFileInfo objects, dictionary of prefixes by object type)
        """
        log_files_info = []
        all_prefixes = {obj_type: set() for obj_type in OBJECT_TYPES}

        for log_file in log_files:
            # Auto-detect object type
            obj_type = LogFileDetector.detect_object_type(log_file)
            if not obj_type:
                print(
                    f"Warning: Skipping {os.path.basename(log_file)} - could not detect object type"
                )
                continue

            # Normalize object type
            obj_type_singular = obj_type.lower()
            if obj_type_singular.endswith("s"):
                obj_type_singular = obj_type_singular[:-1]

            # Auto-detect prefix
            prefix = LogFileDetector.detect_prefix(log_file)

            # Check if file has migration info
            has_info = LogFileDetector.has_migration_info(log_file)

            # Create LogFileInfo object
            log_file_info = LogFileInfo(log_file, obj_type_singular, prefix, has_info)
            log_files_info.append(log_file_info)

            # Add to prefix sets
            if prefix:
                all_prefixes[obj_type_singular].add(prefix.rstrip("_"))

            # Display simple file info
            print(log_file_info)

        return log_files_info, all_prefixes
