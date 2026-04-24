# (C) 2026 GoodData Corporation
"""
Consolidated package for web compare generation and log processing.
This package combines functionality from the former web_compare and log_processing packages.
"""

# Log Processing
# Comparison Results
from gooddata_legacy2cloud.web_compare_processing.cli_processor import (
    find_and_analyze_log_files,
    process_log_directory,
    process_log_files,
    validate_log_directory,
)
from gooddata_legacy2cloud.web_compare_processing.comparison_result import (
    ComparisonItem,
    ComparisonResult,
    ComparisonStatus,
    ComparisonSummary,
)
from gooddata_legacy2cloud.web_compare_processing.detector import LogFileDetector
from gooddata_legacy2cloud.web_compare_processing.discovery import (
    LogFileDiscovery,
    LogFileInfo,
)
from gooddata_legacy2cloud.web_compare_processing.file_utils import (
    FilePathBuilder,
    JsonDataLoader,
)

# Web Compare
from gooddata_legacy2cloud.web_compare_processing.generator import (
    ComparisonGenerator,
    WebCompareUtils,
)
from gooddata_legacy2cloud.web_compare_processing.parsing.log_parser import LogParser
from gooddata_legacy2cloud.web_compare_processing.parsing.metadata import LogMetadata
from gooddata_legacy2cloud.web_compare_processing.processor import LogProcessor

__all__ = [
    # Log Processing
    "LogFileDetector",
    "LogParser",
    "LogMetadata",
    "FilePathBuilder",
    "JsonDataLoader",
    "LogProcessor",
    "LogFileDiscovery",
    "LogFileInfo",
    # Web Compare
    "ComparisonGenerator",
    "WebCompareUtils",
    # CLI Processing
    "validate_log_directory",
    "find_and_analyze_log_files",
    "process_log_files",
    "process_log_directory",
    # Comparison Results
    "ComparisonResult",
    "ComparisonItem",
    "ComparisonSummary",
    "ComparisonStatus",
]
