# (C) 2026 GoodData Corporation
"""
Helper functions for processing web compare CLI operations.
"""

import os
from typing import Dict, List, Tuple

from gooddata_platform2cloud.config.configuration_objects import WebCompareConfig
from gooddata_platform2cloud.web_compare_processing.discovery import (
    LogFileDiscovery,
    LogFileInfo,
)
from gooddata_platform2cloud.web_compare_processing.processor import LogProcessor

# ANSI color codes for terminal output
LIGHT_BLUE = "\033[94m"
UNDERLINE = "\033[4m"
RESET = "\033[0m"


def validate_log_directory(log_dir: str) -> Tuple[bool, str]:
    """
    Validate the log directory exists.

    Args:
        log_dir: Directory path to validate

    Returns:
        Tuple of (is_valid, error_message)
    """
    if not os.path.isdir(log_dir):
        return False, f"Error: {log_dir} is not a directory"
    return True, ""


def find_and_analyze_log_files(
    log_dir: str,
) -> Tuple[bool, str, List[LogFileInfo], Dict[str, List[str]]]:
    """
    Find and analyze log files in the directory.

    Args:
        log_dir: Directory to search for log files

    Returns:
        Tuple of (success, error_message, log_files_info, prefixes_by_object_type)
    """
    # Find all log files matching *_logs.log pattern
    log_files = LogFileDiscovery.find_log_files(log_dir)

    if not log_files:
        return (
            False,
            f"Error: No log files found matching *_logs.log in {log_dir}",
            [],
            {},
        )

    print("\n----Looking for log files----")

    # Analyze log files - types (dashboards, insights...) and prefixes are auto-detected from filename
    # Results are already sorted with unprefixed first and prefixes are returned as sets
    log_files_info, all_prefixes_sets = LogFileDiscovery.analyze_log_files(log_files)

    # Convert sets to sorted lists for compatibility with the rest of the code
    all_prefixes = {
        obj_type: sorted(list(prefixes))
        for obj_type, prefixes in all_prefixes_sets.items()
    }

    return True, "", log_files_info, all_prefixes


def process_log_files(
    processor: LogProcessor,
    log_files_info: List[LogFileInfo],
    all_prefixes: Dict[str, List[str]],
) -> Tuple[int, int, int, str, str]:
    """
    Process a list of log files.

    Args:
        processor: LogProcessor instance
        log_files_info: List of LogFileInfo objects
        all_prefixes: Dictionary of prefixes by object type

    Returns:
        Tuple of (success_count, prefixed_count, unprefixed_count, first_output_path, first_unprefixed_path)
    """
    success_count = 0
    prefixed_count = 0
    unprefixed_count = 0
    first_output_path = ""
    first_unprefixed_path = ""

    print(f"\n----Processing Log files ({len(log_files_info)})----")

    for i, file_info in enumerate(log_files_info, 1):
        result, output_path = processor.process_log_file(
            log_file_path=file_info.path,
            object_type=file_info.object_type,
            client_prefix=file_info.prefix,
            all_prefixes=all_prefixes,
            file_number=i,
        )

        if result == 0:
            success_count += 1
            if file_info.has_prefix:
                prefixed_count += 1
            else:
                unprefixed_count += 1
                if not first_unprefixed_path:
                    first_unprefixed_path = output_path

            # Store the first successful output path
            if not first_output_path:
                first_output_path = output_path

    return (
        success_count,
        prefixed_count,
        unprefixed_count,
        first_output_path,
        first_unprefixed_path,
    )


def process_log_directory(config: WebCompareConfig, processor: LogProcessor):
    """
    Process all log files in a directory.

    Args:
        args: Command line arguments
        processor: LogProcessor instance

    Returns:
        Exit code (0 for success, 1 for error)
    """
    # Get log directory from arguments or use current directory
    log_dir = config.log_dir if config.log_dir else "."

    # Verify directory exists
    is_valid, error_message = validate_log_directory(log_dir)
    if not is_valid:
        print(error_message)
        return 1

    # Find and analyze log files
    success, error_message, log_files_info, all_prefixes = find_and_analyze_log_files(
        log_dir
    )
    if not success:
        print(error_message)
        return 1

    # Process the log files (already sorted with unprefixed first)
    (
        success_count,
        prefixed_count,
        unprefixed_count,
        first_output_path,
        first_unprefixed_path,
    ) = process_log_files(processor, log_files_info, all_prefixes)

    # Determine which path to suggest opening
    suggested_path = (
        first_unprefixed_path if first_unprefixed_path else first_output_path
    )

    # Generate index.html that redirects to suggested_path
    if suggested_path:
        # Get relative path to the suggested file from the output directory
        suggested_rel_path = os.path.relpath(suggested_path, config.output_dir)

        # Create index.html with redirect
        index_path = os.path.join(config.output_dir, "index.html")
        with open(index_path, "w") as f:
            f.write(
                f"""<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="refresh" content="0; url={suggested_rel_path}" />
    <title>GoodData Migration Web Compare</title>
</head>
<body>
    <p>Redirecting to <a href="{suggested_rel_path}">{suggested_rel_path}</a>...</p>
</body>
</html>
"""
            )
        print(f"Generated index.html with redirect to {suggested_rel_path}")

    if suggested_path:
        print("\n----View Results----")
        print(
            f"Start by opening: {LIGHT_BLUE}{UNDERLINE}file://{os.path.abspath(suggested_path)}{RESET}"
        )

    return 0 if success_count == len(log_files_info) else 1
