# Web Compare Processing

This package contains modules for processing log files and generating web comparisons for migration results.

## Structure

- `__init__.py`: Exports all the public classes and functions
- `comparison_result.py`: Data classes for comparison results (ComparisonResult, ComparisonItem, etc.)
- `detector.py`: Utilities for detecting log file properties
- `discovery.py`: Utilities for discovering and analyzing log files with prefix detection
- `file_utils.py`: Utilities for file path building and JSON data loading
- `generator.py`: Utilities for generating web comparison HTML with consistent status formatting
- `parser.py`: Parser for migration log files
- `processor.py`: Processor for migration log files with inheritance support
- `templates/`: HTML templates and associated resources (CSS, JavaScript)

## Key Features

- **Automatic detection** of object types and client prefixes from log filenames
- **Inheritance support** for prefixed clients to inherit objects from unprefixed logs
- **Responsive UI** with interactive filtering, sorting, and side-by-side comparison
- **Consistent status indicators** using centralized status titles
- **Workspace handling** for both prefixed and unprefixed clients
- **Visual enhancements** like status icons with hover effects and filter highlights

## Status Types

The module defines consistent status types for migration objects in `comparison_result.py`:

```python
STATUS_TITLES = {
    "success": "Successfully migrated.",
    "warning": "Migrated with warnings. Requires attention.",
    "skipped": "Deployment skipped - object already existed.",
    "api-error": "Deployment failed - API error.",
    "error": "Migration Failed",
    "inherited": "Not migrated directly but inherited from a workspce hierarchy."
}
```

## Usage

This package is primarily used by the `generate_web_compare.py` script to process log files and generate HTML comparisons.

```python
from gooddata_platform2cloud.web_compare_processing import (
    LogFileDetector, LogProcessor, LogFileDiscovery, LogFileInfo
)

# Create a processor instance with inheritance support
processor = LogProcessor(env_vars, output_dir, use_inheritance=True)

# Find and analyze log files (automatically sorted with unprefixed first)
log_files = LogFileDiscovery.find_log_files(log_dir)
log_files_info, all_prefixes = LogFileDiscovery.analyze_log_files(log_files)

# Process log files
for file_info in log_files_info:
    result, output_path = processor.process_log_file(
        log_file_path=file_info.path,
        object_type=file_info.object_type,
        client_prefix=file_info.prefix,
        all_prefixes=all_prefixes
    )
```

## Workspace Handling

- For unprefixed logs: Always show workspace IDs from log file or .env fallbacks
- For prefixed logs with migration info: Show workspace IDs from the log file
- For prefixed logs without migration info: Show warning and disable workspace links

## Visual Features

- **Summary cards** with status counts and hover effects showing status icons
- **Interactive filtering** by status type with visual indicators
- **Side-by-side comparison** of Platform and Cloud objects
- **Responsive layout** with collapsible sidebar and adaptive element sizing
