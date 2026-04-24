# Report Transformation Modules

This directory contains modules for transforming Legacy report definitions into Cloud visualization objects.

## Module Structure

- `transformation.py` - Main entry point for report transformation
- `chart_processing.py` - Functions for processing chart reports
- `grid_processing.py` - Functions for processing grid/table reports
- `filters.py` - Functions for processing filters
- `mappings.py` - Functions for mapping metrics and attributes
- `common.py` - Common utilities and error/warning handling
- `visualization_adjustments.py` - Functions for adjusting visualization buckets

## Thread Safety

The transformation process is designed to be thread-safe for processing one report per thread:

1. No global state is modified during transformation
2. All state is contained within the context object passed to each function
3. Warning and error collection is thread-local
4. Each report transformation is independent

If implementing multi-threaded processing in the future, ensure:
- Each thread gets its own context object
- Each thread processes one report at a time
- No shared state is modified without proper synchronization

## Modular Design

The codebase has been refactored for maintainability:

1. Clear separation of concerns between chart and grid processing
2. Functions are grouped by their purpose
3. Each function has a single responsibility
4. Comprehensive documentation for each function

## Common Patterns

- Context object (`ctx`) is passed to all functions needing access to Legacy/Cloud APIs or mappings
- Functions return the data structures they create or modify
- Warning and error messages are collected and added to report descriptions
- Functions follow consistent naming and parameter patterns
