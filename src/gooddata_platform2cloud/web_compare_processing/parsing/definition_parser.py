# (C) 2026 GoodData Corporation
"""
Definition parsing utilities for migration log files.
"""

import json
from typing import Any, Optional


def parse_definition(line: str, line_index: int) -> tuple[Any, int]:
    """
    Parse a definition line, attempting JSON parsing.
    First tries standard JSON, then falls back to handling older formats.

    Args:
        line: The line to parse
        line_index: The index of the line

    Returns:
        Tuple of (parsed definition, line index)
    """
    line = line.strip()

    # Try different parsing strategies in order of preference
    parsers = [
        _try_standard_json_parsing,
        _check_for_plain_text,
        _try_python_literal_parsing,
    ]

    for parser in parsers:
        result = parser(line)
        if result is not None:
            return result, line_index

    # If all parsing attempts fail, return the line as is
    return line, line_index


def _try_standard_json_parsing(line: str) -> Optional[Any]:
    """
    Try to parse the line as standard JSON.

    Args:
        line: The line to parse

    Returns:
        Parsed JSON object or None if parsing fails
    """
    if line.startswith("{") or line.startswith("["):
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            return None
    return None


def _check_for_plain_text(line: str) -> Optional[str]:
    """
    Check if the line is plain text without JSON structure.

    Args:
        line: The line to parse

    Returns:
        The line as plain text if it's not JSON, None otherwise
    """
    # Special case for error messages - preserve them exactly
    if line.startswith("ERROR:"):
        return line

    # For regular plain text
    if not (
        line.startswith("{")
        or line.startswith("[")
        or line.startswith('"')
        or line.startswith("'")
        or ":" in line
        or "=" in line
    ):
        # This is likely a plain text value, not JSON
        return line
    return None


def _try_python_literal_parsing(line: str) -> Optional[Any]:
    """
    Try to parse the line as Python literal using ast.literal_eval.

    Args:
        line: The line to parse

    Returns:
        Parsed Python literal or None if parsing fails
    """
    try:
        if line.startswith("{") or line.startswith("["):
            import ast

            python_dict_str = (
                line.replace("null", "None")
                .replace("true", "True")
                .replace("false", "False")
            )
            return ast.literal_eval(python_dict_str)
    except Exception:
        pass
    return None
