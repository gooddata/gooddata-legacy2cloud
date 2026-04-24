# (C) 2026 GoodData Corporation
"""
Module for logging configuration and formatting.
"""

import logging

from gooddata_legacy2cloud.constants import COLOR_RED, COLOR_RESET, COLOR_YELLOW
from gooddata_legacy2cloud.logging.context import get_object_context


class ColoredLevelFormatter(logging.Formatter):
    """Formatter that adds color to level names only."""

    COLORS = {
        logging.WARNING: COLOR_YELLOW,
        logging.ERROR: COLOR_RED,
        logging.CRITICAL: COLOR_RED,
    }

    def format(self, record: logging.LogRecord) -> str:
        """Format log record with colored level name and object context."""
        color = self.COLORS.get(record.levelno, "")
        if color:
            record.levelname = f"{color}{record.levelname}{COLOR_RESET}"

        # Format the message first, then append context
        formatted = super().format(record)

        # Add object context if available (for parallel processing logs)
        context = get_object_context()
        if context:
            formatted = f"{formatted} {context}"

        return formatted


def configure_logger(name: str = "migration") -> logging.Logger:
    """Configure and return a logger with colored level names."""
    log = logging.getLogger(name)
    log.setLevel(logging.INFO)

    if not log.handlers:
        handler = logging.StreamHandler()
        formatter = ColoredLevelFormatter("%(levelname)s %(message)s")
        handler.setFormatter(formatter)
        log.addHandler(handler)

    return log
