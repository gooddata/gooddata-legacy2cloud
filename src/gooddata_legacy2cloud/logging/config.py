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

        # Inject object context (for parallel processing logs) between the
        # level and the message; trailing space lives here so the line has no
        # double-space when no context is set.
        context = get_object_context()
        record.context_str = f"{context} " if context else ""

        return super().format(record)


def configure_logger(name: str = "migration") -> logging.Logger:
    """Configure and return a logger with colored level names."""
    log = logging.getLogger(name)
    log.setLevel(logging.INFO)

    if not log.handlers:
        handler = logging.StreamHandler()
        formatter = ColoredLevelFormatter("%(levelname)s %(context_str)s%(message)s")
        handler.setFormatter(formatter)
        log.addHandler(handler)

    return log
