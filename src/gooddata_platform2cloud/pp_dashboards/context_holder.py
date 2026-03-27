# (C) 2026 GoodData Corporation
"""
DEPRECATED: Global singleton context pattern.

This module is deprecated and should not be used in new code.
Context should be passed explicitly through function calls and class constructors.
This file is kept for backward compatibility only.
"""

from __future__ import annotations

from threading import RLock
from typing import Optional

from gooddata_platform2cloud.pp_dashboards.data_classes import PPDashboardContext

_ctx: Optional[PPDashboardContext] = None
_lock = RLock()


def init_context(ctx: PPDashboardContext) -> None:
    """Initialize the singleton context once."""
    global _ctx
    with _lock:
        if _ctx is not None:
            return  # already initialized; ignore or raise if you prefer
        _ctx = ctx


def get_context() -> PPDashboardContext:
    with _lock:
        if _ctx is None:
            raise RuntimeError(
                "Global Context not initialized; call init_context() first."
            )
        return _ctx


def has_context() -> bool:
    with _lock:
        return _ctx is not None


# Handy for unit tests
def reset_context_for_tests() -> None:
    global _ctx
    with _lock:
        _ctx = None
