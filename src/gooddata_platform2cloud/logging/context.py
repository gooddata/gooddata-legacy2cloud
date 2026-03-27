# (C) 2025 GoodData Corporation
"""Thread-safe logging context for parallel processing.

This module provides context variables to store object identity (id and title)
during parallel processing. The context is automatically included in log messages
by the ColoredLevelFormatter in shared.py.
"""

import json
from contextvars import ContextVar, Token
from typing import Self

# Context variable to store current object being processed
_object_context: ContextVar[str | None] = ContextVar("object_context", default=None)


class ObjectContext:
    def __init__(self, object_id: str, object_title: str) -> None:
        self.object_id: str = object_id
        self.object_title: str = object_title
        self.token: Token[str | None] | None = None

    def __enter__(self) -> Self:
        context = json.dumps({"title": self.object_title, "id": self.object_id})
        self.token = _object_context.set(context)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object | None,
    ) -> None:
        if self.token:
            _object_context.reset(self.token)


def get_object_context() -> str | None:
    """Get the current object context."""
    return _object_context.get()
