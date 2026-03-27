# (C) 2026 GoodData Corporation
from typing import Any

from pydantic import Field, field_validator

from gooddata_platform2cloud.models.base import Base


class Uris(Base):
    uris: list[str] | None = Field(default=None)

    @field_validator("uris", mode="before")
    @classmethod
    def _drop_none_uris(cls, value: Any) -> list[str] | None:
        """Normalize Cloud responses that contain null entries in the uris list."""
        if value is None:
            return None
        if not isinstance(value, list):
            return value

        # Cloud can occasionally return `[null]` (or mix nulls into the list),
        # which fails validation for `list[str]`. Treat "only nulls" as missing.
        filtered = [item for item in value if item is not None]
        return filtered or None
