# (C) 2025 GoodData Corporation
"""Value wrapper models for Cloud Insight filters."""

from typing import Any

from pydantic import Field, field_validator

from gooddata_platform2cloud.models.base import Base


class Values(Base):
    """Wrapper for a list of filter values."""

    values: list[str] | None = Field(default=None)

    @field_validator("values", mode="before")
    @classmethod
    def _drop_none_values(cls, value: Any) -> list[str] | None:
        """Normalize Cloud responses that contain null entries in the values list."""
        if value is None:
            return None
        if not isinstance(value, list):
            return value

        filtered = [item for item in value if item is not None]
        return filtered or None


class InValues(Base):
    """Wrapper for IN clause: {"in": {"values": [...]}}."""

    in_: Values = Field(alias="in")


class NotInValues(Base):
    """Wrapper for NOT IN clause: {"notIn": {"values": [...]}}."""

    not_in: Values
