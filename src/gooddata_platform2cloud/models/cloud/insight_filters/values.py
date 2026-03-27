# (C) 2025 GoodData Corporation
"""Value wrapper models for Cloud Insight filters."""

from pydantic import Field

from gooddata_platform2cloud.models.base import Base


class Values(Base):
    """Wrapper for a list of filter values."""

    values: list[str]


class InValues(Base):
    """Wrapper for IN clause: {"in": {"values": [...]}}."""

    in_: Values = Field(alias="in")


class NotInValues(Base):
    """Wrapper for NOT IN clause: {"notIn": {"values": [...]}}."""

    not_in: Values
