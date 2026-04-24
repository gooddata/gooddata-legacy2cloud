# (C) 2025 GoodData Corporation
"""Date filter models for Cloud Insight filters."""

from typing import TYPE_CHECKING

from pydantic import Field

from gooddata_legacy2cloud.models.base import Base
from gooddata_legacy2cloud.models.cloud.identifier import IdentifierWrapper


class RelativeDateFilterContent(Base):
    """Inner structure of a relative date filter."""

    data_set: IdentifierWrapper
    granularity: str
    to: int | None = Field(default=None)

    if TYPE_CHECKING:
        from_: int | None = Field(alias="from_", default=None)
    else:
        from_: int | None = Field(alias="from", default=None)


class RelativeDateFilter(Base):
    """Top-level wrapper producing {"relativeDateFilter": {...}}."""

    relative_date_filter: RelativeDateFilterContent


class AbsoluteDateFilterContent(Base):
    """Inner structure of an absolute date filter."""

    data_set: IdentifierWrapper
    from_: str = Field(alias="from")
    to: str


class AbsoluteDateFilter(Base):
    """Top-level wrapper producing {"absoluteDateFilter": {...}}."""

    absolute_date_filter: AbsoluteDateFilterContent
