# (C) 2025 GoodData Corporation
"""Attribute filter models for Cloud Insight filters."""

from typing import TYPE_CHECKING

from pydantic import Field

from gooddata_platform2cloud.models.base import Base
from gooddata_platform2cloud.models.cloud.identifier import IdentifierWrapper
from gooddata_platform2cloud.models.cloud.insight_filters.values import Values


class PositiveAttributeFilterContent(Base):
    """Inner structure of a positive attribute filter (IN)."""

    local_identifier: str | None = Field(default=None)
    display_form: IdentifierWrapper
    if TYPE_CHECKING:
        in_: Values = Field(alias="in_")
    else:
        in_: Values = Field(alias="in")


class PositiveAttributeFilter(Base):
    """Top-level wrapper producing {"positiveAttributeFilter": {...}}."""

    positive_attribute_filter: PositiveAttributeFilterContent


class NegativeAttributeFilterContent(Base):
    """Inner structure of a negative attribute filter (NOT IN)."""

    local_identifier: str | None = Field(default=None)
    display_form: IdentifierWrapper
    not_in: Values


class NegativeAttributeFilter(Base):
    """Top-level wrapper producing {"negativeAttributeFilter": {...}}."""

    negative_attribute_filter: NegativeAttributeFilterContent
