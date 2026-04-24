# (C) 2026 GoodData Corporation
"""Models for Cloud analytical dashboard response.

This model is currently used only in scheduled exports migration and does not
cover all the use cases of a real Cloud dashboard. It currently lacks drills
and other features. It can be expanded if needed.
"""

from typing import Any, Literal

from pydantic import Field

from gooddata_legacy2cloud.models.base import Base
from gooddata_legacy2cloud.models.cloud.identifier import IdentifierWrapper
from gooddata_legacy2cloud.models.cloud.meta import Meta


class GridSize(Base):
    """Grid size configuration for dashboard items."""

    grid_height: int
    grid_width: int


class ItemSize(Base):
    """Size configuration for dashboard layout items."""

    xl: GridSize | None = None
    lg: GridSize | None = None
    md: GridSize | None = None
    sm: GridSize | None = None
    xs: GridSize | None = None


class Widget(Base):
    """Dashboard widget configuration."""

    date_data_set: IdentifierWrapper | None = None
    properties: dict[str, Any] = Field(default_factory=dict)
    title: str | None = Field(default=None)
    drills: list[Any] = Field(default_factory=list)
    insight: IdentifierWrapper | None = None
    ignore_dashboard_filters: list[Any] = Field(default_factory=list)
    type: str
    description: str = ""
    local_identifier: str


class DashboardLayoutItem(Base):
    """Individual item within a dashboard layout section."""

    type: Literal["IDashboardLayoutItem"]
    size: ItemSize
    widget: Widget


class SectionHeader(Base):
    """Header configuration for a dashboard section."""

    title: str | None = None


class DashboardLayoutSection(Base):
    """Section within a dashboard layout."""

    type: Literal["IDashboardLayoutSection"]
    header: SectionHeader | None = None
    items: list[DashboardLayoutItem] = Field(default_factory=list)


class DashboardLayout(Base):
    """Layout configuration for a dashboard."""

    type: Literal["IDashboardLayout"]
    sections: list[DashboardLayoutSection] = Field(default_factory=list)


class DashboardContent(Base):
    """Content of an analytical dashboard."""

    filter_context_ref: IdentifierWrapper | None = Field(default=None)
    layout: DashboardLayout
    version: str | None = Field(default=None)


class DashboardAttributes(Base):
    """Attributes of an analytical dashboard."""

    title: str | None = Field(default=None)
    description: str = Field(default="")
    content: DashboardContent
    created_at: str | None = Field(default=None)
    modified_at: str | None = Field(default=None)


class CloudDashboard(Base):
    """Cloud analytical dashboard data."""

    id: str
    type: Literal["analyticalDashboard"]
    attributes: DashboardAttributes
    meta: Meta


class DashboardLinks(Base):
    """Links related to the dashboard."""

    self: str
    next: str | None = Field(default=None)


class CloudDashboardResponse(Base):
    """Response wrapper for Cloud analytical dashboard."""

    data: CloudDashboard
    links: DashboardLinks
