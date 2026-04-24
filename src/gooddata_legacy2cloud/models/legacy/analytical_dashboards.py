# (C) 2026 GoodData Corporation
"""Models for Legacy analytical dashboards."""

from pydantic import Field

from gooddata_legacy2cloud.models.base import Base
from gooddata_legacy2cloud.models.legacy.meta import Meta


class Content(Base):
    """Content of an analytical dashboard."""

    filter_context: str | None = None
    widgets: list = Field(default_factory=list)


class AnalyticalDashboard(Base):
    """Legacy analytical dashboard model."""

    content: Content
    meta: Meta


class AnalyticalDashboardWrapper(Base):
    """Wrapper for Legacy analytical dashboard to match JSON structure."""

    analytical_dashboard: AnalyticalDashboard
