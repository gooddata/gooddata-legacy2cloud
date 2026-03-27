# (C) 2026 GoodData Corporation
"""Models for Platform analytical dashboards."""

from pydantic import Field

from gooddata_platform2cloud.models.base import Base
from gooddata_platform2cloud.models.platform.meta import Meta


class Content(Base):
    """Content of an analytical dashboard."""

    filter_context: str | None = None
    widgets: list = Field(default_factory=list)


class AnalyticalDashboard(Base):
    """Platform analytical dashboard model."""

    content: Content
    meta: Meta


class AnalyticalDashboardWrapper(Base):
    """Wrapper for Platform analytical dashboard to match JSON structure."""

    analytical_dashboard: AnalyticalDashboard
