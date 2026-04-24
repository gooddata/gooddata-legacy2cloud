# (C) 2025 GoodData Corporation
"""Pydantic models for Legacy metric objects.

This module provides models for validating and working with Legacy metric
API responses.
"""

from typing import Any

from gooddata_legacy2cloud.models.base import Base
from gooddata_legacy2cloud.models.legacy.meta import Meta


class MetricLinks(Base):
    """Links associated with a Legacy metric object."""

    explain2: str


class MetricContent(Base):
    """Content of a Legacy metric object including MAQL expression and tree."""

    expression: str
    format: str | None = None
    # TODO: Add typed TreeNode model when needed for tree traversal/validation
    tree: dict[str, Any]
    folders: list[str] | None = None


class Metric(Base):
    """A Legacy metric object."""

    meta: Meta
    content: MetricContent
    links: MetricLinks


class MetricWrapper(Base):
    """Wrapper for a Legacy metric matching the JSON structure."""

    metric: Metric
