# (C) 2025 GoodData Corporation
"""Pydantic models for Platform metric objects.

This module provides models for validating and working with Platform metric
API responses.
"""

from typing import Any

from gooddata_platform2cloud.models.base import Base
from gooddata_platform2cloud.models.platform.meta import Meta


class MetricLinks(Base):
    """Links associated with a Platform metric object."""

    explain2: str


class MetricContent(Base):
    """Content of a Platform metric object including MAQL expression and tree."""

    expression: str
    format: str | None = None
    # TODO: Add typed TreeNode model when needed for tree traversal/validation
    tree: dict[str, Any]
    folders: list[str] | None = None


class Metric(Base):
    """A Platform metric object."""

    meta: Meta
    content: MetricContent
    links: MetricLinks


class MetricWrapper(Base):
    """Wrapper for a Platform metric matching the JSON structure."""

    metric: Metric
