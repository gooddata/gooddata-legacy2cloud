# (C) 2025 GoodData Corporation
"""Measure value filter models for Cloud Insight filters."""

from pydantic import Field

from gooddata_legacy2cloud.models.base import Base
from gooddata_legacy2cloud.models.cloud.identifier import IdentifierWrapper


class Comparison(Base):
    """Comparison condition with operator and value."""

    operator: str
    value: float


class ComparisonWrapper(Base):
    """Wrapper for comparison condition."""

    comparison: Comparison


class MeasureFilterModel(Base):
    """Inner structure of a measure value filter."""

    measure: IdentifierWrapper
    condition: ComparisonWrapper
    dimensionality: list[IdentifierWrapper] | None = Field(default=None)


class MeasureValueFilterWrapper(Base):
    """Top-level wrapper producing {"measureValueFilter": {...}}."""

    measure_value_filter: MeasureFilterModel
