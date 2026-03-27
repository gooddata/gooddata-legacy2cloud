# (C) 2026 GoodData Corporation
"""Models for Platform visualization objects."""

from pydantic import Field

from gooddata_platform2cloud.models.base import Base
from gooddata_platform2cloud.models.platform.meta import Meta


class Links(Base):
    """Links associated with a visualization object."""

    explain: str


class MeasureDefinitionItem(Base):
    """Item reference in a measure definition."""

    uri: str


class MeasureDefinition(Base):
    """Definition of a measure."""

    aggregation: str | None = None
    item: MeasureDefinitionItem


class Measure(Base):
    """Measure configuration."""

    format: str | None = None
    title: str | None = None
    local_identifier: str
    definition: (
        dict  # Measure definitions can be pretty complex and are not currently needed
    )


class DisplayForm(Base):
    """Display form configuration."""

    uri: str


class VisualizationAttribute(Base):
    """Visualization attribute configuration."""

    localIdentifier: str
    display_form: DisplayForm


class BucketItem(Base):
    """Item in a bucket, can be either a measure or visualization attribute."""

    measure: Measure | None = None
    visualization_attribute: VisualizationAttribute | None = None


class Bucket(Base):
    """Bucket configuration."""

    local_identifier: str
    items: list[BucketItem]


class VisualizationClass(Base):
    """Visualization class reference."""

    uri: str


class AttributeFilter(Base):
    """Base class for attribute filters."""

    display_form: DisplayForm


class PositiveAttributeFilter(AttributeFilter):
    """Positive attribute filter configuration."""

    in_: list[str] = Field(alias="in")


class Filter(Base):
    """Filter configuration."""

    positive_attribute_filter: PositiveAttributeFilter | None = None


class Content(Base):
    """Content of a visualization object."""

    buckets: list[Bucket]
    visualization_class: VisualizationClass
    properties: str | None = None
    filters: list[Filter] | None = None


class VisualizationObject(Base):
    """Platform visualization object model."""

    meta: Meta
    links: Links
    content: Content


class VisualizationObjectWrapper(Base):
    """Wrapper for Platform visualization object to match JSON structure."""

    visualization_object: VisualizationObject
