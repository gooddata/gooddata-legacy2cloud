# (C) 2026 GoodData Corporation
from gooddata_legacy2cloud.models.base import Base


class Origin(Base):
    """Origin metadata for the dashboard."""

    origin_type: str
    origin_id: str


class Meta(Base):
    """Metadata for the dashboard."""

    origin: Origin
