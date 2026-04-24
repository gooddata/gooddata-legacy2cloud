# (C) 2026 GoodData Corporation
from pydantic import Field

from gooddata_legacy2cloud.models.base import Base
from gooddata_legacy2cloud.models.legacy.meta import Meta


class VisualizationClass(Base):
    content: dict = Field(description="Object with visualization class content")
    meta: Meta


class VisualizationClassWrapper(Base):
    visualization_class: VisualizationClass
