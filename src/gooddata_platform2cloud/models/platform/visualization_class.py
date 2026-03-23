# (C) 2026 GoodData Corporation
from pydantic import Field

from gooddata_platform2cloud.models.base import Base
from gooddata_platform2cloud.models.platform.meta import Meta


class VisualizationClass(Base):
    content: dict = Field(description="Object with visualization class content")
    meta: Meta


class VisualizationClassWrapper(Base):
    visualization_class: VisualizationClass
