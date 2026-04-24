# (C) 2026 GoodData Corporation
from gooddata_legacy2cloud.models.base import Base
from gooddata_legacy2cloud.models.legacy.meta import Meta


class VisualizationWidgetContent(Base):
    date_data_set: str | None = None
    ignore_dashboard_filters: list
    visualization: str  # visualization uri


class VisualizationWidget(Base):
    meta: Meta
    content: VisualizationWidgetContent


class VisualizationWidgetWrapper(Base):
    visualization_widget: VisualizationWidget
