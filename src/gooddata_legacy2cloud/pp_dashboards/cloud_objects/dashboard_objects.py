# (C) 2026 GoodData Corporation
import uuid
from typing import Any

from pydantic import BaseModel, Field

from gooddata_legacy2cloud.helpers import PP_DASHBOARD_PREFIX, parse_legacy_tags
from gooddata_legacy2cloud.pp_dashboards.legacy_objects.pixel_perfect_dashboard import (
    PixelPerfectDashboard,
    Tab,
)
from gooddata_legacy2cloud.pp_dashboards.utils import get_migration_id


class Identifier(BaseModel):
    id: str
    type: str = "visualizationObject"


class RichTextWidget(BaseModel):
    content: str
    localIdentifier: str = uuid.uuid4().hex
    type: str = "richText"


class Insight(BaseModel):
    identifier: Identifier


class AttributeFilterReferenceDisplayForm(BaseModel):
    identifier: Identifier


class AttributeFilterReference(BaseModel):
    type: str = "attributeFilterReference"
    displayForm: AttributeFilterReferenceDisplayForm


class Configuration(BaseModel):
    hideTitle: bool = False


class DateDataSet(BaseModel):
    identifier: Identifier


class InsightWidget(BaseModel):
    insight: Insight
    title: str | None = ""
    properties: dict | None = Field(default_factory=dict)
    localIdentifier: str = Field(default_factory=lambda: uuid.uuid4().hex)
    dateDataSet: DateDataSet | dict | None = Field(default_factory=dict)
    configuration: Configuration | None = Field(default_factory=dict)
    ignoreDashboardFilters: list[AttributeFilterReference] = Field(default_factory=list)
    drills: list = Field(default_factory=list)
    description: str | None = ""
    type: str = "insight"


class SizeDefinition(BaseModel):
    gridHeight: int
    gridWidth: int


class XLSize(BaseModel):
    xl: SizeDefinition


class LayoutItem(BaseModel):
    size: XLSize
    widget: InsightWidget | RichTextWidget
    type: str = "IDashboardLayoutItem"


class LayoutSection(BaseModel):
    header: dict = Field(default_factory=dict)
    items: list[LayoutItem] = Field(default_factory=list)
    type: str = "IDashboardLayoutSection"


class Layout(BaseModel):
    sections: list[LayoutSection] = Field(default_factory=list)
    type: str = "IDashboardLayout"


class DisplayAsLabel(BaseModel):
    identifier: Identifier


class AttributeFilterConfig(BaseModel):
    displayAsLabel: DisplayAsLabel
    localIdentifier: str = Field(default_factory=lambda: uuid.uuid4().hex)
    mode: str | None = "active"


class FilterContextRef(BaseModel):
    identifier: Identifier


class DashboardTab(BaseModel):
    """A single dashboard tab embedded in dashboard content."""

    localIdentifier: str
    title: str
    filterContextRef: FilterContextRef | None = None
    layout: Layout
    dateFilterConfig: dict[str, Any] | None = None
    dateFilterConfigs: list[dict[str, Any]] | None = None
    attributeFilterConfigs: list[AttributeFilterConfig] | None = None


class Content(BaseModel):
    layout: Layout
    attributeFilterConfigs: list[AttributeFilterConfig] = Field(default_factory=list)
    filterContextRef: FilterContextRef | None = None
    tabs: list[DashboardTab] = Field(default_factory=list)
    version: str = "2"


class Attributes(BaseModel):
    title: str
    content: Content
    description: str | None = ""
    tags: list[str] = Field(default_factory=list)


class CloudDashboard(BaseModel):
    attributes: Attributes
    id: str = uuid.uuid4().hex
    type: str = "analyticalDashboard"
    _public: bool | None = True

    def add_section(self, section: LayoutSection) -> int:
        self.get_sections().append(section)
        return len(self.get_sections()) - 1

    def add_item_to_section(self, layoutItem: LayoutItem, section_idx=0):
        self.get_sections()[section_idx].items.append(layoutItem)

    def get_sections(self) -> list:
        return self.attributes.content.layout.sections

    def add_attribute_filter_config(self, obj: AttributeFilterConfig):
        self.attributes.content.attributeFilterConfigs.append(obj)

    def add_filter_context_ref(self, obj: FilterContextRef):
        self.attributes.content.filterContextRef = obj

    def add_tab(self, tab: DashboardTab) -> None:
        """Append a dashboard tab to the dashboard content."""
        self.attributes.content.tabs.append(tab)

    @classmethod
    def create_from_legacy_definition(
        cls, pixel_perfect_dashboard: PixelPerfectDashboard, tab: Tab, tab_idx: int
    ):
        return cls(
            id=get_migration_id(
                prefix=PP_DASHBOARD_PREFIX,
                legacy_identifier=f"{pixel_perfect_dashboard.meta.identifier}_{tab.identifier}",
                legacy_title=f"{pixel_perfect_dashboard.meta.title}",
            ),
            attributes=Attributes(
                title=f"[PP] {pixel_perfect_dashboard.meta.title} - {tab_idx:02} - {tab.title}",
                content=Content(layout=Layout(sections=[])),
                tags=parse_legacy_tags(pixel_perfect_dashboard.meta.model_dump()),
            ),
        )

    @classmethod
    def create_tabbed_from_legacy_definition(
        cls,
        pixel_perfect_dashboard: PixelPerfectDashboard,
        keep_original_ids: bool = False,
    ):
        """Create a single tabbed Cloud dashboard from a Legacy PP dashboard."""
        if keep_original_ids:
            dashboard_id = pixel_perfect_dashboard.meta.identifier
        else:
            dashboard_id = get_migration_id(
                prefix=PP_DASHBOARD_PREFIX,
                legacy_identifier=pixel_perfect_dashboard.meta.identifier,
                legacy_title=f"{pixel_perfect_dashboard.meta.title}",
            )
        return cls(
            id=dashboard_id,
            attributes=Attributes(
                title=f"[PP] {pixel_perfect_dashboard.meta.title}",
                content=Content(layout=Layout(sections=[])),
                tags=parse_legacy_tags(pixel_perfect_dashboard.meta.model_dump()),
            ),
        )
