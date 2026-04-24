# (C) 2026 GoodData Corporation
import hashlib
import logging
import uuid
from typing import Any, ClassVar, Type, TypeVar, cast

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from gooddata_legacy2cloud.pp_dashboards.legacy_objects.filter_objects import (
    FilterItemContent,
)
from gooddata_legacy2cloud.pp_dashboards.utils import Meta

logger = logging.getLogger("migration")

TItem = TypeVar("TItem", bound="Item")


class WrapperUnwrapMixin:
    """
    Generic mixin to unwrap values wrapped under a specific key before validation.
    Child classes should set `wrapper_key` to the dict key that contains the payload.
    """

    wrapper_key: ClassVar[str] = ""

    @model_validator(mode="before")
    @classmethod
    def unwrap(cls, v):
        if isinstance(v, dict) and cls.wrapper_key and cls.wrapper_key in v:
            return v[cls.wrapper_key]
        # Keep input as-is so union/regular validation can handle it
        return v


class Metric(WrapperUnwrapMixin, BaseModel):
    wrapper_key: ClassVar[str] = "metric"
    meta: Meta


class Report(WrapperUnwrapMixin, BaseModel):
    wrapper_key: ClassVar[str] = "report"
    meta: Meta


class Item(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    migration_id: str | None = Field(
        default_factory=lambda: uuid.uuid4().hex
    )  # Temporal id used during migration
    positionX: int
    positionY: int
    sizeX: int
    sizeY: int
    ctx: Any | None = Field(default=None, exclude=True, repr=False)


class IframeItem(WrapperUnwrapMixin, Item):
    wrapper_key: ClassVar[str] = "iframeItem"
    url: str
    type: str = "iframeItem"


class FilterItem(WrapperUnwrapMixin, Item):
    wrapper_key: ClassVar[str] = "filterItem"
    contentId: str | FilterItemContent
    id: str
    type: str = "filterItem"


class ReportItem(WrapperUnwrapMixin, Item):
    wrapper_key: ClassVar[str] = "reportItem"
    obj: str | Report
    filters: list[str] | list[FilterItem] = Field(default_factory=lambda: [])
    title: str | None = ""
    type: str = "reportItem"

    @property
    def visualisation_id(self):
        # Map id to visualization created in Cloud
        if self.ctx and not isinstance(self.obj, str) and self.obj.meta.identifier:
            return self.ctx.report_mappings.id_mapping[self.obj.meta.identifier]
        else:
            return ""

    def initialize_from_legacy(self):
        """
        Fetch and initialize report object from Legacy API.
        This must be called explicitly after setting context.
        """
        if self.ctx and isinstance(self.obj, str):
            try:
                obj = self.ctx.legacy_client.get_object(obj_link=self.obj)
                self.obj = Report.model_validate(obj)
            except ValidationError:
                logger.error("Error validating report item - skipping")


class HeadlineItem(WrapperUnwrapMixin, Item):
    wrapper_key: ClassVar[str] = "headlineItem"
    title: str
    filters: list[str] | list[FilterItem] = Field(default_factory=lambda: [])
    metric: str | Metric
    type: str = "headlineItem"

    @property
    def cloud_metric_id(self):
        """
        Return corresponding metric id from Cloud
        """
        if self.ctx and not isinstance(self.metric, str):
            return self.ctx.metric_mappings.id_mapping[self.metric.meta.identifier]
        return ""

    def initialize_from_legacy(self):
        """
        Fetch and initialize metric object from Legacy API.
        This must be called explicitly after setting context.
        """
        if self.ctx and isinstance(self.metric, str):
            try:
                obj = self.ctx.legacy_client.get_object(obj_link=self.metric)
                self.metric = Metric.model_validate(obj)
            except ValidationError:
                logger.error("Error validating headline item - skipping")


class TextItem(WrapperUnwrapMixin, Item):
    wrapper_key: ClassVar[str] = "textItem"
    text: str
    textSize: str | None = None
    style: dict[str, Any] = Field(default_factory=dict)
    type: str = "textItem"

    def generate_local_identifier(self, tab_identifier: str, item_order: int) -> str:
        """
        Generate a consistent local identifier based on tab id, text content, and item order.
        This ensures the same value is generated each time the migration is run.
        """
        # Create a hash-based identifier that's deterministic
        content_hash = hashlib.md5(
            f"{tab_identifier}_{self.text}_{item_order}".encode()
        ).hexdigest()
        return f"text_{content_hash[:8]}"


class Tab(BaseModel):
    identifier: str
    title: str
    items: list[ReportItem | HeadlineItem | FilterItem | IframeItem | TextItem] = Field(
        default_factory=lambda: []
    )

    @model_validator(mode="before")
    @classmethod
    def filter_supported_items(cls, v):
        if "items" in v:
            v["items"] = [
                item
                for item in v["items"]
                if any(
                    k in item
                    for k in (
                        "reportItem",
                        "headlineItem",
                        "filterItem",
                        "iframeItem",
                        "textItem",
                    )
                )
            ]
        return v

    def get_items_by_type(self, item_type: Type[TItem]) -> list[TItem]:
        """Return tab items filtered by type (typed)."""
        return [i for i in cast(list[Item], self.items) if isinstance(i, item_type)]


class Content(BaseModel):
    filters: list[FilterItemContent] = Field(default_factory=list)
    tabs: list[Tab] = Field(default_factory=list)


class PixelPerfectDashboard(WrapperUnwrapMixin, BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    wrapper_key: ClassVar[str] = "projectDashboard"
    content: Content
    meta: Meta
    ctx: Any | None = Field(default=None, exclude=True, repr=False)

    def initialize_nested_objects(self):
        """
        Initialize all nested objects by propagating context and fetching data from Legacy.
        This must be called explicitly after setting context on this dashboard.
        """
        if not self.ctx:
            return

        # Propagate context and initialize filter contents
        for filter_content in self.content.filters:
            if hasattr(filter_content, "ctx"):
                filter_content.ctx = self.ctx
                if hasattr(filter_content, "initialize_from_legacy"):
                    filter_content.initialize_from_legacy()

        # Propagate context and initialize tab items
        for tab in self.content.tabs:
            for item in tab.items:
                if hasattr(item, "ctx"):
                    item.ctx = self.ctx
                    if hasattr(item, "initialize_from_legacy"):
                        item.initialize_from_legacy()  # type: ignore

        # Keep even unsupported filter contents (e.g., prompts) so the builder can decide
        # whether to skip the affected tab/dashboard instead of silently dropping filters.

        # Map filter item contents to tabs
        for tab in self.content.tabs:
            supported_items: list[
                ReportItem | HeadlineItem | FilterItem | IframeItem | TextItem
            ] = []
            # Pass 1: resolve FilterItem.contentId so later widget filter refs can rely on it
            for item in tab.items:
                if not isinstance(item, FilterItem):
                    continue
                if not isinstance(item.contentId, str):
                    continue
                try:
                    item.contentId = next(
                        fic for fic in self.content.filters if fic.id == item.contentId
                    )
                    supported_items.append(item)
                except StopIteration:
                    logger.warning(
                        "Unsupported filter item content: %s", item.contentId
                    )

            # Pass 2: add non-filter items and resolve their filter references
            for item in tab.items:
                if isinstance(item, FilterItem):
                    # already handled in pass 1 (or dropped)
                    continue

                if isinstance(item, (ReportItem, HeadlineItem, IframeItem, TextItem)):
                    supported_items.append(item)

                if (
                    hasattr(item, "filters")
                    and item.filters
                    and isinstance(item.filters[0], str)  # type: ignore
                ):
                    resolved_filters = []
                    for filter_item in item.filters:  # type: ignore
                        try:
                            resolved = next(
                                fic
                                for fic in tab.get_items_by_type(FilterItem)
                                if fic.id == filter_item
                            )
                            # Only keep filters whose contentId has been resolved to FilterItemContent
                            if isinstance(resolved.contentId, str):
                                logger.warning(
                                    "Unsupported filter item content: %s",
                                    resolved.contentId,
                                )
                                continue
                            resolved_filters.append(resolved)
                        except StopIteration:
                            logger.warning(
                                "Could not resolve filter %s for item", filter_item
                            )
                    item.filters = resolved_filters  # type: ignore

            tab.items = supported_items
