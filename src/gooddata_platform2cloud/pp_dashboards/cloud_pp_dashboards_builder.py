# (C) 2026 GoodData Corporation
"""Builder for transforming Platform pixel perfect dashboards to Cloud dashboards."""

import concurrent.futures
import json
import logging
import os
import re
import uuid
from typing import Any, cast
from urllib.parse import parse_qs, urlparse

import gooddata_platform2cloud.pp_dashboards.cloud_objects.dashboard_objects as pdo
import gooddata_platform2cloud.pp_dashboards.cloud_objects.filter_objects as ppf
import gooddata_platform2cloud.pp_dashboards.cloud_objects.visualisation_objects as pvo
import gooddata_platform2cloud.pp_dashboards.utils as pp_utils
from gooddata_platform2cloud.helpers import PP_FILTER_CONTEXT_PREFIX, PP_INSIGHT_PREFIX
from gooddata_platform2cloud.pp_dashboards.data_classes import PPDashboardContext
from gooddata_platform2cloud.pp_dashboards.grid_maker import (
    GridConfig,
    migrate_to_sections,
)
from gooddata_platform2cloud.pp_dashboards.helpers import (
    extract_values_by_key,
    prefetch_platform_objects,
)
from gooddata_platform2cloud.pp_dashboards.platform_objects.pixel_perfect_dashboard import (
    FilterItem,
    PixelPerfectDashboard,
)
from gooddata_platform2cloud.reports.filters.date_helpers import (
    get_date_dataset_and_granularity,
)

logger = logging.getLogger("migration")


class CloudPixelPerfectDashboardsBuilder:
    """Builds Cloud dashboards from Platform pixel perfect dashboards.

    This builder processes Platform pixel perfect dashboards and transforms them into
    Cloud responsive dashboards. By default, one Platform dashboard becomes one
    Cloud dashboard with native tabs. Legacy behavior (splitting each tab into
    a separate dashboard) is available behind a flag.
    """

    def __init__(
        self,
        ctx: PPDashboardContext,
        cfg: GridConfig,
        pixel_perfect_prefix: str = "[PP]",
        min_text_length: int = 5,
        supported_items: list[str] | None = None,
        legacy_split_tabs: bool = False,
    ):
        """Initialize the builder with context and grid configuration.

        Args:
            ctx: Migration context containing APIs and mappings
            cfg: Grid configuration for layout calculations
            pixel_perfect_prefix: Prefix for migrated dashboard names
            min_text_length: Minimum text length for textItems to be migrated
            supported_items: List of supported item types (defaults to standard set)
        """
        self.ctx = ctx
        self.cfg = cfg
        self.pixel_perfect_prefix = pixel_perfect_prefix
        self.min_text_length = min_text_length
        self.supported_items = supported_items or [
            "headlineItem",
            "reportItem",
            "textItem",
        ]
        self.legacy_split_tabs = legacy_split_tabs
        self.cloud_dashboards: list[pdo.CloudDashboard] = []
        self.public_dashboard_ids: list[str] = []
        self.cloud_existing_dashboard_ids: list[str] = []

    def process_platform_dashboards(
        self,
        platform_dashboards: list[dict],
        skip_deploy: bool = False,
        overwrite_existing: bool = False,
    ) -> None:
        """Process all Platform pixel perfect dashboards.

        Args:
            platform_dashboards: List of Platform dashboard definitions
            skip_deploy: If True, don't deploy to Cloud
            overwrite_existing: If True, overwrite existing dashboards
        """
        # Get existing dashboard IDs if we need to check for overwrites
        if not skip_deploy:
            self.cloud_existing_dashboard_ids = [
                item["id"] for item in self.ctx.cloud_client.get_dashboards()
            ]

        for dashboard_idx, platform_dashboard in enumerate(platform_dashboards):
            if not platform_dashboard["projectDashboard"]["meta"]["title"]:
                continue

            dashboard_title = platform_dashboard["projectDashboard"]["meta"]["title"]
            logger.info("Processing %d: %s", dashboard_idx + 1, dashboard_title)

            self.process_single_dashboard(platform_dashboard, overwrite_existing)

    def process_single_dashboard(
        self, platform_dashboard_raw: dict, overwrite_existing: bool
    ) -> None:
        """Process a single Platform dashboard and all its tabs.

        Args:
            platform_dashboard_raw: Raw Platform dashboard definition
            overwrite_existing: If True, overwrite existing dashboards
        """
        # Extract and prefetch all 'obj' values from the raw Platform dashboard definition
        obj_values = extract_values_by_key(platform_dashboard_raw, "obj")

        # Prefetch Platform objects concurrently to warm up any underlying caches
        prefetch_platform_objects(self.ctx, list(set(obj_values)))

        # Pre fetch Platform filter elements to extend cache
        obj_values = extract_values_by_key(self.ctx.platform_client.cache, "elements")
        prefetch_platform_objects(self.ctx, list(set(obj_values)))

        # Validate dashboard using pydantic model
        platform_dashboard = PixelPerfectDashboard.model_validate(
            platform_dashboard_raw
        )
        # Set context and initialize nested objects
        platform_dashboard.ctx = self.ctx
        platform_dashboard.initialize_nested_objects()

        if self.legacy_split_tabs:
            # Legacy: each tab becomes a separate dashboard (processed in parallel)
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=os.cpu_count()
            ) as executor:
                futures = [
                    executor.submit(
                        self.process_tab,
                        platform_dashboard,
                        platform_dashboard_raw,
                        tab_idx,
                        tab,
                        overwrite_existing,
                    )
                    for tab_idx, tab in enumerate(platform_dashboard.content.tabs)
                ]
                for future in concurrent.futures.as_completed(futures):
                    pass
            return

        # Default: one dashboard with tabs (deterministic, no shared mutation across threads)
        self._process_dashboard_as_tabbed(
            platform_dashboard, platform_dashboard_raw, overwrite_existing
        )

    def _process_dashboard_as_tabbed(
        self,
        platform_dashboard: PixelPerfectDashboard,
        platform_dashboard_raw: dict,
        overwrite_existing: bool,
    ) -> None:
        """Process a Platform PP dashboard into one Cloud dashboard with native tabs."""
        cloud_dashboard = pdo.CloudDashboard.create_tabbed_from_platform_definition(
            pixel_perfect_dashboard=platform_dashboard
        )

        # Write one-to-one mapping between Platform and Cloud dashboards
        if self.ctx.mapping_logger:
            self.ctx.mapping_logger.write_identifier_relation(
                platform_dashboard.meta.identifier,
                cloud_dashboard.id,
            )

        # Set dashboard public flag (same logic as regular migration)
        cloud_dashboard._public = not platform_dashboard.meta.unlisted or (
            platform_dashboard.meta.unlisted == 0
        )

        if self._should_skip_or_overwrite(
            cloud_dashboard, platform_dashboard.meta.title, overwrite_existing
        ):
            return

        migrated_tabs = 0
        for _tab_idx, tab in enumerate(platform_dashboard.content.tabs):
            unsupported_reasons = self._get_tab_unsupported_reasons(tab)
            if unsupported_reasons:
                logger.warning(
                    "Skipping tab '%s' (%s): %s",
                    tab.title,
                    tab.identifier,
                    "; ".join(unsupported_reasons),
                )
                continue

            tab_local_identifier = pp_utils.sanitize_string(tab.identifier)
            filter_context_id = pp_utils.sanitize_string(
                f"{PP_FILTER_CONTEXT_PREFIX}_{cloud_dashboard.id}_{tab_local_identifier}"
            )

            default_data_set, filter_context_ref, attribute_filter_configs = (
                self._build_filter_context_and_deploy(
                    tab=tab,
                    filter_context_id=filter_context_id,
                )
            )

            tab_layout = self._build_tab_layout(tab, default_data_set)
            dashboard_tab = pdo.DashboardTab(
                localIdentifier=tab_local_identifier,
                title=tab.title,
                filterContextRef=filter_context_ref,
                layout=tab_layout,
                attributeFilterConfigs=attribute_filter_configs or None,
                # dateFilterConfig(s) are optional; we keep them unset for now
                dateFilterConfig=None,
                dateFilterConfigs=None,
            )
            cloud_dashboard.add_tab(dashboard_tab)
            migrated_tabs += 1

        if migrated_tabs == 0:
            logger.warning(
                "Skipping dashboard '%s' - no valid tabs could be migrated",
                platform_dashboard.meta.title,
            )
            return

        self.cloud_dashboards.append(cloud_dashboard)
        if cloud_dashboard._public:
            self.public_dashboard_ids.append(cloud_dashboard.id)

        self._log_transformation_tabbed(
            platform_dashboard, platform_dashboard_raw, cloud_dashboard
        )

    def process_tab(
        self,
        platform_dashboard: PixelPerfectDashboard,
        platform_dashboard_raw: dict,
        tab_idx: int,
        tab: Any,
        overwrite_existing: bool,
    ) -> pdo.CloudDashboard | None:
        """Process a single tab from a dashboard.

        Args:
            platform_dashboard: Validated Platform dashboard object
            platform_dashboard_raw: Raw Platform dashboard definition
            tab_idx: Index of the tab
            tab: Tab object to process
            overwrite_existing: If True, overwrite existing dashboards

        Returns:
            The created Cloud dashboard or None if skipped
        """
        logger.info("Processing tab: %s", tab.title)

        unsupported_reasons = self._get_tab_unsupported_reasons(tab)
        if unsupported_reasons:
            logger.warning(
                "Skipping tab '%s' (%s): %s",
                tab.title,
                tab.identifier,
                "; ".join(unsupported_reasons),
            )
            return None

        # Build minimal dashboard, decide on overwrite/skip
        cloud_min_dashboard = self._create_min_dashboard(
            platform_dashboard, tab, tab_idx
        )
        if self._should_skip_or_overwrite(
            cloud_min_dashboard, tab.title, overwrite_existing
        ):
            return None

        # Filters & filter context
        default_data_set, filter_context_ref, _ = self._build_filter_context_and_deploy(
            tab=tab,
            filter_context_id=pp_utils.sanitize_string(
                f"{PP_FILTER_CONTEXT_PREFIX}_{cloud_min_dashboard.id}"
            ),
        )
        cloud_min_dashboard.add_filter_context_ref(filter_context_ref)

        # Sections & items
        cloud_min_dashboard.attributes.content.layout = self._build_tab_layout(
            tab, default_data_set
        )

        # Keep in-memory for later deployment
        self.cloud_dashboards.append(cloud_min_dashboard)

        # Track public dashboards
        if cloud_min_dashboard._public:
            self.public_dashboard_ids.append(cloud_min_dashboard.id)

        # Log transformation
        self._log_transformation(
            platform_dashboard,
            platform_dashboard_raw,
            tab_idx,
            tab,
            cloud_min_dashboard,
        )

        return cloud_min_dashboard

    def _get_tab_unsupported_reasons(self, tab: Any) -> list[str]:
        """Return reasons why a PP tab cannot be migrated (empty means OK)."""
        reasons: list[str] = []
        for filter_item in tab.get_items_by_type(FilterItem):
            content = getattr(filter_item, "contentId", None)
            if isinstance(content, str):
                reasons.append(f"unresolved filter contentId={content}")
                continue

            filter_item_content = cast(Any, content)
            unsupported_reason = getattr(content, "unsupported_reason", None)
            if unsupported_reason:
                reasons.append(unsupported_reason)
                continue

            if getattr(filter_item_content, "type", None) == "list" and not getattr(
                filter_item_content, "_cloud_filter_id", None
            ):
                reasons.append(
                    "cannot map attribute filter to Cloud "
                    f"(filterItemContentId={filter_item_content.id})"
                )

        return reasons

    def _create_min_dashboard(
        self, platform_dashboard: PixelPerfectDashboard, tab: Any, tab_idx: int
    ) -> pdo.CloudDashboard:
        """Create a minimal Cloud dashboard object from Platform definition.

        Sets the public flag based on Platform metadata (unlisted == 0 or missing -> public).

        Args:
            platform_dashboard: Platform dashboard object
            tab: Tab to convert
            tab_idx: Index of the tab

        Returns:
            Minimal Cloud dashboard object
        """
        cloud_min_dashboard = pdo.CloudDashboard.create_from_platform_definition(
            pixel_perfect_dashboard=platform_dashboard,
            tab=tab,
            tab_idx=tab_idx,
        )
        # Write mapping between Platform and Cloud dashboards
        if self.ctx.mapping_logger:
            self.ctx.mapping_logger.write_identifier_relation(
                f"{platform_dashboard.meta.identifier}_{tab.identifier}",
                cloud_min_dashboard.id,
            )
        # Set dashboard public flag (same logic as regular migration)
        cloud_min_dashboard._public = not platform_dashboard.meta.unlisted or (
            platform_dashboard.meta.unlisted == 0
        )
        return cloud_min_dashboard

    def _should_skip_or_overwrite(
        self,
        cloud_min_dashboard: pdo.CloudDashboard,
        tab_title: str,
        overwrite_existing: bool,
    ) -> bool:
        """Return True if tab should be skipped.

        If overwrite is requested and dashboard exists, remove it first and continue.

        Args:
            cloud_min_dashboard: Dashboard to check
            tab_title: Title of the tab
            overwrite_existing: If True, overwrite existing dashboards

        Returns:
            True if processing should stop for this tab
        """
        if cloud_min_dashboard.id in self.cloud_existing_dashboard_ids:
            if overwrite_existing:
                self.ctx.cloud_client.remove_dashboard(cloud_min_dashboard.id)
                logger.info("Overwriting tab: %s", tab_title)
                return False
            else:
                logger.info("Skipping tab: %s - already exists", tab_title)
                return True
        return False

    def _build_filter_context_and_deploy(
        self,
        tab: Any,
        filter_context_id: str,
    ) -> tuple[str, pdo.FilterContextRef, list[pdo.AttributeFilterConfig]]:
        """Build and deploy filter context.

        Args:
            tab: Tab containing filters
            filter_context_id: Deterministic filter context id to create

        Returns:
            Tuple of (default_data_set, filter_context_ref, attribute_filter_configs)
        """
        default_filter: Any = None
        default_data_set: str | None = None
        filter_context = ppf.FilterContext(
            id=filter_context_id,
            attributes=ppf.Attributes(
                title=filter_context_id,  # Use same value for title
                content=ppf.Content(filters=[]),
            ),
        )
        attribute_filter_configs: list[pdo.AttributeFilterConfig] = []

        # Determine primary date filter, sort others by visual position
        filter_items = tab.get_items_by_type(FilterItem)
        primary_date_filter = None
        other_filters = []
        for filter_item in filter_items:
            filter_item_content = filter_item.contentId
            if filter_item_content.type == "time" and primary_date_filter is None:
                primary_date_filter = filter_item
            else:
                other_filters.append(filter_item)
        other_filters = sorted(
            other_filters,
            key=lambda p: ((p.positionY + 20) // 40) * 10000 + p.positionX,
        )
        sorted_filter_items = (
            [primary_date_filter] if primary_date_filter else []
        ) + other_filters

        for filter_item in sorted_filter_items:
            filter_item_content = filter_item.contentId
            if hasattr(filter_item_content, "unsupported_reason") and getattr(
                filter_item_content, "unsupported_reason"
            ):
                logger.warning(
                    "Skipping unsupported filter in filter context: %s",
                    getattr(filter_item_content, "unsupported_reason"),
                )
                continue
            if not getattr(filter_item_content, "_cloud_filter_id", None):
                logger.warning("Filter %s not found in Cloud", filter_item_content.id)
                continue

            if getattr(filter_item_content, "type", None) == "time":
                # Date filter
                dataset_id, granularity = get_date_dataset_and_granularity(
                    ctx=self.ctx,
                    attribute_identifier=".".join(
                        filter_item_content.obj.content.formOf.content.type.split(".")[
                            1:
                        ]
                    ),
                )
                if not default_filter:
                    # Default date filter doesn't have dataSet attribute
                    filter_object = ppf.DateFilterWrapper(
                        dateFilter=ppf.DefaultDateFilter(
                            type="relative",
                            granularity=granularity,
                            **{
                                "from": filter_item_content.default.from_,
                                "to": filter_item_content.default.to,
                            },
                        )
                    )
                    default_filter = filter_object
                    default_data_set = filter_item_content.dataset_id
                else:
                    if not default_data_set:
                        # If we don't know which dataset to attach, skip extra date filters
                        # (Cloud requires dataSet for non-default date filters).
                        continue
                    filter_object = ppf.DateFilterWrapper(
                        dateFilter=ppf.DateFilter(
                            type="relative",
                            granularity=granularity,
                            **{
                                "from": filter_item_content.default.from_,
                                "to": filter_item_content.default.to,
                            },
                            dataSet=ppf.DataSet(
                                identifier=ppf.Identifier(
                                    id=cast(str, default_data_set),
                                    type="dataset",
                                )
                            ),
                        )
                    )
            else:
                # Attribute filter
                if filter_item_content._cloud_filter_id:
                    attribute_filter_configs.append(
                        pdo.AttributeFilterConfig(
                            displayAsLabel=pdo.DisplayAsLabel(
                                identifier=pdo.Identifier(
                                    id=filter_item_content._cloud_filter_id,
                                    type="label",
                                )
                            ),
                            localIdentifier=pp_utils.sanitize_string(
                                f"ppaf_{filter_item_content.id}"
                            ),
                            mode="active",
                        )
                    )
                attr_filter_object = ppf.AttributeFilterWrapper(
                    attributeFilter=ppf.AttributeFilter(
                        title=filter_item_content.label,
                        selectionMode=(
                            ppf.SelectionMode.MULTI
                            if filter_item_content.multiple
                            else ppf.SelectionMode.SINGLE
                        ),
                        negativeSelection=not bool(filter_item_content.default),
                        attributeElements=ppf.AttributeElements(
                            uris=filter_item_content.default
                        ),
                        localIdentifier=uuid.uuid4().hex,
                        displayForm=ppf.DisplayForm(
                            identifier=ppf.Identifier(
                                id=filter_item_content._cloud_filter_id, type="label"
                            )
                        ),
                    )
                )
                filter_context.add_filter(filter=attr_filter_object)
                continue

            filter_context.add_filter(filter=filter_object)

        # Deploy filter context and attach reference (skip if skip_deploy would be set)
        # Delete old filter context first
        [
            self.ctx.cloud_client.remove_filter_context(item["id"])
            for item in self.ctx.cloud_client.get_filter_contexts()
            if item["id"] == filter_context_id
        ]
        fc = self.ctx.cloud_client.create_filter_context(
            {"data": filter_context.model_dump(by_alias=True)}
        )
        if not fc.ok:
            logger.error("Failed to create filter context - %s", fc.json())
            return (
                default_data_set or "dt_shared_date",
                pdo.FilterContextRef(
                    identifier=pdo.Identifier(
                        id=filter_context_id, type="filterContext"
                    )
                ),
                attribute_filter_configs,
            )

        return (
            default_data_set or "dt_shared_date",
            pdo.FilterContextRef(
                identifier=pdo.Identifier(id=filter_context_id, type="filterContext")
            ),
            attribute_filter_configs,
        )

    def _build_tab_layout(
        self,
        tab: Any,
        default_data_set: str,
    ) -> pdo.Layout:
        """Build a Cloud dashboard layout for a single tab.

        Args:
            tab: Tab containing items
            default_data_set: Default dataset ID for date filters
        """
        layout = pdo.Layout(sections=[])
        # Create sections (aka rows)
        section_items = migrate_to_sections(
            tab.model_dump()["items"], self.cfg, apply_canonical=False
        )
        for _ in range(len({i["section"] for i in section_items})):
            layout.sections.append(pdo.LayoutSection())

        # Add items to sections
        for item_order, item in enumerate(
            sorted(tab.items, key=lambda p: (p.positionX, p.positionY))
        ):
            if item.type not in self.supported_items:
                continue

            if item.type == "headlineItem":
                if not item.cloud_metric_id:
                    logger.warning(
                        "Skipping headlineItem - metric mapping not found for '%s'",
                        item.metric.meta.identifier,
                    )
                    continue
                # Create visualization first
                visualization_title = pp_utils.get_migration_id(
                    prefix=self.pixel_perfect_prefix,
                    platform_title=item.title,
                    platform_identifier=item.metric.meta.identifier,
                )
                # Remove old visualizations with same title
                [
                    self.ctx.cloud_client.remove_insight(obj["id"])
                    for obj in self.ctx.cloud_client.get_insights()
                    if obj["attributes"]["title"] == visualization_title
                ]
                visualisation_maker = pvo.VisualisationMaker(ctx=self.ctx)
                visualization_id = visualisation_maker.create_visualization(
                    cloud_measure_identifier=item.cloud_metric_id,
                    platform_measure_identifier=item.metric.meta.identifier,
                    title=item.title,
                    id_prefix=PP_INSIGHT_PREFIX,
                )
            elif item.type == "reportItem":
                visualization_id = item.visualisation_id
                nearest = pp_utils.nearest_item(
                    target=item, items=tab.items, return_distance=True
                )
                nearest_item, _distance = (
                    nearest if isinstance(nearest, tuple) else (nearest, 0.0)
                )
                # If nearest is iframe with text, try to use it as section header
                if nearest_item and nearest_item.type == "iframeItem":
                    parsed = urlparse(nearest_item.url)
                    params = parse_qs(parsed.query)
                    title = params.get("text", [None])[0]
                    item.title = re.sub(r"<[^>]+>", "", title) if title else None

            if item.type == "textItem":
                if len(item.text.strip()) < self.min_text_length:
                    logger.info(
                        "Skipping textItem (too short): '%s...'", item.text[:20]
                    )
                    continue

            # Map item to its new positioning system in Cloud
            position = next(
                (i for i in section_items if i["migration_id"] == item.migration_id),
                None,
            )
            if position is None:
                # If we can't compute geometry for an item, skip it instead of crashing.
                continue

            # Date dataset assignment for widgets
            if item.type == "textItem":
                layoutItem = pdo.LayoutItem(
                    size=pdo.XLSize(
                        xl=pdo.SizeDefinition(
                            gridHeight=position["size"]["xl"]["gridHeight"],
                            gridWidth=position["size"]["xl"]["gridWidth"],
                        )
                    ),
                    widget=pdo.RichTextWidget(
                        content=item.text,
                        localIdentifier=item.generate_local_identifier(
                            tab.identifier, item_order
                        ),
                    ),
                )
            else:
                has_time_filter = False
                if hasattr(item, "filters") and item.filters:
                    for filter_item in item.filters:
                        content = getattr(filter_item, "contentId", None)
                        if getattr(content, "type", None) == "time":
                            has_time_filter = True
                            break

                dateDataSet: pdo.DateDataSet | dict[str, Any]
                if not has_time_filter:
                    dateDataSet = {}
                else:
                    dateDataSet = pdo.DateDataSet(
                        identifier=pdo.Identifier(id=default_data_set, type="dataset")
                    )
                layoutItem = pdo.LayoutItem(
                    size=pdo.XLSize(
                        xl=pdo.SizeDefinition(
                            gridHeight=position["size"]["xl"]["gridHeight"],
                            gridWidth=position["size"]["xl"]["gridWidth"],
                        )
                    ),
                    widget=pdo.InsightWidget(
                        title=item.title
                        if hasattr(item, "title") and item.title
                        else item.obj.meta.title,
                        description="",
                        configuration=pdo.Configuration(hideTitle=False),
                        ignoreDashboardFilters=[
                            pdo.AttributeFilterReference(
                                displayForm=pdo.AttributeFilterReferenceDisplayForm(
                                    identifier=pdo.Identifier(
                                        id=filter_id, type="label"
                                    )
                                )
                            )
                            for filter_id in set(
                                [
                                    f.contentId._cloud_filter_id
                                    for f in tab.get_items_by_type(FilterItem)
                                    if hasattr(f.contentId, "_cloud_filter_id")
                                    and f.contentId._cloud_filter_id
                                ]
                            )
                            - set(
                                [
                                    f.contentId._cloud_filter_id
                                    for f in item.filters
                                    if hasattr(f.contentId, "_cloud_filter_id")
                                    and f.contentId._cloud_filter_id
                                ]
                            )
                            if hasattr(item, "filters") and item.filters
                        ],
                        dateDataSet=dateDataSet,
                        insight=pdo.Insight(
                            identifier=pdo.Identifier(id=visualization_id)
                        ),
                    ),
                )
            layout.sections[position["section"]].items.append(layoutItem)

        # Remove empty sections
        layout.sections = list(
            filter(
                lambda x: len(x.items) > 0,
                layout.sections,
            )
        )
        return layout

    def _log_transformation_tabbed(
        self,
        platform_dashboard: PixelPerfectDashboard,
        platform_dashboard_raw: dict,
        cloud_dashboard: pdo.CloudDashboard,
    ) -> None:
        """Write one transformation record for a tabbed dashboard."""
        if self.ctx.transformation_logger:
            self.ctx.transformation_logger.write_transformation(
                title=f"{platform_dashboard.meta.title}",
                platform_object=json.dumps(
                    {
                        "projectDashboard": {
                            "meta": platform_dashboard_raw["projectDashboard"]["meta"],
                            "content": platform_dashboard_raw["projectDashboard"][
                                "content"
                            ],
                        }
                    }
                ),
                cloud_object=cloud_dashboard.model_dump_json(),
            )

    def _log_transformation(
        self,
        platform_dashboard: PixelPerfectDashboard,
        platform_dashboard_raw: dict,
        tab_idx: int,
        tab: Any,
        cloud_min_dashboard: pdo.CloudDashboard,
    ) -> None:
        """Write transformation record to log.

        Args:
            platform_dashboard: Platform dashboard object
            platform_dashboard_raw: Raw Platform dashboard definition
            tab_idx: Index of the tab
            tab: Tab object
            cloud_dashboard: Created Cloud dashboard
        """
        if self.ctx.transformation_logger:
            self.ctx.transformation_logger.write_transformation(
                title=f"{platform_dashboard.meta.title} - {tab.title}",
                platform_object=json.dumps(
                    {
                        "projectDashboard": {
                            "meta": platform_dashboard_raw["projectDashboard"]["meta"],
                            "content": {
                                "tabs": platform_dashboard_raw["projectDashboard"][
                                    "content"
                                ]["tabs"][tab_idx],
                                "filters": platform_dashboard_raw["projectDashboard"][
                                    "content"
                                ]["filters"],
                            },
                        }
                    }
                ),
                cloud_object=cloud_min_dashboard.model_dump_json(),
            )

    def get_cloud_dashboards(self) -> list[pdo.CloudDashboard]:
        """Return the list of processed Cloud dashboards."""
        return self.cloud_dashboards

    def get_public_dashboard_ids(self) -> list[str]:
        """Return IDs of dashboards that should be public."""
        return self.public_dashboard_ids
