# (C) 2026 GoodData Corporation
"""
This module contains the CloudDashboard class,
which is responsible for transforming the Platform dashboard to Cloud format.
"""

import logging
from dataclasses import dataclass
from typing import Any, Literal

from gooddata_platform2cloud.dashboards.data_classes import DashboardContext
from gooddata_platform2cloud.dashboards.drill_converter import (
    CurrentDashboard,
    DrillConverter,
)
from gooddata_platform2cloud.dashboards.filter_context import FilterContext
from gooddata_platform2cloud.helpers import dashboard_specific_insight_id, get_cloud_id
from gooddata_platform2cloud.insights.period_comparison_insight import (
    PeriodComparisonInsight,
)
from gooddata_platform2cloud.models.platform.visualization_class import (
    VisualizationClassWrapper,
)

logger = logging.getLogger("migration")


@dataclass
class WidgetWrapper:
    """Wrapper object for a widget content and its type."""

    widget_object: dict
    type: Literal["kpi", "insight"]


class CloudDashboard:
    """
    The CloudDashboard class is responsible for transforming the Platform dashboard to Cloud format.
    """

    def __init__(
        self,
        ctx: DashboardContext,
        metadata: Any,
        skip_deploy: bool,
        overwrite_existing: bool,
    ):
        self.ctx = ctx
        self.meta = metadata["analyticalDashboard"]["meta"]
        self.dashboards_content = metadata["analyticalDashboard"]["content"]
        self.date_filter_config = self.dashboards_content.get("dateFilterConfig")
        self.drill_converter = DrillConverter(ctx)
        self.drill_warnings: list[str] = []  # Initialize drill warnings list
        self.overwrite_existing = overwrite_existing
        # Set cloud_dashboard_id early, before layout processing
        # because drill conversion needs it for self-referencing drills
        self.cloud_dashboard_id = get_cloud_id(
            self.meta["title"], self.meta["identifier"]
        )
        self.ctx.mapping_logger.write_identifier_relation(
            self.meta["identifier"], self.cloud_dashboard_id
        )

        filter_context_class = FilterContext(
            self.ctx, self.dashboards_content.get("filterContext", "")
        )
        self.filter_context_id, filter_context_object = filter_context_class.get()
        if filter_context_object and not skip_deploy:
            self._create_or_update_filter_context(
                filter_context_object, overwrite_existing
            )
        missing_filter_values = filter_context_class.get_missing_filter_values()
        self.cloud_filters = filter_context_class.cloud_filters
        self.attribute_filter_configs = filter_context_class.attribute_filter_configs

        self.layout = self._get_layout(self.dashboards_content.get("layout", []))
        self.title = self.meta["title"]

        # Check for any warnings (filter values or drills) after layout processing
        has_warnings = bool(missing_filter_values or self.drill_warnings)

        if has_warnings:
            # Always log to console, even if suppressing warnings from objects
            warning_details = []
            if missing_filter_values:
                warning_details.append(
                    f"Missing filter values: {missing_filter_values}"
                )
            if self.drill_warnings:
                warning_details.append(f"Drill warnings: {self.drill_warnings}")
            logger.warning(
                "  Dashboard '%s': %s",
                self.meta["title"],
                "; ".join(warning_details),
            )

            # Only add [WARN] prefix and warning widget if not suppressing
            if not self.ctx.suppress_warnings:
                self.title = f"[WARN] {self.meta['title']}"
                self._get_warning_widget(missing_filter_values, self.drill_warnings)
        # The dashboard is public if "unlisted" is 0 or not present in metadata
        self.public = "unlisted" not in self.meta or self.meta["unlisted"] == 0

    def _create_or_update_filter_context(
        self, filter_context_object: dict[str, Any], overwrite_existing: bool
    ):
        """Create or update the filter context."""

        if overwrite_existing:
            # Try to get the filter context
            filter_context = self.ctx.cloud_client.get_filter_context(
                filter_context_object["data"]["id"]
            )

            if filter_context.get("data"):
                # If the filter context exists, the endpoint should return a response
                # with a `data` object. Update the filter context
                self.ctx.cloud_client.update_filter_context(filter_context_object)
                return

        # If overwrite is not requested, or the filter context does not exist, create it
        self.ctx.cloud_client.create_filter_context(filter_context_object)

    def _get_rich_text_for_missing_insight(self, insight_uri):
        platform_domain = self.ctx.platform_client.domain
        workspace_id = self.ctx.platform_client.pid
        insight_id = insight_uri.rsplit("/")[-1]
        platform_obj_uri = (
            f"{platform_domain}/analyze/#/{workspace_id}/{insight_id}/edit"
        )
        return f"""The insight you're looking for is currently unavailable because it was not properly migrated during the insights migration.

You can view the original insight [here]({platform_obj_uri})."""

    def _get_warning_widget(self, missing_filter_values, drill_warnings):
        warning_content_parts = []

        # Add filter warnings if any
        if missing_filter_values:
            formatted_missing_values = "\n\n".join(
                f"{key}: {', '.join(value) if isinstance(value, list) else value}"
                for key, value in missing_filter_values.items()
            )
            warning_content_parts.append(
                f"Missing values in filter context: \n\n{formatted_missing_values}"
            )

        # Add drill warnings if any
        if drill_warnings:
            drill_warnings_text = "\n\n".join(drill_warnings)
            warning_content_parts.append(
                f"Drill migration warnings: \n\n{drill_warnings_text}"
            )

        # Combine all warnings
        warning_content = "\n\n---\n\n".join(warning_content_parts)

        # Calculate grid height based on number of non-empty lines plus two
        # with maximum height of 22
        non_empty_lines = len(
            [line for line in warning_content.split("\n") if line.strip()]
        )
        grid_height = min(non_empty_lines + 2, 22)

        warning_widget = {
            "type": "richText",
            "content": warning_content,
            "description": "",
            "drills": [],
            "ignoreDashboardFilters": [],
        }
        self.layout["sections"].insert(
            0,
            {
                "type": "IDashboardLayoutSection",
                "header": {"title": "Migration errors"},
                "items": [
                    {
                        "type": "IDashboardLayoutItem",
                        "size": {"xl": {"gridHeight": grid_height, "gridWidth": 12}},
                        "widget": warning_widget,
                    }
                ],
            },
        )

    def _get_insight_item(self, obj):
        insight_obj = self.ctx.platform_client.get_object(
            obj["visualizationWidget"]["content"]["visualization"]
        )
        return {
            "identifier": {
                "id": self.ctx.insight_mappings.search_mapping_identifier(
                    insight_obj["visualizationObject"]["meta"]["identifier"]
                ),
                "type": insight_obj["visualizationObject"]["meta"]["category"],
            },
        }

    def _get_dataset_item(self, dataset_uri: str):
        dataset_obj = self.ctx.platform_client.get_object(dataset_uri)
        return {
            "identifier": {
                "id": self.ctx.ldm_mappings.search_mapping_identifier(
                    dataset_obj["dataSet"]["content"]["identifierPrefix"]
                ),
                "type": dataset_obj["dataSet"]["meta"]["category"].lower(),
            }
        }

    @staticmethod
    def _get_configuration(obj: dict, widget_type: str) -> dict:
        new_configuration = {}
        for item, value in obj[widget_type]["content"].get("configuration", {}).items():
            new_configuration[item] = value
        return new_configuration

    @staticmethod
    def _transform_ignore_dashboard_filter_type_value(obj: dict) -> str:
        filter_type = obj["attributeDisplayForm"]["meta"]["category"].lower()
        filter_type = "label" if filter_type == "attributedisplayform" else filter_type
        return filter_type

    def _get_ignore_dashboard_filters(self, obj: dict, widget_type: str) -> list:
        new_ignore_dashboard_filters = []
        for item in obj[widget_type]["content"]["ignoreDashboardFilters"]:
            filter_obj = self.ctx.platform_client.get_object(
                item["attributeFilterReference"]["displayForm"]
            )
            new_ignore_dashboard_filter = {
                "displayForm": {
                    "identifier": {
                        "id": self.ctx.ldm_mappings.search_mapping_identifier(
                            filter_obj["attributeDisplayForm"]["meta"]["identifier"]
                        ),
                        "type": self._transform_ignore_dashboard_filter_type_value(
                            filter_obj
                        ),
                    }
                },
                "type": "attributeFilterReference",
            }
            new_ignore_dashboard_filters.append(new_ignore_dashboard_filter)
        return new_ignore_dashboard_filters

    def _get_widget_drills(self, obj: dict, widget_type: str) -> list:
        """
        Extract and convert drills from a Platform widget to Cloud format.
        """
        if "drills" not in obj[widget_type]["content"]:
            return []

        # Extract widget title for better error messages
        widget_title = None
        if "meta" in obj[widget_type] and "title" in obj[widget_type]["meta"]:
            widget_title = obj[widget_type]["meta"]["title"]

        # Create current dashboard info for self-reference handling
        current_dashboard = CurrentDashboard(
            platform_id=self.meta["identifier"], cloud_id=self.cloud_dashboard_id
        )

        cloud_drills, drill_warnings = self.drill_converter.convert_drills(
            obj[widget_type]["content"]["drills"], widget_title, current_dashboard
        )

        # Collect drill warnings
        if drill_warnings:
            self.drill_warnings.extend(drill_warnings)

        return cloud_drills

    def _resolve_widget_type(self, widget_object: dict) -> Literal["kpi", "insight"]:
        """Resolve widget type based on widget object.

        Returns either "kpi" or "insight" depending on the widget object. Headline
        widgets are evaluated as KPI objects so that they can be handled in the
        same way as KPI widgets when inferring size for Cloud dashboard layout.

        Raises ValueError if the widget type cannot be determined.
        """
        # TODO: widget object shape should be validated. For the moment, we're
        # assuming the object structure
        if "kpi" in widget_object:
            return "kpi"

        if "visualizationWidget" in widget_object:
            insight_obj = self.ctx.platform_client.get_object(
                widget_object["visualizationWidget"]["content"]["visualization"]
            )
            content_visualization_class = insight_obj["visualizationObject"][
                "content"
            ].get("visualizationClass")
            if content_visualization_class:
                visualization_class_uri = content_visualization_class["uri"]
                raw_visualization_class = self.ctx.platform_client.get_object(
                    visualization_class_uri
                )
                visualization_class_wrapper = VisualizationClassWrapper(
                    **raw_visualization_class
                )
                if (
                    visualization_class_wrapper.visualization_class.meta.identifier
                    == "gdc.visualization.headline"
                ):
                    return "kpi"

            # Fallback to insight type
            return "insight"

        raise ValueError(f"Unknown widget type: {widget_object}")

    def _get_widget(self, widget_uri: str) -> WidgetWrapper:
        obj = self.ctx.platform_client.get_object(widget_uri)
        widget_object: dict[str, Any] = {
            "type": "insight",
            "drills": [],
        }
        if "kpi" in obj:
            # Pass the dashboard ID to make the insight ID unique
            dashboard_id = self.meta.get("identifier", "")
            new_insight_id = dashboard_specific_insight_id(
                obj["kpi"]["meta"]["title"], dashboard_id
            )
            widget_object["description"] = obj["kpi"]["meta"]["summary"]
            widget_object["ignoreDashboardFilters"] = (
                self._get_ignore_dashboard_filters(obj, "kpi")
            )
            if "configuration" in obj["kpi"]:
                widget_object["configuration"] = self._get_configuration(obj, "kpi")
            period_comparison_insight = PeriodComparisonInsight(
                self.ctx, obj, new_insight_id, self.cloud_filters
            )
            if "properties" in obj["kpi"]["content"]:
                widget_object["properties"] = obj["kpi"]["content"]["properties"]
            comparison_insight_object = period_comparison_insight.get()
            if not comparison_insight_object:
                widget_object["type"] = "richText"
                widget_object["content"] = self._get_rich_text_for_missing_insight(
                    obj["visualizationWidget"]["content"]["visualization"]
                )
            else:
                period_comparison_insight.create_or_update_insight_from_kpi(
                    comparison_insight_object, self.overwrite_existing
                )
                widget_object["insight"] = {
                    "identifier": {"id": new_insight_id, "type": "visualizationObject"}
                }
                widget_object["title"] = obj["kpi"]["meta"]["title"]
                if "dateDataSet" in obj["kpi"]["content"]:
                    widget_object["dateDataSet"] = self._get_dataset_item(
                        obj["kpi"]["content"]["dateDataSet"]
                    )
                # Process drills for KPI widgets
                widget_object["drills"] = self._get_widget_drills(obj, "kpi")
            widget_object["localIdentifier"] = obj["kpi"]["meta"]["identifier"]
        elif "visualizationWidget" in obj:
            widget_object["description"] = obj["visualizationWidget"]["meta"]["summary"]
            widget_object["ignoreDashboardFilters"] = (
                self._get_ignore_dashboard_filters(obj, "visualizationWidget")
            )
            if "configuration" in obj["visualizationWidget"]["content"]:
                widget_object["configuration"] = self._get_configuration(
                    obj, "visualizationWidget"
                )
            if "properties" in obj["visualizationWidget"]["content"]:
                # Parse properties JSON string from Platform
                try:
                    import json

                    properties_str = obj["visualizationWidget"]["content"]["properties"]
                    properties = json.loads(properties_str)
                    widget_object["properties"] = properties
                except (json.JSONDecodeError, TypeError) as e:
                    logger.warning(
                        "Failed to parse properties for visualization widget: %s", e
                    )
                    widget_object["properties"] = obj["visualizationWidget"]["content"][
                        "properties"
                    ]
            try:
                widget_object["insight"] = self._get_insight_item(obj)
            except ValueError:
                widget_object["type"] = "richText"
                widget_object["content"] = self._get_rich_text_for_missing_insight(
                    obj["visualizationWidget"]["content"]["visualization"]
                )
            else:
                widget_object["title"] = obj["visualizationWidget"]["meta"]["title"]
                if "dateDataSet" in obj["visualizationWidget"]["content"]:
                    widget_object["dateDataSet"] = self._get_dataset_item(
                        obj["visualizationWidget"]["content"]["dateDataSet"]
                    )
                # Process drills for visualization widgets
                widget_object["drills"] = self._get_widget_drills(
                    obj, "visualizationWidget"
                )

            widget_object["localIdentifier"] = obj["visualizationWidget"]["meta"][
                "identifier"
            ]
        if "ignoreDashboardFilters" in obj:
            widget_object["ignoreDashboardFilters"] = obj["ignoreDashboardFilters"]

        widget_type = self._resolve_widget_type(obj)
        return WidgetWrapper(widget_object=widget_object, type=widget_type)

    @staticmethod
    def _get_size(
        size: dict, widget_wrapper: WidgetWrapper, max_row_height: int | None
    ) -> dict:
        size_key = list(size.keys())[0]
        if "height" in size[size_key]:
            height = size[size_key]["height"]
        else:
            if widget_wrapper.type == "kpi":
                comparison_type = widget_wrapper.widget_object.get("content", {}).get(
                    "comparisonType"
                )
                if comparison_type is None:
                    # KPIs without comparisons can be smaller.
                    # When set to 11, they have too much whitespace underneath.
                    height = 8
                else:
                    height = 11
            else:
                height = 22
        width = size[size_key].get("width")
        new_size: dict = {size_key: {}}

        # Limit the height to the max row height
        if max_row_height is not None:
            height = min(height, max_row_height)

        new_size[size_key]["gridHeight"] = height
        if width:
            new_size[size_key]["gridWidth"] = width
        return new_size

    def _get_layout(self, layout_items: dict) -> dict:
        """
        Returns the items.
        """
        rows = layout_items["fluidLayout"]["rows"]
        new_sections = []
        for row in rows:
            # Iterate over the columns to get the max row height
            max_row_height: int | None = None
            for column in row["columns"]:
                size_key: str = list(column["size"].keys())[0]
                column_height = column["size"][size_key].get("height")
                if isinstance(column_height, int):
                    if max_row_height is None:
                        max_row_height = column_height
                    else:
                        max_row_height = max(max_row_height, column_height)

            new_items = []
            for column in row["columns"]:
                widget_wrapper = self._get_widget(
                    column["content"]["widget"]["qualifier"]["uri"]
                )
                new_item = {
                    "type": "IDashboardLayoutItem",
                    "size": self._get_size(
                        column["size"], widget_wrapper, max_row_height
                    ),
                    "widget": widget_wrapper.widget_object,
                }
                new_items.append(new_item)
            new_sections.append(
                {
                    "type": "IDashboardLayoutSection",
                    "header": row.get("header", {}),
                    "items": new_items,
                }
            )
        new_layout = {"type": "IDashboardLayout", "sections": new_sections}

        return new_layout

    def get(self):
        """
        Returns the Cloud dashboard object.
        """
        dashboard_metadata = {
            "data": {
                "id": self.cloud_dashboard_id,
                "type": "analyticalDashboard",
                "attributes": {
                    "title": self.title,
                    "description": self.meta["summary"],
                    "content": {
                        "attributeFilterConfigs": self.attribute_filter_configs,
                        "filterContextRef": self.filter_context_id,
                        "layout": self.layout,
                        "version": "2",
                    },
                },
            }
        }

        if self.date_filter_config:
            dashboard_metadata["data"]["attributes"]["content"]["dateFilterConfig"] = (
                self.date_filter_config
            )

        return dashboard_metadata
