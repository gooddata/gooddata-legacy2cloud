# (C) 2026 GoodData Corporation
import attrs
from gooddata_sdk import CatalogUser
from gooddata_sdk.catalog.workspace.declarative_model.workspace.automation import (
    CatalogDeclarativeAutomation,
)

from gooddata_legacy2cloud.backends.cloud.client import CloudClient
from gooddata_legacy2cloud.models.aliases import (
    CloudDashboardId,
    CloudInsightId,
    CloudWidgetId,
    UserEmail,
)
from gooddata_legacy2cloud.models.cloud.automations import (
    TabularExportRequestPayload,
    VisualExportRequestPayload,
)
from gooddata_legacy2cloud.models.cloud.dashboard import CloudDashboard, Widget
from gooddata_legacy2cloud.models.cloud.filter_context import FilterContextModel
from gooddata_legacy2cloud.models.cloud.identifier import IdentifierWrapper


@attrs.define
class Exports:
    """Container for visual and tabular exports"""

    visual_exports: list[VisualExportRequestPayload] = attrs.field(factory=list)
    tabular_exports: list[TabularExportRequestPayload] = attrs.field(factory=list)


@attrs.define
class CloudMaps:
    cloud_user_map: dict[UserEmail, CatalogUser]
    cloud_dashboard_map: dict[CloudDashboardId, CloudDashboard]
    cloud_dashboard_to_insight_to_widget_id_map: dict[
        CloudDashboardId, dict[CloudInsightId, CloudWidgetId]
    ]
    cloud_widget_map: dict[CloudWidgetId, Widget]
    cloud_dashboard_filter_contexts: list[FilterContextModel]
    existing_automation_ids: set[str]

    def get_widget_date_dataset(
        self,
        cloud_dashboard_id: CloudDashboardId,
        cloud_insight_id: CloudInsightId,
    ) -> IdentifierWrapper:
        """Get Cloud widget`s date dataset based on the dashboard and insight IDs."""
        widget_id = self.cloud_dashboard_to_insight_to_widget_id_map[
            cloud_dashboard_id
        ][cloud_insight_id]
        widget = self.cloud_widget_map[widget_id]

        if not widget.date_data_set:
            raise ValueError(
                f"Widget {widget_id} is expected to have a date dataset but has none"
            )

        return widget.date_data_set

    def get_widget_id(
        self,
        cloud_dashboard_id: CloudDashboardId,
        cloud_insight_id: CloudInsightId,
    ) -> CloudWidgetId:
        """Get Cloud widget ID based on the dashboard and insight IDs."""
        if cloud_dashboard_id not in self.cloud_dashboard_to_insight_to_widget_id_map:
            raise KeyError(f"Dashboard {cloud_dashboard_id} not found in the map")
        if (
            cloud_insight_id
            not in self.cloud_dashboard_to_insight_to_widget_id_map[cloud_dashboard_id]
        ):
            raise KeyError(
                f"Insight {cloud_insight_id} not found in the map for "
                + f"dashboard {cloud_dashboard_id}"
            )
        return self.cloud_dashboard_to_insight_to_widget_id_map[cloud_dashboard_id][
            cloud_insight_id
        ]

    def get_widget_by_id(self, cloud_widget_id: CloudWidgetId) -> Widget:
        """Get Cloud widget by ID."""
        if cloud_widget_id not in self.cloud_widget_map:
            raise KeyError(f"Widget {cloud_widget_id} not found in the map")
        return self.cloud_widget_map[cloud_widget_id]

    def get_dashboard_by_id(
        self, cloud_dashboard_id: CloudDashboardId
    ) -> CloudDashboard:
        """Get Cloud dashboard by ID."""
        if cloud_dashboard_id not in self.cloud_dashboard_map:
            raise KeyError(f"Dashboard {cloud_dashboard_id} not found in the map")
        return self.cloud_dashboard_map[cloud_dashboard_id]

    def get_user_by_email(self, email: UserEmail) -> CatalogUser:
        """Get Cloud user by email."""
        if email not in self.cloud_user_map:
            raise KeyError(f"User {email} not found in the map")
        return self.cloud_user_map[email]

    def get_dashboard_filter_context(
        self,
        cloud_dashboard_id: CloudDashboardId,
    ) -> FilterContextModel:
        """Get the filter context for a given Cloud dashboard ID."""
        for context in self.cloud_dashboard_filter_contexts:
            cloud_filter_context = self.cloud_dashboard_map[
                cloud_dashboard_id
            ].attributes.content.filter_context_ref

            if (
                cloud_filter_context
                and cloud_filter_context.identifier
                and context.id_ == cloud_filter_context.identifier.id_
            ):
                return context

        # Raise an exception if the filter context is not found
        raise Exception(f"Filter context not found for dashboard {cloud_dashboard_id}")


@attrs.define
class Mappers:
    cloud_client: CloudClient

    @staticmethod
    def _create_cloud_dashboard_to_insight_to_widget_id_map(
        dashboards: dict[CloudDashboardId, CloudDashboard],
    ) -> dict[CloudDashboardId, dict[CloudInsightId, CloudWidgetId]]:
        """Creates a map of Cloud dashboard IDs to insight IDs to widget IDs."""
        result_map: dict[CloudDashboardId, dict[CloudInsightId, CloudWidgetId]] = {}

        for dashboard_id, dashboard in dashboards.items():
            for section in dashboard.attributes.content.layout.sections:
                for item in section.items:
                    widget_id = item.widget.local_identifier

                    if not item.widget.insight:
                        continue

                    insight_id = item.widget.insight.identifier.id_

                    if dashboard_id not in result_map:
                        result_map[dashboard_id] = {}

                    result_map[dashboard_id][insight_id] = widget_id

        return result_map

    @staticmethod
    def _create_cloud_widget_map(
        dashboards: dict[CloudDashboardId, CloudDashboard],
    ) -> dict[CloudWidgetId, Widget]:
        """Creates a cache of Cloud widgets indexed by Cloud widget ID."""
        result_map: dict[CloudWidgetId, Widget] = {}
        for _dashboard_id, dashboard in dashboards.items():
            for section in dashboard.attributes.content.layout.sections:
                for item in section.items:
                    widget_id = item.widget.local_identifier
                    result_map[widget_id] = item.widget
        return result_map

    @staticmethod
    def _create_cloud_dashboard_map(
        cloud_client: CloudClient,
    ) -> dict[CloudDashboardId, CloudDashboard]:
        """Creates a cache of cloud dashboards indexed by Legacy dashboard ID."""
        raw_dashboards: list[dict] = cloud_client.get_dashboards()
        dashboards: list[CloudDashboard] = [
            CloudDashboard(**dashboard) for dashboard in raw_dashboards
        ]
        cache: dict[CloudDashboardId, CloudDashboard] = {}
        for dashboard in dashboards:
            cache[dashboard.id] = dashboard
        return cache

    @staticmethod
    def _create_cloud_user_map(
        cloud_client: CloudClient,
    ) -> dict[UserEmail, CatalogUser]:
        """Creates a cache of cloud users indexed by email."""
        users: list[CatalogUser] = cloud_client.sdk.catalog_user.list_users()
        cache: dict[str, CatalogUser] = {}
        for user in users:
            if user.attributes and user.attributes.email:
                cache[user.attributes.email] = user

        return cache

    @classmethod
    def create_maps(cls, cloud_client: CloudClient) -> CloudMaps:
        """Set up caches and maps for Cloud objects"""

        cloud_user_map: dict[UserEmail, CatalogUser] = cls._create_cloud_user_map(
            cloud_client
        )
        cloud_dashboard_map: dict[CloudDashboardId, CloudDashboard] = (
            cls._create_cloud_dashboard_map(cloud_client)
        )
        cloud_dashboard_to_insight_to_widget_id_map: dict[
            CloudDashboardId, dict[CloudInsightId, CloudWidgetId]
        ] = cls._create_cloud_dashboard_to_insight_to_widget_id_map(cloud_dashboard_map)
        cloud_widget_map: dict[CloudWidgetId, Widget] = cls._create_cloud_widget_map(
            cloud_dashboard_map
        )

        cloud_dashboard_filter_contexts: list[FilterContextModel] = (
            cloud_client.get_filter_context_objects()
        )

        cloud_automations: list[CatalogDeclarativeAutomation] = (
            cloud_client.sdk.catalog_workspace.get_declarative_automations(
                workspace_id=cloud_client.ws
            )
        )

        cloud_automation_ids = {automation.id for automation in cloud_automations}

        return CloudMaps(
            cloud_user_map,
            cloud_dashboard_map,
            cloud_dashboard_to_insight_to_widget_id_map,
            cloud_widget_map,
            cloud_dashboard_filter_contexts,
            cloud_automation_ids,
        )
