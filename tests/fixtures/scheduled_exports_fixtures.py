# (C) 2026 GoodData Corporation
"""
This module contains fixtures for the scheduled exports tests.
"""

import json

import pytest
from gooddata_sdk import CatalogUser

from gooddata_legacy2cloud.backends.cloud.client import CloudClient
from gooddata_legacy2cloud.backends.legacy.client import LegacyClient
from gooddata_legacy2cloud.id_mappings import IdMappings
from gooddata_legacy2cloud.models.legacy.analytical_dashboards import (
    AnalyticalDashboardWrapper,
)
from gooddata_legacy2cloud.scheduled_exports.scheduled_export_context import (
    Backends,
    CommandLineArguments,
    Logging,
    Mappings,
    ScheduledExportsContext,
)
from gooddata_legacy2cloud.scheduled_exports.scheduled_exports import (
    ScheduledExportMigrator,
)
from tests.conftest import MAPPING_FILES_DIR

CLOUD_OBJECTS_DIR = "tests/data/scheduled_exports/cloud_objects"
LEGACY_OBJECTS_DIR = "tests/data/scheduled_exports/legacy_objects"


@pytest.fixture
def scheduled_exports_context(
    legacy_client: LegacyClient, cloud_client: CloudClient, mocker
):
    """Mock scheduled exports context object."""
    return ScheduledExportsContext(
        input_file=None,
        notification_channel_id="invalid_channel_id",
        backends=Backends(legacy_client=legacy_client, cloud_client=cloud_client),
        mappings=Mappings(
            ldm_mappings=IdMappings(f"{MAPPING_FILES_DIR}/ldm_mappings.csv"),
            metric_mappings=IdMappings(f"{MAPPING_FILES_DIR}/metric_mappings.csv"),
            insight_mappings=IdMappings(f"{MAPPING_FILES_DIR}/insight_mappings.csv"),
            dashboard_mappings=IdMappings(
                f"{MAPPING_FILES_DIR}/dashboard_mappings.csv"
            ),
            scheduled_export_mappings=IdMappings(
                f"{MAPPING_FILES_DIR}/scheduled_export_mappings.csv"
            ),
        ),
        logging=Logging(
            mapping_logger=mocker.MagicMock(), output_logger=mocker.MagicMock()
        ),
        command_line_arguments=CommandLineArguments(
            dump_legacy=False,
            legacy_dump_file="",
            dump_cloud=False,
            cloud_dump_file="",
            cleanup_target_env=False,
            skip_deploy=False,
            overwrite_existing=False,
            client_prefix="",
        ),
    )


@pytest.fixture
def scheduled_exports_migrator(
    scheduled_exports_context, mocker, legacy_dashboards_for_exports
):
    """Create a scheduled export migrator instance with appropriate mocks"""

    # Create the migrator instance
    scheduled_exports_migrator = ScheduledExportMigrator(scheduled_exports_context)

    # Patch the list_users method from SDK to load users from JSON file and create CatalogUser objects
    def mock_list_users():
        with open(f"{CLOUD_OBJECTS_DIR}/users.json", "r") as file:
            raw_users = json.load(file)
        users = [CatalogUser.from_dict(user) for user in raw_users]
        return users

    mocker.patch.object(
        scheduled_exports_migrator.context.backends.cloud_client.sdk.catalog_user,
        "list_users",
        side_effect=mock_list_users,
    )

    # Patch cloud_client.get_dashboards method to load dashboards from JSON file
    def mock_get_dashboards():
        with open(f"{CLOUD_OBJECTS_DIR}/dashboards.json", "r") as file:
            raw_dashboards = json.load(file)
        return raw_dashboards

    mocker.patch.object(
        scheduled_exports_migrator.context.backends.cloud_client,
        "get_dashboards",
        side_effect=mock_get_dashboards,
    )

    # Mock get_declarative_automations (called to get existing Cloud automations)
    mocker.patch.object(
        scheduled_exports_migrator.context.backends.cloud_client.sdk.catalog_workspace,
        "get_declarative_automations",
        return_value=[],
    )

    # Patch cloud_client.get_filter_contexts method to load filter contexts from JSON file
    def mock_get_filter_contexts():
        with open(f"{CLOUD_OBJECTS_DIR}/filter_contexts.json", "r") as file:
            raw_filter_contexts = json.load(file)
        return raw_filter_contexts

    mocker.patch.object(
        scheduled_exports_migrator.context.backends.cloud_client,
        "get_filter_contexts",
        side_effect=mock_get_filter_contexts,
    )

    # Patch cloud_client.get_attribute_json method to load attributes from JSON file
    def mock_get_attribute_json(attribute_id):
        with open(f"{CLOUD_OBJECTS_DIR}/attributes.json", "r") as file:
            raw_attributes = json.load(file)
        return raw_attributes[attribute_id]

    mocker.patch.object(
        scheduled_exports_migrator.context.backends.cloud_client,
        "get_attribute_json",
        side_effect=mock_get_attribute_json,
    )

    # Load Legacy objects by URI. The mocked Legacy method will return objects from this file.
    with open(f"{LEGACY_OBJECTS_DIR}/objects_by_uri.json", "r") as file:
        objects_by_uri = json.load(file)

    def get_objects_by_uri(uri):
        return objects_by_uri[uri]

    mocker.patch.object(
        scheduled_exports_migrator.context.backends.legacy_client,
        "get_object",
        side_effect=get_objects_by_uri,
    )

    mocker.patch.object(
        scheduled_exports_migrator.context.backends.legacy_client,
        "get_dashboard_objects",
        return_value=legacy_dashboards_for_exports,
    )

    return scheduled_exports_migrator


@pytest.fixture
def legacy_dashboards_for_exports():
    """Load Legacy dashboards for exports."""
    with open(f"{LEGACY_OBJECTS_DIR}/dashboards.json", "r") as file:
        raw_legacy_dashboards = json.load(file)
        return [
            AnalyticalDashboardWrapper(**dashboard)
            for dashboard in raw_legacy_dashboards
        ]


@pytest.fixture
def automation_layout_factory():
    """Factory for creating sample automation layout data.

    Returns a callable that creates automation layouts with specified count.

    Usage:
        layout = automation_layout_factory(count=3)
        empty_layout = automation_layout_factory(count=0)
    """

    def _create_layout(count: int) -> list[dict]:
        """Create automation layout with specified count.

        Args:
            count: Number of automations to generate (1-indexed)

        Returns:
            List of automation dictionaries
        """
        return [
            {
                "id": f"automation_{i}",
                "createdBy": {"id": f"old_author_{i}"},
                "type": "scheduled_export",
            }
            for i in range(1, count + 1)
        ]

    return _create_layout
