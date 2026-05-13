# (C) 2026 GoodData Corporation
"""
This module contains fixtures for the dashboards migration tests.
"""

import json

import pytest

from gooddata_legacy2cloud.backends.cloud.client import CloudClient
from gooddata_legacy2cloud.backends.legacy.client import LegacyClient
from gooddata_legacy2cloud.dashboards.cloud_dashboards_builder import (
    CloudDashboardsBuilder,
)
from gooddata_legacy2cloud.dashboards.data_classes import DashboardContext
from gooddata_legacy2cloud.id_mappings import IdMappings
from tests.conftest import MAPPING_FILES_DIR

LEGACY_OBJECTS_DIR = "tests/data/dashboards/legacy_objects"


@pytest.fixture
def dashboards_context(legacy_client: LegacyClient, cloud_client: CloudClient, mocker):
    """Create Context for dashboards builder with mocked APIs."""
    # Load Legacy objects by URI mapping
    with open(f"{LEGACY_OBJECTS_DIR}/objects_by_uri.json", "r") as file:
        objects_by_uri = json.load(file)

    def get_objects_by_uri(uri):
        return objects_by_uri[uri]

    mocker.patch.object(legacy_client, "get_object", side_effect=get_objects_by_uri)

    # Mock Cloud filter context methods
    def create_filter_context(filter_context_object):
        pass  # Mock implementation

    def update_filter_context(filter_context_object):
        pass  # Mock implementation

    mocker.patch.object(cloud_client, "get_filter_context", return_value={})
    mocker.patch.object(
        cloud_client, "create_filter_context", side_effect=create_filter_context
    )
    mocker.patch.object(
        cloud_client, "update_filter_context", side_effect=update_filter_context
    )

    # Mock Cloud get_dashboards
    mocker.patch.object(cloud_client, "get_dashboards", return_value=[])

    # Mock Cloud get_insights (used by PeriodComparisonInsight for KPI widgets)
    mocker.patch.object(cloud_client, "get_insights", return_value=[])

    # NOTE: Mock create_insight because PeriodComparisonInsight.create_or_update_insight_from_kpi()
    #       doesn't respect skip_deploy=True. When processing KPI widgets in dashboards, it always
    #       calls process_objects() which attempts to create/update insights. Even though
    #       skip_deploy=True is passed to CloudDashboard, the insight creation happens
    #       unconditionally. This mock prevents actual API calls during tests.
    # TODO: Consider updating PeriodComparisonInsight to respect skip_deploy parameter
    #       (similar to how filter contexts are handled in CloudDashboard.__init__)
    mocker.patch.object(
        cloud_client,
        "create_insight",
        return_value=type("MockResponse", (), {"ok": True, "status_code": 201})(),
    )

    # Initialize mappings
    ldm_mappings = IdMappings(f"{MAPPING_FILES_DIR}/ldm_mappings.csv")
    metric_mappings = IdMappings(f"{MAPPING_FILES_DIR}/metric_mappings.csv")
    insight_mappings = IdMappings(f"{MAPPING_FILES_DIR}/insight_mappings.csv")
    dashboard_mappings = IdMappings(f"{MAPPING_FILES_DIR}/dashboard_mappings.csv")

    # Create mapping logger - use mock to avoid overwriting test mapping files
    mapping_logger = mocker.MagicMock()

    return DashboardContext(
        legacy_client=legacy_client,
        cloud_client=cloud_client,
        ldm_mappings=ldm_mappings,
        metric_mappings=metric_mappings,
        insight_mappings=insight_mappings,
        mapping_logger=mapping_logger,
        dashboard_mappings=dashboard_mappings,
        suppress_warnings=False,
        client_prefix=None,
        current_batch_dashboard_mappings=None,
        dashboard_type="analyticalDashboard",
    )


@pytest.fixture
def dashboards_context_keep_ids(
    legacy_client: LegacyClient, cloud_client: CloudClient, mocker
):
    """Create DashboardContext with keep_original_ids=True for testing."""
    with open(f"{LEGACY_OBJECTS_DIR}/objects_by_uri.json", "r") as file:
        objects_by_uri = json.load(file)

    def get_objects_by_uri(uri):
        return objects_by_uri[uri]

    mocker.patch.object(legacy_client, "get_object", side_effect=get_objects_by_uri)
    mocker.patch.object(cloud_client, "get_filter_context", return_value={})
    mocker.patch.object(cloud_client, "create_filter_context", return_value=None)
    mocker.patch.object(cloud_client, "update_filter_context", return_value=None)
    mocker.patch.object(cloud_client, "get_dashboards", return_value=[])
    mocker.patch.object(cloud_client, "get_insights", return_value=[])
    mocker.patch.object(
        cloud_client,
        "create_insight",
        return_value=type("MockResponse", (), {"ok": True, "status_code": 201})(),
    )

    ldm_mappings = IdMappings(f"{MAPPING_FILES_DIR}/ldm_mappings.csv")
    metric_mappings = IdMappings(f"{MAPPING_FILES_DIR}/metric_mappings.csv")
    insight_mappings = IdMappings(f"{MAPPING_FILES_DIR}/insight_mappings.csv")
    dashboard_mappings = IdMappings(f"{MAPPING_FILES_DIR}/dashboard_mappings.csv")
    mapping_logger = mocker.MagicMock()

    return DashboardContext(
        legacy_client=legacy_client,
        cloud_client=cloud_client,
        ldm_mappings=ldm_mappings,
        metric_mappings=metric_mappings,
        insight_mappings=insight_mappings,
        mapping_logger=mapping_logger,
        dashboard_mappings=dashboard_mappings,
        suppress_warnings=False,
        client_prefix=None,
        current_batch_dashboard_mappings=None,
        dashboard_type="analyticalDashboard",
        keep_original_ids=True,
    )


@pytest.fixture
def dashboards_builder(dashboards_context):
    """Create CloudDashboardsBuilder instance."""
    return CloudDashboardsBuilder(dashboards_context)
