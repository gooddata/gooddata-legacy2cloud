# (C) 2026 GoodData Corporation
"""
This module contains fixtures for the pixel perfect dashboards tests.
"""

import json

import pytest

from gooddata_platform2cloud.backends.cloud.client import CloudClient
from gooddata_platform2cloud.backends.platform.client import PlatformClient
from gooddata_platform2cloud.id_mappings import IdMappings
from gooddata_platform2cloud.pp_dashboards.data_classes import PPDashboardContext
from gooddata_platform2cloud.pp_dashboards.grid_maker import GridConfig


@pytest.fixture
def pp_grid_config():
    """Create a GridConfig instance for tests."""
    return GridConfig(
        canvas_width_px=940,
        columns=12,
        gutter_x_px=16,
        gutter_y_px=16,
        row_unit_px=5,
        rounding="nearest",
    )


@pytest.fixture
def pp_context(platform_client: PlatformClient, cloud_client: CloudClient, mocker):
    """Mock pixel perfect context object."""
    # Import here to avoid circular dependencies during test collection

    return PPDashboardContext(
        platform_client=platform_client,
        cloud_client=cloud_client,
        ldm_mappings=IdMappings(
            "tests/data/pixel_perfect_dashboards/mapping_files/ldm_mappings.csv"
        ),
        metric_mappings=IdMappings(
            "tests/data/pixel_perfect_dashboards/mapping_files/metric_mappings.csv"
        ),
        report_mappings=IdMappings(
            "tests/data/pixel_perfect_dashboards/mapping_files/report_mappings.csv"
        ),
        mapping_logger=mocker.MagicMock(),
        suppress_warnings=False,
        client_prefix=None,
    )


@pytest.fixture
def mock_platform_pp_dashboards(mocker, platform_client: PlatformClient):
    """Mock Platform API responses for pixel perfect dashboards."""
    # Load Platform objects by URI
    with open(
        "tests/data/pixel_perfect_dashboards/platform_objects/objects_by_uri.json", "r"
    ) as file:
        objects_by_uri = json.load(file)

    def get_object_by_uri(obj_link):
        if isinstance(obj_link, str):
            return objects_by_uri.get(obj_link, {})
        return {}

    mocker.patch.object(platform_client, "get_object", side_effect=get_object_by_uri)

    # Load dashboard data
    with open(
        "tests/data/pixel_perfect_dashboards/platform_objects/pp_dashboard_simple.json",
        "r",
    ) as file:
        dashboard_data = json.load(file)

    mocker.patch.object(
        platform_client, "get_dashboards", return_value=[dashboard_data]
    )

    return dashboard_data


@pytest.fixture
def mock_cloud_pp_api(mocker, cloud_client: CloudClient):
    """Mock Cloud API responses for pixel perfect dashboards."""
    # Mock get_insights
    with open(
        "tests/data/pixel_perfect_dashboards/cloud_objects/insights.json", "r"
    ) as file:
        insights_data = json.load(file)

    mocker.patch.object(cloud_client, "get_insights", return_value=insights_data)

    # Mock get_dashboards (for checking existing dashboards)
    mocker.patch.object(cloud_client, "get_dashboards", return_value=[])

    # Mock get_filter_contexts
    mocker.patch.object(cloud_client, "get_filter_contexts", return_value=[])

    # Mock create_dashboard - return success
    class MockResponse:
        def __init__(self, ok=True, data=None):
            self.ok = ok
            self._data = data or {}

        def json(self):
            return self._data

    def mock_create_dashboard(data):
        dashboard_id = data.get("data", {}).get("id", "test_dashboard_id")
        dashboard_title = (
            data.get("data", {}).get("attributes", {}).get("title", "Test Dashboard")
        )
        return MockResponse(
            ok=True,
            data={
                "data": {
                    "id": dashboard_id,
                    "type": "analyticalDashboard",
                    "attributes": {"title": dashboard_title},
                }
            },
        )

    mocker.patch.object(
        cloud_client, "create_dashboard", side_effect=mock_create_dashboard
    )

    # Mock create_filter_context
    def mock_create_filter_context(data):
        return MockResponse(
            ok=True,
            data={"data": {"id": "test_filter_context_id", "type": "filterContext"}},
        )

    mocker.patch.object(
        cloud_client, "create_filter_context", side_effect=mock_create_filter_context
    )

    # Mock remove_filter_context
    mocker.patch.object(cloud_client, "remove_filter_context", return_value=None)

    # Mock remove_insight
    mocker.patch.object(cloud_client, "remove_insight", return_value=None)

    # Mock create_dashboard_permissions_for_public_dashboards
    mocker.patch.object(
        cloud_client,
        "create_dashboard_permissions_for_public_dashboards",
        return_value=None,
    )

    # Mock _post used by PP VisualisationMaker (headline insights)
    class MockPostResponse:
        def __init__(self, response_json=None):
            self._response_json = response_json or {}

        def json(self):
            return self._response_json

    def mock_post(endpoint: str, data=None, headers=None):  # noqa: ARG001
        # VisualisationMaker posts to .../visualizationObjects and expects {"data": {"id": ...}}
        if "visualizationObjects" in endpoint:
            return MockPostResponse(
                response_json={"data": {"id": "ppkpinsight_test_headline_001"}}
            )
        return MockPostResponse(response_json={})

    mocker.patch.object(cloud_client, "_post", side_effect=mock_post)

    return cloud_client


@pytest.fixture
def pp_dashboards_builder(pp_context, pp_grid_config):
    """Create CloudPixelPerfectDashboardsBuilder instance for tests."""
    from gooddata_platform2cloud.pp_dashboards.cloud_pp_dashboards_builder import (
        CloudPixelPerfectDashboardsBuilder,
    )

    return CloudPixelPerfectDashboardsBuilder(
        ctx=pp_context,
        cfg=pp_grid_config,
        pixel_perfect_prefix="[PP]",
        min_text_length=5,
        supported_items=["headlineItem", "reportItem", "textItem"],
        legacy_split_tabs=False,
    )


@pytest.fixture
def pp_dashboards_builder_legacy_split(pp_context, pp_grid_config):
    """Create builder instance configured for legacy split-tabs behavior."""
    from gooddata_platform2cloud.pp_dashboards.cloud_pp_dashboards_builder import (
        CloudPixelPerfectDashboardsBuilder,
    )

    return CloudPixelPerfectDashboardsBuilder(
        ctx=pp_context,
        cfg=pp_grid_config,
        pixel_perfect_prefix="[PP]",
        min_text_length=5,
        supported_items=["headlineItem", "reportItem", "textItem"],
        legacy_split_tabs=True,
    )
