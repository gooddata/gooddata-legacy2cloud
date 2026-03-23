# (C) 2026 GoodData Corporation
"""
This module contains fixtures for the insights migration tests.
"""

import json

import pytest

from gooddata_platform2cloud.backends.cloud.client import CloudClient
from gooddata_platform2cloud.backends.platform.client import PlatformClient
from gooddata_platform2cloud.id_mappings import IdMappings
from gooddata_platform2cloud.insights.cloud_insights_builder import CloudInsightsBuilder
from gooddata_platform2cloud.insights.data_classes import InsightContext
from tests.conftest import MAPPING_FILES_DIR

PLATFORM_OBJECTS_DIR = "tests/data/insights/platform_objects"


@pytest.fixture
def insights_context(
    platform_client: PlatformClient, cloud_client: CloudClient, mocker
):
    """Create Context for insights builder with mocked APIs."""
    # Load Platform objects by URI mapping
    with open(f"{PLATFORM_OBJECTS_DIR}/objects_by_uri.json", "r") as file:
        objects_by_uri = json.load(file)

    def get_objects_by_uri(uri):
        return objects_by_uri[uri]

    mocker.patch.object(platform_client, "get_object", side_effect=get_objects_by_uri)

    # Mock Cloud get_attribute_json
    mocker.patch.object(cloud_client, "get_attribute_json", return_value={})

    # Initialize mappings
    ldm_mappings = IdMappings(f"{MAPPING_FILES_DIR}/ldm_mappings.csv")
    metric_mappings = IdMappings(f"{MAPPING_FILES_DIR}/metric_mappings.csv")

    # Create mapping logger - use mock to avoid overwriting test mapping files
    mapping_logger = mocker.MagicMock()

    return InsightContext(
        platform_client=platform_client,
        cloud_client=cloud_client,
        ldm_mappings=ldm_mappings,
        metric_mappings=metric_mappings,
        mapping_logger=mapping_logger,
        report_mappings=None,
        suppress_warnings=False,
        client_prefix=None,
    )


@pytest.fixture
def insights_builder(insights_context):
    """Create CloudInsightsBuilder instance."""
    return CloudInsightsBuilder(insights_context)
