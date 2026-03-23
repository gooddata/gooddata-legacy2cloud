# (C) 2026 GoodData Corporation
"""
This module contains fixtures for the metrics migration tests.
"""

import json

import pytest

from gooddata_platform2cloud.backends.cloud.client import CloudClient
from gooddata_platform2cloud.backends.platform.client import PlatformClient
from gooddata_platform2cloud.id_mappings import IdMappings
from gooddata_platform2cloud.metrics.cloud_metrics_builder import CloudMetricsBuilder
from gooddata_platform2cloud.metrics.data_classes import MetricContext
from tests.conftest import MAPPING_FILES_DIR

PLATFORM_OBJECTS_DIR = "tests/data/metrics/platform_objects"


@pytest.fixture
def metrics_context(platform_client: PlatformClient, cloud_client: CloudClient, mocker):
    """Create Context for metrics builder with mocked APIs."""
    # Load Platform objects by URI mapping
    with open(f"{PLATFORM_OBJECTS_DIR}/objects_by_uri.json", "r") as file:
        objects_by_uri = json.load(file)

    def get_objects_by_uri(uri):
        return objects_by_uri[uri]

    mocker.patch.object(platform_client, "get_object", side_effect=get_objects_by_uri)

    # Initialize mappings
    ldm_mappings = IdMappings(f"{MAPPING_FILES_DIR}/ldm_mappings.csv")

    # Create mapping logger - use mock to avoid overwriting test mapping files
    mapping_logger = mocker.MagicMock()

    return MetricContext(
        platform_client=platform_client,
        cloud_client=cloud_client,
        ldm_mappings=ldm_mappings,
        mapping_logger=mapping_logger,
        keep_original_ids=False,
        ignore_folders=False,
        suppress_warnings=False,
        client_prefix=None,
    )


@pytest.fixture
def metrics_builder(metrics_context):
    """Create CloudMetricsBuilder instance."""
    return CloudMetricsBuilder(metrics_context)
