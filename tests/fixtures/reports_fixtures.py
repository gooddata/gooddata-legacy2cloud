# (C) 2026 GoodData Corporation
"""
This module contains fixtures for the reports migration tests.
"""

import json

import pytest

from gooddata_legacy2cloud.backends.cloud.client import CloudClient
from gooddata_legacy2cloud.backends.legacy.client import LegacyClient
from gooddata_legacy2cloud.id_mappings import IdMappings
from gooddata_legacy2cloud.reports.cloud_reports_builder import CloudReportsBuilder
from gooddata_legacy2cloud.reports.data_classes import ReportContext
from tests.conftest import MAPPING_FILES_DIR

LEGACY_OBJECTS_DIR = "tests/data/reports/legacy_objects"


@pytest.fixture
def reports_context(legacy_client: LegacyClient, cloud_client: CloudClient, mocker):
    """Create Context for reports builder with mocked APIs."""
    # Load Legacy objects by URI mapping
    with open(f"{LEGACY_OBJECTS_DIR}/objects_by_uri.json", "r") as file:
        objects_by_uri = json.load(file)

    def get_objects_by_uri(uri):
        return objects_by_uri[uri]

    mocker.patch.object(legacy_client, "get_object", side_effect=get_objects_by_uri)

    # Mock Cloud get_attribute_json
    mocker.patch.object(cloud_client, "get_attribute_json", return_value={})

    # Initialize mappings
    ldm_mappings = IdMappings(f"{MAPPING_FILES_DIR}/ldm_mappings.csv")
    metric_mappings = IdMappings(f"{MAPPING_FILES_DIR}/metric_mappings.csv")

    # Create mapping logger - use mock to avoid overwriting test mapping files
    mapping_logger = mocker.MagicMock()

    return ReportContext(
        legacy_client=legacy_client,
        cloud_client=cloud_client,
        ldm_mappings=ldm_mappings,
        metric_mappings=metric_mappings,
        mapping_logger=mapping_logger,
        suppress_warnings=False,
        client_prefix=None,
    )


@pytest.fixture
def reports_builder(reports_context):
    """Create CloudReportsBuilder instance."""
    return CloudReportsBuilder(reports_context)
