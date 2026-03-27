# (C) 2026 GoodData Corporation
"""
This module contains fixtures for the LDM migration tests.
"""

import pytest

from gooddata_platform2cloud.ldm.cloud_model_builder import CloudModelBuilder
from gooddata_platform2cloud.ldm.model_builder_config import CloudModelBuilderConfig
from tests.test_utils import load_json

PLATFORM_OBJECTS_DIR = "tests/data/ldm/platform_objects"


@pytest.fixture
def ldm_builder_config(platform_client, mocker):
    """Create CloudModelBuilderConfig with mocked Platform API."""

    # Mock Platform methods that TagProvider needs
    def mock_get_attributes():
        return load_json(f"{PLATFORM_OBJECTS_DIR}/attributes.json")

    def mock_get_facts():
        return load_json(f"{PLATFORM_OBJECTS_DIR}/facts.json")

    mocker.patch.object(
        platform_client, "get_attributes", return_value=mock_get_attributes()
    )
    mocker.patch.object(platform_client, "get_facts", return_value=mock_get_facts())

    # Mock Platform methods that ADSMapping needs
    def mock_get_dataset_mappings():
        return load_json(f"{PLATFORM_OBJECTS_DIR}/dataset_mappings.json")

    def mock_get_output_stage():
        return load_json(f"{PLATFORM_OBJECTS_DIR}/otuput_stage.json")

    mocker.patch.object(
        platform_client,
        "get_dataset_mappings",
        return_value=mock_get_dataset_mappings(),
    )
    mocker.patch.object(
        platform_client, "get_output_stage", return_value=mock_get_output_stage()
    )

    return CloudModelBuilderConfig(
        data_source_id="gdc_csv_ds_sksgz",
        schema="Faked E-Commerce",
        table_prefix="MIGRATION_TEST_",
        ws_data_filter_id="",
        ws_data_filter_column="",
        ws_data_filter_description="",
        platform_client=platform_client,
        ignore_folders=False,
        ignore_explicit_mapping=False,
    )


@pytest.fixture
def ldm_builder(ldm_builder_config):
    """Create CloudModelBuilder instance."""
    return CloudModelBuilder(ldm_builder_config)
