# (C) 2026 GoodData Corporation
import pytest
from pydantic import ValidationError

from gooddata_platform2cloud.config.configuration_objects import (
    MetricConfig,
    ScheduledExportConfig,
    WorkspaceConfig,
    is_client_prefix_used,
    is_mapping_params_used,
)
from gooddata_platform2cloud.config.shared_configs import (
    CommonConfig,
    ObjectFilterConfig,
    ObjectMigrationConfig,
)
from gooddata_platform2cloud.constants import (
    LDM_MAPPING_FILE,
    METRIC_MAPPING_FILE,
)


def test_is_client_prefix_used():
    # Test with empty string (default)
    common_empty = CommonConfig(client_prefix="")

    assert is_client_prefix_used(common_empty) is False

    # Test with non-empty string
    common_prefix = CommonConfig(client_prefix="test_")

    assert is_client_prefix_used(common_prefix) is True


def test_is_mapping_params_used_defaults():
    config = MetricConfig(
        workspace_config=WorkspaceConfig(),
        common_config=CommonConfig(),
        object_migration_config=ObjectMigrationConfig(),
        object_filter_config=ObjectFilterConfig(),
        ldm_mapping_file=[LDM_MAPPING_FILE],
        metric_mapping_file=[METRIC_MAPPING_FILE],
    )
    assert is_mapping_params_used(config) is False


def test_is_mapping_params_used_custom():
    config = MetricConfig(
        workspace_config=WorkspaceConfig(),
        common_config=CommonConfig(),
        object_migration_config=ObjectMigrationConfig(),
        object_filter_config=ObjectFilterConfig(),
        ldm_mapping_file=["custom_ldm.csv"],
    )
    assert is_mapping_params_used(config) is True


def test_validate_config_client_prefix_conflict():
    common = CommonConfig(client_prefix="test_")
    workspace = WorkspaceConfig(platform_ws="b", cloud_ws="p")
    # Non-default mapping file
    # We expect ValidationError because validate_config is called in model_post_init
    with pytest.raises(
        ValidationError, match="`client-prefix` cannot be used together"
    ):
        MetricConfig(
            workspace_config=workspace,
            common_config=common,
            object_migration_config=ObjectMigrationConfig(),
            object_filter_config=ObjectFilterConfig(),
            ldm_mapping_file=["custom_ldm.csv"],
        )


def test_validate_config_missing_workspaces():
    # Missing platform_ws and cloud_ws when client_prefix is used
    common = CommonConfig(client_prefix="test_")
    with pytest.raises(
        ValidationError,
        match="`client-prefix` requires both `platform-ws` and `cloud-ws`",
    ):
        MetricConfig(
            workspace_config=WorkspaceConfig(),
            common_config=common,
            object_migration_config=ObjectMigrationConfig(),
            object_filter_config=ObjectFilterConfig(),
        )


def test_validate_config_side_effects_metric():
    common = CommonConfig(client_prefix="test_")
    workspace = WorkspaceConfig(platform_ws="b", cloud_ws="p")
    config = MetricConfig(
        workspace_config=workspace,
        common_config=common,
        object_migration_config=ObjectMigrationConfig(),
        object_filter_config=ObjectFilterConfig(),
    )

    # After validation (which happened in model_post_init)
    assert config.common_config.output_files_prefix == "test_"
    assert config.common_config.check_parent_workspace is True
    assert config.object_filter_config.without_mapped_objects == "default_only"


def test_validate_config_side_effects_scheduled_export():
    common = CommonConfig(client_prefix="test_")
    workspace = WorkspaceConfig(platform_ws="b", cloud_ws="p")
    config = ScheduledExportConfig(
        workspace_config=workspace,
        common_config=common,
        object_migration_config=ObjectMigrationConfig(),
    )

    # ScheduledExportConfig doesn't have object_filter_config
    # Validation happened in model_post_init
    assert config.common_config.output_files_prefix == "test_"
    assert config.common_config.check_parent_workspace is True
