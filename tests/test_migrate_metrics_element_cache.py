# (C) 2026 GoodData Corporation
"""The Legacy attribute elements cache is only loaded when there are metrics to migrate.

Client workspaces migrated with --client-prefix usually have no custom metrics
left after filtering out the mapped (default) ones; the slow cache fetch must
be skipped for them.
"""

from typing import cast

import pytest
from pytest_mock import MockerFixture

from gooddata_legacy2cloud.config.configuration_objects import MetricConfig
from gooddata_legacy2cloud.workflows import migrate_metrics as workflow


@pytest.mark.parametrize(
    ("legacy_metrics", "cache_loaded"),
    [([], False), ([{"metric": {"meta": {"identifier": "m1"}}}], True)],
)
def test_element_cache_only_loaded_with_metrics(
    legacy_metrics: list[dict], cache_loaded: bool, mocker: MockerFixture
) -> None:
    for name in (
        "EnvVars",
        "CloudClient",
        "IdMappings",
        "OutputWriter",
        "FilterParameters",
        "CloudMetricsBuilder",
        "process_objects",
        "set_output_files_prefix",
        "format_mapping_files_info",
    ):
        mocker.patch.object(workflow, name)
    mocker.patch.object(
        workflow, "get_mapping_files", return_value=(["metric_mappings.csv"], [])
    )
    mocker.patch.object(
        workflow, "fetch_objects_with_filters", return_value=legacy_metrics
    )
    legacy_client = mocker.patch.object(workflow, "LegacyClient").return_value

    config = mocker.MagicMock()
    config.validation_element_lookup = True
    config.element_values_prefetch = False
    config.object_filter_config.without_mapped_objects = None
    config.object_migration_config.dump_legacy = False
    config.object_migration_config.dump_cloud = False
    config.object_migration_config.cleanup_target_env = False
    config.common_config.skip_deploy = True

    workflow.migrate_metrics(cast(MetricConfig, config))

    assert legacy_client.initialize_attribute_elements_cache.called is cache_loaded
