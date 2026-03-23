# (C) 2026 GoodData Corporation
"""
This test aims to verify the transformation of Platform scheduled exports to Cloud.

All calls to Platform and Cloud are mocked, the data is loaded from JSON files stored
in the `tests/data/scheduled_exports` directory.

The test verifies that the transformation of Platform scheduled exports metadata matches
the expected Cloud metadata.
"""

import pytest

from gooddata_platform2cloud.id_mappings import IdMappings
from gooddata_platform2cloud.models.enums import Action
from gooddata_platform2cloud.scheduled_exports.scheduled_export_context import (
    Backends,
    CommandLineArguments,
    Logging,
    Mappings,
    ScheduledExportsContext,
)
from tests.test_utils import dicts_are_equal, load_json

TEST_CASES_DIR = "tests/data/scheduled_exports/test_cases"


@pytest.mark.parametrize(
    "case_file_name",
    [
        "csv_default_filters",
        "csv_absolute_date_filter",
        "csv_all_custom",
        "csv_custom_filters",
        "csv_custom_category_and_name",
        "xlsx_default_filters",
        "pdf_default_filters",
        "pdf_absolute_date_filter",
        "pdf_custom_date",
        "pdf_custom_date_and_filter",
        "pdf_all_custom",
        "pdf_only_accessories",
        "two_csvs_one_pdf",
        "dashboard_filters_with_default_values",
        "dashboard_filters_and_cutom_values",
    ],
)
def test_scheduled_exports_migration(
    case_file_name,
    scheduled_exports_migrator,
    mocker,
    caplog,
):
    """Test the transformation of Platform scheduled exports to Cloud.

    To add a test case, add its name to the test parameters and create two files
    in the `tests/data/scheduled_exports/test_cases` directory:
    - <case_file_name>_platform.json - Platform scheduled email metadata
    - <case_file_name>_cloud.json - Expected Cloud metadata

    Make sure to provide any additional metadata objects needed for processing of
    the new test cases. The tests will fail without it, usually on a KeyError.
    """

    # Patch the _set_automation_author
    mocker.patch.object(
        scheduled_exports_migrator, "_set_automation_author", return_value=None
    )

    # Load Platform scheduled email
    platform_metadata_data = load_json(
        f"{TEST_CASES_DIR}/{case_file_name}_platform.json"
    )
    assert isinstance(platform_metadata_data, list), (
        "Platform metadata should be a list"
    )
    platform_metadata: list[dict] = platform_metadata_data

    # Load expected Cloud metadata (the result of the migration)
    expected_cloud_metadata_data = load_json(
        f"{TEST_CASES_DIR}/{case_file_name}_cloud.json"
    )
    assert isinstance(expected_cloud_metadata_data, dict), (
        "Cloud metadata should be a dict"
    )
    expected_cloud_metadata: dict = expected_cloud_metadata_data

    mocker.patch.object(
        scheduled_exports_migrator.context.backends.platform_client,
        "get_objects_by_category",
        return_value=platform_metadata,
    )

    # Mocke the deployment function to compare the metadata instead
    def check_post_data(automation_id, raw_data, _action):
        """Mocked _post_data method to evaluate the metadata transformation"""
        print(f"Running check_post_data for {automation_id}")
        data = {"data": raw_data}
        # Compare the data with expected_cloud_metadata and fail if not equal
        # Run the comparison both ways to ensure that nothing is missing from the
        # actual data.
        dicts_are_equal(data, expected_cloud_metadata)
        dicts_are_equal(expected_cloud_metadata, data)

    mocker.patch.object(
        scheduled_exports_migrator, "_create_or_update_automation", check_post_data
    )

    # Run the migration script
    scheduled_exports_migrator.migrate()

    # Fail if any ERROR level logs were captured
    errors = [
        record.message for record in caplog.records if record.levelname == "ERROR"
    ]
    if errors:
        pytest.fail("Processing errors encountered:\n" + "\n".join(errors))


@pytest.mark.parametrize("notification_cahnnel_id", [None, ""])
def test_scheduled_exports_migration_falsey_notification_channel_id(
    notification_cahnnel_id, platform_client, cloud_client, mocker
) -> None:
    """Test that the context object raises a ValueError if the notification
    channel id is missing (None or empty string).

    Notification channel ID is required for the migration to proceed.
    """

    with pytest.raises(ValueError, match="Notification channel ID is not set!"):
        ScheduledExportsContext(
            input_file=None,
            notification_channel_id=notification_cahnnel_id,
            backends=Backends(
                platform_client=platform_client, cloud_client=cloud_client
            ),
            mappings=Mappings(
                ldm_mappings=IdMappings(
                    "tests/data/shared/mapping_files/ldm_mappings.csv"
                ),
                metric_mappings=IdMappings(
                    "tests/data/shared/mapping_files/metric_mappings.csv"
                ),
                insight_mappings=IdMappings(
                    "tests/data/scheduled_exports/mapping_files/insight_mappings.csv"
                ),
                dashboard_mappings=IdMappings(
                    "tests/data/scheduled_exports/mapping_files/dashboard_mappings.csv"
                ),
                scheduled_export_mappings=IdMappings(
                    "tests/data/scheduled_exports/mapping_files/scheduled_export_mappings.csv"
                ),
            ),
            logging=Logging(
                mapping_logger=mocker.MagicMock(), output_logger=mocker.MagicMock()
            ),
            command_line_arguments=CommandLineArguments(
                dump_platform=False,
                platform_dump_file="",
                dump_cloud=False,
                cloud_dump_file="",
                cleanup_target_env=False,
                skip_deploy=False,
                overwrite_existing=False,
                client_prefix="",
            ),
        )


@pytest.mark.parametrize(
    ("case_file_name", "expected_warning"),
    [
        ("no_recipients", "Skipping email"),
        ("no_attachments", "Skipping email"),
        ("missing_user", "missing.user@gooddata.com not found"),
    ],
)
def test_intentional_skipping(
    case_file_name,
    expected_warning,
    scheduled_exports_migrator,
    mocker,
    monkeypatch,
    caplog,
) -> None:
    """In some cases, migration of an export can be skipped.

    - When migrated email has no recipients or attachments, the entire export is skipped.
    - When migrated email has a recipient that is not found in Cloud, the recipient is skipped.
    """

    # Load Platform scheduled email
    platform_metadata_data = load_json(
        f"{TEST_CASES_DIR}/{case_file_name}_platform.json"
    )
    assert isinstance(platform_metadata_data, list), (
        "Platform metadata should be a list"
    )
    platform_metadata: list[dict] = platform_metadata_data

    # Patch the _set_automation_author
    mocker.patch.object(
        scheduled_exports_migrator, "_set_automation_author", return_value=None
    )

    mocker.patch.object(
        scheduled_exports_migrator.context.backends.platform_client,
        "get_objects_by_category",
        return_value=platform_metadata,
    )

    # Skip the deployment. In case of missing user, the script would go on with
    # the export. Deployment would fail (expected as the mocked Cloud client does
    # not support it) but it would take some time to do so.
    monkeypatch.setattr(
        scheduled_exports_migrator.context.command_line_arguments,
        "skip_deploy",
        True,
    )

    scheduled_exports_migrator.migrate()
    # Check for membership beacause of ANSI coloring
    warnings = [
        record.message for record in caplog.records if "WARNING" in record.levelname
    ]

    assert len(warnings) == 1
    assert expected_warning in warnings[0]


def test_with_overwrite_existing(scheduled_exports_migrator, mocker) -> None:
    """Test that counts CREATE and UPDATE actions for _create_or_update_automation."""

    # Patch the overwrite_existing attribute to True
    scheduled_exports_migrator.context.command_line_arguments.overwrite_existing = True

    # Patch the _set_automation_author
    mocker.patch.object(
        scheduled_exports_migrator, "_set_automation_author", return_value=None
    )

    csv_data = load_json(f"{TEST_CASES_DIR}/csv_default_filters_platform.json")
    pdf_all_data = load_json(f"{TEST_CASES_DIR}/pdf_all_custom_platform.json")
    pdf_absolute_data = load_json(
        f"{TEST_CASES_DIR}/pdf_absolute_date_filter_platform.json"
    )
    assert isinstance(csv_data, list), "Platform metadata should be a list"
    assert isinstance(pdf_all_data, list), "Platform metadata should be a list"
    assert isinstance(pdf_absolute_data, list), "Platform metadata should be a list"
    platform_metadata: list[dict] = csv_data + pdf_all_data + pdf_absolute_data

    mocker.patch.object(
        scheduled_exports_migrator.context.backends.platform_client,
        "get_objects_by_category",
        return_value=platform_metadata,
    )

    # Mock existing upstream automation
    class MockCatalogDeclarativeAutomation:
        def __init__(self, id: str):
            self.id = id

    cloud_metadata_data = load_json(f"{TEST_CASES_DIR}/pdf_all_custom_cloud.json")
    assert isinstance(cloud_metadata_data, dict), "Cloud metadata should be a dict"
    cloud_metadata: dict = cloud_metadata_data

    mocker.patch.object(
        scheduled_exports_migrator.context.backends.cloud_client.sdk.catalog_workspace,
        "get_declarative_automations",
        return_value=[MockCatalogDeclarativeAutomation(cloud_metadata["data"]["id"])],
    )

    # Dict to count the number of calls
    call_counts = {"create": 0, "update": 0}

    def count_operations(_automation_id, _raw_data, action: Action) -> None:
        if action == Action.CREATE:
            call_counts["create"] += 1
        elif action == Action.UPDATE:
            call_counts["update"] += 1

    mocker.patch.object(
        scheduled_exports_migrator, "_create_or_update_automation", count_operations
    )

    scheduled_exports_migrator.migrate()

    assert call_counts["create"] == 2
    assert call_counts["update"] == 1


def test_set_automation_author_updates_on_mismatch(
    scheduled_exports_migrator, mocker, automation_layout_factory
) -> None:
    """PUT is called only when the author differs, and only the target automation is changed."""
    mock_automations_layout = automation_layout_factory(count=3)

    mock_get = mocker.patch.object(
        scheduled_exports_migrator.context.backends.cloud_client,
        "get_automations_layout",
        return_value=mock_automations_layout,
    )
    mock_put = mocker.patch.object(
        scheduled_exports_migrator.context.backends.cloud_client,
        "put_automations_layout",
    )

    scheduled_exports_migrator._set_automation_author("automation_2", "new_author_2")

    mock_get.assert_called_once()
    mock_put.assert_called_once()
    updated_layout = mock_put.call_args[0][0]

    assert updated_layout[0]["createdBy"]["id"] == "old_author_1"
    assert updated_layout[1]["createdBy"]["id"] == "new_author_2"
    assert updated_layout[2]["createdBy"]["id"] == "old_author_3"


def test_set_automation_author_no_put_if_not_found(
    scheduled_exports_migrator, mocker, automation_layout_factory
) -> None:
    """If the target automation is not present in layout, we do not PUT."""
    mock_automations_layout = automation_layout_factory(count=2)

    mock_get = mocker.patch.object(
        scheduled_exports_migrator.context.backends.cloud_client,
        "get_automations_layout",
        return_value=mock_automations_layout,
    )
    mock_put = mocker.patch.object(
        scheduled_exports_migrator.context.backends.cloud_client,
        "put_automations_layout",
    )

    scheduled_exports_migrator._set_automation_author(
        "automation_999", "new_author_999"
    )

    mock_get.assert_called_once()
    mock_put.assert_not_called()


def test_set_automation_author_no_put_if_author_matches(
    scheduled_exports_migrator, mocker, automation_layout_factory
) -> None:
    """If the author is already correct, we do not PUT."""
    mock_automations_layout = automation_layout_factory(count=2)

    mock_get = mocker.patch.object(
        scheduled_exports_migrator.context.backends.cloud_client,
        "get_automations_layout",
        return_value=mock_automations_layout,
    )
    mock_put = mocker.patch.object(
        scheduled_exports_migrator.context.backends.cloud_client,
        "put_automations_layout",
    )

    scheduled_exports_migrator._set_automation_author("automation_1", "old_author_1")

    mock_get.assert_called_once()
    mock_put.assert_not_called()


def test_set_automation_author_no_put_on_empty_layout(
    scheduled_exports_migrator, mocker, automation_layout_factory
) -> None:
    mock_automations_layout = automation_layout_factory(count=0)

    mock_get = mocker.patch.object(
        scheduled_exports_migrator.context.backends.cloud_client,
        "get_automations_layout",
        return_value=mock_automations_layout,
    )

    mock_put = mocker.patch.object(
        scheduled_exports_migrator.context.backends.cloud_client,
        "put_automations_layout",
    )

    scheduled_exports_migrator._set_automation_author("automation_1", "new_author_1")

    mock_get.assert_called_once()
    mock_put.assert_not_called()
