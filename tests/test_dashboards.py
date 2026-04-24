# (C) 2026 GoodData Corporation
"""
This test aims to verify the transformation of Legacy dashboards to Cloud dashboards.

All calls to Legacy and Cloud are mocked, the data is loaded from JSON files stored
in the `tests/data/dashboards` directory.

The test verifies that the transformation of Legacy dashboards metadata matches
the expected Cloud dashboards metadata.
"""

import pytest
from pytest import CaptureFixture

from gooddata_legacy2cloud.dashboards.cloud_dashboard import CloudDashboard
from gooddata_legacy2cloud.dashboards.cloud_dashboards_builder import (
    CloudDashboardsBuilder,
)
from tests.test_utils import dicts_are_equal, load_json

TEST_CASES_DIR = "tests/data/dashboards/test_cases"


@pytest.mark.parametrize(
    "case_file_name",
    [
        "basic_dashboard",
        "headlines_only",
        "dashboard_with_kpis_and_filters",
        "dashboard_with_attribute_filter_with_multiple_labels_1",
        "dashboard_with_drills",
        "dashboard_with_missing_element_lookup",
        "self_drill",
        "dashboard_with_dependent_filters",
    ],
)
def test_dashboards_migration(
    case_file_name: str,
    dashboards_builder: CloudDashboardsBuilder,
    capsys: CaptureFixture[str],
    mocker,
) -> None:
    """Test the transformation of Legacy dashboards to Cloud dashboards.

    To add a test case, add its name to the test parameters and create two files
    in the `tests/data/dashboards/test_cases` directory:
    - <case_file_name>_legacy.json - Legacy dashboards metadata
    - <case_file_name>_cloud.json - Expected Cloud dashboards metadata
    """

    # Mock _resolve_widget_type to avoid needing visualization class objects in test data.
    # This method determines widget sizing based on visualization type (headline vs other).
    if case_file_name == "headlines_only":
        mocker.patch.object(CloudDashboard, "_resolve_widget_type", return_value="kpi")
    else:
        mocker.patch.object(
            CloudDashboard, "_resolve_widget_type", return_value="insight"
        )

    # Load Legacy dashboards
    legacy_dashboards = load_json(f"{TEST_CASES_DIR}/{case_file_name}_legacy.json")
    assert isinstance(legacy_dashboards, list), "Legacy dashboards should be a list"

    # Load expected Cloud dashboards
    expected_cloud_dashboards = load_json(
        f"{TEST_CASES_DIR}/{case_file_name}_cloud.json"
    )
    assert isinstance(expected_cloud_dashboards, list), (
        "Cloud dashboards should be a list"
    )

    # Process Legacy dashboards (skip_deploy=True to avoid Cloud API calls)
    dashboards_builder.process_legacy_dashboards(
        legacy_dashboards, skip_deploy=True, overwrite_existing=False
    )

    # Get Cloud dashboards
    actual_cloud_dashboards = dashboards_builder.get_cloud_dashboards()

    # Compare the actual data with expected_cloud_dashboards
    # Sort both lists for comparison
    actual_sorted = sorted(actual_cloud_dashboards, key=lambda x: x["data"]["id"])
    expected_sorted = sorted(expected_cloud_dashboards, key=lambda x: x["data"]["id"])

    if len(actual_sorted) != len(expected_sorted):
        pytest.fail(
            f"Dashboard count mismatch: actual={len(actual_sorted)}, expected={len(expected_sorted)}"
        )

    for actual, expected in zip(actual_sorted, expected_sorted):
        dicts_are_equal(actual, expected)
        dicts_are_equal(expected, actual)

    # Fail if any ERROR messages were printed
    captured = capsys.readouterr()
    output = captured.out + captured.err
    if "ERROR" in output:
        pytest.fail(
            f"Processing errors encountered in output:\n"
            f"stdout: {captured.out}\n"
            f"stderr: {captured.err}"
        )
