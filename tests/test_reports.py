# (C) 2026 GoodData Corporation
"""
This test aims to verify the transformation of Platform reports to Cloud reports.

All calls to Platform and Cloud are mocked, the data is loaded from JSON files stored
in the `tests/data/reports` directory.

The test verifies that the transformation of Platform reports metadata matches
the expected Cloud reports metadata.
"""

import pytest
from pytest import CaptureFixture

from gooddata_platform2cloud.reports.cloud_reports_builder import CloudReportsBuilder
from tests.test_utils import dicts_are_equal, load_json

TEST_CASES_DIR = "tests/data/reports/test_cases"


@pytest.mark.parametrize(
    "case_file_name",
    [
        "basic_reports",
        "measure_filter_granularity_subset",
        "date_null_filters",
    ],
)
def test_reports_migration(
    case_file_name: str,
    reports_builder: CloudReportsBuilder,
    capsys: CaptureFixture[str],
) -> None:
    """Test the transformation of Platform reports to Cloud reports.

    To add a test case, add its name to the test parameters and create two files
    in the `tests/data/reports/test_cases` directory:
    - <case_file_name>_platform.json - Platform reports metadata
    - <case_file_name>_cloud.json - Expected Cloud reports metadata
    """

    # Load Platform reports
    platform_reports = load_json(f"{TEST_CASES_DIR}/{case_file_name}_platform.json")
    assert isinstance(platform_reports, list), "Platform reports should be a list"

    # Load expected Cloud reports
    expected_cloud_reports = load_json(f"{TEST_CASES_DIR}/{case_file_name}_cloud.json")
    assert isinstance(expected_cloud_reports, list), "Cloud reports should be a list"

    # Process Platform reports
    reports_builder.process_platform_reports(platform_reports)

    # Get Cloud reports
    actual_cloud_reports = reports_builder.get_cloud_reports()

    # Compare the actual data with expected_cloud_reports
    # Sort both lists for comparison
    actual_sorted = sorted(actual_cloud_reports, key=lambda x: x["data"]["id"])
    expected_sorted = sorted(expected_cloud_reports, key=lambda x: x["data"]["id"])

    if len(actual_sorted) != len(expected_sorted):
        pytest.fail(
            f"Report count mismatch: actual={len(actual_sorted)}, expected={len(expected_sorted)}"
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
