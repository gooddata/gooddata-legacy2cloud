# (C) 2026 GoodData Corporation
"""
This test aims to verify the transformation of Legacy LDM to Cloud LDM.

All calls to Legacy are mocked, the data is loaded from JSON files stored
in the `tests/data/ldm` directory.

The test verifies that the transformation of Legacy LDM metadata matches
the expected Cloud LDM metadata.
"""

import pytest
from pytest import CaptureFixture

from gooddata_legacy2cloud.ldm.cloud_model_builder import CloudModelBuilder
from tests.test_utils import dicts_are_equal, load_json

TEST_CASES_DIR = "tests/data/ldm/test_cases"


@pytest.mark.parametrize(
    "case_file_name",
    [
        "basic_ldm",
    ],
)
def test_ldm_migration(
    case_file_name: str,
    ldm_builder: CloudModelBuilder,
    capsys: CaptureFixture[str],
) -> None:
    """Test the transformation of Legacy LDM to Cloud LDM.

    To add a test case, add its name to the test parameters and create two files
    in the `tests/data/ldm/test_cases` directory:
    - <case_file_name>_legacy.json - Legacy LDM metadata
    - <case_file_name>_cloud.json - Expected Cloud LDM metadata
    """

    # Load Legacy LDM model
    legacy_model = load_json(f"{TEST_CASES_DIR}/{case_file_name}_legacy.json")
    assert isinstance(legacy_model, dict), "Legacy model should be a dict"

    # Load expected Cloud LDM model
    expected_cloud_model = load_json(f"{TEST_CASES_DIR}/{case_file_name}_cloud.json")
    assert isinstance(expected_cloud_model, dict), "Cloud model should be a dict"

    # Load Legacy model into builder
    ldm_builder.load_legacy_model(legacy_model)

    # Get Cloud model
    actual_cloud_model = ldm_builder.get_model()

    # Compare the actual data with expected_cloud_model
    dicts_are_equal(actual_cloud_model, expected_cloud_model)
    dicts_are_equal(expected_cloud_model, actual_cloud_model)

    # Fail if any ERROR messages were printed
    captured = capsys.readouterr()
    output = captured.out + captured.err
    if "ERROR" in output:
        pytest.fail(
            f"Processing errors encountered in output:\n"
            f"stdout: {captured.out}\n"
            f"stderr: {captured.err}"
        )
