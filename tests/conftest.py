# (C) 2026 GoodData Corporation
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest

from gooddata_legacy2cloud.backends.cloud.client import CloudClient
from gooddata_legacy2cloud.backends.legacy.client import LegacyClient

MAPPING_FILES_DIR = "tests/data/shared/mapping_files"

# Load migration-specific fixtures
pytest_plugins = [
    "tests.fixtures.scheduled_exports_fixtures",
    "tests.fixtures.pp_dashboards_fixtures",
    "tests.fixtures.ldm_fixtures",
    "tests.fixtures.metrics_fixtures",
    "tests.fixtures.insights_fixtures",
    "tests.fixtures.dashboards_fixtures",
    "tests.fixtures.reports_fixtures",
]


@pytest.fixture(autouse=True)
def reset_output_prefix(monkeypatch):
    import gooddata_legacy2cloud.helpers as helpers

    monkeypatch.setattr(helpers, "OUTPUT_FILES_PREFIX", "")


@pytest.fixture(autouse=True)
def mock_output_writer(mocker):
    mocker.patch("gooddata_legacy2cloud.metrics.cloud_metrics_builder.OutputWriter")
    mocker.patch("gooddata_legacy2cloud.insights.cloud_insights_builder.OutputWriter")
    mocker.patch("gooddata_legacy2cloud.reports.cloud_reports_builder.OutputWriter")
    mocker.patch(
        "gooddata_legacy2cloud.dashboards.cloud_dashboards_builder.OutputWriter"
    )


# Define common fixtures here:
@pytest.fixture
def legacy_client():
    return LegacyClient(
        domain="https://example.legacy.domain.com", pid="", login="", password=""
    )


@pytest.fixture
def cloud_client():
    return CloudClient(domain="", ws="", token="")
