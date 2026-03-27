# (C) 2026 GoodData Corporation
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest

from gooddata_platform2cloud.backends.cloud.client import CloudClient
from gooddata_platform2cloud.backends.platform.client import PlatformClient

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


# Define common fixtures here:
@pytest.fixture
def platform_client():
    return PlatformClient(
        domain="https://example.platform.domain.com", pid="", login="", password=""
    )


@pytest.fixture
def cloud_client():
    return CloudClient(domain="", ws="", token="")
