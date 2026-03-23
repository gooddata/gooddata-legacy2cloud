# (C) 2026 GoodData Corporation

from dataclasses import dataclass, field

from gooddata_platform2cloud.backends.cloud.client import CloudClient
from gooddata_platform2cloud.backends.platform.client import PlatformClient
from gooddata_platform2cloud.id_mappings import IdMappings
from gooddata_platform2cloud.output_writer import OutputWriter


@dataclass(frozen=True)
class DashboardContext:
    platform_client: PlatformClient
    cloud_client: CloudClient
    ldm_mappings: IdMappings
    metric_mappings: IdMappings
    insight_mappings: IdMappings
    mapping_logger: OutputWriter
    dashboard_mappings: IdMappings | None = field(default=None)
    suppress_warnings: bool = field(
        default=False
    )  # suppress migration warnings from objects
    client_prefix: str | None = field(default=None)
    current_batch_dashboard_mappings: dict[str, str] | None = field(default=None)
    dashboard_type: str = field(default="analyticsDashboard")
