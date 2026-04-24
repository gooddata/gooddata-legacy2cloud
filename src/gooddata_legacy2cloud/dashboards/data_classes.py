# (C) 2026 GoodData Corporation

from dataclasses import dataclass, field

from gooddata_legacy2cloud.backends.cloud.client import CloudClient
from gooddata_legacy2cloud.backends.legacy.client import LegacyClient
from gooddata_legacy2cloud.id_mappings import IdMappings
from gooddata_legacy2cloud.output_writer import OutputWriter


@dataclass(frozen=True)
class DashboardContext:
    legacy_client: LegacyClient
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
