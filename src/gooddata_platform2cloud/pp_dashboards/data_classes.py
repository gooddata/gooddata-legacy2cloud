# (C) 2026 GoodData Corporation
"""Data classes for pixel perfect dashboard migration."""

from dataclasses import dataclass, field

from gooddata_platform2cloud.backends.cloud.client import CloudClient
from gooddata_platform2cloud.backends.platform.client import PlatformClient
from gooddata_platform2cloud.id_mappings import IdMappings
from gooddata_platform2cloud.output_writer import OutputWriter


@dataclass
class PPDashboardContext:
    """Context for pixel perfect dashboard migration containing all required dependencies."""

    platform_client: PlatformClient
    cloud_client: CloudClient
    ldm_mappings: IdMappings
    metric_mappings: IdMappings
    report_mappings: IdMappings
    dashboard_mappings: IdMappings | None = field(default=None)
    mapping_logger: OutputWriter | None = field(default=None)
    transformation_logger: OutputWriter | None = field(default=None)
    suppress_warnings: bool = field(default=False)
    client_prefix: str | None = field(default=None)
    exclude_tabs: list[str] | None = field(default=None)
