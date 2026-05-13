# (C) 2026 GoodData Corporation
"""Data classes for pixel perfect dashboard migration."""

from dataclasses import dataclass, field

from gooddata_legacy2cloud.backends.cloud.client import CloudClient
from gooddata_legacy2cloud.backends.legacy.client import LegacyClient
from gooddata_legacy2cloud.id_mappings import IdMappings
from gooddata_legacy2cloud.output_writer import OutputWriter


@dataclass
class PPDashboardContext:
    """Context for pixel perfect dashboard migration containing all required dependencies."""

    legacy_client: LegacyClient
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
    keep_original_ids: bool = field(default=False)
