# (C) 2026 GoodData Corporation
from dataclasses import dataclass

from gooddata_platform2cloud.backends.platform.client import PlatformClient


@dataclass(frozen=True)
class CloudModelBuilderConfig:
    """
    The CloudModelBuilderConfig class contains the configuration
    required to build a Cloud model.
    """

    data_source_id: str
    schema: str
    table_prefix: str
    ws_data_filter_id: str | None
    ws_data_filter_column: str | None
    ws_data_filter_description: str | None
    platform_client: PlatformClient
    ignore_folders: bool
    ignore_explicit_mapping: bool
