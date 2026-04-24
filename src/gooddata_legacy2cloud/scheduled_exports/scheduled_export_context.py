# (C) 2026 GoodData Corporation
import attrs

from gooddata_legacy2cloud.backends.cloud.client import CloudClient
from gooddata_legacy2cloud.backends.legacy.client import LegacyClient
from gooddata_legacy2cloud.id_mappings import IdMappings
from gooddata_legacy2cloud.output_writer import OutputWriter


@attrs.define
class Backends:
    legacy_client: LegacyClient
    cloud_client: CloudClient


@attrs.define
class Mappings:
    ldm_mappings: IdMappings
    metric_mappings: IdMappings
    insight_mappings: IdMappings
    dashboard_mappings: IdMappings
    scheduled_export_mappings: IdMappings


@attrs.define
class Logging:
    mapping_logger: OutputWriter
    output_logger: OutputWriter


@attrs.define
class CommandLineArguments:
    dump_legacy: bool
    legacy_dump_file: str
    dump_cloud: bool
    cloud_dump_file: str
    cleanup_target_env: bool
    skip_deploy: bool
    overwrite_existing: bool
    client_prefix: str | None


@attrs.define
class ScheduledExportsContext:
    input_file: str | None
    notification_channel_id: str = attrs.field()
    backends: Backends
    mappings: Mappings
    logging: Logging
    command_line_arguments: CommandLineArguments

    @notification_channel_id.validator  # type: ignore
    def _validate_notification_channel_id(self, _attribute, value):
        if not value:
            raise ValueError("Notification channel ID is not set!")
