# (C) 2026 GoodData Corporation
"""
Top level script for migrating scheduled exports from Platform to Cloud.
"""

import logging
from time import time

from gooddata_platform2cloud.arg_parsing.arg_parser import (
    parse_scheduled_export_cli_args,
)
from gooddata_platform2cloud.backends.cloud.client import CloudClient
from gooddata_platform2cloud.backends.platform.client import PlatformClient
from gooddata_platform2cloud.config.configuration_objects import ScheduledExportConfig
from gooddata_platform2cloud.config.env_vars import EnvVars
from gooddata_platform2cloud.helpers import (
    duration,
    prefix_filename,
    set_output_files_prefix,
)
from gooddata_platform2cloud.id_mappings import IdMappings
from gooddata_platform2cloud.logging.config import configure_logger
from gooddata_platform2cloud.mapping.mapping_utils import (
    format_mapping_files_info,
    get_mapping_files,
)
from gooddata_platform2cloud.output_writer import OutputWriter
from gooddata_platform2cloud.scheduled_exports.scheduled_export_context import (
    Backends,
    CommandLineArguments,
    Logging,
    Mappings,
    ScheduledExportsContext,
)
from gooddata_platform2cloud.scheduled_exports.scheduled_exports import (
    ScheduledExportMigrator,
)

PLATFORM_SCHEDULED_EXPORTS_FILE = "platform_scheduled_exports.json"
CLOUD_SCHEDULED_EXPORTS_FILE = "cloud_scheduled_exports.json"
SCHEDULED_EXPORTS_LOGGER_FILE = "scheduled_exports_logs.log"

logger = logging.getLogger("migration")
configure_logger()


def migrate_scheduled_exports(config: ScheduledExportConfig) -> None:
    """Entry point for the scheduled exports migration script."""
    start_time: float = time()

    # Load environment variables
    env_vars = EnvVars(config.env)
    env_vars.resolve_workspaces(config.workspace_config)
    env_vars.log_connection_info()

    # Set output files prefix from command line arguments or client prefix
    if config.common_config.client_prefix:
        set_output_files_prefix(config.common_config.client_prefix)
    else:
        set_output_files_prefix(config.common_config.output_files_prefix)

    # Determine which mapping files to use with their status
    ldm_files, ldm_status = get_mapping_files(
        files=config.ldm_mapping_file,
        client_prefix=config.common_config.client_prefix,
    )

    metric_files, metric_status = get_mapping_files(
        files=config.metric_mapping_file,
        client_prefix=config.common_config.client_prefix,
    )

    insight_files, insight_status = get_mapping_files(
        files=config.insight_mapping_file,
        client_prefix=config.common_config.client_prefix,
    )

    dashboard_files, dashboard_status = get_mapping_files(
        files=config.dashboard_mapping_file,
        client_prefix=config.common_config.client_prefix,
    )

    scheduled_export_files, scheduled_export_status = get_mapping_files(
        files=config.scheduled_export_mapping_file,
        client_prefix=config.common_config.client_prefix,
    )

    # Log information about which files are being used with their status
    logger.info("Mapping files:")
    logger.info("  LDM mappings: %s", format_mapping_files_info(ldm_files, ldm_status))
    logger.info(
        "  Metric mappings: %s", format_mapping_files_info(metric_files, metric_status)
    )
    logger.info(
        "  Insight mappings: %s",
        format_mapping_files_info(insight_files, insight_status),
    )
    logger.info(
        "  Dashboard mappings: %s",
        format_mapping_files_info(dashboard_files, dashboard_status),
    )
    logger.info(
        "  Scheduled export mappings: %s",
        format_mapping_files_info(scheduled_export_files, scheduled_export_status),
    )

    # Initialize mappings with multiple files
    ldm_mappings = IdMappings(ldm_files)
    metric_mappings = IdMappings(metric_files)
    insight_mappings = IdMappings(insight_files)
    dashboard_mappings = IdMappings(dashboard_files)
    scheduled_export_mappings = IdMappings(scheduled_export_files)

    # Set up backend clients
    platform_client = PlatformClient(
        env_vars.platform_domain,
        env_vars.platform_ws,
        env_vars.platform_login,
        env_vars.platform_password,
    )

    cloud_client = CloudClient(
        env_vars.cloud_domain, env_vars.cloud_ws, env_vars.cloud_token
    )

    # First file is used for writing mappings
    primary_scheduled_export_file = (
        scheduled_export_files[0]
        if scheduled_export_files
        else config.scheduled_export_mapping_file
    )
    mapping_logger = OutputWriter(primary_scheduled_export_file)
    output_logger = OutputWriter(SCHEDULED_EXPORTS_LOGGER_FILE)

    platform_dump_file = prefix_filename(PLATFORM_SCHEDULED_EXPORTS_FILE)
    cloud_dump_file = prefix_filename(CLOUD_SCHEDULED_EXPORTS_FILE)

    # Create the context
    backends = Backends(platform_client=platform_client, cloud_client=cloud_client)
    mappings = Mappings(
        ldm_mappings=ldm_mappings,
        metric_mappings=metric_mappings,
        insight_mappings=insight_mappings,
        dashboard_mappings=dashboard_mappings,
        scheduled_export_mappings=scheduled_export_mappings,
    )
    logging = Logging(mapping_logger=mapping_logger, output_logger=output_logger)
    command_line_arguments = CommandLineArguments(
        dump_platform=config.object_migration_config.dump_platform,
        dump_cloud=config.object_migration_config.dump_cloud,
        cleanup_target_env=config.object_migration_config.cleanup_target_env,
        skip_deploy=config.common_config.skip_deploy,
        platform_dump_file=platform_dump_file,
        cloud_dump_file=cloud_dump_file,
        client_prefix=config.common_config.client_prefix,
        overwrite_existing=config.object_migration_config.overwrite_existing,
    )

    if not env_vars.cloud_notification_channel_id:
        raise RuntimeError("Cloud notification channel ID is not set!")

    context = ScheduledExportsContext(
        backends=backends,
        mappings=mappings,
        logging=logging,
        command_line_arguments=command_line_arguments,
        notification_channel_id=env_vars.cloud_notification_channel_id,
        input_file=config.exports_to_migrate,
    )

    # Initialize the builder class
    migrator = ScheduledExportMigrator(context)

    # Build the scheduled exports
    migrator.migrate()

    execution_time = duration(start_time)
    logger.info("----DONE in %.2fs----", execution_time)
    logger.info("----Executed %d Cloud requests----", cloud_client.request_count.get())


def migrate_scheduled_exports_cli():
    args = parse_scheduled_export_cli_args()
    config = ScheduledExportConfig.from_kwargs(**args.__dict__)
    migrate_scheduled_exports(config)
