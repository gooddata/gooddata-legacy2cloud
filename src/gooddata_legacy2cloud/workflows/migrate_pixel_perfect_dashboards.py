# (C) 2026 GoodData Corporation
"""
Pixel Perfect dashboard migration entry point.

By default, each Legacy Pixel Perfect dashboard is migrated one-to-one into a single
Cloud KPI dashboard that uses native dashboard tabs (one Legacy tab -> one Cloud tab).

Legacy behavior (splitting each tab into a separate Cloud dashboard) is available via
`--pp-legacy-split-tabs` for transition periods.
"""

import json
import logging
from time import time
from typing import Any

from gooddata_legacy2cloud.arg_parsing.arg_parser import (
    parse_pixel_perfect_dashboard_cli_args,
)
from gooddata_legacy2cloud.backends.cloud.client import CloudClient
from gooddata_legacy2cloud.backends.cloud.object_creator import (
    process_objects,
    update_dashboards_with_full_content,
)
from gooddata_legacy2cloud.backends.legacy.client import LegacyClient
from gooddata_legacy2cloud.backends.legacy.filters import FilterParameters
from gooddata_legacy2cloud.backends.legacy.objects import fetch_objects_with_filters
from gooddata_legacy2cloud.config.configuration_objects import (
    PixelPerfectDashboardConfig,
)
from gooddata_legacy2cloud.config.env_vars import EnvVars
from gooddata_legacy2cloud.constants import PIXEL_PERFECT_DASHBOARD_MAPPING_FILE
from gooddata_legacy2cloud.helpers import (
    duration,
    prefix_filename,
    set_output_files_prefix,
    write_content_to_file,
)
from gooddata_legacy2cloud.id_mappings import IdMappings
from gooddata_legacy2cloud.logging.config import configure_logger
from gooddata_legacy2cloud.mapping.mapping_utils import (
    filter_objects_by_mapping_files,
    format_mapping_files_info,
    get_mapping_files,
)
from gooddata_legacy2cloud.models.enums import Operation
from gooddata_legacy2cloud.output_writer import OutputWriter
from gooddata_legacy2cloud.pp_dashboards.cloud_pp_dashboards_builder import (
    CloudPixelPerfectDashboardsBuilder,
)
from gooddata_legacy2cloud.pp_dashboards.data_classes import PPDashboardContext
from gooddata_legacy2cloud.pp_dashboards.grid_maker import GridConfig

# =============================
# Configuration & Constants
# =============================
DASHBOARD_TYPE = "projectDashboard"
SUPPORTED_ITEMS = [
    "headlineItem",
    "reportItem",
    "textItem",
]
MIN_TEXT_LENGTH = 5
PIXEL_PERFECT_PREFIX = "[PP]"
PIXEL_PERFECT_LOG_FILE = "pixel_perfect_migration.log"
LEGACY_DASHBOARDS_FILE = "legacy_pp_dashboards.json"
CLOUD_DASHBOARDS_FILE = "cloud_pp_dashboards.json"
CLOUD_FAILED_DASHBOARDS_FILE = "legacy_failed_pp_dashboards.json"
CLOUD_SKIPPED_DASHBOARDS_FILE = "legacy_skipped_pp_dashboards.json"


logger = logging.getLogger("migration")
configure_logger()


def migrate_pixel_perfect_dashboards(config: PixelPerfectDashboardConfig) -> None:
    """The pixel perfect dashboard migration process."""
    start_time = time()

    # Setup file logging for pixel perfect specific logs
    file_logger = logging.getLogger("pp_migration")
    file_logger.setLevel(logging.INFO)

    env_vars = EnvVars(config.env)
    env_vars.resolve_workspaces(config.workspace_config)
    env_vars.log_connection_info()

    # Set output files prefix from command line arguments or client prefix
    if config.common_config.client_prefix:
        set_output_files_prefix(config.common_config.client_prefix)
    else:
        set_output_files_prefix(config.common_config.output_files_prefix)

    # Extract filter parameters from args
    filter_params = FilterParameters.from_config(config.object_filter_config)

    # Update file logging handler to respect output file prefix
    log_file = prefix_filename(PIXEL_PERFECT_LOG_FILE)

    # =============================
    # Resolve Mapping Files
    # =============================
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

    report_files, report_status = get_mapping_files(
        files=config.report_mapping_file,
        client_prefix=config.common_config.client_prefix,
    )

    dashboard_files, dashboard_status = get_mapping_files(
        files=[PIXEL_PERFECT_DASHBOARD_MAPPING_FILE],
        client_prefix=config.common_config.client_prefix,
    )

    # =============================
    # Services Setup
    # =============================
    legacy_client = LegacyClient(
        env_vars.legacy_domain,
        env_vars.legacy_ws,
        env_vars.legacy_login,
        env_vars.legacy_password,
    )
    # Initialize attribute elements cache in case of high probability of missing attributes
    if config.validation_element_lookup:
        legacy_client.initialize_attribute_elements_cache()

    cloud_client = CloudClient(
        env_vars.cloud_domain, env_vars.cloud_ws, env_vars.cloud_token
    )

    # Initialize mappings with multiple files
    ldm_mappings = IdMappings(ldm_files)
    metric_mappings = IdMappings(metric_files)
    # insight_mappings = IdMappings(insight_files)
    report_mappings = IdMappings(report_files)
    dashboard_mappings = IdMappings(dashboard_files)

    # First dashboard file is used for writing mappings
    primary_dashboard_file = (
        dashboard_files[0] if dashboard_files else PIXEL_PERFECT_DASHBOARD_MAPPING_FILE
    )

    # Create mapping and transformation loggers
    transformation_logger = OutputWriter(log_file)
    mapping_logger = OutputWriter(primary_dashboard_file)

    # =============================
    # Global Context & Helpers
    # =============================
    ctx = PPDashboardContext(
        legacy_client=legacy_client,
        cloud_client=cloud_client,
        ldm_mappings=ldm_mappings,
        metric_mappings=metric_mappings,
        report_mappings=report_mappings,
        dashboard_mappings=dashboard_mappings,
        mapping_logger=mapping_logger,
        transformation_logger=transformation_logger,
        suppress_warnings=config.object_migration_config.suppress_migration_warnings,
        client_prefix=config.common_config.client_prefix,
        keep_original_ids=config.keep_original_ids,
    )

    cfg = GridConfig(
        canvas_width_px=940,
        columns=12,
        gutter_x_px=16,
        gutter_y_px=16,
        row_unit_px=5,
        rounding="nearest",
    )

    # Create migration log file metadata
    transformation_logger.write_migration_metadata(
        legacy_hostname=legacy_client.domain,
        legacy_ws=legacy_client.pid,
        cloud_hostname=cloud_client.domain,
        cloud_ws=cloud_client.ws,
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
        "  Report mappings: %s", format_mapping_files_info(report_files, report_status)
    )
    logger.info(
        "  Dashboard mappings: %s",
        format_mapping_files_info(dashboard_files, dashboard_status),
    )

    logger.info("----Fetching Legacy pixel perfect dashboards----")
    # Get Legacy pixel perfect dashboards
    legacy_dashboards = fetch_objects_with_filters(
        legacy_client, DASHBOARD_TYPE, filter_params, "pixel perfect dashboards"
    )

    # Filter objects based on mapping files if requested
    if config.object_filter_config.without_mapped_objects:
        logger.info(
            "Filtering mode: %s (filtering file: %s)",
            config.object_filter_config.without_mapped_objects,
            primary_dashboard_file,
        )
        legacy_dashboards = filter_objects_by_mapping_files(
            legacy_dashboards,
            config.object_filter_config.without_mapped_objects,
            dashboard_mappings,
            primary_dashboard_file,
            "pixel perfect dashboards",
        )

    if config.object_migration_config.dump_legacy:
        write_content_to_file(
            LEGACY_DASHBOARDS_FILE, json.dumps(legacy_dashboards, indent=4)
        )
        logger.info(
            "Legacy dashboards dumped to '%s'",
            prefix_filename(LEGACY_DASHBOARDS_FILE),
        )

    if config.object_migration_config.cleanup_target_env:
        logger.info("----Cleaning up pixel perfect objects only----")
        cloud_client.remove_native_pp_dashboards()
        cloud_client.remove_native_pp_filter_contexts()
        cloud_client.remove_native_pp_insights()
        logger.info("Cleanup completed")

    logger.info(
        "----Processing Pixel Perfect Dashboards (%d)----", len(legacy_dashboards)
    )

    # =============================
    # Processing Phase
    # =============================
    dashboards_builder = CloudPixelPerfectDashboardsBuilder(
        ctx,
        cfg,
        pixel_perfect_prefix=PIXEL_PERFECT_PREFIX,
        min_text_length=MIN_TEXT_LENGTH,
        supported_items=SUPPORTED_ITEMS,
        legacy_split_tabs=config.pp_legacy_split_tabs,
    )

    dashboards_builder.process_legacy_dashboards(
        legacy_dashboards,
        config.common_config.skip_deploy,
        config.object_migration_config.overwrite_existing,
    )

    cloud_dashboards = dashboards_builder.get_cloud_dashboards()
    cloud_dashboard_ids = dashboards_builder.get_public_dashboard_ids()

    total_dashboards = sum(
        1
        for bd in legacy_dashboards
        if bd.get("projectDashboard", {}).get("meta", {}).get("title")
    )
    total_tabs = sum(
        len(bd["projectDashboard"]["content"]["tabs"])
        for bd in legacy_dashboards
        if bd.get("projectDashboard", {}).get("meta", {}).get("title")
    )

    if config.pp_legacy_split_tabs:
        # Legacy: one tab becomes one dashboard
        if total_tabs > len(cloud_dashboards):
            logger.error(
                "----%d (out of %d) tabs could not be migrated----",
                total_tabs - len(cloud_dashboards),
                total_tabs,
            )
        else:
            logger.info("----Successfully processed all %d tabs----", total_tabs)
    else:
        # Default: one dashboard with N tabs
        if total_dashboards > len(cloud_dashboards):
            logger.error(
                "----%d (out of %d) dashboards could not be migrated----",
                total_dashboards - len(cloud_dashboards),
                total_dashboards,
            )
        else:
            logger.info(
                "----Successfully processed %d dashboard(s) containing %d tab(s)----",
                len(cloud_dashboards),
                total_tabs,
            )

    # =============================
    # Deployment Phase
    # =============================
    if not config.common_config.skip_deploy:
        logger.info(
            "----Two-phase PP dashboard migration (%d)----", len(cloud_dashboards)
        )

        # Convert Pydantic models to dictionaries for process_objects
        cloud_dashboards_dicts = [
            {"data": dashboard.model_dump(exclude_none=True)}
            for dashboard in cloud_dashboards
        ]

        # Phase 1: Create placeholder dashboards to establish all IDs
        logger.info(
            "Phase 1: Creating placeholder dashboards (%d)...",
            len(cloud_dashboards_dicts),
        )
        if config.object_migration_config.overwrite_existing:
            operation = Operation.CREATE_OR_UPDATE_WITH_RETRY
        else:
            operation = Operation.CREATE_WITH_RETRY

        _failed_placeholders, skipped_placeholders = process_objects(
            cloud_client=cloud_client,
            objects=cloud_dashboards_dicts,
            object_type="pp_dashboard_placeholder",
            operation=operation,
        )

        # Collect IDs of dashboards that were skipped in Phase 1
        skipped_dashboard_ids = {
            dashboard["data"]["id"] for dashboard in skipped_placeholders
        }

        # Phase 2: Update placeholders with full dashboard content
        if len(cloud_dashboards_dicts) > len(skipped_placeholders):
            dashboards_to_update_count = len(cloud_dashboards_dicts) - len(
                skipped_placeholders
            )
            logger.info(
                "Phase 2: Updating dashboards with full content (%d)...",
                dashboards_to_update_count,
            )
            _failed_updates, _ = update_dashboards_with_full_content(
                cloud_client, cloud_dashboards_dicts, skipped_dashboard_ids
            )
        else:
            logger.info(
                "Phase 2: No dashboards to update (all were skipped in Phase 1)"
            )

        # Set permissions for public dashboards (only for successfully created ones)
        public_dashboards_to_set_permissions = [
            dashboard_id
            for dashboard_id in cloud_dashboard_ids
            if dashboard_id not in skipped_dashboard_ids
        ]
        if public_dashboards_to_set_permissions:
            logger.info(
                "Setting permissions for new %d public dashboards",
                len(public_dashboards_to_set_permissions),
            )
            cloud_client.create_dashboard_permissions_for_public_dashboards(
                public_dashboards_to_set_permissions
            )

    if config.object_migration_config.dump_cloud:
        # Convert Pydantic models to dicts if not already done
        cloud_dashboards_json: list[Any] = []
        if cloud_dashboards and hasattr(cloud_dashboards[0], "model_dump"):
            cloud_dashboards_json = [
                dashboard.model_dump(exclude_none=True)
                for dashboard in cloud_dashboards
            ]
        else:
            cloud_dashboards_json = cloud_dashboards

        write_content_to_file(
            CLOUD_DASHBOARDS_FILE,
            json.dumps(cloud_dashboards_json, indent=4),
        )
        logger.info(
            "Cloud dashboards dumped to '%s'",
            prefix_filename(CLOUD_DASHBOARDS_FILE),
        )

    # =============================
    # Completion
    # =============================
    execution_time = duration(start_time)
    legacy_client.logout()
    logger.info("----DONE in %.2fs----", execution_time)
    logger.info("----Executed %d Cloud requests----", cloud_client.request_count.get())


def migrate_pixel_perfect_dashboards_cli():
    args = parse_pixel_perfect_dashboard_cli_args()
    config = PixelPerfectDashboardConfig.from_kwargs(**args.__dict__)
    migrate_pixel_perfect_dashboards(config)
