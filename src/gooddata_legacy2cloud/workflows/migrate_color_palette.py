# (C) 2026 GoodData Corporation
import logging
from time import time

from gooddata_legacy2cloud.arg_parsing.arg_parser import parse_color_palette_cli_args
from gooddata_legacy2cloud.backends.cloud.client import CloudClient
from gooddata_legacy2cloud.backends.legacy.client import LegacyClient
from gooddata_legacy2cloud.config.configuration_objects import ColorPaletteConfig
from gooddata_legacy2cloud.config.env_vars import EnvVars
from gooddata_legacy2cloud.helpers import duration
from gooddata_legacy2cloud.insights.color_palette import (
    ColorPalette,
    ColorPaletteContext,
)
from gooddata_legacy2cloud.logging.config import configure_logger

logger = logging.getLogger("migration")
configure_logger()


def migrate_color_palette(config: ColorPaletteConfig):
    """Sets the migrated color palette as active in organization settings.
    Removes existing color palettes.
    """

    start_time = time()
    logger.info("----Migrating color palette----")

    env_vars = EnvVars(config.env)
    env_vars.resolve_workspaces(config.workspace_config)
    env_vars.log_connection_info()

    legacy_client = LegacyClient(
        env_vars.legacy_domain,
        env_vars.legacy_ws,
        env_vars.legacy_login,
        env_vars.legacy_password,
    )

    cloud_client = CloudClient(
        env_vars.cloud_domain, env_vars.cloud_ws, env_vars.cloud_token
    )

    ctx = ColorPaletteContext(
        legacy_client=legacy_client,
        cloud_client=cloud_client,
    )

    color_palette = ColorPalette(ctx)
    if not color_palette.legacy_color_palette:
        logger.info("Color palette not found in Legacy")
    else:
        cloud_client.remove_color_palettes()
        color_palette.create_color_palette()
        color_palette.set_color_palette_to_organization()
        logger.info("Color palette migrated successfully")

    execution_time = duration(start_time)
    legacy_client.logout()
    logger.info("----DONE in %.2fs----", execution_time)
    logger.info("----Executed %d Cloud requests----", cloud_client.request_count.get())


def migrate_color_palette_cli():
    args = parse_color_palette_cli_args()
    config = ColorPaletteConfig.from_kwargs(**args.__dict__)
    migrate_color_palette(config)
