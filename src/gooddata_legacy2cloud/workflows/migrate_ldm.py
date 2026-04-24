# (C) 2026 GoodData Corporation
"""
This module is used for migrating LDM models. It includes functionality for
loading environment variables, setting up command line arguments, and running
the main migration process.
"""

import json
import logging
import time

from gooddata_legacy2cloud.arg_parsing.arg_parser import parse_ldm_cli_args
from gooddata_legacy2cloud.backends.cloud.client import CloudClient
from gooddata_legacy2cloud.backends.legacy.client import LegacyClient
from gooddata_legacy2cloud.config.configuration_objects import LDMConfig
from gooddata_legacy2cloud.config.env_vars import EnvVars
from gooddata_legacy2cloud.helpers import (
    duration,
    prefix_filename,
    set_output_files_prefix,
    write_content_to_file,
)
from gooddata_legacy2cloud.ldm.cloud_model_builder import CloudModelBuilder
from gooddata_legacy2cloud.ldm.model_builder_config import CloudModelBuilderConfig
from gooddata_legacy2cloud.logging.config import configure_logger

LEGACY_MODEL_FILE = "legacy_ldm.json"
CLOUD_MODEL_FILE = "cloud_ldm.json"


logger = logging.getLogger("migration")
configure_logger()


def migrate_ldm(config: LDMConfig):
    """
    Main function to parse command line arguments and initiate
    the migration process.
    """
    start_time = time.time()

    env_vars = EnvVars(config.env)
    env_vars.resolve_workspaces(config.workspace_config)
    env_vars.log_connection_info()

    # Set output files prefix from command line arguments or client prefix
    if config.common_config.client_prefix:
        set_output_files_prefix(config.common_config.client_prefix)
    else:
        set_output_files_prefix(config.common_config.output_files_prefix)

    legacy_client = LegacyClient(
        env_vars.legacy_domain,
        env_vars.legacy_ws,
        env_vars.legacy_login,
        env_vars.legacy_password,
    )
    legacy_ldm = legacy_client.get_model()

    if config.object_migration_config.dump_legacy:
        write_content_to_file(LEGACY_MODEL_FILE, json.dumps(legacy_ldm, indent=4))
        logger.info(
            "Legacy LDM model dumped to '%s'", prefix_filename(LEGACY_MODEL_FILE)
        )

    builder_config = CloudModelBuilderConfig(
        env_vars.data_source_id,
        env_vars.schema,
        env_vars.table_prefix,
        env_vars.ws_data_filter_id,
        env_vars.ws_data_filter_column,
        env_vars.ws_data_filter_description,
        legacy_client=legacy_client,
        ignore_folders=config.ignore_folders,
        ignore_explicit_mapping=config.ignore_explicit_mapping,
    )

    # Building Cloud model
    model_builder = CloudModelBuilder(builder_config)
    model_builder.load_legacy_model(legacy_ldm)
    cloud_ldm = model_builder.get_model()

    if config.object_migration_config.dump_cloud:
        write_content_to_file(CLOUD_MODEL_FILE, json.dumps(cloud_ldm, indent=4))
        logger.info("Cloud LDM model dumped to '%s'", prefix_filename(CLOUD_MODEL_FILE))

    if not config.common_config.skip_deploy:
        logger.info("---")
        try:
            cloud_client = CloudClient(
                env_vars.cloud_domain,
                env_vars.cloud_ws,
                env_vars.cloud_token,
            )

            if env_vars.ws_data_filter_id:
                cloud_client.remove_all_ws_data_filters()
                cloud_client.create_workspace_data_filters(
                    model_builder.prepare_ws_data_filter()
                )
                logger.info(
                    "Cloud WS data filter '%s' created.", env_vars.ws_data_filter_id
                )

            put = cloud_client.put_model(cloud_ldm)

            if put.status_code != 204:
                logger.error(
                    "Cloud model import failed. HTTP status code: %s Output: %s",
                    put.status_code,
                    put.text,
                )
            else:
                logger.info("Cloud model imported successfully.")
        except Exception as e:
            logger.error("Cloud model import failed: %s", e)

    execution_time = duration(start_time)
    legacy_client.logout()
    logger.info("----DONE in %.2fs----", execution_time)
    logger.info("----Executed %d Cloud requests----", cloud_client.request_count.get())


def migrate_ldm_cli():
    args = parse_ldm_cli_args()
    config = LDMConfig.from_kwargs(**args.__dict__)
    migrate_ldm(config)
