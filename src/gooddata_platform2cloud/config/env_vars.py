# (C) 2026 GoodData Corporation
import logging
import os
import sys

from dotenv import load_dotenv

from gooddata_platform2cloud.backends.cloud.client import CloudClient
from gooddata_platform2cloud.config.shared_configs import (
    WorkspaceConfig,
)
from gooddata_platform2cloud.logging.config import configure_logger

logger = logging.getLogger("migration")
configure_logger()


class EnvVars:
    """Class for loading environment variables from the .env file and with config object overrides."""

    def __init__(self, env_path: str = ".env"):
        load_dotenv(dotenv_path=env_path, override=True)
        self.platform_domain = os.environ["PLATFORM_DOMAIN"]
        self.platform_login = os.environ["PLATFORM_LOGIN"]
        self.platform_password = os.environ["PLATFORM_PASSWORD"]
        self.platform_ws = os.getenv("PLATFORM_WS", "")

        self.cloud_domain = os.environ["CLOUD_DOMAIN"]
        self.cloud_token = os.environ["CLOUD_TOKEN"]
        self.cloud_ws = os.getenv("CLOUD_WS", "")

        self.data_source_id = os.environ["DATA_SOURCE_ID"]
        self.schema = os.environ["SCHEMA"]
        self.table_prefix = os.environ["TABLE_PREFIX"]
        self.ws_data_filter_id = os.getenv("WS_DATA_FILTER_ID")
        self.ws_data_filter_column = os.getenv("WS_DATA_FILTER_COLUMN")
        self.ws_data_filter_description = os.getenv("WS_DATA_FILTER_DESCRIPTION")
        self.cloud_notification_channel_id: str | None = os.getenv(
            "CLOUD_NOTIFICATION_CHANNEL_ID"
        )

    def resolve_workspaces(self, workspace_config: WorkspaceConfig) -> None:
        """Resolve platform_ws and cloud_ws with CLI overrides and validate."""
        # Check for platform workspace ID
        if workspace_config.platform_ws:
            self.platform_ws = workspace_config.platform_ws

        if not self.platform_ws:
            logger.error(
                "Platform workspace ID is required. Specify it in .env file as "
                + "PLATFORM_WS or use --platform-ws parameter in the command line."
            )
            sys.exit(1)

        # Check for cloud workspace ID
        if workspace_config.cloud_ws:
            self.cloud_ws = workspace_config.cloud_ws

        if not self.cloud_ws:
            logger.error(
                "Cloud workspace ID is required. Specify it in .env file as "
                + "CLOUD_WS or use --cloud-ws parameter in the command line."
            )
            sys.exit(1)

    def log_connection_info(self) -> None:
        """Log the source and target domains and workspaces."""
        logger.info("Source: %s - %s", self.platform_domain, self.platform_ws)
        logger.info("Target: %s - %s", self.cloud_domain, self.cloud_ws)

    def check_parent_workspace(self) -> None:
        """Check if the target Cloud workspace has a parent workspace."""
        if not self.cloud_domain or not self.cloud_ws or not self.cloud_token:
            logger.error("Cloud connection details are missing.")
            sys.exit(1)

        logger.info("----Checking if target workspace has a parent workspace----")
        cloud_client = CloudClient(self.cloud_domain, self.cloud_ws, self.cloud_token)
        parent_id = cloud_client.check_parent_workspace()

        if parent_id is None:
            logger.error(
                "Target Cloud workspace '%s' does not have a parent workspace.",
                self.cloud_ws,
            )
            logger.error(
                "When using --client-prefix or --check-parent-workspace, "
                "the target workspace must be a child workspace."
            )
            logger.error(
                "Please build a parent/child Workspace hierarchy in Cloud "
                "before migrating into client workspaces."
            )
            sys.exit(1)
        else:
            logger.info(
                "Target workspace is a child workspace with parent ID: %s",
                parent_id,
            )
