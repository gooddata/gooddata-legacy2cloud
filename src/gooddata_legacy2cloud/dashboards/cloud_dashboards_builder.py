# (C) 2026 GoodData Corporation
"""
The cloud_dashboards_builder module contains the CloudDashboardsBuilder class,
    which is responsible for building Cloud dashboards.
"""

import concurrent.futures
import logging
from concurrent.futures import ThreadPoolExecutor

from gooddata_legacy2cloud.dashboards.data_classes import DashboardContext
from gooddata_legacy2cloud.dashboards.cloud_dashboard import CloudDashboard
from gooddata_legacy2cloud.helpers import get_cloud_id
from gooddata_legacy2cloud.logging.context import ObjectContext
from gooddata_legacy2cloud.output_writer import OutputWriter

logger = logging.getLogger("migration")

DASHBOARD_LOGGER_FILE = "dashboard_logs.log"
MAX_WORKERS = 5


class CloudDashboardsBuilder:
    """
    The CloudDashboardsBuilder class contains the methods
    required to build Cloud dashboards.
    """

    def __init__(self, ctx: DashboardContext):
        """Constructor."""
        self.legacy_dashboards_raw = None
        self.ctx = ctx
        self.cloud_dashboards = []
        self.public_dashboard_ids = []

    def process_legacy_dashboards(
        self, legacy_dashboards_raw: list, skip_deploy: bool, overwrite_existing: bool
    ):
        """
        Load Legacy dashboards and process them to cloud ones.
        """
        self.legacy_dashboards_raw = legacy_dashboards_raw
        self.dashboards_logger = OutputWriter(DASHBOARD_LOGGER_FILE)

        # Write metadata as the first line in the log file
        self.dashboards_logger.write_migration_metadata(
            self.ctx.legacy_client.domain,
            self.ctx.legacy_client.pid,
            self.ctx.cloud_client.domain,
            self.ctx.cloud_client.ws,
            self.ctx.client_prefix if hasattr(self.ctx, "client_prefix") else None,
        )

        # Build current batch dashboard mappings (Legacy ID -> Cloud ID) for drill conversion
        current_batch_mappings = {}
        for dashboard in self.legacy_dashboards_raw:
            try:
                legacy_id = dashboard[self.ctx.dashboard_type]["meta"]["identifier"]
                dashboard_title = dashboard[self.ctx.dashboard_type]["meta"]["title"]
                cloud_id = get_cloud_id(dashboard_title, legacy_id)
                current_batch_mappings[legacy_id] = cloud_id
            except (KeyError, TypeError) as e:
                logger.warning(
                    "Could not extract dashboard ID from %s: %s", dashboard, e
                )

        logger.info(
            "Built current batch mappings for %s dashboards for drill conversion",
            len(current_batch_mappings),
        )

        # Create a new context with current batch mappings for drill conversion
        from dataclasses import replace

        ctx_with_batch_mappings = replace(
            self.ctx, current_batch_dashboard_mappings=current_batch_mappings
        )

        def worker(index: int, dashboard):
            dashboard_title = dashboard[self.ctx.dashboard_type]["meta"]["title"]
            dashboard_id = dashboard[self.ctx.dashboard_type]["meta"]["identifier"]
            with ObjectContext(dashboard_id, dashboard_title):
                return process_dashboard(index, dashboard)

        # process Legacy dashboards
        def process_dashboard(index, dashboard):
            # TODO: Move the execution logic to a separate private method
            dashboard_obj = ""
            logger.info("Processing %s", index + 1)

            try:
                cloud_dashboard = CloudDashboard(
                    ctx_with_batch_mappings, dashboard, skip_deploy, overwrite_existing
                )
                dashboard_obj = cloud_dashboard.get()
                if cloud_dashboard.public:
                    self.public_dashboard_ids.append(cloud_dashboard.cloud_dashboard_id)

                # append Cloud dashboards to list
                self.cloud_dashboards.append(dashboard_obj)

            except Exception as exc:
                logger.error("Processing %s failed: %s", index + 1, exc)
                dashboard_obj = f"ERROR: {exc}"
            finally:
                self.dashboards_logger.write_transformation(
                    dashboard_title,
                    dashboard,
                    dashboard_obj,
                )

        # Use ThreadPoolExecutor to process dashboards in parallel
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [
                executor.submit(worker, index, dashboard)
                for index, dashboard in enumerate(self.legacy_dashboards_raw)
            ]
            for future in concurrent.futures.as_completed(futures):
                future.result()  # This will raise any exceptions caught during execution

    def get_cloud_dashboards(self):
        """
        Returns the dashboards.
        """
        return self.cloud_dashboards

    def get_public_dashboard_ids(self):
        """
        Returns the public dashboard ids.
        """
        return self.public_dashboard_ids
