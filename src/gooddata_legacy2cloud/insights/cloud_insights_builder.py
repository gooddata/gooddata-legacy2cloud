# (C) 2026 GoodData Corporation
"""
The cloud_insights_builder module contains the CloudInsightsBuilder class,
    which is responsible for building Cloud insights.
"""

import concurrent.futures
import logging
from concurrent.futures import ThreadPoolExecutor

from gooddata_legacy2cloud.insights.data_classes import InsightContext
from gooddata_legacy2cloud.insights.cloud_insight import CloudInsight
from gooddata_legacy2cloud.logging.context import ObjectContext
from gooddata_legacy2cloud.output_writer import OutputWriter

logger = logging.getLogger("migration")

INSIGHTS_LOGGER_FILE = "insight_logs.log"
MAX_WORKERS = 5


class CloudInsightsBuilder:
    """
    The CloudInsightsBuilder class contains the methods
    required to build Cloud insights.
    """

    def __init__(self, ctx: InsightContext):
        """
        Init with API classes and mappings.
        """
        self.legacy_insights_raw = None
        self.ctx = ctx
        self.cloud_insights = []

    def process_legacy_insights(self, legacy_insights_raw: list):
        """
        Load Legacy insights and process them to Cloud ones.
        """
        self.legacy_insights_raw = legacy_insights_raw
        self.insights_logger = OutputWriter(INSIGHTS_LOGGER_FILE)

        # Write metadata as the first line in the log file
        self.insights_logger.write_migration_metadata(
            self.ctx.legacy_client.domain,
            self.ctx.legacy_client.pid,
            self.ctx.cloud_client.domain,
            self.ctx.cloud_client.ws,
            self.ctx.client_prefix if hasattr(self.ctx, "client_prefix") else None,
        )

        def worker(index: int, insight):
            insight_title = insight["visualizationObject"]["meta"]["title"]
            insight_id = insight["visualizationObject"]["meta"]["identifier"]
            with ObjectContext(insight_id, insight_title):
                return process_insight(index, insight, insight_title)

        # process Legacy insights
        def process_insight(index: int, insight, insight_title: str):
            # TODO: Move the execution logic to a separate private method
            insight_obj = ""
            logger.info("Processing %s", index + 1)

            try:
                cloud_insight = CloudInsight(self.ctx, insight)
                insight_obj = cloud_insight.get()
                if not insight_obj:
                    return

                self.cloud_insights.append(insight_obj)

            except Exception as exc:
                logger.error("Processing %s failed: %s", index + 1, exc)
                insight_obj = f"ERROR: {exc}"
            finally:
                self.insights_logger.write_transformation(
                    insight_title,
                    insight,
                    insight_obj,
                )

        # Use ThreadPoolExecutor to process insights in parallel
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [
                executor.submit(worker, index, insight)
                for index, insight in enumerate(self.legacy_insights_raw)
            ]
            for future in concurrent.futures.as_completed(futures):
                future.result()  # This will raise any exceptions caught during execution

    def get_cloud_insights(self):
        """
        Returns the insights.
        """
        return self.cloud_insights
