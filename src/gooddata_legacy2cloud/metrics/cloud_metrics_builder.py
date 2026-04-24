# (C) 2026 GoodData Corporation
"""
The cloud_metrics_builder module contains the CloudMetricsBuilder class,
which is responsible for building Cloud metrics.
"""

import concurrent.futures
import logging

from gooddata_legacy2cloud.logging.context import ObjectContext
from gooddata_legacy2cloud.metrics.data_classes import MetricContext
from gooddata_legacy2cloud.metrics.metrics_sorter import MetricsSorter
from gooddata_legacy2cloud.metrics.cloud_metric import CloudMetric
from gooddata_legacy2cloud.output_writer import OutputWriter

logger = logging.getLogger("migration")

MAQL_LOG_FILE = "metrics_maql.log"
MAX_WORKERS = 5


class CloudMetricsBuilder:
    """
    The CloudMetricsBuilder class contains the methods
    required to build Cloud metrics.
    """

    def __init__(self, ctx: MetricContext):
        self.legacy_metrics_raw = None
        self.ctx = ctx
        self.cloud_metrics = []

    def process_legacy_metrics(self, legacy_metrics_raw: list):
        """
        Load Legacy metrics and process them to Cloud ones.
        """
        self.legacy_metrics_raw = legacy_metrics_raw
        maql_writer = OutputWriter(MAQL_LOG_FILE)

        # Write metadata as the first line in the log file
        maql_writer.write_migration_metadata(
            self.ctx.legacy_client.domain,
            self.ctx.legacy_client.pid,
            self.ctx.cloud_client.domain,
            self.ctx.cloud_client.ws,
            self.ctx.client_prefix if hasattr(self.ctx, "client_prefix") else None,
        )

        def worker(index: int, metric: dict):
            metric_title = metric["metric"]["meta"]["title"]
            metric_id = metric["metric"]["meta"]["identifier"]
            with ObjectContext(metric_id, metric_title):
                return process_metric(index, metric, metric_title)

        # process Legacy metrics
        def process_metric(index, metric, metric_title: str):
            # TODO: Move the execution logic to a separate private method
            cloud_maql = ""
            logger.info("Processing %s", index + 1)

            try:
                cloud_metric = CloudMetric(self.ctx, metric)
                metric_obj = cloud_metric.get()

                # append Cloud metrics to list
                self.cloud_metrics.append(metric_obj)
                cloud_maql = cloud_metric.cloud_maql

            except Exception as exc:
                logger.error("Processing %s failed: %s", index + 1, exc)
                cloud_maql = exc
            finally:
                maql_writer.write_transformation(
                    metric_title,
                    metric["metric"]["content"]["expression"],
                    cloud_maql,
                )

        # Use ThreadPoolExecutor to process metrics in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [
                executor.submit(worker, index, metric)
                for index, metric in enumerate(self.legacy_metrics_raw)
            ]
            for future in concurrent.futures.as_completed(futures):
                future.result()  # This will raise any exceptions caught during execution

        # sorts the metrics based on dependencies
        sorter = MetricsSorter(self.cloud_metrics)
        self.cloud_metrics = sorter.get_sorted()

    def get_cloud_metrics(self):
        """
        Returns the metrics.
        """
        return self.cloud_metrics
