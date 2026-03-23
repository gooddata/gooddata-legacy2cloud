# (C) 2026 GoodData Corporation
"""
This module contains the CloudMetric class,
which is responsible for transforming the Platform metric to Cloud format.
"""

import logging
from typing import Any

from gooddata_platform2cloud.helpers import get_cloud_id
from gooddata_platform2cloud.metrics.data_classes import MetricContext
from gooddata_platform2cloud.metrics.cloud_maql import CloudMaql
from gooddata_platform2cloud.metrics.utils import get_folders_names

logger = logging.getLogger("migration")


class CloudMetric:
    """
    The CloudMetric class is responsible for transforming the Platform metric to Cloud format.
    """

    def __init__(
        self,
        ctx: MetricContext,
        metadata: Any,
    ):
        self.ctx = ctx
        self.meta = metadata["metric"]["meta"]
        self.metric_content = metadata["metric"]["content"]
        self.format = self.metric_content.get("format", "#,##0.00")
        self.platform_maql = self.metric_content["expression"]
        self.description = metadata["metric"]["meta"]["summary"]
        self.tags = self._get_tags()
        self.cloud_maql, self.errors = self._get_cloud_maql(
            self.platform_maql, self.metric_content["tree"]
        )

        self.cloud_metric_id = (
            get_cloud_id(self.meta["title"], self.meta["identifier"])
            if not self.ctx.keep_original_ids
            else self.meta["identifier"]
        )

        self.ctx.mapping_logger.write_identifier_relation(
            self.meta["identifier"], self.cloud_metric_id
        )

    def _get_tags(self):
        """
        Prepares the tags for the metric.
        @param tags_str: The metadata tags string.
        """
        tags_str = self.meta.get("tags", "")
        tags = [
            tag.strip()
            for part in tags_str.split(",")
            for tag in part.split()
            if tag.strip()
        ]

        # add Platform folders to Cloud tags
        if "folders" in self.metric_content and not self.ctx.ignore_folders:
            folders = get_folders_names(
                self.ctx.platform_client, self.metric_content["folders"]
            )
            tags.extend(folders)

        if self.meta["deprecated"] == "1":
            tags.append("Migrated Hidden Metrics")

        return tags

    def get_errors(self):
        """
        Returns the errors found during the transformation.
        """
        return self.errors

    def _get_cloud_maql(self, platform_maql: str, content_tree: Any):
        """
        Transforms the Platform expression to Cloud format.
        """
        try:
            cloud_maql = CloudMaql(self.ctx, platform_maql, content_tree)
            new_maql = cloud_maql.get()
            maql_errors = cloud_maql.get_errors()

            # process maql errors
            if len(maql_errors) > 0:
                self.tags.append("MAQL errors")
                if not self.ctx.suppress_warnings:
                    maql_errors_str = "\n\n".join(maql_errors)
                    self.description += f"MAQL errors: \n\n{maql_errors_str}"
                # Print warning to console
                logger.warning("Metric '%s': %s", self.meta["title"], maql_errors)

        except Exception as e:
            raise Exception(f"ERROR `{self.meta['title']}`: {e}") from e

        return new_maql, maql_errors

    def get(self):
        """
        Returns the Cloud metric object.
        """

        if self.get_errors() and not self.ctx.suppress_warnings:
            self.meta["cloud_title"] = f"[WARN] {self.meta['title']}"

        return {
            "data": {
                "id": self.cloud_metric_id,
                "type": "metric",
                "attributes": {
                    "title": self.meta.get("cloud_title", self.meta["title"]),
                    "tags": self.tags,
                    "description": self.description,
                    "content": {
                        "format": self.format,
                        "maql": self.cloud_maql,
                    },
                },
            }
        }
