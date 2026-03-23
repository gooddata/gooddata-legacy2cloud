# (C) 2026 GoodData Corporation
"""
This module is responsible for converting the Platform MAQL expression
into Cloud MAQL expression.
"""

from typing import Dict

from gooddata_platform2cloud.metrics.data_classes import MetricContext
from gooddata_platform2cloud.metrics.maql.content_tree_modifier import (
    ContentTreeModifier,
)
from gooddata_platform2cloud.metrics.maql.maql_builder import MaqlBuilder


class CloudMaql:
    """
    The CloudMaql class is responsible for converting the Platform MAQL expression
    into Cloud MAQL expression.
    """

    def __init__(
        self, ctx: MetricContext, platform_maql: str, content_tree_platform: Dict
    ):
        self.ctx = ctx
        self.errors = []
        self.platform_maql = platform_maql
        self.content_tree = content_tree_platform
        self.cloud_maql = self._prepare_cloud_maql()

    def _prepare_cloud_maql(self) -> str:
        """
        Returns the Cloud MAQL expression.
        """

        contentTreeModifier = ContentTreeModifier(self.ctx, self.content_tree)
        modifier_content_tree = contentTreeModifier.get()
        self.errors.extend(contentTreeModifier.get_errors())

        maqlBuilder = MaqlBuilder(self.ctx, modifier_content_tree, True)
        expression = maqlBuilder.get()
        self.errors.extend(maqlBuilder.get_errors())

        return expression

    def get_errors(self):
        return self.errors

    def get(self) -> str:
        """
        Returns the Cloud MAQL expression.
        """
        return self.cloud_maql
