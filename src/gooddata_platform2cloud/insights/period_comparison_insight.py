# (C) 2026 GoodData Corporation
"""
This module contains the PeriodComparisonInsight class,
which is used to build period comparison insights.
"""

import logging
import uuid
from typing import Any

from gooddata_platform2cloud.backends.cloud.object_creator import process_objects
from gooddata_platform2cloud.dashboards.data_classes import DashboardContext
from gooddata_platform2cloud.insights.data_classes import InsightContext
from gooddata_platform2cloud.models.enums import Operation

# Constants for KPI comparison measure aliases
PREVIOUS_PERIOD_ALIAS = "prev. period"
LAST_YEAR_ALIAS = "prev. year"

logger = logging.getLogger("migration")


class PeriodComparisonInsight:
    def __init__(
        self,
        ctx: InsightContext | DashboardContext,
        platform_definition,
        new_insight_id: str | None = None,
        cloud_filters: list | None = None,
    ):
        self.ctx = ctx
        self.platform_measure_definition = platform_definition
        self.obj = None
        self.new_insight_id = new_insight_id

        if cloud_filters is None:
            cloud_filters_list = []
        else:
            cloud_filters_list = cloud_filters

        self.cloud_filters = cloud_filters_list

    @staticmethod
    def _get_measure_previous_period(
        new_dataset_identifier,
        new_local_identifier,
        new_measure_identifier,
        new_previous_period,
    ):
        measure = {
            "measure": {
                "localIdentifier": new_local_identifier,
                "definition": {
                    "previousPeriodMeasure": {
                        "measureIdentifier": new_measure_identifier,
                    }
                },
            }
        }
        if new_dataset_identifier:
            measure["measure"]["definition"]["previousPeriodMeasure"][
                "dateDataSets"
            ] = [
                {
                    "dataSet": {
                        "identifier": {
                            "id": new_dataset_identifier,
                            "type": "dataset",
                        }
                    },
                    "periodsAgo": new_previous_period,
                }
            ]
        return measure

    @staticmethod
    def _get_measure_last_year(
        new_identifier, new_local_identifier, new_measure_identifier
    ):
        # here we need to add .year to the dataset identifier to get the attribute from the dataset
        new_attribute_from_dataset = (
            new_identifier
            if new_identifier.endswith(".year")
            else new_identifier + ".year"
        )
        return {
            "measure": {
                "localIdentifier": new_local_identifier,
                "definition": {
                    "popMeasureDefinition": {
                        "measureIdentifier": new_measure_identifier,
                        "popAttribute": {
                            "identifier": {
                                "id": new_attribute_from_dataset,
                                "type": "attribute",
                            }
                        },
                    }
                },
            },
        }

    def get_measure(self):
        if (
            "previousPeriodMeasure"
            in self.platform_measure_definition["measure"]["definition"]
        ):
            platform_link = self.platform_measure_definition["measure"]["definition"][
                "previousPeriodMeasure"
            ]["dateDataSets"][0]["dataSet"]["uri"]
            self.obj = self.ctx.platform_client.get_object(platform_link)
            new_dataset_id = self.ctx.ldm_mappings.search_mapping_identifier(
                self.obj["dataSet"]["content"]["identifierPrefix"]
            )
            measure = self._get_measure_previous_period(
                new_dataset_id,
                self.platform_measure_definition["measure"]["localIdentifier"],
                self.platform_measure_definition["measure"]["definition"][
                    "previousPeriodMeasure"
                ]["measureIdentifier"],
                self.platform_measure_definition["measure"]["definition"][
                    "previousPeriodMeasure"
                ]["dateDataSets"][0]["periodsAgo"],
            )
            # Preserve alias from original Platform measure if it exists
            if "alias" in self.platform_measure_definition["measure"]:
                measure["measure"]["alias"] = self.platform_measure_definition[
                    "measure"
                ]["alias"]
            return measure
        elif (
            "popMeasureDefinition"
            in self.platform_measure_definition["measure"]["definition"]
        ):
            platform_link = self.platform_measure_definition["measure"]["definition"][
                "popMeasureDefinition"
            ]["popAttribute"]["uri"]
            self.obj = self.ctx.platform_client.get_object(platform_link)
            identifier = self.ctx.ldm_mappings.search_mapping_identifier(
                self.obj["attribute"]["meta"]["identifier"]
            )
            measure = self._get_measure_last_year(
                identifier,
                self.platform_measure_definition["measure"]["localIdentifier"],
                self.platform_measure_definition["measure"]["definition"][
                    "popMeasureDefinition"
                ]["measureIdentifier"],
            )
            # Preserve alias from original Platform measure if it exists
            if "alias" in self.platform_measure_definition["measure"]:
                measure["measure"]["alias"] = self.platform_measure_definition[
                    "measure"
                ]["alias"]
            return measure

    @staticmethod
    def _get_metric_measure(new_metric_id, new_local_identifier, new_title):
        return {
            "measure": {
                "localIdentifier": new_local_identifier,
                "definition": {
                    "measureDefinition": {
                        "item": {
                            "identifier": {
                                "id": new_metric_id,
                                "type": "metric",
                            }
                        },
                        "filters": [],
                    }
                },
                "title": new_title,
            }
        }

    def _get_insight_filter(self, obj):
        # Only date filter is enough to enable comparison and other filters will be applied in the dashboard
        new_insight_filters = []
        for filter in self.cloud_filters:
            if "dateFilter" in filter:
                new_filter_name = f"{filter['dateFilter']['type']}DateFilter"
                new_filter = {new_filter_name: {}}
                if "dateDataSet" in obj["kpi"]["content"]:
                    new_filter[new_filter_name]["dataSet"] = self._get_dataset_item(
                        obj["kpi"]["content"]["dateDataSet"]
                    )
                if filter["dateFilter"]["type"] == "absolute":
                    new_filter[new_filter_name]["from"] = filter["dateFilter"]["from"]
                    new_filter[new_filter_name]["to"] = filter["dateFilter"]["to"]
                elif filter["dateFilter"]["type"] == "relative":
                    new_filter[new_filter_name]["from"] = int(
                        filter["dateFilter"]["from"]
                    )
                    new_filter[new_filter_name]["to"] = int(filter["dateFilter"]["to"])
                    new_filter[new_filter_name]["granularity"] = filter["dateFilter"][
                        "granularity"
                    ]
                new_insight_filters.append(new_filter)
        return new_insight_filters

    def _get_dataset_item(self, dataset_uri: str):
        dataset_item_obj = self.ctx.platform_client.get_object(dataset_uri)
        return {
            "identifier": {
                "id": self.ctx.ldm_mappings.search_mapping_identifier(
                    dataset_item_obj["dataSet"]["content"]["identifierPrefix"]
                ),
                "type": dataset_item_obj["dataSet"]["meta"]["category"].lower(),
            }
        }

    @staticmethod
    def _get_properties(obj, effective_comparison_type: str):
        comparison: dict[str, Any] = {"enabled": False}
        properties: dict[str, Any] = {"controls": {"comparison": comparison}}
        if effective_comparison_type != "none":
            comparison["enabled"] = True
            if obj["kpi"]["content"].get("comparisonDirection") == "growIsBad":
                comparison["colorConfig"] = {
                    "negative": {"type": "guid", "value": "positive"},
                    "positive": {"type": "guid", "value": "negative"},
                }
        return properties

    def create_insight_object_from_kpi(self):
        buckets = []
        self.obj = self.platform_measure_definition
        metric_obj = self.ctx.platform_client.get_object(
            self.obj["kpi"]["content"]["metric"]
        )
        new_metric_id = self.ctx.metric_mappings.search_mapping_identifier(
            metric_obj["metric"]["meta"]["identifier"]
        )
        if "dateDataSet" in self.obj["kpi"]["content"]:
            dataset_obj = self.ctx.platform_client.get_object(
                self.obj["kpi"]["content"]["dateDataSet"]
            )
            new_dataset_id = self.ctx.ldm_mappings.search_mapping_identifier(
                dataset_obj["dataSet"]["content"]["identifierPrefix"]
            )
            has_date_dataset = True
        else:
            new_dataset_id = ""
            has_date_dataset = False

        raw_comparison_type = self.obj["kpi"]["content"].get("comparisonType", "none")
        if raw_comparison_type != "none" and not has_date_dataset:
            logger.warning(
                "Dropping period comparison for KPI %r: no date dataset on widget, "
                "but comparisonType=%s. Cloud headline requires a date dataset for "
                "period comparison; producing plain headline.",
                self.obj["kpi"]["meta"]["title"],
                raw_comparison_type,
            )
            effective_comparison_type = "none"
        else:
            effective_comparison_type = raw_comparison_type

        new_local_identifier = uuid.uuid4().hex

        if effective_comparison_type == "previousPeriod":
            new_local_identifier_previous = f"{new_local_identifier}_previous_period"
            primary_measure = {
                "items": [
                    self._get_metric_measure(
                        new_metric_id,
                        new_local_identifier,
                        self.obj["kpi"]["meta"]["title"],
                    )
                ],
                "localIdentifier": "measures",
            }
            secondary_measure_item = self._get_measure_previous_period(
                new_dataset_id,
                new_local_identifier_previous,
                new_local_identifier,
                1,
            )
            secondary_measure_item["measure"]["alias"] = PREVIOUS_PERIOD_ALIAS
            secondary_measure = {
                "items": [secondary_measure_item],
                "localIdentifier": "secondary_measures",
            }
            buckets = [primary_measure, secondary_measure]
        elif effective_comparison_type == "lastYear":
            new_local_identifier_pop = f"{new_local_identifier}_pop"
            primary_measure = {
                "items": [
                    self._get_metric_measure(
                        new_metric_id,
                        new_local_identifier,
                        self.obj["kpi"]["meta"]["title"],
                    )
                ],
                "localIdentifier": "measures",
            }
            secondary_measure_item = self._get_measure_last_year(
                new_dataset_id, new_local_identifier_pop, new_local_identifier
            )
            secondary_measure_item["measure"]["alias"] = LAST_YEAR_ALIAS
            secondary_measure = {
                "items": [secondary_measure_item],
                "localIdentifier": "secondary_measures",
            }
            buckets = [primary_measure, secondary_measure]
        elif effective_comparison_type == "none":
            buckets = [
                {
                    "items": [
                        self._get_metric_measure(
                            new_metric_id,
                            new_local_identifier,
                            self.obj["kpi"]["meta"]["title"],
                        )
                    ],
                    "localIdentifier": "measures",
                }
            ]
        if not buckets:
            return
        return {
            "data": {
                "id": self.new_insight_id,
                "type": "visualizationObject",
                "attributes": {
                    "title": self.obj["kpi"]["meta"]["title"],
                    "description": self.obj["kpi"]["meta"]["summary"],
                    "content": {
                        "buckets": buckets,
                        "filters": self._get_insight_filter(self.obj),
                        "sorts": [],
                        "properties": self._get_properties(
                            self.obj, effective_comparison_type
                        ),
                        "visualizationUrl": "local:headline",
                        "version": "2",
                    },
                },
            }
        }

    def create_or_update_insight_from_kpi(
        self, new_comparison_insight: Any, overwrite_existing: bool
    ) -> None:
        """Creates or updates the insight from the KPI."""
        if overwrite_existing:
            operation = Operation.CREATE_OR_UPDATE_WITH_RETRY
        else:
            operation = Operation.CREATE_WITH_RETRY
        process_objects(
            self.ctx.cloud_client,
            [new_comparison_insight],
            "kpi_comparison_insight",
            write_skipped=False,
            operation=operation,
        )

    def get(self):
        if "measure" in self.platform_measure_definition:
            return self.get_measure()
        elif "kpi" in self.platform_measure_definition:
            return self.create_insight_object_from_kpi()
