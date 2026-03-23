# (C) 2026 GoodData Corporation
"""
This module contains the CloudModelBuilder class which is used for building
the model for Cloud from the Platform model. It includes functionality for
retrieving datasets and handling environment variables.
"""

import dataclasses
from pathlib import Path

from gooddata_platform2cloud.constants import (
    LDM_MAPPING_FILE,
    UNKNOWN_DATE_MIGRATION_GRANULARITY,
)
from gooddata_platform2cloud.helpers import prefix_filename
from gooddata_platform2cloud.ldm.ads_mapping import ADSMapping
from gooddata_platform2cloud.ldm.attribute_builder import AttributeBuilder
from gooddata_platform2cloud.ldm.data_classes import (
    AttributeBuilderConfig,
    FactBuilderConfig,
)
from gooddata_platform2cloud.ldm.fact_builder import FactBuilder
from gooddata_platform2cloud.ldm.model_builder_config import CloudModelBuilderConfig
from gooddata_platform2cloud.ldm.tag_provider import TagProvider
from gooddata_platform2cloud.ldm.utils import get_cloud_id_date_dimension
from gooddata_platform2cloud.output_writer import OutputWriter

from ..id_mappings import IdMappings
from .model_helpers import (
    get_data_source_table,
    get_date_model,
    transform_platform_identifier,
)
from .reference_builder import ReferenceBuilder

LOG_PATH = prefix_filename("./logs")

DATE_MAPPINGS = [
    # Platform, Cloud
    # OLD DATE MAPPINGS
    ["date.yyyymmdd", "day"],
    ["date.ddmmyyyy", "day"],
    ["date.mmddyyyy", "day"],
    ["date.eddmmyyyy", "day"],
    ["date.long", "day"],
    ["date.mdyy", "day"],
    ["abU81lMifn6q", "dayOfWeek"],
    ["abW81lMifn6q", "dayOfWeek"],
    ["abY81lMifn6q", "dayOfWeek"],
    ["abE81lMifn6q", "dayOfYear"],
    ["aam81lMifn6q", "quarterOfYear"],
    ["abm81lMifn6q", "monthOfYear"],
    ["abo81lMifn6q", "monthOfYear"],
    ["abq81lMifn6q", "monthOfYear"],
    ["abs81lMifn6q", "monthOfYear"],
    ["aas81lMifn6q", "week"],
    ["aau81lMifn6q", "week"],
    ["aaw81lMifn6q", "week"],
    ["aay81lMifn6q", "week"],
    ["aaA81lMifn6q", "week"],
    ["aaC81lMifn6q", "week"],
    ["aaU81lMifn6q", "week"],
    ["aaW81lMifn6q", "week"],
    ["aaY81lMifn6q", "week"],
    ["aa081lMifn6q", "week"],
    ["aa281lMifn6q", "week"],
    ["aa481lMifn6q", "week"],
    ["aaI81lMifn6q", "weekOfYear"],
    ["abK81lMifn6q", "dayOfWeek"],
    ["abM81lMifn6q", "dayOfWeek"],
    ["abO81lMifn6q", "dayOfWeek"],
    ["act81lMifn6q", "month"],
    ["acv81lMifn6q", "month"],
    ["acx81lMifn6q", "month"],
    ["aca81lMifn6q", "dayOfMonth"],
    ["aag81lMifn6q", "year"],
    ["aba81lMifn6q", "weekOfYear"],
    ["aci81lMifn6q", "quarter"],
    ["year", "year"],
    ["quarter.in.year", "quarterOfYear"],
    ["week", "week"],
    ["week.in.year", "weekOfYear"],
    ["euweek", "week"],
    ["euweek.in.year", "weekOfYear"],
    ["month.in.year", "monthOfYear"],
    ["day.in.year", "dayOfYear"],
    ["day.in.week", "dayOfWeek"],
    ["day.in.euweek", "dayOfWeek"],
    ["day.in.month", "dayOfMonth"],
    ["quarter", "quarter"],
    ["month", "month"],
    ["date", "day"],
    ["date.day", "day"],
    ["date.day.uk", "day"],
    ["date.day.us", "day"],
    ["date.day.eu", "day"],
    ["day", "dayOfWeek"],
    # NEW DATE MAPPINGS without duplicities from old mappings
    ["date.day.yyyy_mm_dd", "day"],
    ["date.day.uk.dd_mm_yyyy", "day"],
    ["date.day.us.mm_dd_yyyy", "day"],
    ["date.day.eu.dd_mm_yyyy", "day"],
    ["date.day.us.long", "day"],
    ["date.day.us.m_d_yy", "day"],
    ["day.in.euweek.short", "dayOfWeek"],
    ["day.in.euweek.number", "dayOfWeek"],
    ["day.in.euweek.long", "dayOfWeek"],
    ["day.in.year.default", "dayOfYear"],
    ["quarter.in.year.default", "quarterOfYear"],
    ["month.in.year.short", "monthOfYear"],
    ["month.in.year.m_q", "monthOfYear"],
    ["month.in.year.number", "monthOfYear"],
    ["month.in.year.long", "monthOfYear"],
    ["week.wk_qtr_year", "week"],
    ["week.from_to", "week"],
    ["week.starting", "week"],
    ["week.wk_year_cont", "week"],
    ["week.wk_year", "week"],
    ["week.wk_qtr_year_cont", "week"],
    ["euweek.wk_qtr_year", "week"],
    ["euweek.from_to", "week"],
    ["euweek.starting", "week"],
    ["euweek.wk_year_cont", "week"],
    ["euweek.wk_year", "week"],
    ["euweek.wk_qtr_year_cont", "week"],
    ["week.in.year.number_us", "weekOfYear"],
    ["day.in.week.short", "dayOfWeek"],
    ["day.in.week.number", "dayOfWeek"],
    ["day.in.week.long", "dayOfWeek"],
    ["month.short", "month"],
    ["month.number", "month"],
    ["month.long", "month"],
    ["day.in.month.default", "dayOfMonth"],
    ["year.default", "year"],
    ["euweek.in.year.number_eu", "weekOfYear"],
    ["quarter.short_us", "quarter"],
    ["year.for.week.number", "year"],
    ["year.for.euweek.number", "year"],
    ["quarter.for.week.number", "quarter"],
    ["quarter.for.euweek.number", "quarter"],
    ["aby81lMifn6q", f"{UNKNOWN_DATE_MIGRATION_GRANULARITY}.aby81lMifn6q"],
    ["aaO81lMifn6q", f"{UNKNOWN_DATE_MIGRATION_GRANULARITY}.aaO81lMifn6q"],
    ["abg81lMifn6q", f"{UNKNOWN_DATE_MIGRATION_GRANULARITY}.abg81lMifn6q"],
    ["ab481lMifn6q", f"{UNKNOWN_DATE_MIGRATION_GRANULARITY}.ab481lMifn6q"],
    ["week.in.quarter", f"{UNKNOWN_DATE_MIGRATION_GRANULARITY}.week.in.quarter"],
    ["euweek.in.quarter", f"{UNKNOWN_DATE_MIGRATION_GRANULARITY}.euweek.in.quarter"],
    ["month.in.quarter", f"{UNKNOWN_DATE_MIGRATION_GRANULARITY}.month.in.quarter"],
    ["day.in.quarter", f"{UNKNOWN_DATE_MIGRATION_GRANULARITY}.day.in.quarter"],
    [
        "month.in.quarter.number",
        f"{UNKNOWN_DATE_MIGRATION_GRANULARITY}.month.in.quarter.number",
    ],
    [
        "week.in.quarter.number_us",
        f"{UNKNOWN_DATE_MIGRATION_GRANULARITY}.week.in.quarter.number_us",
    ],
    [
        "euweek.in.quarter.number_eu",
        f"{UNKNOWN_DATE_MIGRATION_GRANULARITY}.euweek.in.quarter.number_eu",
    ],
    [
        "day.in.quarter.default",
        f"{UNKNOWN_DATE_MIGRATION_GRANULARITY}.day.in.quarter.default",
    ],
]


class CloudModelBuilder:
    """
    This class is responsible for building the Cloud model.

    It takes a GDC model as input and transforms it into a Cloud model.
    The class provides methods to process datasets, enhance the model
    with additional information, and generate the corresponding DDL in SQL.
    """

    def __init__(
        self,
        config: CloudModelBuilderConfig,
    ):
        self.model = {"ldm": {"datasets": [], "dateInstances": []}}
        self.platform_model = {}
        self.mappings = IdMappings()
        self.config: CloudModelBuilderConfig = self.adjust_config(config)
        self.ADSMapping = ADSMapping(self.config)
        self.TagProvider = TagProvider(self.config.platform_client)
        self.ignore_folders = config.ignore_folders
        self.ignore_explicit_mapping = config.ignore_explicit_mapping

    def adjust_config(self, config: CloudModelBuilderConfig) -> CloudModelBuilderConfig:
        """
        Adjusts the configuration.
        Put ws_data_filter_id to ws_data_filter_column if column is not set.
        """
        new_column = (
            config.ws_data_filter_column
            if config.ws_data_filter_column is not None
            else config.ws_data_filter_id
        )
        return dataclasses.replace(config, ws_data_filter_column=new_column)

    def _get_cloud_identifier(
        self, platform_identifier: str, dataset: str, log_mapping=True
    ):
        """
        Creates a Cloud identifier based on the Platform model.
        """
        new_id = transform_platform_identifier(platform_identifier, dataset)
        if log_mapping:
            self.mappings.add_mapping_identifier(platform_identifier, new_id)
        return new_id

    def prepare_ws_data_filter(self):
        """
        Prepares a workspace data filter based on the Platform model.
        it is submitted to workspaceDataFilters endpoint
        """
        return [
            {
                "data": {
                    "id": self.config.ws_data_filter_id,
                    "type": "workspaceDataFilter",
                    "attributes": {
                        "columnName": self.config.ws_data_filter_column,
                        "description": self.config.ws_data_filter_description,
                        "title": self.config.ws_data_filter_id,
                    },
                }
            }
        ]

    def get_tags(self, platform_identifier, folder):
        """
        Returns tags for the given identifier.
        """
        tags = self.TagProvider.get_tags(platform_identifier)

        if self.ignore_folders or folder is None:
            return tags

        tags.append(folder)
        # make sure there are no duplicities
        return list(set(tags))

    def _process_dataset(self, dataset):
        """
        This method is responsible for transforming a Platform dataset into
        a Cloud dataset. It also logs the transformation process.
        """
        platform_dataset_id = dataset["identifier"]
        dataset_id = transform_platform_identifier(dataset["identifier"])
        dataset_title = dataset["title"]

        # Ensure the logs directory exists, using Path to create it if needed
        Path(LOG_PATH).mkdir(parents=True, exist_ok=True)
        filename_log = f"{LOG_PATH}/{dataset_title}.csv"
        logger = OutputWriter(filename_log)

        logger.write_identifier_relation(platform_dataset_id, dataset_id)

        attr_builder = AttributeBuilder(
            AttributeBuilderConfig(
                platform_dataset_id,
                logger,
                ADSMapping=self.ADSMapping,
                TagProvider=self.TagProvider,
                ignore_folders=self.ignore_folders,
            )
        )

        attributes = []
        grain = []

        anchor_attribute = dataset["anchor"]["attribute"]
        anchor_attr_id = anchor_attribute["identifier"]

        if anchor_attr_id.endswith(".factsof"):
            # Common operations for all ".factsof" cases
            if "grain" in anchor_attribute and "labels" not in anchor_attribute:
                # setting up invalid Platform grain for further postprocessing
                grain = anchor_attribute["grain"]

                id_pk = self._get_cloud_identifier(
                    anchor_attr_id, dataset_id, log_mapping=False
                )
                self.mappings.add_mapping_identifier(anchor_attr_id, dataset_id)

            elif "labels" in anchor_attribute:
                # special case for factsof with labels
                id_pk = self._get_cloud_identifier(
                    anchor_attr_id, dataset_id, log_mapping=False
                )
                logger.write_identifier_relation(anchor_attr_id, dataset_id)

                anchor_attr_id = anchor_attribute["labels"][0]["label"]["identifier"]
                id_pk = self._get_cloud_identifier(
                    anchor_attr_id, dataset_id
                )  # Update id_pk with new anchor_attr_id
                grain = [{"id": id_pk, "type": "attribute"}]
            elif "grain" not in anchor_attribute:
                id_pk = self._get_cloud_identifier(
                    anchor_attr_id, dataset_id, log_mapping=False
                )
                logger.write_identifier_relation(anchor_attr_id, dataset_id)
                # Case where "grain" is not in attribute and it ends with ".factsof"
                grain = []
        else:
            # Case for identifiers not ending with ".factsof"
            if "grain" in anchor_attribute and "labels" not in anchor_attribute:
                grain = anchor_attribute["grain"]
                id_pk = self._get_cloud_identifier(
                    anchor_attr_id, dataset_id, log_mapping=False
                )

                # need to handle specific mapping for factsof
                self.mappings.add_mapping_identifier(anchor_attr_id, dataset_id)
            else:
                id_pk = self._get_cloud_identifier(anchor_attr_id, dataset_id)
                grain = [{"id": id_pk, "type": "attribute"}]

        if "labels" in anchor_attribute:
            # create generic attribute that is going to be enhanced by anchor specifics
            new_anchor_attribute = attr_builder.get_attribute(anchor_attribute)

            # need to override sourceColumn for anchor attribute type
            id_pk_column = self.ADSMapping.get_anchor_attribute_column(
                platform_dataset_id, anchor_attr_id
            )
            new_anchor_attribute["sourceColumn"] = id_pk_column
            new_anchor_attribute["id"] = id_pk

            # need to override sortColumn for anchor attribute type
            if "sortOrder" in anchor_attribute:
                sort_details = anchor_attribute["sortOrder"]["attributeSortOrder"]

                new_anchor_attribute["sortColumn"] = (
                    self.ADSMapping.get_anchor_attribute_column(
                        platform_dataset_id, sort_details["label"]
                    )
                )

            # add anchor to attributes
            attributes.append(new_anchor_attribute)

        if "attributes" in dataset:
            attributes = attributes + attr_builder.get_attributes(dataset["attributes"])

        facts = []
        if "facts" in dataset:
            fact_config = FactBuilderConfig(
                dataset["facts"],
                platform_dataset_id,
                logger,
                ADSMapping=self.ADSMapping,
                TagProvider=self.TagProvider,
                ignore_folders=self.ignore_folders,
            )
            fact_builder = FactBuilder(fact_config)
            facts = fact_builder.get()

        # references are process within postprocessing once all datasets are loaded
        references = []

        data_source_table_id = get_data_source_table(
            self.config.data_source_id,
            self.config.schema,
            dataset["identifier"],
            dataset_id,
            self.ADSMapping,
        )

        dataset = {
            "attributes": attributes,
            "description": "",
            "facts": facts,
            "grain": grain,  # contains Platform's format that is adjusted in postprocessing with refereces
            "id": dataset_id,
            "references": references,
            "tags": [dataset_title],
            "title": dataset_title,
            "dataSourceTableId": data_source_table_id,
        }

        if self.config.ws_data_filter_id:
            dataset["workspaceDataFilterColumns"] = [
                {
                    "name": f"{self.config.ws_data_filter_id}",
                    "dataType": "STRING",
                }
            ]
            dataset["workspaceDataFilterReferences"] = [
                {
                    "filterColumn": self.config.ws_data_filter_column,
                    "filterColumnDataType": "STRING",
                    "filterId": {
                        "id": self.config.ws_data_filter_id,
                        "type": "workspaceDataFilter",
                    },
                }
            ]

        self.model["ldm"]["datasets"].append(dataset)
        self.mappings.add_mapping_array(logger.get_mappings())

    def _process_date_dimensions(self):
        """
        Append date instances to the model.
        """
        model = self.platform_model["projectModelView"]["model"]["projectModel"]
        date_dimensions = model.get("dateDimensions", [])
        for date in date_dimensions:
            platform_id = date["dateDimension"]["identifierPrefix"]
            cloud_id = get_cloud_id_date_dimension(platform_id)
            date_title = date["dateDimension"]["title"]

            new_date = get_date_model(cloud_id, date_title, [date_title])
            self.model["ldm"]["dateInstances"].append(new_date)

            self.mappings.add_mapping_identifier(platform_id, cloud_id)

            # add additional datetime mappings
            for date_mapping in DATE_MAPPINGS:
                self.mappings.add_mapping_identifier(
                    f"{platform_id}.{date_mapping[0]}", f"{cloud_id}.{date_mapping[1]}"
                )

    def _process_data_datasets(self):
        """
        Enhances the Cloud model with information from the Platform model.
        """
        model = self.platform_model["projectModelView"]["model"]["projectModel"]
        for dataset in model["datasets"]:
            self._process_dataset(dataset["dataset"])

        # handle references and grains within model
        self.process_references_and_grains()

    def process_references_and_grains(self):
        reference_builder = ReferenceBuilder(
            self.platform_model, self.ADSMapping, self.mappings
        )
        for dataset_index, _dataset in enumerate(self.model["ldm"]["datasets"]):
            references, grain = reference_builder.get_references_and_grains(
                dataset_index, self.model
            )
            self.model["ldm"]["datasets"][dataset_index]["references"] = references
            self.model["ldm"]["datasets"][dataset_index]["grain"] = grain

    def load_platform_model(self, platform_model):
        """
        Load the Platform model into the CloudModelBuilder.
        """
        self.platform_model = platform_model
        self._process_date_dimensions()
        self._process_data_datasets()

        # Write mappings to file using OutputWriter
        mapping_logger = OutputWriter(LDM_MAPPING_FILE)
        for platform_id, cloud_id in self.mappings.get().items():
            mapping_logger.write_identifier_relation(platform_id, cloud_id)

    def get_model(self):
        """
        Returns the Cloud model.
        """
        return self.model
