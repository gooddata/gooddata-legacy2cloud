# (C) 2026 GoodData Corporation
"""
This module contains the FactBuilder class,
which is responsible for building facts for a dataset.
"""

from gooddata_legacy2cloud.ldm.data_classes import FactBuilderConfig
from gooddata_legacy2cloud.ldm.model_helpers import (
    transform_legacy_identifier,
)


class FactBuilder:
    """
    A class that builds facts for a dataset.
    """

    def __init__(self, config: FactBuilderConfig):
        self.legacy_facts = config.legacy_fact
        self.legacy_dataset_id = config.legacy_dataset_id
        self.dataset_id = transform_legacy_identifier(config.legacy_dataset_id)
        self.logger = config.logger
        self.facts = []
        self.ADSMapping = config.ADSMapping
        self.TagProvider = config.TagProvider
        self.ignore_folders = config.ignore_folders
        self._process_facts()

    def get_tags(self, identifier, folder):
        """
        Get the tags for the given identifier.
        """
        tags = self.TagProvider.get_tags(identifier)

        if self.ignore_folders or folder is None:
            return tags

        tags.append(folder)
        # make sure there are no duplicities
        return list(set(tags))

    def _process_facts(self):
        """
        Process the Legacy facts and build the facts.
        """
        for fact in self.legacy_facts:
            legacy_fact_id = fact["fact"]["identifier"]
            cloud_fact_id = transform_legacy_identifier(legacy_fact_id, self.dataset_id)

            fact_column = self.ADSMapping.get_fact_column(
                self.legacy_dataset_id, legacy_fact_id
            )

            folder = fact["fact"].get("folder", None)

            self.logger.write_identifier_relation(legacy_fact_id, cloud_fact_id)
            self.facts.append(
                {
                    "description": fact["fact"]["title"],
                    "id": cloud_fact_id,
                    "sourceColumn": fact_column,
                    "sourceColumnDataType": "NUMERIC",
                    "tags": self.get_tags(legacy_fact_id, folder),
                    "title": fact["fact"]["title"],
                }
            )

    def get(self):
        """
        Get the processed facts.
        """
        return self.facts
