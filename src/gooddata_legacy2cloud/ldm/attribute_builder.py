# (C) 2026 GoodData Corporation
"""
This module contains the AttributeBuilder class,
which is responsible for processing Legacy attributes and
preparing Cloud attributes.
"""

from gooddata_legacy2cloud.ldm.data_classes import AttributeBuilderConfig
from gooddata_legacy2cloud.ldm.model_helpers import (
    get_label_value_type,
    transform_legacy_identifier,
)
from gooddata_legacy2cloud.ldm.utils import (
    find_value_of_default_label,
    get_unique_id,
)


class AttributeBuilder:
    """
    The AttributeBuilder class is responsible for processing Legacy attributes
    and preparing Cloud attributes.
    """

    def __init__(self, config: AttributeBuilderConfig):
        """
        Initializes an AttributeBuilder object.
        """
        self.legacy_dataset_id = config.legacy_dataset_id
        self.dataset_id = transform_legacy_identifier(config.legacy_dataset_id)
        self.logger = config.logger
        self.ADSMapping = config.ADSMapping
        self.TagProvider = config.TagProvider
        self.ignore_folders = config.ignore_folders

    def get_attributes(self, legacy_attributes):
        """
        Processes the Legacy attributes and prepares Cloud attributes.
        """
        attributes = []
        for dataset_attribute in legacy_attributes:
            new_attribute = self.get_attribute(dataset_attribute["attribute"])
            attributes.append(new_attribute)
        return attributes

    def _get_tags(self, identifier, folder):
        """
        Returns the tags for the given identifier.
        """
        tags = self.TagProvider.get_tags(identifier)

        if self.ignore_folders or folder is None:
            return tags

        tags.append(folder)
        # make sure there are no duplicities
        return list(set(tags))

    def get_cloud_labels(self, legacy_labels):
        """
        Returns labels for Cloud.
        """
        cloud_dataset_id = transform_legacy_identifier(self.legacy_dataset_id)
        labels = []
        for label in legacy_labels:
            label = label["label"]
            label_id = label["identifier"]

            cloud_label_id = self._get_cloud_identifier(label_id, cloud_dataset_id)

            self.logger.write_identifier_relation(label_id, cloud_label_id)

            # If label is would be the same as attribute name,
            # it is being created automatically in Cloud,
            # therefore we skip it to not create two same attributes
            if label_id.count(".") == 2:
                continue

            label_column = self.ADSMapping.get_attribute_column(
                self.legacy_dataset_id, label_id
            )
            label_value_type = get_label_value_type(label["type"])

            labels.append(
                {
                    "description": "",
                    "id": cloud_label_id,
                    "sourceColumn": label_column,
                    "sourceColumnDataType": "STRING",
                    "tags": [label["title"]],
                    "title": label["title"],
                    "valueType": label_value_type,
                }
            )

        return labels

    def _get_cloud_identifier(self, legacy_identifier, dataset_id):
        """
        Returns a unique Cloud identifier for the given Legacy identifier.
        """
        candidate_new_id = transform_legacy_identifier(legacy_identifier, dataset_id)

        # need to make sure the id is unique
        return get_unique_id(self.logger, legacy_identifier, candidate_new_id)

    def get_attribute(self, legacy_attribute):
        """
        Prepares a Cloud attribute based on the Legacy model.
        """
        if "labels" in legacy_attribute:
            labels = self.get_cloud_labels(legacy_attribute["labels"])
        else:
            labels = []

        attr_id = self._get_cloud_identifier(
            legacy_attribute["identifier"], self.dataset_id
        )

        attr_column = self.ADSMapping.get_attribute_column(
            self.legacy_dataset_id, legacy_attribute["identifier"]
        )

        self.logger.write_identifier_relation(legacy_attribute["identifier"], attr_id)

        folder = legacy_attribute.get("folder", None)

        new_attribute = {
            "id": attr_id,
            "description": legacy_attribute["title"],
            "labels": labels,
            "sourceColumn": attr_column,
            "sourceColumnDataType": "STRING",
            "tags": self._get_tags(legacy_attribute["identifier"], folder),
            "title": legacy_attribute["title"],
        }

        if "defaultLabel" in legacy_attribute:
            default_label_id = find_value_of_default_label(
                self.logger, legacy_attribute["defaultLabel"]
            )

            self.logger.write_identifier_relation(
                legacy_attribute["defaultLabel"], default_label_id
            )

            # sets default label only if it is different from attribute
            # AND if there are labels
            if default_label_id != attr_id and labels:
                new_attribute["defaultView"] = {
                    "id": default_label_id,
                    "type": "label",
                }

        if "sortOrder" in legacy_attribute:
            sort_details = legacy_attribute["sortOrder"]["attributeSortOrder"]

            sort_column = self.ADSMapping.get_attribute_column(
                self.legacy_dataset_id, sort_details["label"]
            )

            new_attribute["sortColumn"] = sort_column
            new_attribute["sortDirection"] = sort_details["direction"]

        return new_attribute
