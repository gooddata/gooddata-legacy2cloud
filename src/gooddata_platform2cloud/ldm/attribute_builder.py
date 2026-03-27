# (C) 2026 GoodData Corporation
"""
This module contains the AttributeBuilder class,
which is responsible for processing Platform attributes and
preparing Cloud attributes.
"""

from gooddata_platform2cloud.ldm.data_classes import AttributeBuilderConfig
from gooddata_platform2cloud.ldm.model_helpers import (
    get_label_value_type,
    transform_platform_identifier,
)
from gooddata_platform2cloud.ldm.utils import (
    find_value_of_default_label,
    get_unique_id,
)


class AttributeBuilder:
    """
    The AttributeBuilder class is responsible for processing Platform attributes
    and preparing Cloud attributes.
    """

    def __init__(self, config: AttributeBuilderConfig):
        """
        Initializes an AttributeBuilder object.
        """
        self.platform_dataset_id = config.platform_dataset_id
        self.dataset_id = transform_platform_identifier(config.platform_dataset_id)
        self.logger = config.logger
        self.ADSMapping = config.ADSMapping
        self.TagProvider = config.TagProvider
        self.ignore_folders = config.ignore_folders

    def get_attributes(self, platform_attributes):
        """
        Processes the Platform attributes and prepares Cloud attributes.
        """
        attributes = []
        for dataset_attribute in platform_attributes:
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

    def get_cloud_labels(self, platform_labels):
        """
        Returns labels for Cloud.
        """
        cloud_dataset_id = transform_platform_identifier(self.platform_dataset_id)
        labels = []
        for label in platform_labels:
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
                self.platform_dataset_id, label_id
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

    def _get_cloud_identifier(self, platform_identifier, dataset_id):
        """
        Returns a unique Cloud identifier for the given Platform identifier.
        """
        candidate_new_id = transform_platform_identifier(
            platform_identifier, dataset_id
        )

        # need to make sure the id is unique
        return get_unique_id(self.logger, platform_identifier, candidate_new_id)

    def get_attribute(self, platform_attribute):
        """
        Prepares a Cloud attribute based on the Platform model.
        """
        if "labels" in platform_attribute:
            labels = self.get_cloud_labels(platform_attribute["labels"])
        else:
            labels = []

        attr_id = self._get_cloud_identifier(
            platform_attribute["identifier"], self.dataset_id
        )

        attr_column = self.ADSMapping.get_attribute_column(
            self.platform_dataset_id, platform_attribute["identifier"]
        )

        self.logger.write_identifier_relation(platform_attribute["identifier"], attr_id)

        folder = platform_attribute.get("folder", None)

        new_attribute = {
            "id": attr_id,
            "description": platform_attribute["title"],
            "labels": labels,
            "sourceColumn": attr_column,
            "sourceColumnDataType": "STRING",
            "tags": self._get_tags(platform_attribute["identifier"], folder),
            "title": platform_attribute["title"],
        }

        if "defaultLabel" in platform_attribute:
            default_label_id = find_value_of_default_label(
                self.logger, platform_attribute["defaultLabel"]
            )

            self.logger.write_identifier_relation(
                platform_attribute["defaultLabel"], default_label_id
            )

            # sets default label only if it is different from attribute
            # AND if there are labels
            if default_label_id != attr_id and labels:
                new_attribute["defaultView"] = {
                    "id": default_label_id,
                    "type": "label",
                }

        if "sortOrder" in platform_attribute:
            sort_details = platform_attribute["sortOrder"]["attributeSortOrder"]

            sort_column = self.ADSMapping.get_attribute_column(
                self.platform_dataset_id, sort_details["label"]
            )

            new_attribute["sortColumn"] = sort_column
            new_attribute["sortDirection"] = sort_details["direction"]

        return new_attribute
