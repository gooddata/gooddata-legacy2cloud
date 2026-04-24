# (C) 2026 GoodData Corporation
"""
This module contains the ReferenceBuilder class, which is used to build
and validate references.
"""

from .model_helpers import (
    transform_legacy_identifier,
)
from .utils import get_cloud_id_date_dimension


class ReferenceBuilder:
    """
    A class that builds reference objects for datasets and bridges.
    """

    def __init__(self, legacy_model, ADSMapping, mappings):
        self.legacy_model = legacy_model
        self.ADSMapping = ADSMapping
        self.mappings = mappings

    def _create_reference_object(self, dataset, column, data_type, multivalue):
        """
        Creates a reference object for a dataset.
        """
        return {
            "identifier": {"id": dataset, "type": "dataset"},
            "multivalue": multivalue,
            "sourceColumns": [column],
            "sourceColumnDataTypes": [data_type],
        }

    def _create_grain_reference_object(self, data_type, dataset, identifier, column):
        """
        Returns a reference object for a given grain.
        """
        if data_type == "date":
            return {
                "identifier": {"id": dataset, "type": "dataset"},
                "multivalue": False,
                "sources": [
                    {
                        "column": column,
                        "dataType": "DATE",
                        "target": {"id": identifier, "type": "date"},
                    }
                ],
            }
        if data_type == "attribute":
            return {
                "identifier": {"id": dataset, "type": "dataset"},
                "multivalue": False,
                "sources": [
                    {
                        "column": column,
                        "dataType": "STRING",
                        "target": {"id": identifier, "type": "attribute"},
                    }
                ],
            }

        raise RuntimeError(f"Unknown datatype {data_type}")

    def _create_update_dataset_reference(self, references, reference):
        """
        Create or Update the dataset reference.
        """
        for index, item in enumerate(references):
            if item["identifier"]["id"] == reference["identifier"]["id"]:
                reference["multivalue"] = references[index]["multivalue"]

                references[index] = reference
                return references

        references.append(reference)
        return references

    def _get_reference_obj(self, current_dataset, reference_id):
        """
        Returs the reference object for a dataset.
        """
        model = self.legacy_model["projectModelView"]["model"]["projectModel"]
        multivalue = self._is_multivalue(current_dataset, reference_id)
        column = self.ADSMapping.get_dataset_reference_column(
            current_dataset, reference_id
        )

        for dataset in model["datasets"]:
            if dataset["dataset"]["identifier"] == reference_id:
                cloud_id = transform_legacy_identifier(reference_id)
                data_type = "STRING"

                return self._create_reference_object(
                    cloud_id, column, data_type, multivalue
                )

        for dataset in model["dateDimensions"]:
            if dataset["dateDimension"]["identifierPrefix"] == reference_id:
                cloud_id = get_cloud_id_date_dimension(reference_id)
                data_type = "DATE"

                return self._create_reference_object(
                    cloud_id, column, data_type, multivalue
                )

        return {}

    def _is_multivalue(self, source_dataset_id, reference_id):
        """
        Checks if a reference is multivalue.
        """
        model = self.legacy_model["projectModelView"]["model"]["projectModel"]

        for dataset in model["datasets"]:
            if (
                dataset["dataset"]["identifier"] == reference_id
                and "bridges" in dataset["dataset"]
                and source_dataset_id in dataset["dataset"]["bridges"]
            ):
                return True

        return False

    def _get_reference_and_grain(self, source_dataset, grain_item):
        """
        Set composite key based on Legacy model.
        @param source dataset: Legacy dataset id
        @param grain_item: grain item
        """
        if "dateDimension" in grain_item:
            id_pk = get_cloud_id_date_dimension(grain_item["dateDimension"])
            data_type = "date"
            composite_key = {"id": id_pk, "type": data_type}

            column = self.ADSMapping.get_grain_column(
                source_dataset,
                grain_item["dateDimension"],
                grain_item["dateDimension"],
                "dateDimension",
            )

            return (
                self._create_grain_reference_object(data_type, id_pk, id_pk, column),
                composite_key,
            )

        elif "attribute" in grain_item:
            id_pk = self.mappings.search_mapping_identifier(grain_item["attribute"])
            dataset = id_pk.split(".")[0]
            referenced_dataset = f"dataset.{dataset}"
            data_type = "attribute"
            composite_key = {"id": id_pk, "type": data_type}

            column = self.ADSMapping.get_grain_column(
                source_dataset,
                referenced_dataset,
                grain_item["attribute"],
                "attribute",
            )

            return (
                self._create_grain_reference_object(data_type, dataset, id_pk, column),
                composite_key,
            )
        else:
            raise ValueError(
                (f"transform_dataset_grain_keys - unknown graintype: {grain_item} ")
            )

    def get_references_and_grains(self, dataset_index, cloud_model):
        """
        Returns the adjusted references and grains
        (as grain contains Legacy format that needs to be adjusted requires) for the dataset.
        """
        references = []
        grains = []
        legacy_dataset = self.legacy_model["projectModelView"]["model"]["projectModel"][
            "datasets"
        ][dataset_index]["dataset"]
        legacy_dataset_id = legacy_dataset["identifier"]
        cloud_dataset = cloud_model["ldm"]["datasets"][dataset_index]

        if "references" in legacy_dataset:
            for reference_id in legacy_dataset["references"]:
                references.append(
                    self._get_reference_obj(legacy_dataset["identifier"], reference_id)
                )

        if "grain" in cloud_dataset:
            if len(cloud_dataset["grain"]) == 1:
                grains.append(cloud_dataset["grain"][0])

            elif len(cloud_dataset["grain"]) > 1:
                for grain_item in cloud_dataset["grain"]:
                    reference, grain_item = self._get_reference_and_grain(
                        legacy_dataset_id, grain_item
                    )
                    grains.append(grain_item)

                    # do not append reference to itself
                    if cloud_dataset["id"] == grain_item["id"].split(".")[0]:
                        continue

                    references = self._create_update_dataset_reference(
                        references, reference
                    )

        return references, grains
