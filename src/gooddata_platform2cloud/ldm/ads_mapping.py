# (C) 2026 GoodData Corporation
import logging

from gooddata_platform2cloud.ldm.model_builder_config import CloudModelBuilderConfig

logger = logging.getLogger("migration")


class ADSMapping:
    """
    A class that handles the mapping between Platform model and ADS.
    """

    def __init__(self, ctx: CloudModelBuilderConfig):
        self.ctx: CloudModelBuilderConfig = ctx
        self.dataset_mappings = {}
        self.table_mapping = {}
        self.table_prefix = self._get_table_prefix()

        if self.ctx.ignore_explicit_mapping:
            logger.info(
                "Explicit mapping is configured to be ignored. Using naming convention."
            )
        else:
            self._process_dataset_mappings()

    def _process_dataset_mappings(self):
        """
        Get explicit mapping from the configuration file.
        """
        platform_mappings = self.ctx.platform_client.get_dataset_mappings()

        if len(platform_mappings["datasetMappings"]["items"]) > 0:
            logger.info("Explicit mapping exists and is used.")
            dataset_mappings = platform_mappings["datasetMappings"]["items"]

            # prepare object mappings (attributes, labels, facts)
            for dataset in dataset_mappings:
                dataset_id = dataset["datasetMapping"]["identifier"]
                fields = {}
                references = {}
                self.table_mapping[dataset_id] = dataset["datasetMapping"]["source"][
                    "identifier"
                ]

                if "fields" in dataset["datasetMapping"]:
                    fields = self._get_fields_objects(
                        dataset["datasetMapping"]["fields"]
                    )

                if "references" in dataset["datasetMapping"]:
                    # prepare reference mappings
                    references = self._get_references_objects(
                        dataset["datasetMapping"]["references"]
                    )

                self.dataset_mappings[dataset_id] = {
                    "references": references,
                    "fields": fields,
                }
        else:
            logger.info("No explicit mapping found, naming convention is used.")

    def _get_references_objects(self, raw_references):
        references = {}
        # prepare reference mappings
        for reference in raw_references:
            mapping = reference["referenceMapping"]
            platform_id = mapping["target"]["identifier"]
            references[platform_id] = {
                "type": mapping["target"]["type"],
                "column": mapping["sourceColumn"]["name"],
                "dataType": mapping["sourceColumn"]["dataType"],
            }

        return references

    def _get_fields_objects(self, raw_fields):
        fields = {}
        for field in raw_fields:
            mapping = field["fieldMapping"]
            platform_id = mapping["field"]["identifier"]
            fields[platform_id] = {
                "column": mapping["sourceColumn"]["name"],
                "dataType": mapping["sourceColumn"]["dataType"],
            }

            # artificial field to cover label options
            if platform_id.startswith("label."):
                attr_platform_id = platform_id.replace("label.", "attr.")
                fields[attr_platform_id] = {
                    "column": mapping["sourceColumn"]["name"],
                    "dataType": mapping["sourceColumn"]["dataType"],
                }
        return fields

    def _get_object_mapping(self, dataset, obj_id):
        """
        Get the mapping for the given obj_id.
        @param dataset: the Platform dataset
        @param obj_id: the Platform identifier
        """
        if dataset in self.dataset_mappings:
            if obj_id in self.dataset_mappings[dataset]["fields"]:
                return self.dataset_mappings[dataset]["fields"][obj_id]
        return

    def _get_reference_mapping(self, current_dataset, reference_dataset):
        """
        Get the mapping for the given platform_id.
        @param current_dataset: the current Platform dataset
        @param reference_dataset: the referenced Platform dataset
        """
        if current_dataset in self.dataset_mappings:
            if (
                reference_dataset
                in self.dataset_mappings[current_dataset]["references"]
            ):
                return self.dataset_mappings[current_dataset]["references"][
                    reference_dataset
                ]
        return

    def _get_table_prefix(self):
        """
        Get table prefix from the output stage.
        """
        outputStage = self.ctx.platform_client.get_output_stage()

        if "outputStagePrefix" in outputStage["outputStage"]:
            return outputStage["outputStage"]["outputStagePrefix"]

        # otherwise return the one from the config
        return self.ctx.table_prefix

    def get_table_mapping(self, dataset_id):
        return self.table_mapping.get(dataset_id, None)

    def get_anchor_attribute_column(self, dataset, platform_id):
        """
        Returns the column name for the anchor attribute
        @param dataset: the Platform dataset
        @param platform_id: the Platform identifier
        """
        mapping = self._get_object_mapping(dataset, platform_id)
        if mapping is not None:
            return mapping["column"]

        parts = platform_id.split(".")

        if len(parts) == 3:
            if f"dataset.{parts[1]}" == dataset:
                return f"cp__{parts[2]}"
            else:
                return f"cp__{parts[1]}__{parts[2]}"
        elif len(parts) == 4:
            if f"dataset.{parts[1]}" == dataset:
                return f"l__{parts[2]}__{parts[3]}"
            else:
                return f"l__{parts[1]}__{parts[2]}__{parts[3]}"

        raise ValueError("ADS mapping - invalid identifier", platform_id)

    def get_grain_column(self, current_dataset, reference_dataset, platform_id, type):
        """
        Get the column name for grain
        @param current_dataset: the current Platform dataset
        @param reference_dataset: the referenced Platform dataset
        @param platform_id: the Platform identifier
        @param type: the type of the identifier e.g. attribute, dateDimension
        """
        mapping = self._get_reference_mapping(current_dataset, reference_dataset)
        if mapping is not None:
            return mapping["column"]

        if type == "attribute":
            parts = platform_id.split(".")
            return "r__" + parts[1]
        if type == "dateDimension":
            return f"d__{platform_id}"

        raise ValueError(
            "ADS mapping - invalid anchor attribute grain identifier", platform_id
        )

    def get_attribute_column(self, dataset, attr_id):
        """
        Returns the column name for the attribute
        @param dataset: the Platform dataset
        @param attr_id: the Platform attribute identifier
        """
        mapping = self._get_object_mapping(dataset, attr_id)
        if mapping is not None:
            return mapping["column"]

        parts = attr_id.split(".")

        if len(parts) == 3:
            if f"dataset.{parts[1]}" == dataset:
                return f"a__{parts[2]}"
            else:
                return f"a__{parts[1]}__{parts[2]}"
        elif len(parts) == 4:
            if f"dataset.{parts[1]}" == dataset:
                return f"l__{parts[2]}__{parts[3]}"
            else:
                return f"l__{parts[1]}__{parts[2]}__{parts[3]}"
        elif len(parts) == 5:
            if f"dataset.{parts[1]}" == dataset:
                return f"l__{parts[3]}__{parts[2]}_{parts[4]}"

        raise ValueError("ADS mapping - invalid identifier", attr_id)

    def get_fact_column(self, dataset, fact_id):
        """
        Returns the column name for the fact
        @param dataset: the Platform dataset
        @param fact_id: the Platform fact identifier
        """
        mapping = self._get_object_mapping(dataset, fact_id)
        if mapping is not None:
            return mapping["column"]

        parts = fact_id.split(".")

        if f"dataset.{parts[1]}" == dataset:
            return f"f__{parts[2]}"
        else:
            return f"f__{parts[1]}__{parts[2]}"

    def get_dataset_reference_column(self, dataset, reference_id):
        """
        Returns the column name for the dataset reference
        @param dataset: the Platform dataset
        @param reference_id: the Platform reference identifier
        """
        mapping = self._get_reference_mapping(dataset, reference_id)
        if mapping is not None:
            return mapping["column"]

        parts = reference_id.split(".")

        if len(parts) > 0 and parts[0] == "dataset":
            return f"r__{parts[1]}"
        else:
            return f"d__{reference_id}"
