# (C) 2026 GoodData Corporation
"""
This module contains helper functions for transforming identifiers
and getting reference objects for grains.
"""


def transform_legacy_identifier(legacy_identifier, dataset=""):
    """
    Transforms a Legacy identifier into a Cloud identifier.
    """
    items = legacy_identifier.split(".")
    match items[0]:
        case "attr":
            new_id = f"{dataset}." + ".".join(items[2:])
        case "fact":
            new_id = f"{dataset}.f_{items[-1]}"
        case "label":
            new_id = f"{dataset}." + ".".join(items[2:])
        case "dataset":
            new_id = items[1]
        case _:
            raise ValueError(
                f"Unknown Cloud identifier transformation: {legacy_identifier}"
            )

    return new_id


def get_reference_for_grain(data_type, dataset, identifier, column):
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


def get_data_source_table(
    data_source_id, schema, legacy_dataset_id, cloud_dataset_id, ADSMapping
):
    """
    Returns a data source table.
    """

    id = ADSMapping.get_table_mapping(legacy_dataset_id)
    if id is None:
        table_prefix = ADSMapping.table_prefix
        id = f"{table_prefix}{cloud_dataset_id}"

    return {
        "dataSourceId": data_source_id,
        "id": id,
        "path": [schema, id],
        "type": "dataSource",
    }


def get_date_model(model_id, title, tags):
    """
    Returns date model.
    """
    return {
        "description": "",
        "granularities": [
            "DAY",
            "WEEK",
            "MONTH",
            "QUARTER",
            "YEAR",
            "DAY_OF_WEEK",
            "DAY_OF_MONTH",
            "DAY_OF_YEAR",
            "WEEK_OF_YEAR",
            "MONTH_OF_YEAR",
            "QUARTER_OF_YEAR",
        ],
        "granularitiesFormatting": {
            "titleBase": "",
            "titlePattern": "%titleBase - %granularityTitle",
        },
        "id": model_id,
        "tags": tags,
        "title": title,
    }


def get_label_value_type(label_type):
    """
    Returns a label value type.
    """
    if label_type == "GDC.text":
        return "TEXT"

    if label_type:
        return "HYPERLINK"

    raise ValueError(f"Unknown label type: {label_type}")
