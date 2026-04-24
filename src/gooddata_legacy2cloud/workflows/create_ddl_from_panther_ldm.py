# (C) 2026 GoodData Corporation
"""
This module contains the main function for creating
a Data Definition Language (DDL)
from a Cloud's Logical Data Model (LDM).
"""

import argparse
import logging

from gooddata_legacy2cloud.helpers import (
    append_content_to_file,
    get_json_content_from_file,
)
from gooddata_legacy2cloud.logging.config import configure_logger

DEFAULT_OUTPUT_FILE = "DDL.sql"
EXPORT_MODE = "postgre"

logger = logging.getLogger("migration")


def define_mode(input_source):
    """
    Defines the mode of the input source based on the EXPORT_MODE.
    """
    if EXPORT_MODE == "":
        return input_source
    if EXPORT_MODE == "postgre":
        if input_source == "STRING":
            return "VARCHAR(128)"
        if input_source == "NUMERIC":
            return "DECIMAL(12,2)"
        if input_source == "DATE":
            return "DATE"

        raise RuntimeError("Unknown Type of input source")

    return RuntimeError("Unknown export mode")


def create_ddl_for_dataset(dataset, output_filename):
    """
    Creates a Data Definition Language (DDL) for a given dataset.

    This function takes a dataset from the Cloud model as input and generates
    the corresponding DDL in SQL. The DDL includes the creation of tables,
    columns, and other necessary SQL constructs.
    """

    table_name = dataset["id"]
    drop_view_ddl = f"DROP VIEW IF EXISTS out_vw_migration_{table_name};\n\n"
    drop_table_ddl = f"DROP TABLE IF EXISTS out_migration_{table_name};\n\n"
    view_ddl = (
        f"CREATE OR REPLACE VIEW out_vw_migration_{table_name} \n"
        f"    AS SELECT * FROM out_migration_{table_name};\n\n"
    )
    table_ddl = f"CREATE TABLE out_migration_{table_name} (\n"

    pk_constaint = ""
    if len(dataset["grain"]) == 1:
        grain_id = dataset["grain"][0]["id"].split(".", 1)[1]
        pk_constaint = (
            f"CONSTRAINT C_PRIMARY_out_migration_{table_name} "
            f"PRIMARY KEY ({grain_id}),\n"
        )

    for attribute in dataset["attributes"]:
        # attribute_id = attribute["id"]
        source_column = attribute["sourceColumn"]
        source_column_type = define_mode(attribute["sourceColumnDataType"])

        table_ddl += f"{source_column} {source_column_type},\n"

        for label in attribute["labels"]:
            label_column = label["sourceColumn"]
            label_source_column_type = define_mode(label["sourceColumnDataType"])
            # If for some reason we have label thats same as in attribute,
            # we dont create column twice (BB athleticenrollement, season)
            if label_column == source_column:
                continue
            table_ddl += f"{label_column} {label_source_column_type},\n"

    for fact in dataset["facts"]:
        source_column = fact["sourceColumn"]
        source_column_type = define_mode(fact["sourceColumnDataType"])
        table_ddl += f"{source_column} {source_column_type},\n"

    if len(dataset["references"]):
        for reference in dataset["references"]:
            source_column = reference["sourceColumns"][0]
            source_column_type = define_mode(reference["sourceColumnDataTypes"][0])
            table_ddl += f"{source_column} {source_column_type},\n"

    table_ddl += pk_constaint
    table_ddl = table_ddl.rstrip(",\n")
    table_ddl += "\n)\n;\n"

    append_content_to_file(output_filename, drop_view_ddl)
    append_content_to_file(output_filename, drop_table_ddl)
    append_content_to_file(output_filename, table_ddl)
    append_content_to_file(output_filename, view_ddl)
    logger.info("processing %s", table_name)


def main():
    """
    The main function of the script.
    """
    configure_logger()
    parser = argparse.ArgumentParser(description="Script inputs")
    parser.add_argument("ldm_filename", help="Source file of Cloud's LDM")
    parser.add_argument(
        "--output",
        dest="output",
        default=DEFAULT_OUTPUT_FILE,
        help="SQL output filename",
    )
    args = parser.parse_args()

    input_filename = args.ldm_filename
    output_filename = args.output

    try:
        ldm = get_json_content_from_file(input_filename)
    except FileNotFoundError:
        logger.error("Source LDM File not found")
        return

    # create output file
    with open(output_filename, "w", encoding="utf-8"):
        pass

    # prepare DDL
    for dataset in ldm["ldm"]["datasets"]:
        create_ddl_for_dataset(dataset, output_filename)

    logger.info("DDL has been stored in '%s'", output_filename)


if __name__ == "__main__":
    main()
