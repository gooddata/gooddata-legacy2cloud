# (C) 2026 GoodData Corporation
"""
Tool to get dependencies of objects on a Platform domain.

Currently supported object types are listed in the ObjectType enum.
"""

import argparse
import csv
import os
from enum import StrEnum

from dotenv import load_dotenv

from gooddata_platform2cloud.backends.platform.client import PlatformClient
from gooddata_platform2cloud.logging.config import configure_logger
from gooddata_platform2cloud.models.platform.metrics import MetricWrapper
from gooddata_platform2cloud.models.platform.used_by import Entry, QueryResultWrapper

logger = configure_logger("platform_metric_dependencies")

BUFFER_SIZE = 100


class OutputSchema(Entry):
    """Defines output schema of the script. Extends Entry model from used_by.py."""

    lookup_object_id: str
    found_in_workspace_id: str


class ObjectType(StrEnum):
    """Supported input object types."""

    METRIC = "metric"
    ATTRIBUTE = "attribute"
    INSIGHT = "insight"


supported_types = [object_type for object_type in ObjectType]


def parse_arguments():
    """Parse arguments from the command line."""
    parser = argparse.ArgumentParser(description="Get object dependencies")
    parser.add_argument(
        "input_csv", help="Input CSV file with object identifiers", type=str
    )
    parser.add_argument(
        "output_csv", help="Output CSV file with dependencies", type=str
    )
    parser.add_argument("--env", help="Environment file", default=None)
    parser.add_argument(
        "--object-type",
        help="Type of objects to process",
        type=ObjectType,
        choices=supported_types,
    )

    # Workspace selection - mutually exclusive options
    workspace_group = parser.add_mutually_exclusive_group(required=True)
    workspace_group.add_argument(
        "--workspaces-csv",
        help="CSV file with workspace IDs (one per line)",
        type=str,
    )
    workspace_group.add_argument(
        "--dynamic-workspace-lookup",
        help="Dynamically fetch all workspaces from the Platform domain",
        action="store_true",
    )

    args = parser.parse_args()
    return args


def load_items_from_csv(path_to_csv: str) -> list[str]:
    """Load items from a CSV file (one per line, first column)."""
    if not isinstance(path_to_csv, str):
        raise ValueError("Path to CSV must be a string")
    if path_to_csv is None or path_to_csv == "":
        raise ValueError("Path to CSV is required")
    if not os.path.exists(path_to_csv):
        raise FileNotFoundError(f"File not found: {path_to_csv}")
    if not os.path.isfile(path_to_csv):
        raise ValueError(f"Path to CSV is not a file: {path_to_csv}")

    with open(path_to_csv, "r") as file:
        reader = csv.reader(file)
        items = []
        for row in reader:
            if row:  # Skip empty lines
                items.append(row[0])

    if len(items) == 0:
        raise ValueError(f"No items found in CSV: {path_to_csv}")

    return items


class RequiredEnvVars:
    platform_domain: str
    platform_login: str
    platform_password: str

    def __init__(
        self, platform_domain: str, platform_login: str, platform_password: str
    ):
        self.platform_domain = platform_domain
        self.platform_login = platform_login
        self.platform_password = platform_password

    @classmethod
    def from_env(cls, env_file: str | None = None):
        if env_file is not None:
            load_dotenv(dotenv_path=env_file, override=True)
        missing_vars = []
        platform_login = os.getenv("PLATFORM_LOGIN")
        platform_password = os.getenv("PLATFORM_PASSWORD")
        platform_domain = os.getenv("PLATFORM_DOMAIN")

        if platform_login is None:
            missing_vars.append("PLATFORM_LOGIN")
        if platform_password is None:
            missing_vars.append("PLATFORM_PASSWORD")
        if platform_domain is None:
            missing_vars.append("PLATFORM_DOMAIN")
        if missing_vars:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing_vars)}"
            )
        return cls(
            platform_domain=os.environ["PLATFORM_DOMAIN"],
            platform_login=os.environ["PLATFORM_LOGIN"],
            platform_password=os.environ["PLATFORM_PASSWORD"],
        )


def prepare_output_csv(path_to_csv: str) -> None:
    """
    Prepare output CSV file.
    """
    if not isinstance(path_to_csv, str):
        raise ValueError("Path to CSV must be a string")
    if path_to_csv is None or path_to_csv == "":
        raise ValueError("Path to CSV is required")

    if not path_to_csv.endswith(".csv"):
        raise ValueError("Path to CSV must end with .csv")

    if os.path.exists(path_to_csv):
        if os.path.isdir(path_to_csv):
            raise ValueError(f"Path to CSV is a directory: {path_to_csv}")

        logger.warning(f"Overwriting existing output CSV file: {path_to_csv}")
        os.remove(path_to_csv)

    with open(path_to_csv, "w") as file:
        writer = csv.DictWriter(file, fieldnames=OutputSchema.model_fields.keys())
        writer.writeheader()


def write_output_data_to_csv(path_to_csv: str, output_data: list[OutputSchema]) -> None:
    with open(path_to_csv, "a") as file:
        writer = csv.DictWriter(file, fieldnames=OutputSchema.model_fields.keys())
        writer.writerows([row.model_dump() for row in output_data])


def main() -> None:
    # Parse arguments - input csv
    args = parse_arguments()
    path_to_csv = args.input_csv

    if args.object_type not in ObjectType:
        raise ValueError(f"Invalid object type: {args.object_type}")

    object_type = ObjectType(args.object_type)

    # Load environment variables
    env_vars = RequiredEnvVars.from_env(args.env)

    # Setup Platform API
    platform_client = PlatformClient(
        domain=env_vars.platform_domain,
        pid="placeholder_pid",
        login=env_vars.platform_login,
        password=env_vars.platform_password,
    )

    # Load items from csv
    input_items = load_items_from_csv(path_to_csv)

    # Prepare output csv
    prepare_output_csv(args.output_csv)

    # Initialize array to store output data
    output_data_buffer: list[OutputSchema] = []
    row_count = 0

    # Resolve workspace IDs based on user selection
    if args.dynamic_workspace_lookup:
        logger.info("Using dynamic workspace lookup from Platform domain")
        metadata = platform_client.get_domain_metadata()
        workspace_ids = [link.identifier for link in metadata.about.links]
    else:
        logger.info(f"Loading workspaces from CSV: {args.workspaces_csv}")
        workspace_ids = load_items_from_csv(args.workspaces_csv)

    logger.info(f"Found {len(workspace_ids)} workspaces to process")

    # Iterate through workspace IDs
    for workspace_id in workspace_ids:
        logger.info(f"Processing workspace: {workspace_id}")

        # Set the workspace ID to the Platform client
        platform_client.pid = workspace_id

        # Get all objects from workspace based on type
        workspace_objects_by_id: dict[str, str] = {}
        if object_type == ObjectType.METRIC:
            raw_objects = platform_client.get_metrics()
            validated_metrics = [MetricWrapper(**metric) for metric in raw_objects]
            workspace_objects_by_id = {
                wrapper.metric.meta.identifier: wrapper.metric.meta.uri
                for wrapper in validated_metrics
            }

        elif object_type == ObjectType.ATTRIBUTE:
            raw_objects = platform_client.get_attributes()
            validated_objects = QueryResultWrapper(**raw_objects)
            entries = validated_objects.query.entries
            workspace_objects_by_id = {
                entry.identifier: entry.link for entry in entries
            }
        elif object_type == ObjectType.INSIGHT:
            query_result = platform_client.get_insights_query()
            entries = query_result.query.entries
            workspace_objects_by_id = {
                entry.identifier: entry.link for entry in entries
            }
        else:
            raise ValueError(f"Unsupported object type: {object_type}")

        # Iterate through input items
        for input_item in input_items:
            if input_item not in workspace_objects_by_id:
                # If object is not present in the workspace, skip it
                continue

            # Find object dependencies
            object_uri = workspace_objects_by_id[input_item]
            dependencies = platform_client.get_object_dependencies(object_uri)
            for dependency in dependencies.entries:
                output_schema = OutputSchema(
                    lookup_object_id=input_item,
                    found_in_workspace_id=workspace_id,
                    **dependency.model_dump(),
                )

                output_data_buffer.append(output_schema)

                if len(output_data_buffer) >= BUFFER_SIZE:
                    logger.info(
                        f"Writing output data to CSV: {len(output_data_buffer)} rows"
                    )
                    write_output_data_to_csv(args.output_csv, output_data_buffer)
                    row_count += len(output_data_buffer)
                    output_data_buffer = []

    if len(output_data_buffer) > 0:
        logger.info(
            f"Writing remaining output data to CSV: {len(output_data_buffer)} rows"
        )
        write_output_data_to_csv(args.output_csv, output_data_buffer)
        row_count += len(output_data_buffer)
        output_data_buffer = []

    logger.info(f"Total rows written to {args.output_csv}: {row_count}")


if __name__ == "__main__":
    main()
