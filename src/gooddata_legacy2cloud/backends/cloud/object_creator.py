# (C) 2026 GoodData Corporation
import concurrent.futures
import json
import logging
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Set,
    Tuple,
    Type,
    TypeVar,
)

from gooddata_legacy2cloud.backends.cloud.client import CloudClient
from gooddata_legacy2cloud.backends.cloud.object_creator_strategy import (
    OBJECT_CONFIG,
    CreationStrategy,
    ObjectConfig,
)
from gooddata_legacy2cloud.helpers import (
    append_content_to_file,
    write_content_to_file,
)
from gooddata_legacy2cloud.logging.context import ObjectContext
from gooddata_legacy2cloud.models.enums import Action, Operation, SkippingOrUpdating
from gooddata_legacy2cloud.output_writer import OutputWriter

logger = logging.getLogger("migration")

MAX_WORKERS = 1

# Define generic type for objects
T = TypeVar("T", bound=Dict[str, Any])


class CloudObjectCreator:
    """Main class for creating objects using a specific strategy"""

    def __init__(
        self,
        cloud_client: CloudClient,
        strategy: CreationStrategy,
        failed_objects_file: str | None = None,
        log_file_mode="w",
        skipped_objects_file: str | None = None,
    ):
        self.cloud_client: CloudClient = cloud_client
        self.strategy: CreationStrategy = strategy
        self.failed_objects_file: str = failed_objects_file or "failed_objects.json"
        self.skipped_objects_file: str = skipped_objects_file or "skipped_objects.json"
        self.log_file_mode: str = log_file_mode

    def has_element(self, elements, element_id):
        """Checks if an element with a given ID exists in elements"""
        for element in elements:
            if element["id"] == element_id:
                return True
        return False

    def filter_objects(
        self,
        objects: List[T],
        skipping_or_updating: SkippingOrUpdating = SkippingOrUpdating.SKIPPING,
    ) -> Tuple[List[T], List[T]]:
        """
        Filters the provided objects into those that already exist (skipped)
        and those that need to be created.
        Logs the number of skipped and to-be-created objects.
        """
        existing_objects = self.strategy.get_existing_objects()
        to_be_created_objects = []
        skipped_objects = []
        for obj in objects:
            try:
                # Attempt to access the expected keys
                if self.has_element(existing_objects, obj["data"]["id"]):
                    skipped_objects.append(obj)
                else:
                    to_be_created_objects.append(obj)
            except KeyError as e:
                logger.warning(
                    "Skipping object due to unexpected structure: %s - %s", obj, e
                )
            except Exception as e:
                logger.warning(
                    "Skipping object due to unexpected error: %s - %s", obj, e
                )
                # Continue processing the next object
        if skipped_objects:
            logger.info(
                "----%s existing objects (%s)----",
                skipping_or_updating.value,
                len(skipped_objects),
            )
        if to_be_created_objects:
            logger.info("----Creating new objects (%s)----", len(to_be_created_objects))

        # TODO: return an object instead of a tuple for more readability, something like:
        #
        # @attrs.define
        # class FilteredObjects:
        #     objects_not_in_cloud: List[T]
        #     objects_in_cloud: List[T]

        return to_be_created_objects, skipped_objects

    def write_skipped_objects_to_file(
        self, skipped_objects: List[T], write_skipped: bool = True
    ):
        """
        Helper method to write skipped objects to a file.

        Args:
            skipped_objects: List of skipped objects
            write_skipped: Whether to write skipped objects to file
        """
        if not write_skipped:
            return

        skipped_writer = OutputWriter(self.skipped_objects_file)
        skipped_writer.write_skipped_objects(
            skipped_objects, self.strategy.get_object_type()
        )

    def create_objects_with_retry(
        self,
        objects: List[T],
        action: Action,
        print_object_link=False,
    ) -> List[T]:
        """Create objects and retry failed objects"""
        failed_objects = self.create_objects(
            objects=objects,
            action=action,
            print_object_link=print_object_link,
            max_workers=1,
        )

        # Retry failed objects
        if failed_objects and len(objects) > len(failed_objects):
            logger.info("----Retrying failed objects (%s)----", len(failed_objects))
            return self.create_objects_with_retry(
                objects=failed_objects,
                action=action,
                print_object_link=print_object_link,
            )
        elif failed_objects:
            logger.error("----%s objects failed to process----", len(failed_objects))

            if self.log_file_mode == "a":
                append_content_to_file(
                    self.failed_objects_file,
                    json.dumps(failed_objects, indent=4),
                )
            else:
                write_content_to_file(
                    self.failed_objects_file,
                    json.dumps(failed_objects, indent=4),
                )

            return failed_objects

        return []

    def create_objects(
        self,
        objects: List[T],
        action: Action,
        print_object_link=False,
        max_workers=MAX_WORKERS,
    ) -> List[T]:
        """Creates objects on the server in parallel"""
        failed_objects = []

        def worker(index: int, obj: T, action: Action):
            title = obj["data"]["attributes"]["title"]
            obj_id = obj["data"]["id"]
            with ObjectContext(obj_id, title):
                return create_object_wrapper(index, obj, action)

        def create_object_wrapper(index: int, obj: T, action: Action):
            """Wrapper function to create an object"""
            # TODO: Move the execution logic to a separate private method

            logger.info("%s %s", action.continuous().capitalize(), index + 1)

            create_or_update: Callable[[T], Any]
            if action == Action.CREATE:
                create_or_update = self.strategy.create_object
            elif action == Action.UPDATE:
                create_or_update = self.strategy.update_object

            try:
                response = create_or_update(obj)
            except Exception as e:
                logger.error(
                    "[Index:%s] Exception %s object - %s",
                    index + 1,
                    action.continuous(),
                    e,
                )
                return obj, None, str(e)

            if not response.ok:
                logger.error(
                    "[Index:%s] Error %s\n%s - %s\n",
                    index + 1,
                    action.continuous(),
                    response.status_code,
                    response.text,
                )
                return obj, response.status_code, response.text

            if print_object_link:
                link = self.strategy.get_object_link(
                    self.cloud_client.domain, self.cloud_client.ws, obj["data"]["id"]
                )
                logger.info("%s - %s\n", action.past().capitalize(), link)

            return None, None, None

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(worker, index, obj, action)
                for index, obj in enumerate(objects)
            ]
            for future in concurrent.futures.as_completed(futures):
                obj, status_code, response_text = future.result()
                if obj is not None:
                    failed_objects.append(obj)
        return failed_objects

    def create_objects_with_error_fallback(
        self,
        objects: List[T],
        action: Action = Action.CREATE,
    ) -> None:
        """
        Filters objects to create (skipping existing ones) and then creates each using
        the error fallback strategy.
        """

        if not objects:
            return

        def worker(action: Action, index: int, obj: T):
            title: str = obj["data"]["attributes"]["title"]
            obj_id: str = obj["data"]["id"]
            with ObjectContext(obj_id, title):
                logger.info("%s %s...", action.continuous().capitalize(), index + 1)
                self.strategy.with_error_fallback(action, obj)

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [
                executor.submit(worker, action, index, obj)
                for index, obj in enumerate(objects)
            ]
            concurrent.futures.wait(futures)


def get_object_creator(
    cloud_client: CloudClient, object_type: str
) -> CloudObjectCreator:
    """Factory function to get a properly configured CloudObjectCreator"""
    if object_type not in OBJECT_CONFIG:
        raise ValueError(f"Unsupported object type: {object_type}")

    config: ObjectConfig = OBJECT_CONFIG[object_type]
    strategy_type: Type[CreationStrategy] = config["strategy_class"]
    strategy: CreationStrategy = strategy_type(cloud_client)

    return CloudObjectCreator(
        cloud_client,
        strategy,
        config["failed_file"],
        config["log_file_mode"],
        config["skipped_file"],
    )


def process_objects(
    cloud_client: CloudClient,
    objects: List[T],
    object_type,
    operation: Operation,
    print_object_link=False,
    write_skipped=True,
) -> Tuple[List, List]:
    """
    Generic function to process objects based on their type and desired operation.
    """
    creator: CloudObjectCreator = get_object_creator(cloud_client, object_type)

    if operation == Operation.CREATE_WITH_RETRY:
        # Creates objects that do not exist in upstream Cloud. On failure, creation
        # will be retried. Objects already present upstream will be skipped.

        # Filter objects
        objects_to_create, objects_to_skip = creator.filter_objects(objects)

        # Write skipped objects to file if requested
        creator.write_skipped_objects_to_file(objects_to_skip, write_skipped)

        failed_objects = creator.create_objects_with_retry(
            objects=objects_to_create,
            print_object_link=print_object_link,
            action=Action.CREATE,
        )
        return failed_objects, objects_to_skip

    elif operation == Operation.CREATE_OR_UPDATE_WITH_RETRY:
        # Creates objects that do not exist in upstream Cloud. On failure, creation
        # will be retried. Objects already present upstream will be updated.

        # Filter objects
        objects_to_create, objects_to_update = creator.filter_objects(
            objects, SkippingOrUpdating.UPDATING
        )

        # If skipped file is requested, write an empty list to it to truncate the file
        creator.write_skipped_objects_to_file([], write_skipped)

        failed_objects = []

        # Create objects with retry
        failed_objects.extend(
            creator.create_objects_with_retry(
                objects=objects_to_create,
                print_object_link=print_object_link,
                action=Action.CREATE,
            )
        )

        # Update objects with retry
        failed_objects.extend(
            creator.create_objects_with_retry(
                objects=objects_to_update,
                print_object_link=print_object_link,
                action=Action.UPDATE,
            )
        )
        return failed_objects, []

    elif operation == Operation.CREATE_WITH_ERROR_FALLBACK:
        # Create objects that do not exist in upstream Cloud. Objects which
        # are already present upstream will be skipped.
        # If an error occurs, the creator will attempt to create the object with
        # an error object (i.e., a disabled metric).

        # Filter objects
        objects_to_create, objects_to_skip = creator.filter_objects(objects)

        # Write skipped objects to file if requested
        creator.write_skipped_objects_to_file(objects_to_skip, write_skipped)

        # Create objects
        if objects_to_create:
            creator.create_objects_with_error_fallback(
                objects=objects_to_create,
                action=Action.CREATE,
            )

        return [], []

    elif operation == Operation.CREATE_OR_UPDATE_WITH_ERROR_FALLBACK:
        # Create objects that do not exist in upstream Cloud. Objects which
        # are already present upstream will be updated instead of skipped.
        # If an error occurs, the creator will attempt to create the object with
        # an error object (i.e., a disabled metric).

        # Filter objects
        objects_to_create, objects_to_update = creator.filter_objects(
            objects, SkippingOrUpdating.UPDATING
        )

        # If skipped file is requested, write an empty list to it to truncate the file
        creator.write_skipped_objects_to_file([], write_skipped)

        # Create objects
        creator.create_objects_with_error_fallback(
            objects=objects_to_create,
            action=Action.CREATE,
        )

        # Update objects
        creator.create_objects_with_error_fallback(
            objects=objects_to_update,
            action=Action.UPDATE,
        )

        return [], []
    else:
        raise ValueError(f"Unsupported operation: {operation}")


# TODO: this should eventually go through the process_objects function so that
# there is a standardized way we handle object creation
def update_dashboards_with_full_content(
    cloud_client: CloudClient,
    dashboards: List[T],
    skipped_dashboard_ids: Set[str] | None = None,
):
    """Update existing placeholder dashboards with full content.

    Args:
        cloud_client: The CloudClient instance
        dashboards: List of dashboard objects to update
        skipped_dashboard_ids: Set of dashboard IDs that were skipped in Phase 1
    """
    # Filter out dashboards that were skipped in Phase 1
    if skipped_dashboard_ids:
        dashboards_to_update = [
            dashboard
            for dashboard in dashboards
            if dashboard["data"]["id"] not in skipped_dashboard_ids
        ]
        if len(dashboards_to_update) < len(dashboards):
            skipped_count = len(dashboards) - len(dashboards_to_update)
            logger.info(
                "----Skipping update for %s dashboard(s) that were not created in Phase 1----",
                skipped_count,
            )
    else:
        dashboards_to_update = dashboards

    if not dashboards_to_update:
        return [], []

    creator: CloudObjectCreator = get_object_creator(cloud_client, "dashboard_update")

    # For updates, we don't need to check existing objects since we're updating placeholders
    # we created in Phase 1, so we'll use create_objects_with_retry directly
    failed_objects = creator.create_objects_with_retry(
        objects=dashboards_to_update,
        action=Action.UPDATE,
    )
    return failed_objects, []
