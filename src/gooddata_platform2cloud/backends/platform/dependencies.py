# (C) 2026 GoodData Corporation


import logging
from json import JSONDecodeError

from gooddata_platform2cloud.backends.platform.client import PlatformClient
from gooddata_platform2cloud.constants import PLATFORM_REQUEST_PAGE_SIZE

logger = logging.getLogger("migration")


def get_object_dependencies(
    platform_client: PlatformClient, obj_uris: list[str], category: str
) -> list[str]:
    """
    Get all objects of a specific category that depend on the given objects.

    Args:
        platform_client: PlatformClient instance
        obj_uris (list): List of object URIs to find dependencies for
                        (in form /gdc/md/{pid}/obj/{id})
        category (str): Category of objects to look for (e.g., "metric", "visualizationObject")

    Returns:
        list: Deduplicated list of URIs including both input URIs and all dependent objects
    """
    if not obj_uris:
        return []

    # Use a set for deduplication of results
    result_uris = set(obj_uris)

    # Calculate number of batches
    total_objects = len(obj_uris)
    total_batches = (
        total_objects + PLATFORM_REQUEST_PAGE_SIZE - 1
    ) // PLATFORM_REQUEST_PAGE_SIZE

    logger.info(
        "Fetching dependencies of %s objects for category: %s",
        total_objects,
        category,
    )

    # Process in batches
    for i in range(0, total_objects, PLATFORM_REQUEST_PAGE_SIZE):
        # Get the current batch
        batch = obj_uris[i : i + PLATFORM_REQUEST_PAGE_SIZE]

        # Prepare the API request for this batch
        url = f"{platform_client.domain}/gdc/md/{platform_client.pid}/using2/"
        payload = {"inUseMany": {"uris": batch, "types": [category], "nearest": "0"}}

        # Call the API
        response = platform_client._post(url, payload)

        # Process the response
        if response.status_code != 200:
            logger.error(
                "Error in batch %s/%s: %s",
                i // PLATFORM_REQUEST_PAGE_SIZE + 1,
                total_batches,
                response.status_code,
            )
            continue

        try:
            response_data = response.json()

            if "useMany" in response_data:
                for item in response_data["useMany"]:
                    if "entries" in item and item["entries"]:
                        for entry in item["entries"]:
                            if "link" in entry:
                                result_uris.add(entry["link"])
        except (KeyError, JSONDecodeError) as e:
            logger.error(
                "Error parsing response in batch %s: %s",
                i // PLATFORM_REQUEST_PAGE_SIZE + 1,
                str(e),
            )
    additional_objects = len(result_uris) - len(obj_uris)
    logger.info(" Done. (%s dependent objects found)", additional_objects)

    return list(result_uris)


def get_uris_from_identifiers(
    platform_client: PlatformClient, identifiers: list[str]
) -> list[str]:
    """
    Convert alphanumeric identifiers to object URIs.

    Args:
        platform_client: PlatformClient instance
        identifiers (list): List of alphanumeric identifiers to convert

    Returns:
        list: List of object URIs corresponding to the provided identifiers
    """
    if not identifiers:
        return []

    result_uris = []
    total_identifiers = len(identifiers)

    logger.info("Converting %s identifiers to URIs", total_identifiers)

    # Process in batches of 50
    for i in range(0, total_identifiers, PLATFORM_REQUEST_PAGE_SIZE):
        batch = identifiers[i : i + PLATFORM_REQUEST_PAGE_SIZE]

        # Prepare the payload
        payload = {"identifierToUri": batch}

        # Make the API call
        url = f"{platform_client.domain}/gdc/md/{platform_client.pid}/identifiers"
        response = platform_client._post(url, payload)

        # Process the response
        if response.status_code == 200:
            json_data = response.json()
            if "identifiers" in json_data:
                for item in json_data["identifiers"]:
                    if "uri" in item:
                        result_uris.append(item["uri"])
        else:
            logger.error("Error converting identifiers: %s", response.status_code)

    logger.info(" Done")

    # Only log warning message if some identifiers were not found
    if len(result_uris) < total_identifiers:
        not_found_count = total_identifiers - len(result_uris)
        logger.warning(
            "Retrieved %s URIs from %s identifiers. %s identifiers not found.",
            len(result_uris),
            total_identifiers,
            not_found_count,
        )

    return result_uris
