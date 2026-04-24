# (C) 2026 GoodData Corporation

import logging
from typing import Any

from gooddata_legacy2cloud.backends.legacy.client import LegacyClient
from gooddata_legacy2cloud.backends.legacy.dependencies import (
    get_object_dependencies,
    get_uris_from_identifiers,
)
from gooddata_legacy2cloud.backends.legacy.filters import (
    FilterParameters,
    filter_objects_by_creator_profiles,
    filter_objects_by_locked_flag,
    filter_objects_by_tags,
)
from gooddata_legacy2cloud.backends.legacy.utils import (
    extract_category_from_object,
    extract_uri_from_object,
)
from gooddata_legacy2cloud.constants import LEGACY_REQUEST_PAGE_SIZE

logger = logging.getLogger("migration")

MAX_WORKERS_FOR_REPORT_MERGE = 3  # Limit concurrent API calls when processing reports


def cache_objects(legacy_client: LegacyClient, objects: list[Any]) -> int:
    """
    Add objects to the cache for future reference.

    Args:
        legacy_client: LegacyClient instance
        objects (list): List of objects to cache

    Returns:
        int: Number of objects cached
    """
    cached_count = 0
    for obj in objects:
        uri = extract_uri_from_object(obj)
        if uri:
            obj_url = legacy_client.domain + uri
            legacy_client.cache[obj_url] = obj
            cached_count += 1
    return cached_count


def get_objects_by_ids(
    legacy_client: LegacyClient,
    obj_ids,
    category=None,
    message_prefix="objects",
) -> list[Any]:
    """
    Retrieves multiple objects by their IDs.
    Checks cache first, then makes batch API calls for objects not found in cache.

    Args:
        legacy_client: LegacyClient instance
        obj_ids (list): List of object IDs to retrieve
        category (str, optional): Category to filter objects by. Objects not matching this category
                                  will be excluded with an error message.
        message_prefix (str): The prefix for progress messages (e.g., "metrics", "dashboards")

    Returns:
        list: List of objects
    """
    if not obj_ids:
        return []

    # Validate that all IDs are integers
    invalid_ids = [obj_id for obj_id in obj_ids if not str(obj_id).isdigit()]
    if invalid_ids:
        raise ValueError(f"Invalid object IDs (must be integers): {invalid_ids}")

    result_objects = []
    objects_to_fetch = []
    all_batch_objects = []

    # Check which objects are already in cache
    for obj_id in obj_ids:
        obj_uri = f"/gdc/md/{legacy_client.pid}/obj/{obj_id}"
        obj_url = legacy_client.domain + obj_uri

        if obj_url in legacy_client.cache:
            cached_obj = legacy_client.cache[obj_url]
            result_objects.append(cached_obj)
        else:
            objects_to_fetch.append(obj_uri)

    cached_count = len(result_objects)
    if cached_count > 0:
        logger.info("Found %s objects in cache", cached_count)

    # If all objects were in cache or we have nothing to fetch, return them
    if not objects_to_fetch:
        return result_objects

    # Fetch objects not found in cache in batches
    logger.info("Fetching %s objects from API", len(objects_to_fetch))

    # Process in batches of 50
    batch_size = LEGACY_REQUEST_PAGE_SIZE
    for i in range(0, len(objects_to_fetch), batch_size):
        batch = objects_to_fetch[i : i + batch_size]

        # Prepare the request payload
        payload = {"get": {"items": batch}}

        # Make the API call
        url = f"{legacy_client.domain}/gdc/md/{legacy_client.pid}/objects/get"
        response = legacy_client._post(url, payload)

        # Process the response
        if response.status_code == 200:
            json_data = response.json()
            if "objects" in json_data and "items" in json_data["objects"]:
                # Collect batch objects for later processing
                all_batch_objects.extend(json_data["objects"]["items"])
        else:
            logger.error("Error fetching %s: %s", message_prefix, response.status_code)

    # Log completion after all API calls are completed
    logger.info(" Done")

    # Now process all the fetched objects
    for obj in all_batch_objects:
        uri = extract_uri_from_object(obj)
        if uri:
            obj_url = legacy_client.domain + uri
            # Check category if specified
            if category:
                obj_category = extract_category_from_object(obj)
                obj_id = uri.split("/")[-1] if uri else "unknown"

                # Extract object identifier from meta if available
                obj_identifier = "unknown"
                for key in obj:
                    if (
                        isinstance(obj[key], dict)
                        and "meta" in obj[key]
                        and "identifier" in obj[key]["meta"]
                    ):
                        obj_identifier = obj[key]["meta"]["identifier"]
                        break

                if obj_category != category:
                    logger.warning(
                        "Object %s is %s not %s - skipping",
                        obj_identifier,
                        obj_category,
                        category,
                    )
                    continue

            # Add to results if it passes category check
            result_objects.append(obj)

    logger.info("Retrieved %s %s in total", len(result_objects), message_prefix)

    return result_objects


def fetch_objects_with_filters(
    legacy_client: LegacyClient, category, filter_params=None, message_prefix=None
):
    """
    Comprehensive method for fetching objects with filtering options.
    Handles IDs, identifiers, dependencies, and tag filtering in a consistent way.

    Args:
        legacy_client: LegacyClient instance
        category (str): The object category (e.g., 'metric', 'visualizationObject')
        filter_params (FilterParameters): Object containing all filtering parameters
        message_prefix (str): Prefix for progress messages (e.g., "metrics")

    Returns:
        list: Filtered list of objects
    """
    if not message_prefix:
        message_prefix = category + "s"

    if not filter_params:
        filter_params = FilterParameters()

    # Fetch objects based on provided parameters
    if filter_params.identifiers_with_deps:
        # This branch handles --only-identifiers-with-dependencies parameter
        object_uris = get_uris_from_identifiers(
            legacy_client, filter_params.identifiers_with_deps
        )
        expanded_uris = get_object_dependencies(legacy_client, object_uris, category)
        expanded_ids = [uri.split("/")[-1] for uri in expanded_uris]

        objects = get_objects_by_ids(
            legacy_client, expanded_ids, category, message_prefix
        )
    elif filter_params.identifiers:
        # This branch handles --only-identifiers parameter
        object_uris = get_uris_from_identifiers(
            legacy_client, filter_params.identifiers
        )
        specific_ids = [uri.split("/")[-1] for uri in object_uris]

        objects = get_objects_by_ids(
            legacy_client, specific_ids, category, message_prefix
        )
    elif filter_params.object_ids_with_deps:
        # This branch handles --only-object-ids-with-dependencies parameter
        object_uris = [
            f"/gdc/md/{legacy_client.pid}/obj/{obj_id}"
            for obj_id in filter_params.object_ids_with_deps
        ]
        expanded_uris = get_object_dependencies(legacy_client, object_uris, category)
        expanded_ids = [uri.split("/")[-1] for uri in expanded_uris]

        objects = get_objects_by_ids(
            legacy_client, expanded_ids, category, message_prefix
        )
    elif filter_params.object_ids:
        # This branch handles --only-object-ids parameter
        objects = get_objects_by_ids(
            legacy_client, filter_params.object_ids, category, message_prefix
        )
    else:
        # Default case when no specific object filtering parameters are provided
        # Fetch all objects of the category
        objects = legacy_client.get_objects_by_category(category, message_prefix)

    total_objects = len(objects)

    # Apply tag filtering if needed - parameters --with-tags and --without-tags
    if filter_params.positive_tags or filter_params.negative_tags:
        logger.info("----Applying tag filters to %s----", message_prefix)
        objects = filter_objects_by_tags(objects, filter_params)
        logger.info(
            "%s out of %s %s passed the tag filters",
            len(objects),
            total_objects,
            message_prefix,
        )
        total_objects = len(objects)  # Update count for locked flag filtering message

    # Apply locked flag filtering if needed - parameters --with-locked-flag and --without-locked-flag
    if filter_params.with_locked_flag or filter_params.without_locked_flag:
        logger.info("----Applying locked flag filters to %s----", message_prefix)
        objects = filter_objects_by_locked_flag(objects, filter_params)
        logger.info(
            "%s out of %s %s passed the locked flag filters",
            len(objects),
            total_objects,
            message_prefix,
        )
        total_objects = len(
            objects
        )  # Update count for creator profile filtering message

    # Apply creator profile filtering if needed - parameters --with-creator-profiles and --without-creator-profiles
    if filter_params.with_creator_profiles or filter_params.without_creator_profiles:
        logger.info("----Applying creator profile filters to %s----", message_prefix)
        objects = filter_objects_by_creator_profiles(objects, filter_params)
        logger.info(
            "%s out of %s %s passed the creator profile filters",
            len(objects),
            total_objects,
            message_prefix,
        )

    # Cache the filtered objects
    cache_objects(legacy_client, objects)

    return objects


def fetch_reports_with_filters(
    legacy_client: LegacyClient, filter_params=None, message_prefix="reports"
):
    """
    Specialized function for fetching Legacy reports with filtering options.
    For each report, it fetches the last report definition and merges it with the parent metadata.

    This replaces the functionality previously provided by get_merged_reports.

    Args:
        legacy_client: LegacyClient instance
        filter_params (FilterParameters): Object containing filtering parameters
        message_prefix (str): Prefix for progress messages

    Returns:
        list: Filtered and processed list of report objects
    """
    # First, fetch the raw report objects using the standard filtering logic
    raw_reports = fetch_objects_with_filters(
        legacy_client, "report", filter_params, message_prefix
    )

    # Step 1: Collect all the last definition links to batch fetch them
    def_links_mapping = {}  # Maps definition link to original report index
    reports_without_definitions: list[
        tuple[int, Any]
    ] = []  # Reports without definitions to process separately

    for idx, report in enumerate(raw_reports):
        if "report" not in report:
            logger.warning("Unexpected report format: %s", report.keys())
            reports_without_definitions.append((idx, report))
            continue

        definitions = report["report"]["content"].get("definitions", [])
        if not definitions:
            # No definitions found, use the report directly
            reports_without_definitions.append((idx, report))
            continue

        # Get the last definition link
        last_definition_link = definitions[-1]
        # Extract object ID from link for batch fetching
        if isinstance(last_definition_link, str):
            # Extract object ID from URI format like "/gdc/md/{pid}/obj/{id}"
            parts = last_definition_link.split("/")
            if len(parts) >= 2:
                obj_id = parts[-1]
                def_links_mapping[obj_id] = idx
            else:
                logger.warning(
                    "Unexpected definition link format: %s", last_definition_link
                )
                reports_without_definitions.append((idx, report))
        else:
            logger.warning("Definition link is not a string: %s", last_definition_link)
            reports_without_definitions.append((idx, report))

    # Step 2: Batch fetch all report definitions at once
    processed_reports: list[Any] = [None] * len(
        raw_reports
    )  # Pre-allocate results list

    # Process reports without definitions first
    for idx, report in reports_without_definitions:
        processed_reports[idx] = {"reportDefinition": report.get("report", {})}

    # If we have definitions to fetch, use batch API
    if def_links_mapping:
        logger.info("Batch fetching %s report definitions...", len(def_links_mapping))
        definition_ids = list(def_links_mapping.keys())

        # Use get_objects_by_ids to fetch all definitions in batches
        definition_objects = get_objects_by_ids(
            legacy_client, definition_ids, "reportDefinition", "report definitions"
        )

        # Step 3: Process the definitions and merge with parent reports
        for def_obj in definition_objects:
            # Extract object ID from the fetched definition
            obj_uri = extract_uri_from_object(def_obj)
            if obj_uri:
                obj_id = obj_uri.split("/")[-1]
                if obj_id in def_links_mapping:
                    idx = def_links_mapping[obj_id]
                    original_report = raw_reports[idx]
                    report_meta = original_report["report"]["meta"]

                    # Create a merged copy by replacing metadata
                    if "reportDefinition" in def_obj:
                        merged = def_obj.copy()
                        merged["reportDefinition"]["meta"] = report_meta
                        merged["reportDefinition"]["meta"]["category"] = "report"
                        processed_reports[idx] = merged
                    else:
                        # Fallback if structure is unexpected
                        processed_reports[idx] = {
                            "reportDefinition": {
                                "meta": report_meta,
                                "content": def_obj,
                            }
                        }

        # Handle any missing definitions (should be rare since we batch fetched them)
        for obj_id, idx in def_links_mapping.items():
            if processed_reports[idx] is None:
                logger.warning(
                    "Could not fetch definition for report %s, using original report",
                    idx,
                )
                processed_reports[idx] = {
                    "reportDefinition": raw_reports[idx]["report"]
                }

    # Verify all reports were processed
    for idx, report in enumerate(processed_reports):
        if report is None:
            logger.warning(
                "Report at index %s was not processed, using original report", idx
            )
            processed_reports[idx] = {
                "reportDefinition": raw_reports[idx].get("report", {})
            }

    logger.info("Done. Processed %s reports.", len(processed_reports))
    return processed_reports


def fetch_dashboard_content(
    legacy_client: LegacyClient,
    dashboards,
    message_prefix="dashboard widgets & filter contexts",
):
    """
    Fetch all dashboard-related content (widgets and filter contexts) to populate LegacyClient's internal cache.
    This enables efficient processing by avoiding individual HTTP requests during dashboard conversion.

    Args:
        legacy_client: LegacyClient instance
        dashboards: List of dashboard objects
        message_prefix (str): Prefix for progress messages
    """
    # Extract all widget IDs and filter context IDs from all dashboards
    widget_ids = set()
    filter_context_ids = set()

    for dashboard in dashboards:
        try:
            content = dashboard["analyticalDashboard"]["content"]

            # Extract widget IDs from dashboard layout
            if "layout" in content and "fluidLayout" in content["layout"]:
                for row in content["layout"]["fluidLayout"]["rows"]:
                    for column in row["columns"]:
                        widget_uri = column["content"]["widget"]["qualifier"]["uri"]
                        # Extract ID from URI format "/gdc/md/{pid}/obj/{id}"
                        widget_id = widget_uri.split("/")[-1]
                        if widget_id.isdigit():  # Ensure it's a valid ID
                            widget_ids.add(widget_id)
                        else:
                            logger.warning(
                                "Invalid widget ID extracted from URI: %s", widget_uri
                            )

            # Extract filter context ID if present
            if "filterContext" in content and content["filterContext"]:
                filter_context_uri = content["filterContext"]
                # Extract ID from URI format "/gdc/md/{pid}/obj/{id}"
                filter_context_id = filter_context_uri.split("/")[-1]
                if filter_context_id.isdigit():  # Ensure it's a valid ID
                    filter_context_ids.add(filter_context_id)
                else:
                    logger.warning(
                        "Invalid filter context ID extracted from URI: %s",
                        filter_context_uri,
                    )

        except (KeyError, TypeError) as e:
            logger.warning("Error processing dashboard for content extraction: %s", e)
            continue

    # Combine all IDs for batch fetching
    all_content_ids = list(widget_ids) + list(filter_context_ids)

    # Batch fetch ALL content at once - this populates legacy_client.cache automatically
    if all_content_ids:
        logger.info(
            "Found %s widgets and %s filter contexts to fetch",
            len(widget_ids),
            len(filter_context_ids),
        )
        objects = get_objects_by_ids(
            legacy_client, all_content_ids, None, message_prefix
        )
        cache_objects(legacy_client, objects)
    else:
        logger.info("No dashboard content found to fetch.")
