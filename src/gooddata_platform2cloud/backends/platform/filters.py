# (C) 2026 GoodData Corporation

from typing import Self

from gooddata_platform2cloud.config.shared_configs import ObjectFilterConfig


# TODO refactor to use attrs (reduce boilerplate)
class FilterParameters:
    """
    Class to hold filtering parameters for object selection.
    """

    def __init__(
        self,
        positive_tags: list[str] | None = None,
        negative_tags: list[str] | None = None,
        object_ids: list[int] | None = None,
        object_ids_with_deps: list[int] | None = None,
        identifiers: list[str] | None = None,
        identifiers_with_deps: list[str] | None = None,
        with_locked_flag: bool | None = None,
        without_locked_flag: bool | None = None,
        with_creator_profiles: list[str] | None = None,
        without_creator_profiles: list[str] | None = None,
    ):
        self.positive_tags = positive_tags
        self.negative_tags = negative_tags
        self.object_ids = object_ids
        self.object_ids_with_deps = object_ids_with_deps
        self.identifiers = identifiers
        self.identifiers_with_deps = identifiers_with_deps
        self.with_locked_flag = with_locked_flag
        self.without_locked_flag = without_locked_flag
        self.with_creator_profiles = with_creator_profiles
        self.without_creator_profiles = without_creator_profiles

    @classmethod
    def from_config(cls, object_filter_config: ObjectFilterConfig) -> Self:
        """Create FilterParameters from configuration object.

        Returns:
            FilterParameters: Object with extracted filter parameters
        """
        return cls(
            positive_tags=object_filter_config.with_tags
            if object_filter_config.with_tags
            else None,
            negative_tags=object_filter_config.without_tags
            if object_filter_config.without_tags
            else None,
            object_ids=(
                object_filter_config.only_object_ids
                if object_filter_config.only_object_ids
                else None
            ),
            object_ids_with_deps=(
                object_filter_config.only_object_ids_with_dependencies
                if object_filter_config.only_object_ids_with_dependencies
                else None
            ),
            identifiers=(
                object_filter_config.only_identifiers
                if object_filter_config.only_identifiers
                else None
            ),
            identifiers_with_deps=(
                object_filter_config.only_identifiers_with_dependencies
                if object_filter_config.only_identifiers_with_dependencies
                else None
            ),
            with_locked_flag=(True if object_filter_config.with_locked_flag else None),
            without_locked_flag=(
                True if object_filter_config.without_locked_flag else None
            ),
            with_creator_profiles=(
                object_filter_config.with_creator_profiles
                if object_filter_config.with_creator_profiles
                else None
            ),
            without_creator_profiles=(
                object_filter_config.without_creator_profiles
                if object_filter_config.without_creator_profiles
                else None
            ),
        )


def filter_objects_by_tags(objects: list, filter_params: FilterParameters) -> list:
    """
    Filter a list of objects based on tag criteria from FilterParameters.

    Args:
        objects: List of objects to filter
        filter_params: Object containing positive_tags and negative_tags

    Returns:
        Filtered list of objects that match the tag criteria
    """
    positive_tags = filter_params.positive_tags
    negative_tags = filter_params.negative_tags

    if not (positive_tags or negative_tags):
        return objects

    filtered_objects = []
    for obj in objects:
        # Extract tags based on the object structure
        tags = extract_tags_from_object(obj)

        # Check positive tags (object must have at least one)
        passes_positive = True
        if positive_tags:
            passes_positive = any(tag in tags for tag in positive_tags)

        # Check negative tags (object must not have any)
        passes_negative = True
        if negative_tags:
            passes_negative = not any(tag in tags for tag in negative_tags)

        # Include the object only if it passes both filters
        if passes_positive and passes_negative:
            filtered_objects.append(obj)

    return filtered_objects


def filter_objects_by_locked_flag(
    objects: list, filter_params: FilterParameters
) -> list:
    """
    Filter a list of objects based on their locked flag status from FilterParameters.

    Args:
        objects: List of objects to filter
        filter_params: Object containing with_locked_flag and without_locked_flag

    Returns:
        Filtered list of objects based on locked flag criteria
    """
    with_locked = filter_params.with_locked_flag
    without_locked = filter_params.without_locked_flag

    if not (with_locked or without_locked):
        return objects

    filtered_objects = []
    for obj in objects:
        # Extract locked status based on the object structure
        locked_status = extract_locked_status_from_object(obj)

        # Apply filters based on locked status
        passes_filter = True

        if with_locked:
            # Only include objects with locked=1
            passes_filter = locked_status == 1

        if without_locked:
            # Only include objects with locked=0 or no locked field
            passes_filter = locked_status in [0, None]

        # Include the object only if it passes the filter
        if passes_filter:
            filtered_objects.append(obj)

    return filtered_objects


def extract_tags_from_object(obj: dict) -> list:
    """Extract tags from different object structures returned by the API.

    Args:
        obj: The object to extract tags from

    Returns:
        A list of tags found in the object
    """
    # There's always exactly one object, but we don't know its name
    # So just get the first value, which will be the actual object
    if obj and len(obj) == 1:
        # Get the first (and only) value
        root_obj = next(iter(obj.values()))
        if isinstance(root_obj, dict) and "meta" in root_obj:
            obj_meta = root_obj["meta"]
            if "tags" in obj_meta and obj_meta["tags"]:
                return obj_meta["tags"].split()

    return []


def extract_locked_status_from_object(obj: dict) -> int | None:
    """Extract locked status from different object structures returned by the API.

    Args:
        obj: The object to extract locked status from

    Returns:
        1 if locked, 0 if unlocked, None if locked field not present
    """
    # There's always exactly one object, but we don't know its name
    # So just get the first value, which will be the actual object
    if obj and len(obj) == 1:
        # Get the first (and only) value
        root_obj = next(iter(obj.values()))
        if isinstance(root_obj, dict) and "meta" in root_obj:
            obj_meta = root_obj["meta"]
            if "locked" in obj_meta:
                return obj_meta["locked"]

    return None


def filter_objects_by_creator_profiles(
    objects: list, filter_params: FilterParameters
) -> list:
    """
    Filter a list of objects based on their creator profile IDs from FilterParameters.

    Args:
        objects: List of objects to filter
        filter_params: Object containing with_creator_profiles and without_creator_profiles

    Returns:
        Filtered list of objects based on creator profile criteria
    """
    with_creators = filter_params.with_creator_profiles
    without_creators = filter_params.without_creator_profiles

    if not (with_creators or without_creators):
        return objects

    filtered_objects = []
    for obj in objects:
        # Extract creator profile ID from the object
        creator_id = extract_creator_profile_id_from_object(obj)

        # Apply filters based on creator
        passes_filter = True

        if with_creators:
            # Only include objects created by one of the specified profiles
            passes_filter = creator_id in with_creators if creator_id else False

        if without_creators:
            # Only include objects NOT created by any of the specified profiles
            passes_filter = creator_id not in without_creators if creator_id else True

        # Include the object only if it passes the filter
        if passes_filter:
            filtered_objects.append(obj)

    return filtered_objects


def extract_creator_profile_id_from_object(obj: dict) -> str | None:
    """Extract creator profile ID from different object structures returned by the API.

    The author field format is: /gdc/account/profile/{profile_id}
    This function extracts just the profile_id part.

    Args:
        obj: The object to extract creator profile ID from

    Returns:
        The profile ID (without /gdc/account/profile/ prefix) if found, None otherwise
    """
    # There's always exactly one object, but we don't know its name
    # So just get the first value, which will be the actual object
    if obj and len(obj) == 1:
        # Get the first (and only) value
        root_obj = next(iter(obj.values()))
        if isinstance(root_obj, dict) and "meta" in root_obj:
            obj_meta = root_obj["meta"]
            if "author" in obj_meta and obj_meta["author"]:
                author = obj_meta["author"]
                # Extract the profile ID from the URI
                # Format: /gdc/account/profile/{profile_id}
                if author.startswith("/gdc/account/profile/"):
                    return author.split("/")[-1]
                # In case it's already just the ID
                return author

    return None
