# (C) 2026 GoodData Corporation


def extract_uri_from_object(obj):
    """Extract URI from different object structures returned by the API.

    Args:
        obj (dict): The object to extract URI from

    Returns:
        str: The URI of the object, or None if not found
    """
    # There's always exactly one object, but we don't know its name
    # So just get the first value, which will be the actual object
    if obj and len(obj) == 1:
        # Get the first (and only) value
        root_obj = next(iter(obj.values()))
        if isinstance(root_obj, dict) and "meta" in root_obj:
            obj_meta = root_obj["meta"]
            if "uri" in obj_meta:
                return obj_meta["uri"]

    # Fallback check for top-level meta (though this case shouldn't happen)
    if "meta" in obj and "uri" in obj["meta"]:
        return obj["meta"]["uri"]

    return None


def extract_category_from_object(obj):
    """Extract category from different object structures returned by the API.

    Args:
        obj (dict): The object to extract category from

    Returns:
        str: The category of the object, or None if not found
    """
    # There's always exactly one object, but we don't know its name
    # So just get the first value, which will be the actual object
    if obj and len(obj) == 1:
        # Get the first (and only) value
        root_obj = next(iter(obj.values()))
        if isinstance(root_obj, dict) and "meta" in root_obj:
            obj_meta = root_obj["meta"]
            if "category" in obj_meta:
                return obj_meta["category"]

    return None
