# (C) 2026 GoodData Corporation
def get_cloud_id_date_dimension(platform_id):
    """
    Returns the date dimension ID.
    """
    return f"dt_{platform_id}"


def are_identifiers_similar(id1, id2):
    """
    Compares two identifiers to check if they have the same first part
    and the same part after the second dot.

    Examples:
    - 'attr.spendtransaction.natureandpurpose1' vs 'attr.natureandpurpose1.natureandpurpose1'
    - First part: 'attr' == 'attr' ✓
    - Part after second dot: 'natureandpurpose1' == 'natureandpurpose1' ✓
    """
    if not id1 or not id2:
        return False

    # Early return if strings are identical
    if id1 == id2:
        return True

    # Split strings only once
    parts1 = id1.split(".")
    parts2 = id2.split(".")

    # Check if both have at least 3 parts
    if len(parts1) < 3 or len(parts2) < 3:
        return False

    # Check if first parts are equal
    if parts1[0] != parts2[0]:
        return False

    # Check if parts after second dot are equal
    # Join all parts after index 2 (second dot onwards)
    after_second_1 = ".".join(parts1[2:])
    after_second_2 = ".".join(parts2[2:])

    return after_second_1 == after_second_2


def find_value_of_default_label(logger, default_label):
    """
    Finds the value for the default label.
    Note: default label can reference the attribute or the label
    """
    # try to find the value of the default label
    label_id = logger.get_value_by_key(default_label)
    if label_id is not None:
        return label_id

    # if not found, try to find the value of similar attribute identifier
    replaced_label = replace_first_part(default_label, "attr")
    label_id = logger.get_value_by_key(replaced_label)
    return label_id


def replace_first_part(value, new_prefix="label"):
    """
    Replaces the first part of the value with the new prefix.
    """
    return f"{new_prefix}.{'.'.join(value.split('.')[1:])}" if "." in value else value


def get_unique_id(logger, platform_attr_id, attr_id):
    """
    Helper searches for an unique identifier
    """
    # First, check if the proposed identifier has any conflicts
    keys_for_attr_id = logger.get_keys_by_value(attr_id)

    # If no keys use this attr_id, it's safe to use as-is
    if not keys_for_attr_id:
        return attr_id

    # Check if any existing keys are similar to our Platform identifier
    has_conflicting_key = any(
        are_identifiers_similar(key, platform_attr_id) for key in keys_for_attr_id
    )

    # If no conflicts found, we can use the original identifier
    if not has_conflicting_key:
        return attr_id

    # We have a conflict, so we need to find a unique identifier with a suffix
    return get_unique_identifier_with_suffix(logger, platform_attr_id, attr_id)


def get_unique_identifier_with_suffix(logger, platform_attr_id, base_attr_id):
    """
    Helper method to find a unique identifier by appending numeric suffixes.

    Args:
        platform_attr_id: The original Platform identifier to check similarity against
        base_attr_id: The base identifier to append suffixes to

    Returns:
        str: A unique identifier with numeric suffix
    """
    suffix = 1

    while True:
        candidate_id = f"{base_attr_id}_{suffix}"
        keys_using_candidate = logger.get_keys_by_value(candidate_id)

        # If no keys use this candidate, it's available
        if not keys_using_candidate:
            return candidate_id

        # Check if any keys using this candidate are similar to our Platform identifier
        candidate_has_similar_conflict = any(
            are_identifiers_similar(key, platform_attr_id)
            for key in keys_using_candidate
        )

        # If no similar conflicts, this candidate is safe to use
        if not candidate_has_similar_conflict:
            return candidate_id

        # Try the next suffix
        suffix += 1
