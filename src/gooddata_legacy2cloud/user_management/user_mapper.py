# (C) 2026 GoodData Corporation

import logging

from gooddata_legacy2cloud.user_management.data_classes import UserMapping

logger = logging.getLogger("migration")


def map_legacy_to_cloud_users(
    legacy_user_email_map: dict[str, str],
    cloud_email_user_map: dict[str, str],
    verbose: bool = False,
) -> dict[str, UserMapping]:
    """
    Maps Legacy users to Cloud users based on email addresses.

    Args:
        legacy_user_email_map: Dictionary mapping Legacy user ID to email/login identifier
        cloud_email_user_map: Dictionary mapping email to Cloud user ID
        verbose: If True, print detailed mapping information for each user

    Returns:
        dict: A dictionary mapping Legacy user ID to UserMapping objects
    """
    mappings: dict[str, UserMapping] = {}

    for legacy_user_id, legacy_identifier in legacy_user_email_map.items():
        # Look up Cloud user by the Legacy identifier (which could be email or login)
        cloud_user_id = cloud_email_user_map.get(legacy_identifier)

        mapping = UserMapping(
            legacy_user_id=legacy_user_id,
            legacy_email=legacy_identifier,
            cloud_user_id=cloud_user_id,
            # If matched, Cloud user's email is the same identifier we used for lookup
            cloud_email=legacy_identifier if cloud_user_id else None,
        )

        mappings[legacy_user_id] = mapping

        # Log the mapping result
        if verbose:
            if cloud_user_id:
                logger.info(
                    "Legacy user '%s' mapped to Cloud user: %s",
                    legacy_identifier,
                    cloud_user_id,
                )
            else:
                logger.warning(
                    "Legacy user '%s' not found in Cloud organization, skipping their objects.",
                    legacy_identifier,
                )

    return mappings


def get_mapping_statistics(
    user_mappings: dict[str, UserMapping],
) -> tuple[int, int, int]:
    """
    Calculates statistics about user mappings.

    Args:
        user_mappings: Dictionary of UserMapping objects

    Returns:
        tuple: (successfully_mapped, no_identifier, not_in_cloud)
    """
    successfully_mapped = sum(1 for m in user_mappings.values() if m.is_mapped)
    no_identifier = sum(1 for m in user_mappings.values() if m.legacy_email is None)
    not_in_cloud = sum(
        1
        for m in user_mappings.values()
        if m.legacy_email is not None and not m.is_mapped
    )

    return successfully_mapped, no_identifier, not_in_cloud
