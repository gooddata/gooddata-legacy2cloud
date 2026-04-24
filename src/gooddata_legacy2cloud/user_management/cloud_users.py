# (C) 2026 GoodData Corporation


def build_cloud_email_user_map(users: list[dict]) -> dict[str, str]:
    """
    Builds a mapping from email addresses to Cloud user IDs.

    Args:
        users: List of Cloud user dictionaries

    Returns:
        dict: A dictionary mapping email address to Cloud user ID
    """
    user_map: dict[str, str] = {}

    for user in users:
        user_id = user.get("id")
        # Get email from attributes
        attributes = user.get("attributes", {})
        email = attributes.get("email")

        if user_id and email:
            user_map[email.lower()] = user_id  # Store lowercase for comparison

    return user_map


def extract_unique_emails_from_legacy_user_map(
    legacy_user_email_map: dict[str, str],
) -> list[str]:
    """
    Extracts unique email addresses from Legacy user email map.

    Args:
        legacy_user_email_map: Dictionary mapping Legacy user ID to email/login

    Returns:
        list: List of unique email addresses (lowercase)
    """
    unique_emails: set[str] = set()
    for email in legacy_user_email_map.values():
        if email:
            unique_emails.add(email.lower())

    return list(unique_emails)
