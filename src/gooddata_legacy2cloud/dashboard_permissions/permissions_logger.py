# (C) 2026 GoodData Corporation
"""Logger for dashboard permissions changes."""

from gooddata_legacy2cloud.dashboard_permissions.change_formatter import (
    format_change_line,
)
from gooddata_legacy2cloud.dashboard_permissions.data_classes import ActualChange
from gooddata_legacy2cloud.output_writer import OutputWriter


class PermissionsLogger:
    """
    Handles logging of permission changes to a log file.

    Writes a consolidated log of all dashboard permission and creator changes,
    with one line per change for easy readability.

    Uses OutputWriter for metadata and consistency with other migration scripts.
    """

    @staticmethod
    def write_permissions_changes_log(
        filename: str,
        actual_changes: list[ActualChange],
        user_mappings: dict,
        cloud_user_map: dict[str, str],
        cloud_usergroup_map: dict[str, str],
        legacy_hostname: str,
        legacy_ws: str,
        cloud_hostname: str,
        cloud_ws: str,
        client_prefix: str | None = None,
    ) -> None:
        """
        Write a consolidated log of all permission and creator changes made.

        Args:
            filename: Name of the log file (without path prefix)
            actual_changes: List of ActualChange instances
            user_mappings: User mappings for email lookup
            cloud_user_map: Dictionary mapping email to Cloud user ID
            cloud_usergroup_map: Dictionary mapping group name to Cloud group ID
            legacy_hostname: Legacy hostname/domain
            legacy_ws: Legacy workspace ID
            cloud_hostname: Cloud hostname/domain
            cloud_ws: Cloud workspace ID
            client_prefix: Optional client prefix used for this migration
        """
        # Use OutputWriter to create file and write metadata header
        logger = OutputWriter(filename)
        logger.write_migration_metadata(
            legacy_hostname, legacy_ws, cloud_hostname, cloud_ws, client_prefix
        )

        # Create reverse map: Cloud user ID -> email
        cloud_id_to_email = {
            user_id: email for email, user_id in cloud_user_map.items()
        }

        # Create reverse map: Cloud group ID -> name
        cloud_id_to_groupname = {
            group_id: name for name, group_id in cloud_usergroup_map.items()
        }

        for idx, change in enumerate(actual_changes):
            if not change.assignee_label and change.user_or_group_id:
                label = cloud_id_to_email.get(change.user_or_group_id) or (
                    cloud_id_to_groupname.get(change.user_or_group_id)
                )
                noun = "user" if change.assignee_type == "user" else "user group"
                change.assignee_label = f"{noun} '{label or change.user_or_group_id}'"
            line = format_change_line(change).strip()
            prefix = "\n" if idx > 0 else ""
            logger.append_content(f"{prefix}{line}\n")
