# (C) 2026 GoodData Corporation

from dataclasses import dataclass


@dataclass
class UserMapping:
    """
    Represents a mapping between a Platform user and a Cloud user.

    cloud_user_id and cloud_email are nullable because the values may not be found
    for a particular Platform user on Cloud.
    """

    platform_user_id: str
    platform_email: str | None
    cloud_user_id: str | None = None
    cloud_email: str | None = None

    @property
    def is_mapped(self) -> bool:
        """Returns True if the Platform user is mapped to a Cloud user."""
        return self.cloud_user_id is not None


@dataclass
class ObjectUpdate:
    """
    Represents an update to be made to an object's creator/modifier fields.

    Attributes:
        platform_object_id: The Platform object ID
        platform_object_title: The Platform object title
        cloud_object_id: The Cloud object ID
        object_type: The type of object (e.g., "dashboard", "insight", "metric")
        platform_creator_id: The Platform creator user ID
        platform_modifier_id: The Platform modifier user ID
        cloud_creator_id: The Cloud creator user ID (if mapped)
        cloud_modifier_id: The Cloud modifier user ID (if mapped)
        grantee_user_ids: List of Cloud user IDs who should have EDIT permission
        grantee_group_ids: List of Cloud group IDs who should have permissions (future)
    """

    platform_object_id: str
    platform_object_title: str
    cloud_object_id: str
    object_type: str
    platform_creator_id: str | None
    platform_modifier_id: str | None
    cloud_creator_id: str | None = None
    cloud_modifier_id: str | None = None
    grantee_user_ids: list[str] | None = None
    grantee_group_ids: list[str] | None = None

    @property
    def has_creator_mapping(self) -> bool:
        """Returns True if the creator is mapped."""
        return (
            self.platform_creator_id is not None and self.cloud_creator_id is not None
        )

    @property
    def has_modifier_mapping(self) -> bool:
        """Returns True if the modifier is mapped."""
        return (
            self.platform_modifier_id is not None and self.cloud_modifier_id is not None
        )

    @property
    def is_fully_mapped(self) -> bool:
        """Returns True if both creator and modifier are mapped."""
        return self.has_creator_mapping and self.has_modifier_mapping

    @property
    def is_partially_mapped(self) -> bool:
        """Returns True if only one of creator or modifier is mapped."""
        return (
            self.has_creator_mapping or self.has_modifier_mapping
        ) and not self.is_fully_mapped
