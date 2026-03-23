# (C) 2026 GoodData Corporation
from typing import Literal

from pydantic import BaseModel, Field, model_validator


class BaseConfig(BaseModel):
    """Base configuration for all migration scripts."""

    env: str = Field(
        default=".env",
        description="Path to the .env file",
    )


class WorkspaceConfig(BaseModel):
    """Configuration for source and target workspaces."""

    platform_ws: str | None = Field(
        default=None,
        description="Source Platform workspace ID. Overrides PLATFORM_WS from the .env "
        + "file if exists.",
    )
    cloud_ws: str | None = Field(
        default=None,
        description="Target Cloud workspace ID. Overrides CLOUD_WS from the .env "
        + "file if exists.",
    )


class CommonConfig(BaseModel):
    """Common configuration for all migration scripts."""

    skip_deploy: bool = Field(
        default=False,
        description="Skips PUT request to Cloud. Useful for testing purposes.",
    )
    output_files_prefix: str = Field(
        default="",
        description="Prefix to add to all output files. Default is empty (no prefix).",
    )
    client_prefix: str = Field(
        default="",
        description="Client prefix to automatically set output-files-prefix and "
        + "include client-specific mapping files.",
    )
    check_parent_workspace: bool = Field(
        default=False,
        description="Check if the target Cloud workspace has a parent workspace. "
        + "Required for client workspace migrations.",
    )


class ObjectMigrationConfig(BaseModel):
    """Configuration for object migration behavior."""

    dump_platform: bool = Field(
        default=False,
        description="Dumps Platform objects to a JSON file.",
    )
    dump_cloud: bool = Field(
        default=False,
        description="Dumps Cloud objects to a JSON file.",
    )
    overwrite_existing: bool = Field(
        default=False,
        description="Overwrites existing objects in Cloud instead of skipping "
        + "them during migration.",
    )
    cleanup_target_env: bool = Field(
        default=False,
        description="Prior to the migration, all pre-existing objects will be "
        + "deleted from the target environment.",
    )
    suppress_migration_warnings: bool = Field(
        default=False,
        description="Suppress migration warnings from being added to object titles "
        + "and descriptions. Warnings will still be printed to console.",
    )


class ObjectFilterConfig(BaseModel):
    """Configuration for filtering objects during migration."""

    with_tags: list[str] | None = Field(
        default=None,
        description="Only migrate objects with at least one of the specified tags.",
    )
    without_tags: list[str] | None = Field(
        default=None,
        description="Only migrate objects that don't have any of the specified tags.",
    )
    with_creator_profiles: list[str] | None = Field(
        default=None,
        description="Only migrate objects created by one of the specified Platform user "
        + "profile IDs (/gdc/account/profile/ prefix).",
    )
    without_creator_profiles: list[str] | None = Field(
        default=None,
        description="Only migrate objects NOT created by any of the specified Platform "
        + "user profile IDs (without /gdc/account/profile/ prefix).",
    )
    with_locked_flag: bool = Field(
        default=False,
        description="Only migrate objects that have locked=1 flag in their metadata.",
    )
    without_locked_flag: bool = Field(
        default=False,
        description="Only migrate objects that have locked=0 or no locked flag in "
        + "their metadata.",
    )
    without_mapped_objects: Literal["default_only", "all"] | None = Field(
        default=None,
        description="Filter out objects present in mapping files. When used without "
        + "value, filters objects in ANY mapping file. With 'default_only', only "
        + "checks the default mapping file.",
    )
    only_object_ids: list[int] | None = Field(
        default=None,
        description="Only migrate specific objects by their IDs (integer Legacy object IDs).",
    )
    only_object_ids_with_dependencies: list[int] | None = Field(
        default=None,
        description="Only migrate specific objects by their IDs (integer Legacy object IDs) and include "
        + "their dependencies.",
    )
    only_identifiers: list[str] | None = Field(
        default=None,
        description="Only migrate specific objects by their alphanumeric identifiers. ",
    )
    only_identifiers_with_dependencies: list[str] | None = Field(
        default=None,
        description="Only migrate specific objects by their alphanumeric identifiers "
        + "and include their dependencies.",
    )

    @model_validator(mode="after")
    def validate_filters(self) -> "ObjectFilterConfig":
        """Validate mutually exclusive filter groups."""

        # Validate mutually exclusive locked flag group
        if sum([self.with_locked_flag, self.without_locked_flag]) > 1:
            raise ValueError(
                "Cannot use both with_locked_flag and without_locked_flag together"
            )

        # Validate mutually exclusive filter group
        if (
            sum(
                [
                    bool(self.only_object_ids),
                    bool(self.only_object_ids_with_dependencies),
                    bool(self.only_identifiers),
                    bool(self.only_identifiers_with_dependencies),
                ]
            )
            > 1
        ):
            raise ValueError(
                "Only one of only_object_ids, only_object_ids_with_dependencies, "
                + "only_identifiers, only_identifiers_with_dependencies can be used together"
            )
        return self
