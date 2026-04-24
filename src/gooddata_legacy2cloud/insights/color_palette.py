# (C) 2026 GoodData Corporation
import uuid

import attrs

from gooddata_legacy2cloud.backends.cloud.client import CloudClient
from gooddata_legacy2cloud.backends.legacy.client import LegacyClient


@attrs.define
class ColorPaletteContext:
    """The ColorPaletteContext class contains the configuration for color palette migration"""

    legacy_client: LegacyClient
    cloud_client: CloudClient


class ColorPalette:
    def __init__(self, ctx: ColorPaletteContext):
        if not isinstance(ctx, ColorPaletteContext):
            raise ValueError("ctx must be a ColorPaletteContext")
        self.ctx: ColorPaletteContext = ctx
        self.legacy_color_palette = self.ctx.legacy_client.get_color_palette()
        if not self.legacy_color_palette:
            return
        self.cloud_color_palette_id = f"{self.ctx.cloud_client.ws}_color_palette"
        self.cloud_color_palette = self.transform_color_palette()

    def transform_color_palette(self):
        new_palette = {"colorPalette": []}
        for color in self.legacy_color_palette["styleSettings"]["chartPalette"]:
            new_color = {
                "fill": color["fill"],
                "guid": color["guid"].replace("guid", ""),
            }
            new_palette["colorPalette"].append(new_color)
        return {
            "data": {
                "id": self.cloud_color_palette_id,
                "type": "colorPalette",
                "attributes": {
                    "name": f"{self.ctx.cloud_client.ws} color palette",
                    "content": new_palette,
                },
            }
        }

    def create_color_palette(self):
        self.ctx.cloud_client.create_color_palette(self.cloud_color_palette)

    def _build_organization_setting_payload(self, setting_id):
        """
        Builds the organization setting payload for ACTIVE_COLOR_PALETTE.

        Args:
            setting_id: The ID for the organization setting

        Returns:
            dict: The complete organization setting payload
        """
        return {
            "data": {
                "type": "organizationSetting",
                "id": setting_id,
                "attributes": {
                    "content": {
                        "id": self.cloud_color_palette_id,
                        "type": "colorPalette",
                    },
                    "type": "ACTIVE_COLOR_PALETTE",
                },
            }
        }

    def set_color_palette_to_organization(self):
        """
        Sets the color palette as active in organization settings.
        Creates a new setting if none exists, updates existing one otherwise.
        """
        # Use filtered API call to get only ACTIVE_COLOR_PALETTE settings
        filter_param = "type%3D%3DACTIVE_COLOR_PALETTE"
        active_palette_settings = (
            self.ctx.cloud_client.get_organization_settings_with_filter(filter_param)
        )

        if active_palette_settings["data"]:
            # Update existing ACTIVE_COLOR_PALETTE setting
            existing_setting_id = active_palette_settings["data"][0]["id"]
            organization_setting = self._build_organization_setting_payload(
                existing_setting_id
            )
            self.ctx.cloud_client.put_organization_settings(organization_setting)
        else:
            # Create new ACTIVE_COLOR_PALETTE setting
            new_setting_id = str(uuid.uuid4())
            organization_setting = self._build_organization_setting_payload(
                new_setting_id
            )
            self.ctx.cloud_client.create_organization_setting(organization_setting)
