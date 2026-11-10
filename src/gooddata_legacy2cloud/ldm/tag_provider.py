# (C) 2026 GoodData Corporation
from gooddata_legacy2cloud.backends.legacy.client import LegacyClient

DEPRECATED_TAGS = ["_lcm_managed_object"]


class TagProvider:
    """
    The TagProvider class is responsible for providing Legacy tags and
    descriptions for a given identifier.
    """

    def __init__(self, legacy_client: LegacyClient):
        self.legacy_client = legacy_client
        entries = (
            self.legacy_client.get_attributes()["query"]["entries"]
            + self.legacy_client.get_facts()["query"]["entries"]
        )
        # contains the mapping of identifiers to respective tags
        self.tags_mapping = self.get_tags_mapping(entries)
        # contains the mapping of identifiers to Legacy descriptions (summary)
        self.descriptions_mapping: dict[str, str] = {
            entry["identifier"]: entry.get("summary", "") for entry in entries
        }

    def get_tags_mapping(self, entries):
        """
        Returns a dictionary containing the mapping of identifiers to tags.
        """
        tags_mapping = {}

        for entry in entries:
            tags = entry["tags"].split()
            filtered_tags = [tag for tag in tags if tag not in DEPRECATED_TAGS]
            tags_mapping[entry["identifier"]] = filtered_tags

        return tags_mapping

    def get_tags(self, identifier):
        """
        Returns the tags for the given identifier.
        """
        tags = self.tags_mapping.get(identifier, None)

        if tags is not None and len(tags) > 0:
            return tags

        return []

    def get_description(self, identifier: str) -> str:
        """
        Returns the Legacy description for the given identifier.
        """
        return self.descriptions_mapping.get(identifier, "")
