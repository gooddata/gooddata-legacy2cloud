# (C) 2026 GoodData Corporation
from gooddata_legacy2cloud.backends.legacy.client import LegacyClient

DEPRECATED_TAGS = ["_lcm_managed_object"]


class TagProvider:
    """
    The TagProvider class is responsible for providing Legacy tags for a given identifier.
    """

    def __init__(self, legacy_client: LegacyClient):
        self.legacy_client = legacy_client
        # contains the mapping of identifiers to respective tags
        self.tags_mapping = self.get_tags_mapping()

    def get_tags_mapping(self):
        """
        Returns a dictionary containing the mapping of identifiers to tags.
        """
        tags_mapping = {}
        attributes = self.legacy_client.get_attributes()
        facts = self.legacy_client.get_facts()

        for attribute in attributes["query"]["entries"]:
            tags = attribute["tags"].split()
            filtered_tags = [tag for tag in tags if tag not in DEPRECATED_TAGS]
            tags_mapping[attribute["identifier"]] = filtered_tags

        for fact in facts["query"]["entries"]:
            tags = fact["tags"].split()
            filtered_tags = [tag for tag in tags if tag not in DEPRECATED_TAGS]
            tags_mapping[fact["identifier"]] = filtered_tags

        return tags_mapping

    def get_tags(self, identifier):
        """
        Returns the tags for the given identifier.
        """
        tags = self.tags_mapping.get(identifier, None)

        if tags is not None and len(tags) > 0:
            return tags

        return []
